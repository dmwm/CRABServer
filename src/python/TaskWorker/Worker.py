import multiprocessing
from Queue import Empty
import logging
import time
import traceback
from base64 import b64encode
import urllib

from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import WorkerHandlerException
from RESTInteractions import HTTPRequests


## Creating configuration globals to avoid passing these around at every request
global WORKER_CONFIG


def processWorker(inputs, results, instance, resturl):
    """Wait for an reference to appear in the input queue, call the referenced object
       and write the output in the output queue.

       :arg Queue inputs: the queue where the inputs are shared by the master
       :arg Queue results: the queue where this method writes the output
       :return: default returning zero, but not really needed."""
    logger = logging.getLogger()
    procName = multiprocessing.current_process().name
    while True:
        try:
            workid, work, task, inputargs = inputs.get()
        except (EOFError, IOError):
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            logger.error(crashMessage)
            break
        if work == 'STOP':
            break

        outputs = None
        t0 = time.time()
        logger.debug("%s: Starting %s on %s" %(procName, str(work), task['tm_taskname']))
        try:
            msg = None
            outputs = work(instance, resturl, WORKER_CONFIG, task, inputargs)
        except WorkerHandlerException, we:
            outputs = Result(task=task, err=str(we))
            msg = str(we)
        except Exception, exc:
            outputs = Result(task=task, err=str(exc))
            msg = "%s: I just had a failure for %s" % (procName, str(exc))
            msg += "\n\tworkid=" + str(workid)
            msg += "\n\ttask=" + str(task['tm_taskname'])
            msg += "\n" + str(traceback.format_exc())
        finally:
            if msg:
                try:
                    server = HTTPRequests(instance, WORKER_CONFIG.TaskWorker.cmscert, WORKER_CONFIG.TaskWorker.cmskey)
                    configreq = {  'workflow': task['tm_taskname'],
                                   'status': "FAILED",
                                   'subresource': 'failure',
                                   'failure': b64encode(msg)}

                    server.post(resturl, data = urllib.urlencode(configreq))
                    logger.info("Error message successfully uploaded to the REST")
                except Exception, exc:
                    logger.warning("Cannot uploaded failure message to the REST for workflow %s.\nReason: %s" % (task['tm_taskname'], exc))
        t1 = time.time()
        logger.debug("%s: ...work on %s completed in %d seconds: %s" % (procName, task['tm_taskname'], t1-t0, outputs))

        results.put({
                     'workid': workid,
                     'out' : outputs
                    })
    logger.debug("Slave exiting.")
    return 0



class Worker(object):
    """Worker class providing all the functionalities to manage all the slaves
       and distribute the work"""

    def __init__(self, config, instance, resturl):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg str instance: the hostname where the rest interface is running
        :arg str resturl: the rest base url to contact."""
        self.logger = logging.getLogger(type(self).__name__)
        global WORKER_CONFIG
        WORKER_CONFIG = config
        #global WORKER_DBCONFIG
        self.pool = []
        self.nworkers = WORKER_CONFIG.TaskWorker.nslaves if getattr(WORKER_CONFIG.TaskWorker, 'nslaves', None) is not None else multiprocessing.cpu_count()
        ## limit the size of the queue to be al maximum twice then the number of worker
        self.leninqueue = self.nworkers*2
        self.inputs  = multiprocessing.Queue(self.leninqueue)
        self.results = multiprocessing.Queue()
        self.working = {}
        self.instance = instance
        self.resturl = resturl

    def __del__(self):
        """When deleted shutting down all slaves"""
        self.end()

    def begin(self):
        """Starting up all the slaves"""
        if len(self.pool) == 0:
            # Starting things up
            for x in range(self.nworkers):
                self.logger.debug("Starting process %i" %x)
                p = multiprocessing.Process(target = processWorker, args = (self.inputs, self.results, self.instance, self.resturl))
                p.start()
                self.pool.append(p)
        self.logger.info("Started %d slaves"% len(self.pool))

    def end(self):
        """Stopping all the slaves"""
        self.logger.debug("Ready to close all %i started processes "%len(self.pool))
        for x in self.pool:
            try:
                self.logger.debug("Shutting down %s "%str(x))
                self.inputs.put(('-1', 'STOP', 'control', []))
            except Exception, ex:
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                self.logger.error(msg)

        self.pool = []
        self.logger.info('Slaves stop messages sent!')
        return

    def injectWorks(self, items):
        """Takes care of iterating on the input works to do and
           injecting them into the queue shared with the slaves

           :arg list of tuple items: list of tuple, where each element
                                     contains the type of work to be
                                     done, the task object and the args."""
        self.logger.debug("Ready to inject %d items"%len(items))
        workid = 0 if len(self.working.keys()) == 0 else max(self.working.keys()) + 1
        for work in items:
            worktype, task, arguments = work
            self.inputs.put((workid, worktype, task, arguments))
            self.working[workid] = {'workflow': task['tm_taskname'], 'injected': time.time()}
            self.logger.info('Injecting work %d' %workid)
            workid += 1
        self.logger.debug("Injection completed.")

    def checkFinished(self):
        """Verifies if there are any finished jobs in the output queue

           :return Result: the output of the work completed."""
        if len(self.working.keys()) == 0:
            return []
        allout = []
        self.logger.info("%d work on going, checking if some has finished" % len(self.working.keys()))
        for i in xrange(len(self.working.keys())):
            out = None
            try:
                out = self.results.get_nowait()
            except Empty, e:
                pass
            if out is not None:
               self.logger.debug('Retrieved work %s'% str(out))
               if isinstance(out['out'], list):
                   allout.extend(out['out'])
               else:
                   allout.append(out['out'])
               del self.working[out['workid']]
        return allout

    def freeSlaves(self):
        """Count how many unemployed slaves are there

        :return int: number of free slaves."""
        if self.queuedTasks() >= len(self.pool):
            return 0
        return len(self.pool) - self.queuedTasks()

    def queuedTasks(self):
        """Count how many busy slaves are out there

        :return int: number of working slaves."""
        return len(self.working)

    def queueableTasks(self):
        """Depending on the queue size limit
           return the number of free solts in
           the working queue.

           :return int: number of acquirable tasks."""
        if self.queuedTasks() >= self.leninqueue:
            return 0
        return self.leninqueue - self.queuedTasks()

    def pendingTasks(self):
        """Return the number of tasks pending
           to be processed and already in the
           queue.

           :return int: number of tasks waiting
                        in the queue."""
        if len(self.working) <= len(self.pool):
            return 0
        return len(self.working) - len(self.pool)

if __name__ == '__main__':

    from TaskWorker.Actions.Handler import handleNewTask

    a = Worker()
    a.begin()
    a.injectWorks([(Task(), handleNewTask, 'pippo'),({'tm_taskname': 'pippo'}, handleNewTask, 'pippo')])
    while(True):
        out = a.checkFinished()
        time.sleep(1)
        if ok is not None:
            print out
            break
    a.end()
