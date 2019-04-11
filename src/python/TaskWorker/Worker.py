from __future__ import print_function
import os
import time
import urllib
import logging
import traceback
import multiprocessing
from Queue import Empty
from base64 import b64encode
from logging import FileHandler
from httplib import HTTPException
from logging.handlers import TimedRotatingFileHandler

from RESTInteractions import HTTPRequests
from TaskWorker.DataObjects.Result import Result
from ServerUtilities import truncateError, executeCommand
from TaskWorker.WorkerExceptions import WorkerHandlerException, TapeDatasetException

## Creating configuration globals to avoid passing these around at every request
global WORKER_CONFIG


def addTaskLogHandler(logger, username, taskname, logsDir):
    #set the logger to save the tasklog
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    taskdirname = logsDir+"/tasks/%s/" % username
    try:
        os.mkdir(taskdirname)
    except OSError as ose:
        if ose.errno != 17: #ignore the "Directory already exists error" but print other errors traces
            logger.exception("Cannot set task handler logfile for task %s. Ignoring and continuing normally." % taskname)
    taskhandler = FileHandler(taskdirname + taskname + '.log')
    taskhandler.setFormatter(formatter)
    taskhandler.setLevel(logging.DEBUG)
    logger.addHandler(taskhandler)


    return taskhandler


def failTask(taskName, HTTPServer, resturi, msg, log, failstatus='FAILED'):
    try:
        log.info("Uploading failure message to the REST:\n%s", msg)
        truncMsg = truncateError(msg)
        configreq = {'workflow': taskName,
                     'status': failstatus,
                     'subresource': 'failure',
                     # Limit the message to 7500 chars, which means no more than 10000 once encoded. That's the limit in the REST
                     'failure': b64encode(truncMsg)}
        HTTPServer.post(resturi, data = urllib.urlencode(configreq))
        log.info("Failure message successfully uploaded to the REST")
    except HTTPException as hte:
        log.warning("Cannot upload failure message to the REST for task %s. HTTP exception headers follows:", taskName)
        log.error(hte.headers)
    except Exception as exc: #pylint: disable=broad-except
        log.warning("Cannot upload failure message to the REST for workflow %s.\nReason: %s", taskName, exc)
        log.exception('Traceback follows:')


def removeTaskLogHandler(logger, taskhandler):
    taskhandler.flush()
    taskhandler.close()
    logger.removeHandler(taskhandler)


def processWorkerLoop(inputs, results, resthost, resturi, procnum, logger, logsDir):
    procName = "Process-%s" % procnum
    while True:
        try:
            ## Get (and remove) an item from the input queue. If the queue is empty, wait
            ## until an item is available.
            workid, work, task, failstatus, inputargs = inputs.get()
            if work == 'STOP':
                break
            taskhandler = addTaskLogHandler(logger, task['tm_username'], task['tm_taskname'], logsDir)
        except (EOFError, IOError):
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            logger.error(crashMessage)
            break

        outputs = None
        t0 = time.time()
        logger.debug("%s: Starting %s on %s", procName, str(work), task['tm_taskname'])
        try:
            msg = None
            outputs = work(resthost, resturi, WORKER_CONFIG, task, procnum, inputargs)
        except TapeDatasetException as tde:
            outputs = Result(task=task, err=str(tde))
        except WorkerHandlerException as we:
            outputs = Result(task=task, err=str(we))
            msg = str(we)
        except Exception as exc: #pylint: disable=broad-except
            outputs = Result(task=task, err=str(exc))
            msg = "%s: I just had a failure for %s" % (procName, str(exc))
            msg += "\n\tworkid=" + str(workid)
            msg += "\n\ttask=" + str(task['tm_taskname'])
            msg += "\n" + str(traceback.format_exc())
        finally:
            if msg:
                server = HTTPRequests(resthost, WORKER_CONFIG.TaskWorker.cmscert, WORKER_CONFIG.TaskWorker.cmskey, retry = 20, logger = logger)
                failTask(task['tm_taskname'], server, resturi, msg, logger, failstatus)
        t1 = time.time()
        logger.debug("%s: ...work on %s completed in %d seconds: %s", procName, task['tm_taskname'], t1-t0, outputs)

        try:
            out, _, _ = executeCommand("ps u -p %s | awk '{sum=sum+$6}; END {print sum/1024}'" % os.getpid())
            msg = "RSS after finishing %s: %s MB" % (task['tm_taskname'], out.strip())
            logger.debug(msg)
        except:
            logger.exception("Problem getting worker RSS:")

        removeTaskLogHandler(logger, taskhandler)

        results.put({
                     'workid': workid,
                     'out' : outputs
                    })


def processWorker(inputs, results, resthost, resturi, logsDir, procnum):
    """Wait for an reference to appear in the input queue, call the referenced object
       and write the output in the output queue.

       :arg Queue inputs: the queue where the inputs are shared by the master
       :arg Queue results: the queue where this method writes the output
       :return: default returning zero, but not really needed."""
    logger = setProcessLogger(str(procnum), logsDir)
    logger.info("Process %s is starting. PID %s", procnum, os.getpid())
    try:
        processWorkerLoop(inputs, results, resthost, resturi, procnum, logger, logsDir)
    except: #pylint: disable=bare-except
        #if enything happen put the log inside process logfiles instead of nohup.log
        logger.exception("Unexpected error in process worker!")
    logger.debug("Slave %s exiting.", procnum)
    return 0


def setProcessLogger(name, logsDir):
    """ Set the logger for a single process. The file used for it is logsiDir/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    handler = TimedRotatingFileHandler(logsDir+'/processes/proc.c3id_%s.pid_%s.txt' % (name, os.getpid()), 'midnight', backupCount=30)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


class Worker(object):
    """Worker class providing all the functionalities to manage all the slaves
       and distribute the work"""

    def __init__(self, config, resthost, resturi):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg str instance: the hostname where the rest interface is running
        :arg str resturi: the rest base url to contact."""
        self.logger = logging.getLogger("master")
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
        self.resthost = resthost
        self.resturi = resturi

    def begin(self):
        """Starting up all the slaves"""
        if len(self.pool) == 0:
            # Starting things up
            for x in xrange(1, self.nworkers + 1):
                self.logger.debug("Starting process %i", x)
                p = multiprocessing.Process(target = processWorker, args = (self.inputs, self.results, self.resthost, self.resturi, WORKER_CONFIG.TaskWorker.logsDir, x))
                p.start()
                self.pool.append(p)
        self.logger.info("Started %d slaves", len(self.pool))

    def end(self):
        """Stopping all the slaves"""
        self.logger.debug("Ready to close all %i started processes ", len(self.pool))
        for p in self.pool:
            try:
                ## Put len(self.pool) messages in the subprocesses queue.
                ## Each subprocess will work on one stop message and exit
                self.logger.debug("Putting stop message in the queue for %s ", str(p))
                self.inputs.put(('-1', 'STOP', 'control', 'STOPFAILED', []))
            except Exception as ex: #pylint: disable=broad-except
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                self.logger.error(msg)
        self.logger.info('Slaves stop messages sent. Waiting for subprocesses.')
        for p in self.pool:
            try:
                p.join()
            except Exception as ex: #pylint: disable=broad-except
                msg =  "Hit some exception in join\n"
                msg += str(ex)
                self.logger.error(msg)
        self.logger.info('Subprocesses ended!')

        self.pool = []
        return

    def injectWorks(self, items):
        """Takes care of iterating on the input works to do and
           injecting them into the queue shared with the slaves

           :arg list of tuple items: list of tuple, where each element
                                     contains the type of work to be
                                     done, the task object and the args."""
        self.logger.debug("Ready to inject %d items", len(items))
        workid = 0 if len(self.working.keys()) == 0 else max(self.working.keys()) + 1
        for work in items:
            worktype, task, failstatus, arguments = work
            self.inputs.put((workid, worktype, task, failstatus, arguments))
            self.working[workid] = {'workflow': task['tm_taskname'], 'injected': time.time()}
            self.logger.info('Injecting work %d: %s', workid, task['tm_taskname'])
            workid += 1
        self.logger.debug("Injection completed.")

    def checkFinished(self):
        """Verifies if there are any finished jobs in the output queue

           :return Result: the output of the work completed."""
        if len(self.working.keys()) == 0:
            return []
        allout = []
        self.logger.info("%d work on going, checking if some has finished", len(self.working.keys()))
        for _ in xrange(len(self.working.keys())):
            out = None
            try:
                out = self.results.get_nowait()
            except Empty:
                pass
            if out is not None:
                self.logger.debug('Retrieved work %s', str(out))
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
           return the number of free slots in
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
        if self.queuedTasks() <= len(self.pool):
            return 0
        return self.queuedTasks() - len(self.pool)
