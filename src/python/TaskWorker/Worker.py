from __future__ import print_function
import os
import time
import logging
import traceback
import multiprocessing
from queue import Empty
from logging import FileHandler
from http.client import HTTPException
from logging.handlers import TimedRotatingFileHandler

import sys
if sys.version_info >= (3, 0):
    from urllib.parse import urlencode  # pylint: disable=no-name-in-module
if sys.version_info < (3, 0):
    from urllib import urlencode

from RESTInteractions import CRABRest
from TaskWorker.DataObjects.Result import Result
from ServerUtilities import truncateError, executeCommand, FEEDBACKMAIL
from TaskWorker.WorkerExceptions import WorkerHandlerException, TapeDatasetException,\
    ChildUnexpectedExitException, ChildTimeoutException, SubmissionRefusedException
from TaskWorker.ChildWorker import startChildWorker


## Creating configuration globals to avoid passing these around at every request
## and tell pylink to bare with this :-)
# pylint: disable=W0604, W0601
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


def failTask(taskName, crabserver, msg, log, failstatus='FAILED'):
    try:
        log.info("Uploading failure message to the REST:\n%s", msg)
        truncMsg = truncateError(msg)
        configreq = {'workflow': taskName,
                     'status': failstatus,
                     'subresource': 'failure',
                     # Limit the message to 1000. That's the limit in the REST
                     'failure': truncMsg}
        crabserver.post(api='workflowdb', data = urlencode(configreq))
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


def processWorkerLoop(inputs, results, resthost, dbInstance, procnum, logger, logsDir):
    procName = "Process-%s" % procnum
    while True:
        try:
            ## Get (and remove) an item from the input queue. If the queue is empty, wait
            ## until an item is available. Item content is:
            ##  workid : an integer assigne by the queue module
            ##  work   : a function handler to the needed action e.g. function handleNewTask
            ##  task   : a task dictionary
            ##  failstatus : the status to assign to the task if work fails (e.g. 'SUBMITFAILED')
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
        #log entry below is used for logs parsing, therefore, changing it might require to update logstash configuration
        logger.debug("%s: Starting %s on %s", procName, str(work), task['tm_taskname'])
        try:
            msg = None
            if hasattr(WORKER_CONFIG, 'FeatureFlags') and \
               hasattr(WORKER_CONFIG.FeatureFlags, 'childWorker') and \
               WORKER_CONFIG.FeatureFlags.childWorker:
                logger.debug(f'Run {work.__name__} in childWorker.')
                args = (resthost, dbInstance, WORKER_CONFIG, task, procnum, inputargs)
                outputs = startChildWorker(WORKER_CONFIG, work, args, logger)
            else:
                outputs = work(resthost, dbInstance, WORKER_CONFIG, task, procnum, inputargs)
        except TapeDatasetException as tde:
            outputs = Result(task=task, err=str(tde))
        except SubmissionRefusedException as sre:
            outputs = Result(task=task, err=str(sre))
        except WorkerHandlerException as we:
            outputs = Result(task=task, err=str(we))
            msg = str(we)
        except (ChildUnexpectedExitException, ChildTimeoutException) as e:
            # custom message
            outputs = Result(task=task, err=str(e))
            msg =  f"Server-side failed with an error: {str(e)}"
            msg +=  "\n This could be a temporary glitch. Please try again later."
            msg += f"\n If the error persists, please send an e-mail to {FEEDBACKMAIL}."
        except Exception as exc: #pylint: disable=broad-except
            outputs = Result(task=task, err=str(exc))
            msg = "%s: I just had a failure for %s" % (procName, str(exc))
            msg += "\n\tworkid=" + str(workid)
            msg += "\n\ttask=" + str(task['tm_taskname'])
            msg += "\n" + str(traceback.format_exc())
        finally:
            if msg:
                crabserver = CRABRest(resthost, WORKER_CONFIG.TaskWorker.cmscert, WORKER_CONFIG.TaskWorker.cmskey,
                                      retry=20, logger=logger, userAgent='CRABTaskWorker')
                crabserver.setDbInstance(dbInstance)
                failTask(task['tm_taskname'], crabserver, msg, logger, failstatus)
        t1 = time.time()
        workType = task.get('tm_task_command', 'RECURRING')
        #log entry below is used for logs parsing, therefore, changing it might require to update logstash configuration
        logger.debug("%s: %s work on %s completed in %d seconds: %s", procName, workType, task['tm_taskname'], t1-t0, outputs)

        try:
            out, _, _ = executeCommand("ps u -p %s | awk '{sum=sum+$6}; END {print sum/1024}'" % os.getpid())
            msg = "RSS after finishing %s: %s MB" % (task['tm_taskname'], out.strip())
            logger.debug(msg)
        except Exception:
            logger.exception("Problem getting worker RSS:")

        removeTaskLogHandler(logger, taskhandler)

        results.put({
                     'workid': workid,
                     'out' : outputs
                    })


def processWorker(inputs, results, resthost, dbInstance, logsDir, procnum):
    """Wait for an reference to appear in the input queue, call the referenced object
       and write the output in the output queue.

       :arg Queue inputs: the queue where the inputs are shared by the master
       :arg Queue results: the queue where this method writes the output
       :return: default returning zero, but not really needed."""
    logger = setProcessLogger(str(procnum), logsDir)
    logger.info("Process %s is starting. PID %s", procnum, os.getpid())
    try:
        processWorkerLoop(inputs, results, resthost, dbInstance, procnum, logger, logsDir)
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

    def __init__(self, config, resthost, dbInstance):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg str instance: the hostname where the rest interface is running
        :arg str dbInstance: the DB instance to use"""
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
        self.dbInstance = dbInstance

    def begin(self):
        """Starting up all the slaves"""
        if len(self.pool) == 0:
            # Starting things up
            for x in range(1, self.nworkers + 1):
                self.logger.debug("Starting process %i", x)
                p = multiprocessing.Process(target = processWorker, args = (self.inputs, self.results, self.resthost, self.dbInstance, WORKER_CONFIG.TaskWorker.logsDir, x))
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
        for _ in range(len(self.working.keys())):
            out = None
            try:
                out = self.results.get_nowait()
            except Empty:
                pass
            if out is not None:
                # getting output from queue returns the workid in the queue and
                # the Result object from the action handler
                workid = out['workid']
                result = out['out']
                if result:
                    taskname = result.task['tm_taskname']
                    self.logger.debug('Completed work %d on %s', workid, taskname)
                else:
                    # recurring actions do not return a Result object
                    self.logger.debug('Completed work %s', str(out))

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
