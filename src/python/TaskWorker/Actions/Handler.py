from __future__ import print_function
import os
import time
import logging
import tempfile
import traceback
from httplib import HTTPException

from WMCore.Services.UserFileCache.UserFileCache import UserFileCache

from RESTInteractions import HTTPRequests
from RucioUtils import getNativeRucioClient

from TaskWorker import __version__
from TaskWorker.Actions.Splitter import Splitter
from TaskWorker.Actions.DagmanKiller import DagmanKiller
from TaskWorker.Actions.MyProxyLogon import MyProxyLogon
from TaskWorker.Actions.DagmanCreator import DagmanCreator
from TaskWorker.Actions.StageoutCheck import StageoutCheck
from TaskWorker.Actions.DryRunUploader import DryRunUploader
from TaskWorker.Actions.MakeFakeFileSet import MakeFakeFileSet
from TaskWorker.Actions.DagmanSubmitter import DagmanSubmitter
from TaskWorker.Actions.DBSDataDiscovery import DBSDataDiscovery
from TaskWorker.Actions.UserDataDiscovery import UserDataDiscovery
from TaskWorker.Actions.DagmanResubmitter import DagmanResubmitter
from TaskWorker.WorkerExceptions import WorkerHandlerException, TapeDatasetException, TaskWorkerException


class TaskHandler(object):
    """Handling the set of operations to be performed."""


    def __init__(self, task, procnum, server, config, workFunction, createTempDir=False):
        """Initializer

        :arg TaskWorker.DataObjects.Task task: the task to work on."""
        self.logger = logging.getLogger(str(procnum))
        self.procnum = procnum
        self.server = server
        self.config = config
        self.workFunction = workFunction
        self._work = []
        self._task = task
        self.tempDir = None
        if createTempDir:
            self.tempDir = self.createTempDir()
            self._task['scratch'] = self.tempDir


    def createTempDir(self):
        if hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'scratchDir'):
            tempDir = tempfile.mkdtemp(prefix='_' + self._task['tm_taskname'], dir=self.config.TaskWorker.scratchDir)
        else:
            msg = "The 'scratchDir' parameter is not set in the config.TaskWorker section of the CRAB server backend configuration file."
            raise Exception(msg)
        return tempDir


    def addWork(self, work):
        """Appending a new action to be performed on the task

        :arg callable work: a new callable to be called :)"""
        if work not in self._work:
            self._work.append(work)


    def getWorks(self):
        """Retrieving the queued actions

        :return: generator of actions to be performed."""
        for w in self._work:
            yield w


    def executeAction(self, nextinput, work):
        """ Execute an action and deal with the error handling and upload of the tasklogfile to the crabcache
        """
        try:
            output = work.execute(nextinput, task=self._task, tempDir=self.tempDir)
        except TapeDatasetException as tde:
            raise TapeDatasetException(str(tde))
        except TaskWorkerException as twe:
            self.logger.debug(str(traceback.format_exc())) #print the stacktrace only in debug mode
            raise WorkerHandlerException(str(twe), retry = twe.retry) #TaskWorker error, do not add traceback to the error propagated to the REST
        except Exception as exc:
            msg = "Problem handling %s because of %s failure, traceback follows\n" % (self._task['tm_taskname'], str(exc))
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            raise WorkerHandlerException(msg) #Errors not foreseen. Print everything!
        finally:
            #TODO: we need to do that also in Worker.py otherwise some messages might only be in the TW file but not in the crabcache.
            logpath = self.config.TaskWorker.logsDir+'/tasks/%s/%s.log' % (self._task['tm_username'], self._task['tm_taskname'])
            if os.path.isfile(logpath) and 'user_proxy' in self._task: #the user proxy might not be there if myproxy retrieval failed
                cacheurldict = {'endpoint':self._task['tm_cache_url'], 'cert':self._task['user_proxy'], 'key':self._task['user_proxy']}
                try:
                    ufc = UserFileCache(cacheurldict)
                    logfilename = self._task['tm_taskname'] + '_TaskWorker.log'
                    ufc.uploadLog(logpath, logfilename)
                except HTTPException as hte:
                    msg = "Failed to upload the logfile to %s for task %s. More details in the http headers and body:\n%s\n%s" % (self._task['tm_cache_url'], self._task['tm_taskname'], hte.headers, hte.result)
                    self.logger.error(msg)
                except Exception: #pylint: disable=broad-except
                    msg = "Unknown error while uploading the logfile for task %s" % self._task['tm_taskname']
                    self.logger.exception(msg) #upload logfile of the task to the crabcache

        return output


    def actionWork(self, *args, **kwargs): #pylint: disable=unused-argument
        """Performing the set of actions"""
        nextinput = args

        #Loop that iterates over the actions to be performed
        for action in self.getWorks():
            self.logger.debug("Starting %s on %s", str(action), self._task['tm_taskname'])
            t0 = time.time()
            retryCount = 0
            #execute the current action dealing with retriesz
            MAX_RETRIES = 5
            while retryCount < MAX_RETRIES:
                try:
                    output = self.executeAction(nextinput, action)
                except WorkerHandlerException as whe:
                    retryCount += 1
                    if whe.retry == False or retryCount >= MAX_RETRIES:
                        raise
                    time.sleep(60 * retryCount)
                else:
                    break
            t1 = time.time()
            #log entry below is used for logs parsing, therefore, changing it might require to update logstash configuration
            self.logger.info("Finished %s on %s in %d seconds", str(action), self._task['tm_taskname'], t1 - t0)

            #XXX MM - Not really sure what this is and why it's here, but I hate this..
            #XXX SB - if there is a next action to dot, the result field of the Result object returned by this action (!)
            # will contain the needed input for the next action. I also hate this, but could not find a better way
            try:
                nextinput = output.result
            except AttributeError:
                nextinput = output

        return output


def handleNewTask(resthost, resturi, config, task, procnum, *args, **kwargs):
    """Performs the injection of a new task

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the handler."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry=20,
                          logger=logging.getLogger(str(procnum)), userAgent='CRABTaskWorker', version=__version__)
    handler = TaskHandler(task, procnum, server, config, 'handleNewTask', createTempDir=True)
    rucioClient = getNativeRucioClient(config=config, logger=handler.logger)
    handler.addWork(MyProxyLogon(config=config, server=server, resturi=resturi, procnum=procnum, myproxylen=60 * 60 * 24))
    handler.addWork(StageoutCheck(config=config, server=server, resturi=resturi, procnum=procnum, rucioClient=rucioClient))
    if task['tm_job_type'] == 'Analysis':
        if task.get('tm_user_files'):
            handler.addWork(UserDataDiscovery(config=config, server=server, resturi=resturi, procnum=procnum))
        else:
            handler.addWork(DBSDataDiscovery(config=config, server=server, resturi=resturi, procnum=procnum, rucioClient=rucioClient))
    elif task['tm_job_type'] == 'PrivateMC':
        handler.addWork(MakeFakeFileSet(config=config, server=server, resturi=resturi, procnum=procnum))
    handler.addWork(Splitter(config=config, server=server, resturi=resturi, procnum=procnum))
    handler.addWork(DagmanCreator(config=config, server=server, resturi=resturi, procnum=procnum, rucioClient=rucioClient))
    if task['tm_dry_run'] == 'T':
        handler.addWork(DryRunUploader(config=config, server=server, resturi=resturi, procnum=procnum))
    else:
        handler.addWork(DagmanSubmitter(config=config, server=server, resturi=resturi, procnum=procnum))

    return handler.actionWork(args, kwargs)


def handleResubmit(resthost, resturi, config, task, procnum, *args, **kwargs):
    """Performs the re-injection of failed jobs

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry=20,
                          logger=logging.getLogger(str(procnum)), userAgent='CRABTaskWorker', version=__version__)
    handler = TaskHandler(task, procnum, server, config, 'handleResubmit')
    handler.addWork(MyProxyLogon(config=config, server=server, resturi=resturi, procnum=procnum, myproxylen=60 * 60 * 24))
    handler.addWork(DagmanResubmitter(config=config, server=server, resturi=resturi, procnum=procnum))

    return handler.actionWork(args, kwargs)


def handleKill(resthost, resturi, config, task, procnum, *args, **kwargs):
    """Asks to kill jobs

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry=20,
                          logger=logging.getLogger(str(procnum)), userAgent='CRABTaskWorker', version=__version__)
    handler = TaskHandler(task, procnum, server, config, 'handleKill')
    handler.addWork(MyProxyLogon(config=config, server=server, resturi=resturi, procnum=procnum, myproxylen=60 * 5))
    handler.addWork(DagmanKiller(config=config, server=server, resturi=resturi, procnum=procnum))

    return handler.actionWork(args, kwargs)
