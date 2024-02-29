from __future__ import print_function
import os
import time
import logging
import tempfile
import traceback
import copy

from RESTInteractions import CRABRest
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
from TaskWorker.Actions.RucioDataDiscovery import RucioDataDiscovery
from TaskWorker.Actions.DagmanResubmitter import DagmanResubmitter
from TaskWorker.WorkerExceptions import WorkerHandlerException, TapeDatasetException, TaskWorkerException

from ServerUtilities import uploadToS3


class TaskHandler(object):
    """Handling the set of operations to be performed."""


    def __init__(self, task, procnum, crabserver, config, workFunction, createTempDir=False):
        """Initializer

        :arg TaskWorker.DataObjects.Task task: the task to work on."""
        self.logger = logging.getLogger(str(procnum))
        self.procnum = procnum
        self.crabserver = crabserver
        self.config = config
        self.workFunction = workFunction
        self._work = []
        self._task = task
        self.taskname = task['tm_taskname']
        self.tempDir = None
        if createTempDir:
            self.tempDir = self.createTempDir()
            self._task['scratch'] = self.tempDir

        # tm_start_time will be used to set task end time. Make sure
        # it is up to date, in case task site a while in DB e.g. for tape recall.
        # self._task is passed in input to all actions
        self._task['tm_start_time'] = int(time.time())


    def createTempDir(self):
        if hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'scratchDir'):
            tempDir = tempfile.mkdtemp(prefix='_' + self.taskname, dir=self.config.TaskWorker.scratchDir)
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
            raise TapeDatasetException(str(tde)) from tde
        except TaskWorkerException as twe:
            self.logger.debug(str(traceback.format_exc()))  # print the stacktrace only in debug mode
            raise WorkerHandlerException(str(twe), retry=twe.retry) from twe  # TaskWorker error, do not add traceback
        except Exception as exc:
            msg = "Problem handling %s because of %s failure, traceback follows\n" % (self.taskname, str(exc))
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            raise WorkerHandlerException(msg) from exc  # Errors not foreseen. Print everything!
        finally:
            #TODO: we need to do that also in Worker.py otherwise some messages might only be in the TW file but not in the crabcache.
            logpath = self.config.TaskWorker.logsDir+'/tasks/%s/%s.log' % (self._task['tm_username'], self.taskname)
            if os.path.isfile(logpath) and 'user_proxy' in self._task: #the user proxy might not be there if myproxy retrieval failed
                try:
                    uploadToS3(crabserver=self.crabserver, objecttype='twlog', filepath=logpath,
                               taskname=self.taskname, logger=self.logger)
                except Exception as e:
                    msg = 'Failed to upload logfile to S3 for task %s. ' % self.taskname
                    msg += 'Details:\n%s' % str(e)
                    self.logger.error(msg)
        return output


    def actionWork(self, *args, **kwargs): #pylint: disable=unused-argument
        """Performing the set of actions"""
        nextinput = args

        #Loop that iterates over the actions to be performed
        for action in self.getWorks():
            self.logger.debug("Starting %s on %s", str(action), self.taskname)
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
            self.logger.info("Finished %s on %s in %d seconds", str(action), self.taskname, t1 - t0)

            # if there is a next action to do, the result field of the Result object returned by this action (!)
            # will contain the needed input for the next action. I also hate this, but could not find a better way
            try:
                nextinput = output.result
            except AttributeError:
                nextinput = output

        return output


def handleNewTask(resthost, dbInstance, config, task, procnum, *args, **kwargs):
    """Performs the injection of a new task

    :arg str resthost: the hostname where the rest interface is running
    :arg str dbInstance: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the handler."""
    crabserver = CRABRest(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry=20,
                          logger=logging.getLogger(str(procnum)), userAgent='CRABTaskWorker', version=__version__)
    crabserver.setDbInstance(dbInstance)
    handler = TaskHandler(task, procnum, crabserver, config, 'handleNewTask', createTempDir=True)
    rucioClient = getNativeRucioClient(config=config, logger=handler.logger)
    # Temporary use `crab_input` account to checking other account quota.
    # See discussion in https://mattermost.web.cern.ch/cms-o-and-c/pl/ej7zwkr747rifezzcyyweisx9r
    tmpConfig = copy.deepcopy(config)
    tmpConfig.Services.Rucio_account = 'crab_input'
    privilegedRucioClient = getNativeRucioClient(tmpConfig, handler.logger)

    # start to work
    handler.addWork(MyProxyLogon(config=config, crabserver=crabserver, procnum=procnum, myproxylen=60 * 60 * 24))
    handler.addWork(StageoutCheck(config=config, crabserver=crabserver, procnum=procnum, rucioClient=privilegedRucioClient))
    if task['tm_job_type'] == 'Analysis':
        if task.get('tm_user_files'):
            handler.addWork(UserDataDiscovery(config=config, crabserver=crabserver, procnum=procnum))
        elif ':' in task.get('tm_input_dataset'):  # Rucio DID is scope:name
            handler.addWork(RucioDataDiscovery(config=config, crabserver=crabserver, procnum=procnum, rucioClient=rucioClient))
        else:
            handler.addWork(DBSDataDiscovery(config=config, crabserver=crabserver, procnum=procnum, rucioClient=rucioClient))
    elif task['tm_job_type'] == 'PrivateMC':
        handler.addWork(MakeFakeFileSet(config=config, crabserver=crabserver, procnum=procnum))
    handler.addWork(Splitter(config=config, crabserver=crabserver, procnum=procnum))
    handler.addWork(DagmanCreator(config=config, crabserver=crabserver, procnum=procnum, rucioClient=rucioClient))
    if task['tm_dry_run'] == 'T':
        handler.addWork(DryRunUploader(config=config, crabserver=crabserver, procnum=procnum))
    else:
        handler.addWork(DagmanSubmitter(config=config, crabserver=crabserver, procnum=procnum))

    return handler.actionWork(args, kwargs)


def handleResubmit(resthost, dbInstance, config, task, procnum, *args, **kwargs):
    """Performs the re-injection of failed jobs

    :arg str resthost: the hostname where the rest interface is running
    :arg str dbInstance: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    crabserver = CRABRest(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry=20,
                          logger=logging.getLogger(str(procnum)), userAgent='CRABTaskWorker', version=__version__)
    crabserver.setDbInstance(dbInstance)
    handler = TaskHandler(task, procnum, crabserver, config, 'handleResubmit')
    handler.addWork(MyProxyLogon(config=config, crabserver=crabserver, procnum=procnum, myproxylen=60 * 60 * 24))
    handler.addWork(DagmanResubmitter(config=config, crabserver=crabserver, procnum=procnum))

    return handler.actionWork(args, kwargs)


def handleKill(resthost, dbInstance, config, task, procnum, *args, **kwargs):
    """Asks to kill jobs

    :arg str resthost: the hostname where the rest interface is running
    :arg str dbInstance: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    crabserver = CRABRest(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry=20,
                          logger=logging.getLogger(str(procnum)), userAgent='CRABTaskWorker', version=__version__)
    crabserver.setDbInstance(dbInstance)
    handler = TaskHandler(task, procnum, crabserver, config, 'handleKill')
    handler.addWork(MyProxyLogon(config=config, crabserver=crabserver, procnum=procnum, myproxylen=60 * 5))
    handler.addWork(DagmanKiller(config=config, crabserver=crabserver, procnum=procnum))

    return handler.actionWork(args, kwargs)
