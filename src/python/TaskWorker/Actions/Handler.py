import logging
import time
import traceback

from RESTInteractions import HTTPRequests

from TaskWorker.Actions.DBSDataDiscovery import DBSDataDiscovery
from TaskWorker.Actions.UserDataDiscovery import UserDataDiscovery
from TaskWorker.Actions.MakeFakeFileSet import MakeFakeFileSet
from TaskWorker.Actions.Splitter import Splitter
from TaskWorker.Actions.PanDABrokerage import PanDABrokerage
from TaskWorker.Actions.PanDAInjection import PanDAInjection
from TaskWorker.Actions.PanDAgetSpecs import PanDAgetSpecs
from TaskWorker.Actions.PanDAKill import PanDAKill
from TaskWorker.Actions.PanDASpecs2Jobs import PanDASpecs2Jobs
from TaskWorker.Actions.MyProxyLogon import MyProxyLogon
from TaskWorker.WorkerExceptions import WorkerHandlerException, StopHandler, TaskWorkerException
from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.DagmanCreator import DagmanCreator
from TaskWorker.Actions.DagmanSubmitter import DagmanSubmitter
from TaskWorker.Actions.DagmanResubmitter import DagmanResubmitter
from TaskWorker.Actions.DagmanKiller import DagmanKiller

DEFAULT_BACKEND = 'panda'

class TaskHandler(object):
    """Handling the set of operations to be performed."""

    def __init__(self, task):
        """Initializer

        :arg TaskWorker.DataObjects.Task task: the task to work on."""
        self.logger = logging.getLogger()
        self._work = []
        self._task = task

    def addWork(self, work):
        """Appending a new action to be performed on the task

        :arg callable work: a new callable to be called :)"""
        if work not in self._work:
            self._work.append( work )

    def getWorks(self):
        """Retrieving the queued actions

        :return: generator of actions to be performed."""
        for w in self._work:
            yield w

    def actionWork(self, *args, **kwargs):
        """Performing the set of actions"""
        nextinput = args
        for work in self.getWorks():
            self.logger.debug("Starting %s on %s" % (str(work), self._task['tm_taskname']))
            t0 = time.time()
            try:
                output = work.execute(nextinput, task=self._task)
            except StopHandler, sh:
                msg = "Controlled stop of handler for %s on %s " % (self._task, str(sh))
                self.logger.error(msg)
                nextinput = Result(task=self._task, result='StopHandler exception received, controlled stop')
                break #exit normally. Worker will not notice there was an error
            except TaskWorkerException, twe:
                self.logger.debug(str(traceback.format_exc())) #print the stacktrace only in debug mode
                raise WorkerHandlerException(str(twe)) #TaskWorker error, do not add traceback to the error propagated to the REST
            except Exception, exc:
                msg = "Problem handling %s because of %s failure, traceback follows\n" % (self._task['tm_taskname'], str(exc))
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                raise WorkerHandlerException(msg) #Errors not foreseen. Print everything!
            t1 = time.time()
            self.logger.info("Finished %s on %s in %d seconds" % (str(work), self._task['tm_taskname'], t1-t0))
            try:
                nextinput = output.result
            except AttributeError:
                nextinput = output
        tot1 = time.time()
        return nextinput

def handleNewTask(resthost, resturi, config, task, *args, **kwargs):
    """Performs the injection of a new task

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the handler."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry = 2)
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, server=server, resturi=resturi, myproxylen=60*60*24) )
    if task['tm_job_type'] == 'Analysis': 
        if task.get('tm_arguments', {}).get('userfiles'):
            handler.addWork( UserDataDiscovery(config=config, server=server, resturi=resturi) )
        else:
            handler.addWork( DBSDataDiscovery(config=config, server=server, resturi=resturi) )
    elif task['tm_job_type'] == 'PrivateMC': 
        handler.addWork( MakeFakeFileSet(config=config, server=server, resturi=resturi) )
    handler.addWork( Splitter(config=config, server=server, resturi=resturi) )

    def glidein(config):
        """Performs the injection of a new task into Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanCreator(config=config, server=server, resturi=resturi) )
        handler.addWork( DagmanSubmitter(config=config, server=server, resturi=resturi) )

    def panda(config):
        """Performs the injection into PanDA of a new task
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDABrokerage(pandaconfig=config, server=server, resturi=resturi) )
        handler.addWork( PanDAInjection(pandaconfig=config, server=server, resturi=resturi) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args)

def handleResubmit(resthost, resturi, config, task, *args, **kwargs):
    """Performs the re-injection of failed jobs

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry = 2)
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, server=server, resturi=resturi, myproxylen=60*60*24) )
    def glidein(config):
        """Performs the re-injection into Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanResubmitter(config=config, server=server, resturi=resturi) )

    def panda(config):
        """Performs the re-injection into PanDA
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDAgetSpecs(pandaconfig=config, server=server, resturi=resturi) )
        handler.addWork( PanDASpecs2Jobs(pandaconfig=config, server=server, resturi=resturi) )
        handler.addWork( PanDABrokerage(pandaconfig=config, server=server, resturi=resturi) )
        handler.addWork( PanDAInjection(pandaconfig=config, server=server, resturi=resturi) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args)

def handleKill(resthost, resturi, config, task, *args, **kwargs):
    """Asks to kill jobs

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry = 2)
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, server=server, resturi=resturi, myproxylen=60*5) )
    def glidein(config):
        """Performs kill of jobs sent through Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanKiller(config=config, server=server, resturi=resturi) )

    def panda(config):
        """Performs the re-injection into PanDA
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDAKill(pandaconfig=config, server=server, resturi=resturi) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args, kwargs)

if __name__ == '__main__':
    print "New task"
    handleNewTask(None, None, None, task={})
    print "\nResubmit task"
    handleResubmit(None, None, None, task={})
    print "\nKill task"
    handleKill(None, None, None, task={})
