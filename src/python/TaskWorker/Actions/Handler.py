import logging
import time
import traceback

from RESTInteractions import HTTPRequests

from TaskWorker.Actions.DBSDataDiscovery import DBSDataDiscovery
from TaskWorker.Actions.MakeFakeFileSet import MakeFakeFileSet
from TaskWorker.Actions.Splitter import Splitter
from TaskWorker.Actions.PanDABrokerage import PanDABrokerage
from TaskWorker.Actions.PanDAInjection import PanDAInjection
from TaskWorker.Actions.PanDAgetSpecs import PanDAgetSpecs
from TaskWorker.Actions.PanDAKill import PanDAKill
from TaskWorker.Actions.PanDASpecs2Jobs import PanDASpecs2Jobs
from TaskWorker.Actions.MyProxyLogon import MyProxyLogon
from TaskWorker.WorkerExceptions import WorkerHandlerException, StopHandler
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
                break
            except Exception, exc:
                msg = "Problem handling %s because of %s failure, traceback follows\n" % (self._task['tm_taskname'], str(exc))
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                raise WorkerHandlerException(msg)
            t1 = time.time()
            self.logger.info("Finished %s on %s in %d seconds" % (str(work), self._task['tm_taskname'], t1-t0))
            try:
                nextinput = output.result
            except AttributeError:
                nextinput = output
        tot1 = time.time()
        return nextinput

def handleNewTask(instance, resturl, config, task, *args, **kwargs):
    """Performs the injection of a new task

    :arg str instance: the hostname where the rest interface is running
    :arg str resturl: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the handler."""
    server = HTTPRequests(instance, config.TaskWorker.cmscert, config.TaskWorker.cmskey)
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, server=server, resturl=resturl, myproxylen=60*60*24) )
    if task['tm_job_type'] == 'Analysis': 
        handler.addWork( DBSDataDiscovery(config=config, server=server, resturl=resturl) )
    elif task['tm_job_type'] == 'PrivateMC': 
        handler.addWork( MakeFakeFileSet(config=config, server=server, resturl=resturl) )
    handler.addWork( Splitter(config=config, server=server, resturl=resturl) )

    def glidein(config):
        """Performs the injection of a new task into Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanCreator(config=config, server=server, resturl=resturl) )
        handler.addWork( DagmanSubmitter(config=config, server=server, resturl=resturl) )

    def panda(config):
        """Performs the injection into PanDA of a new task
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDABrokerage(pandaconfig=config, server=server, resturl=resturl) )
        handler.addWork( PanDAInjection(pandaconfig=config, server=server, resturl=resturl) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args)

def handleResubmit(instance, resturl, config, task, *args, **kwargs):
    """Performs the re-injection of failed jobs

    :arg str instance: the hostname where the rest interface is running
    :arg str resturl: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    server = HTTPRequests(instance, config.TaskWorker.cmscert, config.TaskWorker.cmskey)
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, server=server, resturl=resturl, myproxylen=60*60*24) )
    def glidein(config):
        """Performs the re-injection into Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanResubmitter(config=config, server=server, resturl=resturl) )

    def panda(config):
        """Performs the re-injection into PanDA
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDAgetSpecs(pandaconfig=config, server=server, resturl=resturl) )
        handler.addWork( PanDASpecs2Jobs(pandaconfig=config, server=server, resturl=resturl) )
        handler.addWork( PanDABrokerage(pandaconfig=config, server=server, resturl=resturl) )
        handler.addWork( PanDAInjection(pandaconfig=config, server=server, resturl=resturl) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args)

def handleKill(instance, resturl, config, task, *args, **kwargs):
    """Asks to kill jobs

    :arg str instance: the hostname where the rest interface is running
    :arg str resturl: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    server = HTTPRequests(instance, config.TaskWorker.cmscert, config.TaskWorker.cmskey)
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, server=server, resturl=resturl, myproxylen=60*5) )
    def glidein(config):
        """Performs kill of jobs sent through Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanKiller(config=config, server=server, resturl=resturl) )

    def panda(config):
        """Performs the re-injection into PanDA
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDAKill(pandaconfig=config, server=server, resturl=resturl) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args, kwargs)

if __name__ == '__main__':
    print "New task"
    handleNewTask(None, None, None, task={})
    print "\nResubmit task"
    handleResubmit(None, None, None, task={})
    print "\nKill task"
    handleKill(None, None, None, task={})
