import os
import time
import logging
import traceback
from logging import FileHandler
from httplib import HTTPException

from WMCore.Services.UserFileCache.UserFileCache import UserFileCache

from RESTInteractions import HTTPRequests

from TaskWorker.Actions.Splitter import Splitter
from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.PanDAKill import PanDAKill
from TaskWorker.Actions.DagmanKiller import DagmanKiller
from TaskWorker.Actions.MyProxyLogon import MyProxyLogon
from TaskWorker.Actions.DagmanCreator import DagmanCreator
from TaskWorker.Actions.PanDAgetSpecs import PanDAgetSpecs
from TaskWorker.Actions.PanDABrokerage import PanDABrokerage
from TaskWorker.Actions.PanDAInjection import PanDAInjection
from TaskWorker.Actions.DryRunUploader import DryRunUploader
from TaskWorker.Actions.PanDASpecs2Jobs import PanDASpecs2Jobs
from TaskWorker.Actions.MakeFakeFileSet import MakeFakeFileSet
from TaskWorker.Actions.DagmanSubmitter import DagmanSubmitter
from TaskWorker.Actions.DBSDataDiscovery import DBSDataDiscovery
from TaskWorker.Actions.UserDataDiscovery import UserDataDiscovery
from TaskWorker.Actions.DagmanResubmitter import DagmanResubmitter
from TaskWorker.WorkerExceptions import WorkerHandlerException, StopHandler, TaskWorkerException

DEFAULT_BACKEND = 'panda'

class TaskHandler(object):
    """Handling the set of operations to be performed."""

    def __init__(self, task, procnum):
        """Initializer

        :arg TaskWorker.DataObjects.Task task: the task to work on."""
        self.logger = logging.getLogger(str(procnum))
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

        #set the logger to save the tasklog
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
        taskdirname = "logs/tasks/%s/" % self._task['tm_username']
        if not os.path.isdir(taskdirname):
            os.mkdir(taskdirname)
        taskhandler = FileHandler(taskdirname + self._task['tm_taskname'] + '.log')
        taskhandler.setLevel(logging.DEBUG)
        self.logger.addHandler(taskhandler)

        for work in self.getWorks():
            self.logger.debug("Starting %s on %s" % (str(work), self._task['tm_taskname']))
            t0 = time.time()
            try:
                output = work.execute(nextinput, task=self._task)
            except StopHandler as sh:
                msg = "Controlled stop of handler for %s on %s " % (self._task, str(sh))
                self.logger.error(msg)
                nextinput = Result(task=self._task, result='StopHandler exception received, controlled stop')
                break #exit normally. Worker will not notice there was an error
            except TaskWorkerException as twe:
                self.logger.debug(str(traceback.format_exc())) #print the stacktrace only in debug mode
                raise WorkerHandlerException(str(twe)) #TaskWorker error, do not add traceback to the error propagated to the REST
            except Exception as exc:
                msg = "Problem handling %s because of %s failure, traceback follows\n" % (self._task['tm_taskname'], str(exc))
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                raise WorkerHandlerException(msg) #Errors not foreseen. Print everything!
            finally:
                #upload logfile of the task to the crabcache
                logpath = 'logs/tasks/%s/%s.log' % (self._task['tm_username'], self._task['tm_taskname'])
                if os.path.isfile(logpath) and 'user_proxy' in self._task: #the user proxy might not be there if myproxy retrieval failed
                    cacheurldict = {'endpoint': self._task['tm_cache_url'], 'cert' : self._task['user_proxy'], 'key' : self._task['user_proxy']}
                    try:
                        ufc = UserFileCache(cacheurldict)
                        logfilename = self._task['tm_taskname'] + '_TaskWorker.log'
                        ufc.uploadLog(logpath, logfilename)
                    except HTTPException as hte:
                        msg = ("Failed to upload the logfile to %s for task %s. More details in the http headers and body:\n%s\n%s" %
                               (self._task['tm_cache_url'], self._task['tm_taskname'], hte.headers, hte.result))
                        self.logger.error(msg)
                    except Exception as e:
                        msg = "Unknown error while uploading the logfile for task %s" % self._task['tm_taskname']
                        self.logger.exception(msg)
                taskhandler.flush()
                taskhandler.close()
                self.logger.removeHandler(taskhandler)
            t1 = time.time()
            self.logger.info("Finished %s on %s in %d seconds" % (str(work), self._task['tm_taskname'], t1-t0))
            try:
                nextinput = output.result
            except AttributeError:
                nextinput = output

        taskhandler.close()
        self.logger.removeHandler(taskhandler)

        return nextinput

def handleNewTask(resthost, resturi, config, task, procnum, *args, **kwargs):
    """Performs the injection of a new task

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the handler."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry = 2)
    handler = TaskHandler(task, procnum)
    handler.addWork( MyProxyLogon(config=config, server=server, resturi=resturi, procnum=procnum, myproxylen=60*60*24) )
    if task['tm_job_type'] == 'Analysis': 
        if task.get('tm_user_files'):
            handler.addWork( UserDataDiscovery(config=config, server=server, resturi=resturi, procnum=procnum) )
        elif task.get('tm_arguments', {}).get('userfiles'): ## For backward compatibility only.
            handler.addWork( UserDataDiscovery(config=config, server=server, resturi=resturi, procnum=procnum) )
        else:
            handler.addWork( DBSDataDiscovery(config=config, server=server, resturi=resturi, procnum=procnum) )
    elif task['tm_job_type'] == 'PrivateMC':
        handler.addWork( MakeFakeFileSet(config=config, server=server, resturi=resturi, procnum=procnum) )
    handler.addWork( Splitter(config=config, server=server, resturi=resturi, procnum=procnum) )

    def glidein(config):
        """Performs the injection of a new task into Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanCreator(config=config, server=server, resturi=resturi, procnum=procnum) )
        if task['tm_dry_run'] == 'T':
            handler.addWork( DryRunUploader(config=config, server=server, resturi=resturi, procnum=procnum) )
        else:
            handler.addWork( DagmanSubmitter(config=config, server=server, resturi=resturi, procnum=procnum) )

    def panda(config):
        """Performs the injection into PanDA of a new task
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDABrokerage(pandaconfig=config, server=server, resturi=resturi, procnum=procnum) )
        handler.addWork( PanDAInjection(pandaconfig=config, server=server, resturi=resturi, procnum=procnum) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args)

def handleResubmit(resthost, resturi, config, task, procnum, *args, **kwargs):
    """Performs the re-injection of failed jobs

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry = 2)
    handler = TaskHandler(task, procnum)
    handler.addWork( MyProxyLogon(config=config, server=server, resturi=resturi, procnum=procnum, myproxylen=60*60*24) )
    def glidein(config):
        """Performs the re-injection into Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanResubmitter(config=config, server=server, resturi=resturi, procnum=procnum) )

    def panda(config):
        """Performs the re-injection into PanDA
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDAgetSpecs(pandaconfig=config, server=server, resturi=resturi, procnum=procnum) )
        handler.addWork( PanDASpecs2Jobs(pandaconfig=config, server=server, resturi=resturi, procnum=procnum) )
        handler.addWork( PanDABrokerage(pandaconfig=config, server=server, resturi=resturi, procnum=procnum) )
        handler.addWork( PanDAInjection(pandaconfig=config, server=server, resturi=resturi, procnum=procnum) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args)

def handleKill(resthost, resturi, config, task, procnum, *args, **kwargs):
    """Asks to kill jobs

    :arg str resthost: the hostname where the rest interface is running
    :arg str resturi: the rest base url to contact
    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :arg int procnum: the process number taking care of the work
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry = 2)
    handler = TaskHandler(task, procnum)
    handler.addWork( MyProxyLogon(config=config, server=server, resturi=resturi, procnum=procnum, myproxylen=60*5) )
    def glidein(config):
        """Performs kill of jobs sent through Glidein
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( DagmanKiller(config=config, server=server, resturi=resturi, procnum=procnum) )

    def panda(config):
        """Performs the re-injection into PanDA
        :arg WMCore.Configuration config: input configuration"""
        handler.addWork( PanDAKill(pandaconfig=config, server=server, resturi=resturi, procnum=procnum) )

    locals()[getattr(config.TaskWorker, 'backend', DEFAULT_BACKEND).lower()](config)
    return handler.actionWork(args, kwargs)

if __name__ == '__main__':
    print "New task"
    handleNewTask(None, None, None, task={})
    print "\nResubmit task"
    handleResubmit(None, None, None, task={})
    print "\nKill task"
    handleKill(None, None, None, task={})
