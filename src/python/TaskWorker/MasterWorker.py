#!/usr/bin/tenv python
#external dependencies
from __future__ import print_function
import os
import shutil
import sys
import time
import urllib
import signal
import logging
from httplib import HTTPException

#WMcore dependencies
from WMCore.Configuration import loadConfigurationFile

#CAFUtilities dependencies
from RESTInteractions import HTTPRequests

import HTCondorLocator
from ServerUtilities import newX509env
from ServerUtilities import SERVICE_INSTANCES
from TaskWorker.TestWorker import TestWorker
from MultiProcessingLog import MultiProcessingLog
from TaskWorker.Worker import Worker, setProcessLogger
from TaskWorker.WorkerExceptions import ConfigException
from TaskWorker.Actions.Recurring.BaseRecurringAction import handleRecurring
from TaskWorker.Actions.Handler import handleResubmit, handleNewTask, handleKill

## NOW placing this here, then to be verified if going into Action.Handler, or TSM
## The meaning of the elements in the 3-tuples are as follows:
## 1st) the command to be executed on the task;
## 2nd) the work that should be do;
## 3nd) the new status that the task should get in case of failure.
STATE_ACTIONS_MAP = {'SUBMIT' : (handleNewTask, 'SUBMITFAILED'), 'KILL' : (handleKill, 'KILLFAILED'), 'RESUBMIT' : (handleResubmit, 'RESUBMITFAILED')}

MODEURL = {'cmsweb-dev': {'host': 'cmsweb-dev.cern.ch', 'instance':  'dev'},
           'cmsweb-test': {'host': 'cmsweb-test.cern.ch', 'instance': 'preprod'},
           'cmsweb-preprod': {'host': 'cmsweb-testbed.cern.ch', 'instance': 'preprod'},
           'cmsweb-prod': {'host': 'cmsweb.cern.ch', 'instance':  'prod'},
           'test' :{'host': None, 'instance': 'preprod'},
           'private': {'host': None, 'instance':  'dev'},
          }

def validateConfig(config):
    """Verify that the input configuration contains all needed info

    :arg WMCore.Configuration config: input configuration
    :return bool, string: flag for validation result and a message."""
    if getattr(config, 'TaskWorker', None) is None:
        return False, "Configuration problem: Task worker section is missing. "
    if not hasattr(config.TaskWorker, 'scheddPickerFunction'):
        config.TaskWorker.scheddPickerFunction = HTCondorLocator.memoryBasedChoices
    return True, 'Ok'


class MasterWorker(object):
    """I am the master of the TaskWorker"""

    def __init__(self, config, logWarning, logDebug, sequential=False, console=False, name='master'):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg bool logWarning: it tells if a quiet logger is needed
        :arg bool logDebug: it tells if needs a verbose logger
        :arg bool sequential: it tells if to run in sequential (no subprocesses) mode.
        :arg bool console: it tells if to log to console.
        :arg string name: defines a name for the log of this master process"""


        def createLogdir(dirname):
            """ Create the directory dirname ignoring errors in case it exists. Exit if
                the directory cannot be created.
            """
            try:
                os.mkdir(dirname)
            except OSError as ose:
                if ose.errno != 17: #ignore the "Directory already exists error"
                    print(str(ose))
                    print("The task worker need to access the '%s' directory" % dirname)
                    sys.exit(1)

        def createAndCleanLogDirectories(logsDir):
            # it can be named with the time stamp a TW started
            createLogdir(logsDir)
            createLogdir(logsDir+'/tasks')
            currentProcessesDir = logsDir + '/processes/'
            createLogdir(currentProcessesDir)
            # when running inside a container process logs will start with same
            # process numbers, i.e. same name, at any container restart.
            # to avoid clashes and confusion, we will put away all previous processes
            # logs when a TW instance starts. To this goal each TW which runs
            # creates a directory where new containers will move its logs, so
            # identify LastLogs_timestamp directory
            latestLogDir = None  # the logs directory could be empty
            files = os.listdir(currentProcessesDir)
            files.sort(reverse=True)   # if there are multiple Latest*, will hit the latest first
            for f in files:
                if f.startswith('Latest'):
                    latestLogDir = currentProcessesDir + f
                    break
            if files and latestLogDir:
                # rename from Latest to Old
                oldLogsDir = latestLogDir.replace('Latest','Old')
                shutil.move(latestLogDir, oldLogsDir)
            else:
                print("LatestLogDir not found in logs/processes, create a dummy dir to store old files")
                oldLogsDir = currentProcessesDir + 'OldLog-Unknwown'
                createLogdir(oldLogsDir)
            # move process logs for latest TW run to old directory
            for f in files:
                if f.startswith('proc.c3id'):
                    shutil.move(currentProcessesDir + f, oldLogsDir)

            # create a new LateastLogs directory where to store logs from this TaskWorker
            YYMMDD_HHMMSS= time.strftime('%y%m%d_%H%M%S', time.localtime())
            myDir = currentProcessesDir + 'LatestLogs-' + YYMMDD_HHMMSS
            createLogdir(myDir)


        def setRootLogger(logWarning, logDebug, console, name):
            """Sets the root logger with the desired verbosity level
               The root logger logs to logsDir/twlog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool logWarning: it tells if a quiet logger is needed
            :arg bool logDebug: it tells if needs a verbose logger
            :arg bool console: it tells if to log to console
            :arg string name: define a name for the log file of this master process
            :return logger: a logger with the appropriate logger level."""

            # this must only done for real Master, not when it is used by TapeRecallStatus
            logsDir = config.TaskWorker.logsDir
            if name == 'master':
                createAndCleanLogDirectories(logsDir)

            if console:
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog(logsDir+'/twlog.txt', when='midnight')
                logFormatter = \
                    logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
                logHandler.setFormatter(logFormatter)
                logging.getLogger().addHandler(logHandler)
            loglevel = logging.INFO
            if logWarning:
                loglevel = logging.WARNING
            if logDebug:
                loglevel = logging.DEBUG
            logging.getLogger().setLevel(loglevel)
            logger = setProcessLogger(name, logsDir)
            logger.debug("PID %s.", os.getpid())
            logger.debug("Logging level initialized to %s.", loglevel)
            return logger


        self.STOP = False
        self.TEST = sequential
        self.logger = setRootLogger(logWarning, logDebug, console, name)
        self.config = config
        self.restHost = None
        dbInstance = None

        try:
            instance = self.config.TaskWorker.instance
        except:
            msg = "No instance provided: need to specify config.TaskWorker.instance in the configuration"
            raise ConfigException(msg)

        if instance in SERVICE_INSTANCES:
            self.logger.info('Will connect to CRAB service: %s', instance)
            self.restHost = SERVICE_INSTANCES[instance]['restHost']
            dbInstance = SERVICE_INSTANCES[instance]['dbInstance']
        else:
            msg = "Invalid instance value '%s'" % instance
            raise ConfigException(msg)
        if instance is 'other':
            self.logger.info('Will use restHost and dbInstance from config file')
            try:
                self.restHost = self.config.TaskWorker.restHost
                dbInstance = self.config.TaskWorker.dbInstance
            except:
                msg = "Need to specify config.TaskWorker.restHost and dbInstance in the configuration"
                raise ConfigException(msg)
        self.restURInoAPI = '/crabserver/' + dbInstance

        self.logger.info('Will connect via URL: https://%s/%s', self.restHost, self.restURInoAPI)
        
        #Let's increase the server's retries for recoverable errors in the MasterWorker
        #60 means we'll keep retrying for 1 hour basically (we retry at 20*NUMRETRY seconds, so at: 20s, 60s, 120s, 200s, 300s ...)
        self.server = HTTPRequests(self.restHost, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, retry=20,
                                   logger=self.logger, userAgent='CRABMasterWorker')
        self.logger.debug("Hostcert: %s, hostkey: %s", str(self.config.TaskWorker.cmscert), str(self.config.TaskWorker.cmskey))
        # Retries for any failures
        if not hasattr(self.config.TaskWorker, 'max_retry'):
            self.config.TaskWorker.max_retry = 0
        if not hasattr(self.config.TaskWorker, 'retry_interval'):
            self.config.TaskWorker.retry_interval = [retry*20*2 for retry in range(self.config.TaskWorker.max_retry)]
        if not len(self.config.TaskWorker.retry_interval) == self.config.TaskWorker.max_retry:
            raise ConfigException("No correct max_retry and retry_interval specified; len of retry_interval must be equal to max_retry.")
        # use the config to pass some useful global stuff to all workers
        # will use TaskWorker.cmscert/key to talk with CMSWEB
        self.config.TaskWorker.envForCMSWEB = newX509env(X509_USER_CERT=self.config.TaskWorker.cmscert,
                                                         X509_USER_KEY=self.config.TaskWorker.cmskey)

        if self.TEST:
            self.slaves = TestWorker(self.config, self.restHost, self.restURInoAPI + '/workflowdb')
        else:
            self.slaves = Worker(self.config, self.restHost, self.restURInoAPI + '/workflowdb')
        self.slaves.begin()
        recurringActionsNames = getattr(self.config.TaskWorker, 'recurringActions', [])
        self.recurringActions = [self.getRecurringActionInst(name) for name in recurringActionsNames]


    def getRecurringActionInst(self, actionName):
        mod = __import__('TaskWorker.Actions.Recurring.%s' % actionName, fromlist=actionName)
        return getattr(mod, actionName)(self.config.TaskWorker.logsDir)


    def _lockWork(self, limit, getstatus, setstatus):
        """Today this is always returning true, because we do not want the worker to die if
           the server endpoint is not avaialable.
           Prints a log entry if answer is greater than 400:
            * the server call succeeded or
            * the server could not find anything to update or
            * the server has an internal error"""

        configreq = {'subresource': 'process', 'workername': self.config.TaskWorker.name, 'getstatus': getstatus, 'limit': limit, 'status': setstatus}
        try:
            self.server.post(self.restURInoAPI + '/workflowdb', data=urllib.urlencode(configreq))
        except HTTPException as hte:
            msg = "HTTP Error during _lockWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
            return False
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the _lockWork request (prameters are %s)", configreq)
            return False

        return True


    def getWork(self, limit, getstatus, ignoreTWName=False):
        configreq = {'limit': limit, 'workername': self.config.TaskWorker.name, 'getstatus': getstatus}
        if ignoreTWName:
            configreq['workername'] = '%'

        pendingwork = []
        try:
            pendingwork = self.server.get(self.restURInoAPI + '/workflowdb', data=configreq)[0]['result']
        except HTTPException as hte:
            msg = "HTTP Error during getWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the getWork request (prameters are %s)", configreq)
        return pendingwork


    def quit_(self, dummyCode, dummyTraceback):
        self.logger.info("Received kill request. Setting STOP flag in the master process...")
        self.STOP = True


    def updateWork(self, taskname, command, status):
        """ Update taskname setting the status and the command for it
            Return True if the change succeded, False otherwise
        """

        configreq = {'workflow': taskname, 'command': command, 'status': status, 'subresource': 'state'}
        try:
            self.server.post(self.restURInoAPI + '/workflowdb', data=urllib.urlencode(configreq))
        except HTTPException as hte:
            msg = "HTTP Error during updateWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the updateWork request (prameters are %s)", configreq)
        else:
            return True #success
        return False #failure


    def restartQueuedTasks(self):
        """ This method is used at the TW startup and it restarts QUEUED tasks 
            setting them  back again to NEW.
        """
        limit = self.slaves.nworkers * 2
        total = 0
        while True:
            pendingwork = self.getWork(limit=limit, getstatus='QUEUED')
            for task in pendingwork:
                self.logger.debug("Restarting QUEUED task %s", task['tm_taskname'])
                self.updateWork(task['tm_taskname'], task['tm_task_command'], 'NEW')
            if not len(pendingwork):
                self.logger.info("Finished restarting QUEUED tasks (total %s)", total)
                break #too bad "do..while" does not exist in python...
            else:
                total += len(pendingwork)
                self.logger.info("Restarted %s tasks (limit %s), getting next chunk of tasks", len(pendingwork), limit)


    def failBannedTask(self, task):
        """ This method is used at the TW startup and it fails NEW tasks which I do not like
        The method put those task to SUBMITFAILED, KILLFAILED, RESUBMITFAILED depending on the value of
         the command field.
        Initial implementation bans based on a list of usernames, other task attributes can
          be checked if needed by adding a bit of code
        Returns:
            True : if the task was declared bad and was failed
            False: for normal (good) tasks
        """
        bannedUsernames = getattr(self.config.TaskWorker, 'bannedUsernames', [])
        if task['tm_username'] in bannedUsernames:
            self.logger.debug("Forcefully failing task %s", task['tm_taskname'])
            if task['tm_task_command']:
                dummyWorktype, failstatus = STATE_ACTIONS_MAP[task['tm_task_command']]
            else:
                failstatus = 'FAILED'
            self.updateWork(task['tm_taskname'], task['tm_task_command'], failstatus)
            # TODO look into logging a message for the user
            return True
        return False

    def algorithm(self):
        """I'm the intelligent guy taking care of getting the work
           and distributing it to the slave processes."""

        self.logger.debug("Restarting QUEUED tasks before startup.")
        self.restartQueuedTasks()
        self.logger.debug("Master Worker Starting Main Cycle.")
        while not self.STOP:
            limit = self.slaves.queueableTasks()
            if not self._lockWork(limit=limit, getstatus='NEW', setstatus='HOLDING'):
                time.sleep(self.config.TaskWorker.polling)
                continue

            pendingwork = self.getWork(limit=limit, getstatus='HOLDING')

            if len(pendingwork) > 0:
                keys = ['tm_task_command', 'tm_taskname']
                tasksInfo = [{k:v for k, v in task.items() if k in keys} for task in pendingwork]
                self.logger.info("Retrieved a total of %d works", len(pendingwork))
                self.logger.debug("Retrieved the following works: \n%s", str(tasksInfo))

            toInject = []
            for task in pendingwork:
                if self.failBannedTask(task):
                    continue
                if self.updateWork(task['tm_taskname'], task['tm_task_command'], 'QUEUED'):
                    worktype, failstatus = STATE_ACTIONS_MAP[task['tm_task_command']]
                    toInject.append((worktype, task, failstatus, None))
                else:
                    #The task stays in HOLDING and will be acquired again later
                    self.logger.info("Skipping %s since it could not be updated to QUEUED. Will be retried in the next iteration", task['tm_taskname'])

            self.slaves.injectWorks(toInject)

            for action in self.recurringActions:
                if action.isTimeToGo():
                    #Maybe we should use new slaves and not reuse the ones used for the tasks
                    self.logger.debug("Injecting recurring action: \n%s", (str(action.__module__)))
                    self.slaves.injectWorks([(handleRecurring, {'tm_username': 'recurring', 'tm_taskname' : action.__module__}, 'FAILED', action.__module__)])

            self.logger.info('Master Worker status:')
            self.logger.info(' - free slaves: %d', self.slaves.freeSlaves())
            self.logger.info(' - acquired tasks: %d', self.slaves.queuedTasks())
            self.logger.info(' - tasks pending in queue: %d', self.slaves.pendingTasks())

            time.sleep(self.config.TaskWorker.polling)

            dummyFinished = self.slaves.checkFinished()

        self.logger.debug("Master Worker Exiting Main Cycle.")


if __name__ == '__main__':
    from optparse import OptionParser

    usage = "usage: %prog [options] [args]"
    parser = OptionParser(usage=usage)

    parser.add_option("-d", "--logDebug",
                      action="store_true",
                      dest="logDebug",
                      default=False,
                      help="print extra messages to stdout")
    parser.add_option("-w", "--logWarning",
                      action="store_true",
                      dest="logWarning",
                      default=False,
                      help="don't print any messages to stdout")
    parser.add_option("-s", "--sequential",
                      action="store_true",
                      dest="sequential",
                      default=False,
                      help="run in sequential (no subprocesses) mode")
    parser.add_option("-c", "--console",
                      action="store_true",
                      dest="console",
                      default=False,
                      help="log to console")

    parser.add_option("--config",
                      dest="config",
                      default=None,
                      metavar="FILE",
                      help="configuration file path")

    (options, args) = parser.parse_args()

    if not options.config:
        raise ConfigException("Configuration not found")

    configuration = loadConfigurationFile(os.path.abspath(options.config))
    status_, msg_ = validateConfig(configuration)
    if not status_:
        raise ConfigException(msg_)

    mw = None
    try:
        mw = MasterWorker(configuration, logWarning=options.logWarning, logDebug=options.logDebug, sequential=options.sequential, console=options.console)
        signal.signal(signal.SIGINT, mw.quit_)
        signal.signal(signal.SIGTERM, mw.quit_)
        mw.algorithm()
    except:
        if mw:
            mw.logger.exception("Unexpected and fatal error. Exiting task worker")
        #don't really wanna miss this, propagating the exception and exiting really bad
        raise
    finally:
        #there can be an exception before slaves are created, e.g. in the __init__
        if hasattr(mw, 'slaves'):
            mw.slaves.end()
