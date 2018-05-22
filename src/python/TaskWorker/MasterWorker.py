#!/usr/bin/tenv python
#external dependencies
from __future__ import print_function
import os
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
STATE_ACTIONS_MAP = { 'SUBMIT' : (handleNewTask, 'SUBMITFAILED'), 'KILL' : (handleKill, 'KILLFAILED'), 'RESUBMIT' : (handleResubmit, 'RESUBMITFAILED')}

MODEURL = {'cmsweb-dev': {'host': 'cmsweb-dev.cern.ch', 'instance':  'dev'},
           'cmsweb-preprod': {'host': 'cmsweb-testbed.cern.ch', 'instance': 'preprod'},
           'cmsweb-prod': {'host': 'cmsweb.cern.ch', 'instance':  'prod'},
           'private': {'host': None, 'instance':  'dev'},}

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

    def __init__(self, config, quiet, debug, test=False):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg bool quiet: it tells if a quiet logger is needed
        :arg bool debug: it tells if needs a verbose logger
        :arg bool test: it tells if to run in test (no subprocesses) mode."""


        def createLogdir(dirname):
            """ Create the directory dirname ignoring erors in case it exists. Exit if
                the directory cannot be created.
            """
            try:
                os.mkdir(dirname)
            except OSError as ose:
                if ose.errno != 17: #ignore the "Directory already exists error"
                    print(str(ose))
                    print("The task worker need to access the '%s' directory" % dirname)
                    sys.exit(1)


        def setRootLogger(quiet, debug):
            """Sets the root logger with the desired verbosity level
               The root logger logs to logs/twlog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :return logger: a logger with the appropriate logger level."""

            createLogdir('logs')
            createLogdir('logs/processes')
            createLogdir('logs/tasks')

            if self.TEST:
                #if we are testing log to the console is easier
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog('logs/twlog.txt', when='midnight')
                logFormatter = \
                    logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
                logHandler.setFormatter(logFormatter)
                logging.getLogger().addHandler(logHandler)
            loglevel = logging.INFO
            if quiet:
                loglevel = logging.WARNING
            if debug:
                loglevel = logging.DEBUG
            logging.getLogger().setLevel(loglevel)
            logger = setProcessLogger("master")
            logger.debug("PID %s.", os.getpid())
            logger.debug("Logging level initialized to %s.", loglevel)
            return logger


        self.STOP = False
        self.TEST = test
        self.logger = setRootLogger(quiet, debug)
        self.config = config
        resthost = None
        self.restURInoAPI = None
        if not self.config.TaskWorker.mode in MODEURL.keys():
            raise ConfigException("No mode provided: need to specify config.TaskWorker.mode in the configuration")
        elif MODEURL[self.config.TaskWorker.mode]['host'] is not None:
            resthost = MODEURL[self.config.TaskWorker.mode]['host']
            self.restURInoAPI = '/crabserver/' + MODEURL[self.config.TaskWorker.mode]['instance']
        else:
            resthost = self.config.TaskWorker.resturl #this should be called resthost in the TaskWorkerConfig -_-
            self.restURInoAPI = '/crabserver/' + MODEURL[self.config.TaskWorker.mode]['instance']
        if resthost is None:
            raise ConfigException("No correct mode provided: need to specify config.TaskWorker.mode in the configuration")
        #Let's increase the server's retries for recoverable errors in the MasterWorker
        #60 means we'll keep retrying for 1 hour basically (we retry at 20*NUMRETRY seconds, so at: 20s, 60s, 120s, 200s, 300s ...)
        self.server = HTTPRequests(resthost, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, retry = 20,
                                   logger = self.logger)
        self.logger.debug("Hostcert: %s, hostkey: %s", str(self.config.TaskWorker.cmscert), str(self.config.TaskWorker.cmskey))
        # Retries for any failures
        if not hasattr(self.config.TaskWorker, 'max_retry'):
            self.config.TaskWorker.max_retry = 0
        if not hasattr(self.config.TaskWorker, 'retry_interval'):
            self.config.TaskWorker.retry_interval = [retry*20*2 for retry in range(self.config.TaskWorker.max_retry)]
        if not len(self.config.TaskWorker.retry_interval) == self.config.TaskWorker.max_retry:
            raise ConfigException("No correct max_retry and retry_interval specified; len of retry_interval must be equal to max_retry.")
        if self.TEST:
            self.slaves = TestWorker(self.config, resthost, self.restURInoAPI + '/workflowdb')
        else:
            self.slaves = Worker(self.config, resthost, self.restURInoAPI + '/workflowdb')
        self.slaves.begin()
        recurringActionsNames = getattr(self.config.TaskWorker, 'recurringActions', [])
        self.recurringActions = [self.getRecurringActionInst(name) for name in recurringActionsNames]


    def getRecurringActionInst(self, actionName):
        mod = __import__('TaskWorker.Actions.Recurring.%s' % actionName, fromlist=actionName)
        return getattr(mod, actionName)()


    def _lockWork(self, limit, getstatus, setstatus):
        """Today this is always returning true, because we do not want the worker to die if
           the server endpoint is not avaialable.
           Prints a log entry if answer is greater than 400:
            * the server call succeeded or
            * the server could not find anything to update or
            * the server has an internal error"""

        configreq = {'subresource': 'process', 'workername': self.config.TaskWorker.name, 'getstatus': getstatus, 'limit': limit, 'status': setstatus}
        try:
            self.server.post(self.restURInoAPI + '/workflowdb', data = urllib.urlencode(configreq))
        except HTTPException as hte:
            msg = "HTTP Error during _lockWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
            return False
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the _lockWork request (prameters are %s)", configreq)
            return False

        return True


    def _getWork(self, limit, getstatus):
        configreq = {'limit': limit, 'workername': self.config.TaskWorker.name, 'getstatus': getstatus}
        pendingwork = []
        try:
            pendingwork = self.server.get(self.restURInoAPI + '/workflowdb', data = configreq)[0]['result']
        except HTTPException as hte:
            msg = "HTTP Error during _getWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the _getWork request (prameters are %s)", configreq)
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
            self.server.post(self.restURInoAPI + '/workflowdb', data = urllib.urlencode(configreq))
        except HTTPException as hte:
            msg = "HTTP Error during updateWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the updateWork request (prameters are %s)", configreq)
        else:
            return True #success
        return False #failure


    def failQueuedTasks(self):
        """ This method is used at the TW startup and it fails QUEUED tasks that supposedly
            could not communicate with the REST and update their status. The method put those
            task to SUBMITFAILED, KILLFAILED, RESUBMITFAILED depending on the value of
            the command field.
        """
        limit = self.slaves.nworkers * 2
        total = 0
        while True:
            pendingwork = self._getWork(limit=limit, getstatus='QUEUED')
            for task in pendingwork:
                self.logger.debug("Failing QUEUED task %s", task['tm_taskname'])
                if task['tm_task_command']:
                    dummyWorktype, failstatus = STATE_ACTIONS_MAP[task['tm_task_command']]
                else:
                    failstatus = 'FAILED'
                self.updateWork(task['tm_taskname'], task['tm_task_command'], failstatus)
            if not len(pendingwork):
                self.logger.info("Finished failing QUEUED tasks (total %s)", total)
                break #too bad "do..while" does not exist in python...
            else:
                total += len(pendingwork)
                self.logger.info("Failed %s tasks (limit %s), getting next chunk of tasks", len(pendingwork), limit)


    def algorithm(self):
        """I'm the intelligent guy taking care of getting the work
           and distributing it to the slave processes."""

        self.logger.debug("Failing QUEUED tasks before startup.")
        self.failQueuedTasks()
        self.logger.debug("Starting main loop.")
        while(not self.STOP):
            limit = self.slaves.queueableTasks()
            if not self._lockWork(limit=limit, getstatus='NEW', setstatus='HOLDING'):
                time.sleep(self.config.TaskWorker.polling)
                continue

            pendingwork = self._getWork(limit=limit, getstatus='HOLDING')

            if len(pendingwork) > 0:
                self.logger.info("Retrieved a total of %d works", len(pendingwork))
                self.logger.debug("Retrieved the following works: \n%s", str(pendingwork))

            toInject = []
            for task in pendingwork:
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

        self.logger.debug("Master Worker Exiting Main Cycle")


if __name__ == '__main__':
    from optparse import OptionParser

    usage  = "usage: %prog [options] [args]"
    parser = OptionParser(usage=usage)

    parser.add_option( "-d", "--debug",
                       action = "store_true",
                       dest = "debug",
                       default = False,
                       help = "print extra messages to stdout" )
    parser.add_option( "-q", "--quiet",
                       action = "store_true",
                       dest = "quiet",
                       default = False,
                       help = "don't print any messages to stdout" )

    parser.add_option( "--config",
                       dest = "config",
                       default = None,
                       metavar = "FILE",
                       help = "configuration file path" )

    (options, args) = parser.parse_args()

    if not options.config:
        raise ConfigException("Configuration not found")

    configuration = loadConfigurationFile( os.path.abspath(options.config) )
    status_, msg_ = validateConfig(configuration)
    if not status_:
        raise ConfigException(msg_)

    mw = None
    try:
        mw = MasterWorker(configuration, quiet=options.quiet, debug=options.debug)
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
