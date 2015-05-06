#!/usr/bin/env python
#external dependencies
import os
import time
import urllib
import signal
import random
import logging
import traceback
from httplib import HTTPException
from logging.handlers import TimedRotatingFileHandler

#WMcore dependencies
from WMCore.Configuration import loadConfigurationFile, Configuration

#CAFUtilities dependencies
from RESTInteractions import HTTPRequests

from TaskWorker.WorkerExceptions import *
from TaskWorker.TestWorker import TestWorker
from MultiProcessingLog import MultiProcessingLog
from TaskWorker.Worker import Worker, setProcessLogger
import TaskWorker.Actions.Recurring.BaseRecurringAction
from TaskWorker.Actions.Recurring.BaseRecurringAction import handleRecurring
from TaskWorker.Actions.Handler import handleResubmit, handleNewTask, handleKill

## NOW placing this here, then to be verified if going into Action.Handler, or TSM
## This is a list because we want to preserve the order
STATE_ACTIONS_MAP = [("NEW", handleNewTask), ("KILL", handleKill), ("RESUBMIT", handleResubmit)]
def states():
    for st in STATE_ACTIONS_MAP:
        yield st

def handler(status):
    for st in STATE_ACTIONS_MAP:
        if st[0] == status:
            return st[1]

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
    return True, 'Ok'


class MasterWorker(object):
    """I am the master of the TaskWorker"""

    def __init__(self, config, quiet, debug, test=False):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg logging logger: the logger
        :arg bool quiet: it tells if a quiet logger is needed
        :arg bool debug: it tells if needs a verbose logger."""


        def createLogdir(dirname):
            """ Create the directory dirname ignoring erors in case it exists. Exit if
                the directory cannot be created.
            """
            try:
                os.mkdir(dirname)
            except OSError as ose:
                if ose.errno != 17: #ignore the "Directory already exists error"
                    print str(ose)
                    print "The task worker need to access the '%s' directory" % dirname
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
                    logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
                logHandler.setFormatter(logFormatter)
                logging.getLogger().addHandler(logHandler)
            loglevel = logging.INFO
            if quiet:
                loglevel = logging.WARNING
            if debug:
                loglevel = logging.DEBUG
            logging.getLogger().setLevel(loglevel)
            logger = setProcessLogger("master")
            logger.debug("PID %s." % os.getpid())
            logger.debug("Logging level initialized to %s." % loglevel)
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
        self.server = HTTPRequests(resthost, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, retry = 2)
        self.logger.debug("Hostcert: %s, hostkey: %s" %(str(self.config.TaskWorker.cmscert), str(self.config.TaskWorker.cmskey)))
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
        """Today this is alays returning true, because we do not want the worker to day if
           the server endpoint is not avaialable.
           Prints a log entry if answer is greater then 400:
            * the server call succeeded or
            * the server could not find anything to update or
            * the server has an internal error"""
        configreq = {'subresource': 'process', 'workername': self.config.TaskWorker.name, 'getstatus': getstatus, 'limit': limit, 'status': setstatus}
        try:
            self.server.post(self.restURInoAPI + '/workflowdb', data = urllib.urlencode(configreq))
        except HTTPException as hte:
            #Using a msg variable and only one self.logger.error so that messages do not get shuffled
            msg = "Task Worker could not update a task status (HTTPException): %s\nConfiguration parameters=%s\n" % (str(hte), configreq)
            if not hte.headers.get('X-Error-Detail', '') == 'Required object is missing' or \
               not hte.headers.get('X-Error-Http', -1) == '400':
                msg += "Task Worker could not update work to the server: \n" +\
                                  "\tstatus: %s\n" %(hte.headers.get('X-Error-Http', 'unknown')) +\
                                  "\treason: %s\n" %(hte.headers.get('X-Error-Detail', 'unknown'))
                msg += "Probably no task to be updated\n"
            if hte.headers.get('X-Error-Http', 'unknown') in ['unknown']:
                msg += "TW could not update work to the server:\n"
                msg += "%s \n" %(str(traceback.format_exc()))
                msg += "\turl: %s\n" %(getattr(hte, 'url', 'unknown'))
                msg += "\tresult: %s\n" %(getattr(hte, 'result', 'unknown'))
            self.logger.error(msg)
        except Exception as exc:
            msg = "Task Worker could not update a task status: %s\nConfiguration parameters=%s\n" % (str(exc), configreq)
            self.logger.error(msg + traceback.format_exc())

        return True

    def _getWork(self, limit, getstatus):
        configreq = {'limit': limit, 'workername': self.config.TaskWorker.name, 'getstatus': getstatus}
        pendingwork = []
        try:
            pendingwork = self.server.get(self.restURInoAPI + '/workflowdb', data = configreq)[0]['result']
        except HTTPException as hte:
            self.logger.error("HTTP Error during _getWork: %s" % str(hte))
            self.logger.error("Could not get any work from the server: \n" +
                              "\tstatus: %s\n" %(hte.headers.get('X-Error-Http', 'unknown')) +
                              "\treason: %s" %(hte.headers.get('X-Error-Detail', 'unknown')))
            if hte.headers.get('X-Error-Http', 'unknown') in ['unknown']:
                self.logger.error("Server could not acquire any work from the server:")
                self.logger.error("%s " %(str(traceback.format_exc())))
                self.logger.error("\turl: %s\n" %(getattr(hte, 'url', 'unknown')))
                self.logger.error("\tresult: %s\n" %(getattr(hte, 'result', 'unknown')))
        except Exception as exc:
            self.logger.error("Server could not process the request: %s" %(str(exc)))
            self.logger.error(traceback.format_exc())
        return pendingwork

    def quit(self, code, traceback_):
        self.logger.info("Received kill request. Waiting for the workers...")
        self.STOP = True

    def updateWork(self, task, status):
        configreq = {'workflow': task, 'status': status, 'subresource': 'state'}
        retry = True
        while retry:
            try:
                self.server.post(self.restURInoAPI + '/workflowdb', data = urllib.urlencode(configreq))
                retry = False
            except HTTPException as hte:
                #Using a msg variable and only one self.logger.error so that messages do not get shuffled
                msg = "Task Worker could not update a task status (HTTPException): %s\nConfiguration parameters=%s\n" % (str(hte), configreq)
                msg += "\tstatus: %s\n" %(hte.headers.get('X-Error-Http', 'unknown'))
                msg += "\treason: %s\n" %(hte.headers.get('X-Error-Detail', 'unknown'))
                msg += "\turl: %s\n" %(getattr(hte, 'url', 'unknown'))
                msg += "\tresult: %s\n" %(getattr(hte, 'result', 'unknown'))
                msg += "%s \n" %(str(traceback.format_exc()))
                self.logger.error(msg)
                retry = False
                if int(hte.headers.get('X-Error-Http', '0')) == 503:
                    #503 - Database/Service unavailable. Maybe Intervention of CMSWEB ongoing?
                    retry = True
                    time_sleep = 30 + random.randint(10,30)
                    self.logger.info("Sleeping %s seconds and will try to update again." % str(time_sleep))
                    time.sleep(time_sleep)
            except Exception as exc:
                msg = "Task Worker could not update a task status: %s\nConfiguration parameters=%s\n" % (str(exc), configreq)
                self.logger.error(msg + traceback.format_exc())
                retry = False

    def algorithm(self):
        """I'm the intelligent guy taking care of getting the work
           and distribuiting it to the slave processes."""

        self.logger.debug("Starting")
        while(not self.STOP):
            for status, worktype in states():
                limit = self.slaves.queueableTasks()
                if not self._lockWork(limit=limit, getstatus=status, setstatus='HOLDING'):
                    continue
                pendingwork = self._getWork(limit=limit, getstatus='HOLDING')
                self.logger.info("Retrieved a total of %d %s works" %(len(pendingwork), worktype))
                self.logger.debug("Retrieved the following works: \n%s" %(str(pendingwork)))
                self.slaves.injectWorks([(worktype, work, None) for work in pendingwork])
                for task in pendingwork:
                    self.updateWork(task['tm_taskname'], 'QUEUED')

            for action in self.recurringActions:
                if action.isTimeToGo():
                    #Maybe we should use new slaves and not reuse the ones used for the tasks
                    self.logger.debug("Injecting recurring action: \n%s" %(str(action.__module__)))
                    self.slaves.injectWorks([(handleRecurring, {'tm_taskname' : action.__module__}, action.__module__)])

            self.logger.info('Master Worker status:')
            self.logger.info(' - free slaves: %d' % self.slaves.freeSlaves())
            self.logger.info(' - acquired tasks: %d' % self.slaves.queuedTasks())
            self.logger.info(' - tasks pending in queue: %d' % self.slaves.pendingTasks())

            finished = self.slaves.checkFinished()

            time.sleep(self.config.TaskWorker.polling)
        self.logger.debug("Master Worker Exiting Main Cycle")

#    def __del__(self):
#        """Shutting down all the slaves"""
#        self.slaves.end()

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
    status, msg = validateConfig(configuration)
    if not status:
        raise ConfigException(msg)

    mw = MasterWorker(configuration, quiet=options.quiet, debug=options.debug)
    signal.signal(signal.SIGINT, mw.quit)
    signal.signal(signal.SIGTERM, mw.quit)
    mw.algorithm()
    mw.slaves.end()
