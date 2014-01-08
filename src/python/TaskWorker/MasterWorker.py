#!/usr/bin/env python
#external dependencies
import time
import logging
import os
from logging.handlers import TimedRotatingFileHandler
import urllib
from httplib import HTTPException
import traceback
from base64 import b64encode
from MultiProcessingLog import MultiProcessingLog

#WMcore dependencies
from WMCore.Configuration import loadConfigurationFile, Configuration

#CAFUtilities dependencies
from RESTInteractions import HTTPRequests

from TaskWorker.TestWorker import TestWorker
from TaskWorker.Worker import Worker
from TaskWorker.WorkerExceptions import *
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

def validateDbConfig(config):
    """Verify that the input configuration contains all needed info

    :arg WMCore.Configuration config: input configuration
    :return bool, string: flag for validation result and a message."""
    if getattr(config, 'CoreDatabase', None) is None:
        return False, "Configuration problem: Core Database section is missing. "
    return True, 'Ok'

class MasterWorker(object):
    """I am the master of the TaskWorker"""

    def __init__(self, config, quiet, debug, test=False):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg logging logger: the logger
        :arg bool quiet: it tells if a quiet logger is needed
        :arg bool debug: it tells if needs a verbose logger."""
        def getLogging(quiet, debug):
            """Retrieves a logger and set the proper level

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :return logger: a logger with the appropriate logger level."""

            if self.TEST:
                #if we are testing log to the console is easier
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog('twlog.log', when="midnight")
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
            logger = logging.getLogger()
            logger.debug("Logging level initialized to %s." %loglevel)
            return logger

        self.TEST = test
        self.logger = getLogging(quiet, debug)
        self.config = config
        restinstance = None
        self.resturl = '/crabserver/prod/workflowdb'
        if not self.config.TaskWorker.mode in MODEURL.keys():
            raise ConfigException("No mode provided: need to specify config.TaskWorker.mode in the configuration")
        elif MODEURL[self.config.TaskWorker.mode]['host'] is not None:
            restinstance = MODEURL[self.config.TaskWorker.mode]['host']
            self.resturl = self.resturl.replace('prod', MODEURL[self.config.TaskWorker.mode]['instance'])
        else:
            restinstance = self.config.TaskWorker.resturl
            self.resturl = self.resturl.replace('prod', MODEURL[self.config.TaskWorker.mode]['instance'])
        if self.resturl is None or restinstance is None:
            raise ConfigException("No correct mode provided: need to specify config.TaskWorker.mode in the configuration")
        self.server = HTTPRequests(restinstance, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey)
        self.logger.debug("Hostcert: %s, hostkey: %s" %(str(self.config.TaskWorker.cmscert), str(self.config.TaskWorker.cmskey)))
        if self.TEST:
            self.slaves = TestWorker(self.config, restinstance, self.resturl)
        else:
            self.slaves = Worker(self.config, restinstance, self.resturl)
        self.slaves.begin()

    def _lockWork(self, limit, getstatus, setstatus):
        """Today this is alays returning true, because we do not want the worker to day if
           the server endpoint is not avaialable. 
           Prints a log entry if answer is greater then 400:
            * the server call succeeded or
            * the server could not find anything to update or
            * the server has an internal error"""  
        configreq = {'subresource': 'process', 'workername': self.config.TaskWorker.name, 'getstatus': getstatus, 'limit': limit, 'status': setstatus}
        try:
            self.server.post(self.resturl, data = urllib.urlencode(configreq))
        except HTTPException, hte:
            if not hte.headers.get('X-Error-Detail', '') == 'Required object is missing' or \
               not hte.headers.get('X-Error-Http', -1) == '400':
                self.logger.error("Server could not acquire any work from the server: \n" +
                                  "\tstatus: %s\n" %(hte.headers.get('X-Error-Http', 'unknown')) +
                                  "\treason: %s" %(hte.headers.get('X-Error-Detail', 'unknown')))
                self.logger.error("Probably no task to be processed")
            if hte.headers.get('X-Error-Http', 'unknown') in ['unknown']:
                self.logger.error("Server could not acquire any work from the server:")
                self.logger.error("%s " %(str(traceback.format_exc())))
                self.logger.error("\turl: %s\n" %(getattr(hte, 'url', 'unknown')))
                self.logger.error("\tresult: %s\n" %(getattr(hte, 'result', 'unknown')))
        except Exception, exc:
            self.logger.error("Server could not process the request: %s" %(str(exc)))
            self.logger.error(traceback.format_exc())  
        return True

    def _getWork(self, limit, getstatus):
        configreq = {'limit': limit, 'workername': self.config.TaskWorker.name, 'getstatus': getstatus}
        pendingwork = []
        try:
            pendingwork = self.server.get(self.resturl, data = configreq)[0]['result']
        except HTTPException, hte:
            self.logger.error("Could not get any work from the server: \n" +
                              "\tstatus: %s\n" %(hte.headers.get('X-Error-Http', 'unknown')) +
                              "\treason: %s" %(hte.headers.get('X-Error-Detail', 'unknown')))
            if hte.headers.get('X-Error-Http', 'unknown') in ['unknown']:
                self.logger.error("Server could not acquire any work from the server:")
                self.logger.error("%s " %(str(traceback.format_exc())))
                self.logger.error("\turl: %s\n" %(getattr(hte, 'url', 'unknown')))
                self.logger.error("\tresult: %s\n" %(getattr(hte, 'result', 'unknown')))
        except Exception, exc:
            self.logger.error("Server could not process the request: %s" %(str(exc)))
            self.logger.error(traceback.format_exc())
        return pendingwork

    def updateWork(self, task, status):
        configreq = {'workflow': task, 'status': status, 'subresource': 'state'}
        self.server.post(self.resturl, data = urllib.urlencode(configreq))

    def updateFinished(self, finished):
        for res in finished:
            if hasattr(res, 'error') and res.error:
                self.logger.error("Setting %s as failed" % str(res.task['tm_taskname']))
                configreq = {'workflow': res.task['tm_taskname'],
                             'status': "FAILED",
                             'subresource': 'failure',
                             'failure': b64encode(res.error)}
                self.logger.error("Issue: %s" % (str(res.error)))
                self.server.post(self.resturl, data = urllib.urlencode(configreq))

    def algorithm(self):
        """I'm the intelligent guy taking care of getting the work
           and distribuiting it to the slave processes."""
        self.logger.debug("Starting")
        while(True):
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
            self.logger.info('Worker status:')
            self.logger.info(' - free slaves: %d' % self.slaves.freeSlaves())
            self.logger.info(' - acquired tasks: %d' % self.slaves.queuedTasks())
            self.logger.info(' - tasks pending in queue: %d' % self.slaves.pendingTasks())

            finished = self.slaves.checkFinished()
            self.updateFinished(finished)

            time.sleep(self.config.TaskWorker.polling)
        self.logger.debug("Stopping")

    def __del__(self):
        """Shutting down all the slaves"""
        self.slaves.end()

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
    mw.algorithm()
    mw.slaves.stop()
    del mw
