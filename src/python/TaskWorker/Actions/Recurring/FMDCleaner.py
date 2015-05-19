import os
import sys
import urllib
import logging
from datetime import date
from httplib import HTTPException

from RESTInteractions import HTTPRequests
from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

class FMDCleaner(BaseRecurringAction):
    pollingTime = 60*24 #minutes

    def _execute(self, resthost, resturi, config, task):
        self.logger.info('Cleaning filemetadata older than 30 days..')
        server = HTTPRequests(resthost, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry = 2)
        ONE_MONTH = 24 * 30
        try:
            instance = resturi.split('/')[2]
            server.delete('/crabserver/%s/filemetadata' % instance, data=urllib.urlencode({'hours': ONE_MONTH}))
#TODO return fro the server a value (e.g.: ["ok"]) to see if everything is ok
#            result = server.delete('/crabserver/dev/filemetadata', data=urllib.urlencode({'hours': ONE_MONTH}))[0]['result'][0]
#            self.logger.info('FMDCleaner, got %s' % result)
        except HTTPException as hte:
            self.logger.error(hte.headers)


if __name__ == '__main__':
    """ Simple main to execute the action standalone. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change resthost, resturi, and twconfig.
    """
    resthost = 'cmsweb.cern.ch'
    resturi = '/crabserver/prod/filemetadata'
    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(twconfig)

    resthost = 'mmascher-dev6.cern.ch'
    resturi = '/crabserver/dev/filemetadata'
    fmdc = FMDCleaner()
    fmdc._execute(resthost, resturi, cfg, None)
