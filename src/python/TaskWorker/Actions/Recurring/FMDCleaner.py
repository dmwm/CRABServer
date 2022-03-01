#pylint: disable= W0621, W0105, W0212
"""
This module is obsolete since we now cleanup FileMetaData and Transfersds tables
via periodical partition dropping, but is kept as reference of how to use
the filemetadata delete API
"""
import sys
import logging
from http.client import HTTPException

from RESTInteractions import CRABRest
from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

class FMDCleaner(BaseRecurringAction):
    pollingTime = 60*24 #minutes

    def _execute(self, crabserver):
        self.logger.info('Cleaning filemetadata older than 30 days..')
        ONE_MONTH = 24 * 30
        try:
            crabserver.delete(api='filemetadata', data=urlencode({'hours': ONE_MONTH}))
#TODO return from the server a value (e.g.: ["ok"]) to see if everything is ok
#            result = crabserver.delete(api='filemetadata', data=urlencode({'hours': ONE_MONTH}))[0]['result'][0]
#            self.logger.info('FMDCleaner, got %s' % result)
        except HTTPException as hte:
            self.logger.error(hte.headers)


if __name__ == '__main__':
    """ Simple main to execute the action standalone. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change resthost, resturi, and twconfig.
    """
    resthost = 'cmsweb.cern.ch'
    dbinstance = 'prod'
    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(twconfig)

    resthost = 'cmsweb-testbed.cern.ch'
    dbinstance = 'dev'
    fmdc = FMDCleaner(cfg.TaskWorker.logsDir)
    crabserver = CRABRest(hostname=resthost, localcert=cfg.TaskWorker.cmscert,
                          localkey=cfg.TaskWorker.cmskey, retry=2,
                          logger=logger)
    crabserver.setDbInstance(dbInstance=dbinstance)
    fmdc._execute(crabserver)
