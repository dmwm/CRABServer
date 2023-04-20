# pylint: disable=pointless-string-statement
import os
import sys
import json
import logging

from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction
from RucioUtils import getNativeRucioClient


class reportQuota(BaseRecurringAction):
    """
    Recurring action to get every hour the used disk quota for crab_tape_recall account,
    then save it to `logsDir/tape_recall_quota.json`
    """
    pollingTime = 60 #minutes

    def _execute(self, config, task):
        account = 'crab_tape_recall'
        self.logger.info(f"Looking up used quota for Rucio account: {account}")

        config.Services.Rucio_account = account
        rucioClient = getNativeRucioClient(config=config, logger=self.logger)
        usageGenerator = rucioClient.get_local_account_usage(account=account)
        totalBytes = 0
        report = {}
        for usage in usageGenerator:
            rse = usage['rse']
            used = usage['bytes']
            report[rse] = used // 1e12
            totalBytes += used
        totalTB = totalBytes // 1e12
        report['totalTB'] = totalTB
        saveFile = os.path.join(config.TaskWorker.logsDir, "tape_recall_quota.json")
        with open(saveFile, 'w', encoding='utf-8') as fd:
            json.dump(report, fd)

if __name__ == '__main__':
    """ Simple main to execute the action standalone. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change resthost, resturi, and twconfig.
    """

    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(twconfig)

    rq = reportQuota(cfg.TaskWorker.logsDir)
    rq._execute(cfg, None)  # pylint: disable=protected-access
