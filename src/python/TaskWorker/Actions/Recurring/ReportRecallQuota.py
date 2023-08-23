"""
report used Rucio quota to ElasticSearch via MONIT
"""
import copy
import json
import logging
import sys
from socket import gethostname

import requests
from RucioUtils import getNativeRucioClient
from TaskWorker.Actions.Recurring.BaseRecurringAction import \
    BaseRecurringAction


class ReportRecallQuota(BaseRecurringAction):
    """
    Recurring action to get every hour the used disk quota for crab_tape_recall
    and crab_input  accounts
    then save it to `logsDir/tape_recall_quota.json`
    """
    pollingTime = 60  # minutes

    def createQuotaReport(self, config=None, account=None):
        """
        create a dictionary with the quota report to be sent to MONIT
        even if we do not report usage at single RSE's now, let's collect that info as well
        returns {'rse1':bytes, 'rse':bytes,..., 'totalTB':TBypte}
        """
        msg = f"Looking up used quota for Rucio account: {account}"
        self.logger.info(msg)
        myconfig = copy.deepcopy(config)
        myconfig.Services.Rucio_account = account
        rucioClient = getNativeRucioClient(config=myconfig, logger=self.logger)
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
        return report

    def _execute(self, config, task):  # pylint: disable=unused-argument, invalid-name
        """
        this is the method called by the TW recurring action handling code.
        Need to be named _execute  !
        """
        # prepare a JSON to be sent to MONIT
        jsonDoc = {'producer': 'crab', 'type': 'taskworker', 'hostname': gethostname()}

        # get info for the used account, each will be sent with ad-hoc tag in the JSON
        accounts = [{'name': 'crab_tape_recall', 'tag': 'tape_recall_total_TB'},
                    {'name': 'crab_input', 'tag': 'crab_input_total_TB'}]
        for account in accounts:
            report = self.createQuotaReport(config=config, account=account['name'])
            jsonDoc[account['tag']] = report['totalTB']

        # sends this document to Elastic Search via MONIT
        response = requests.post('http://monit-metrics:10012/', data=json.dumps(jsonDoc),
                                  headers={"Content-Type": "application/json; charset=UTF-8"})
        if response.status_code == 200:
            self.logger.info("Report successfully sent to Monit")
        else:
            msg = f"sending of {jsonDoc} to MONIT failed. Status code {response.status_code}. Message{response.text}"
            self.logger.error(msg)


if __name__ == '__main__':
    # Simple main to execute the action standalone. You just need to set the task worker environment.
    #  The main is set up to work with the production task worker. If you want to use it on your own
    #  instance you need to change resthost, resturi, and TWCONFIG.

    TWCONFIG = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(TWCONFIG)

    rq = ReportRecallQuota(cfg.TaskWorker.logsDir)
    rq._execute(cfg, None)  # pylint: disable=protected-access
