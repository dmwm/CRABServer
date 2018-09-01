from __future__ import division

from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction
from TaskWorker.MasterWorker import MasterWorker
from TaskWorker.Actions.DDMRequests import statusRequest
from RESTInteractions import HTTPRequests
from TaskWorker.Actions.MyProxyLogon import MyProxyLogon

import logging
import sys
import os

class TapeRecallStatus(BaseRecurringAction):
    pollingTime = 60*4 # minutes

    def _execute(self, resthost, resturi, config, task):
        mw = MasterWorker(config, quiet=False, debug=False, test=True)

        tapeRecallStatus = 'TAPERECALL'
        self.logger.info("Retrieving %s tasks", tapeRecallStatus)
        recallingTasks = mw.getWork(limit=999999, getstatus=tapeRecallStatus)
        if len(recallingTasks) > 0:
            self.logger.info("Retrieved a total of %d %s tasks", len(recallingTasks), tapeRecallStatus)
            self.logger.debug("Retrieved the following %s tasks: \n%s", tapeRecallStatus, str(recallingTasks))
            for recallingTask in recallingTasks:
                if not recallingTask['tm_DDM_reqid']:
                    self.logger.debug("tm_DDM_reqid' is not defined for task %s, skipping such task", recallingTask['tm_taskname'])
                    continue
                ddmRequest = statusRequest(recallingTask['tm_DDM_reqid'], config.TaskWorker.DDMServer, config.TaskWorker.cmscert, config.TaskWorker.cmskey, verbose=False)
                self.logger.info("Contacted %s using %s and %s, got:\n%s", config.TaskWorker.DDMServer, config.TaskWorker.cmscert, config.TaskWorker.cmskey, ddmRequest)
                # The query above returns a JSON with a format {"result": "OK", "message": "Request found", "data": [{"request_id": 14, "site": <site>, "item": [<list of blocks>], "group": "AnalysisOps", "n": 1, "status": "new", "first_request": "2018-02-26 23:25:41", "last_request": "2018-02-26 23:25:41", "request_count": 1}]}
                if ddmRequest["data"][0]["status"] == "completed": # possible values: new, activated, updated, completed, rejected, cancelled
                    self.logger.info("Request %d is completed, setting status of task %s to NEW", recallingTask['tm_DDM_reqid'], recallingTask['tm_taskname'])
                    mw.updateWork(recallingTask['tm_taskname'], recallingTask['tm_task_command'], 'NEW')
                    # Delete all task warnings (the tapeRecallStatus added a dataset warning which is no longer valid now)
                    server = HTTPRequests(config.TaskWorker.resturl, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry=20, logger=self.logger)
                    mpl = MyProxyLogon(config=config, server=server, resturi=config.TaskWorker.restURInoAPI, myproxylen=self.pollingTime)
                    mpl.execute(task=recallingTask) # this adds 'user_proxy' to recallingTask
                    mpl.deleteWarnings(recallingTask['user_proxy'], recallingTask['tm_taskname'])


if __name__ == '__main__':
    """ Simple main to execute the action standalone. You just need to set the task worker environment. """

    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(twconfig)

    trs = TapeRecallStatus()
    trs.logger = logger
    trs._execute(None, None, cfg, None)

