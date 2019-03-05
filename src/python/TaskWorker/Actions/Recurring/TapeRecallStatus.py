from __future__ import division

import logging
import sys
import os, time

from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction
from TaskWorker.MasterWorker import MasterWorker
from TaskWorker.Actions.DDMRequests import statusRequest
from RESTInteractions import HTTPRequests
from TaskWorker.Actions.MyProxyLogon import MyProxyLogon
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import MAX_DAYS_FOR_TAPERECALL, getTimeFromTaskname
from TaskWorker.Worker import failTask

class TapeRecallStatus(BaseRecurringAction):
    pollingTime = 60*4 # minutes

    def _execute(self, resthost, resturi, config, task):
        mw = MasterWorker(config, logWarning=False, logDebug=False, sequential=True, console=False)

        tapeRecallStatus = 'TAPERECALL'
        self.logger.info("Retrieving %s tasks", tapeRecallStatus)
        recallingTasks = mw.getWork(limit=999999, getstatus=tapeRecallStatus, ignoreTWName=True)
        if len(recallingTasks) > 0:
            self.logger.info("Retrieved a total of %d %s tasks", len(recallingTasks), tapeRecallStatus)
            for recallingTask in recallingTasks:
                taskName = recallingTask['tm_taskname']
                self.logger.info("Working on task %s", taskName)

                reqId = recallingTask['tm_DDM_reqid']
                if not reqId:
                    self.logger.debug("tm_DDM_reqid' is not defined for task %s, skipping such task", taskName)
                    continue

                server = HTTPRequests(config.TaskWorker.resturl, config.TaskWorker.cmscert, config.TaskWorker.cmskey, retry=20, logger=self.logger)
                if (time.time() - getTimeFromTaskname(str(taskName)) > MAX_DAYS_FOR_TAPERECALL*24*60*60):
                    self.logger.info("Task %s is older than %d days, setting its status to FAILED", taskName, MAX_DAYS_FOR_TAPERECALL)
                    msg = "The disk replica request (ID: %d) for the input dataset did not complete in %d days." % (reqId, MAX_DAYS_FOR_TAPERECALL)
                    failTask(taskName, server, config.TaskWorker.restURInoAPI+'workflowdb', msg, self.logger, 'FAILED')
                    continue

                mpl = MyProxyLogon(config=config, server=server, resturi=config.TaskWorker.restURInoAPI, myproxylen=self.pollingTime)
                user_proxy = True
                try:
                    mpl.execute(task=recallingTask) # this adds 'user_proxy' to recallingTask
                except TaskWorkerException as twe:
                    user_proxy = False
                    self.logger.exception(twe)

                # Make sure the task sandbox in the crabcache is not deleted until the tape recall is completed
                if user_proxy:
                    from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
                    ufc = UserFileCache({'cert': recallingTask['user_proxy'], 'key': recallingTask['user_proxy'], 'endpoint': recallingTask['tm_cache_url'], "pycurl": True})
                    sandbox = recallingTask['tm_user_sandbox'].replace(".tar.gz","")
                    sandboxPath = os.path.join(config.TaskWorker.logsDir, sandbox)
                    try:
                        ufc.download(sandbox, sandboxPath, recallingTask['tm_username'])
                        self.logger.info("Successfully touched input sandbox (%s) of task %s (frontend: %s) using the '%s' username (request_id = %d).",
                                         sandbox, taskName, recallingTask['tm_cache_url'], recallingTask['tm_username'], reqId)
                    except Exception as ex:
                        self.logger.info("The CRAB3 server backend could not download the input sandbox (%s) of task %s from the frontend (%s) using the '%s' username (request_id = %d)."+\
                                         " This could be a temporary glitch, will try again in next occurrence of the recurring action."+\
                                         " Error reason:\n%s", sandbox, taskName, recallingTask['tm_cache_url'], recallingTask['tm_username'], reqId, str(ex))
                    finally:
                        if os.path.exists(sandboxPath): os.remove(sandboxPath)

                ddmRequest = statusRequest(reqId, config.TaskWorker.DDMServer, config.TaskWorker.cmscert, config.TaskWorker.cmskey, verbose=False)
                # The query above returns a JSON with a format {"result": "OK", "message": "Request found", "data": [{"request_id": 14, "site": <site>, "item": [<list of blocks>], "group": "AnalysisOps", "n": 1, "status": "new", "first_request": "2018-02-26 23:25:41", "last_request": "2018-02-26 23:25:41", "request_count": 1}]}                
                self.logger.info("Contacted %s using %s and %s for request_id = %d, got:\n%s", config.TaskWorker.DDMServer, config.TaskWorker.cmscert, config.TaskWorker.cmskey, reqId, ddmRequest)

                if ddmRequest["message"] == "Request found":
                    status = ddmRequest["data"][0]["status"]
                    if status == "completed": # possible values: new, activated, updated, completed, rejected, cancelled
                        self.logger.info("Request %d is completed, setting status of task %s to NEW", reqId, taskName)
                        mw.updateWork(taskName, recallingTask['tm_task_command'], 'NEW')
                        # Delete all task warnings (the tapeRecallStatus added a dataset warning which is no longer valid now)
                        if user_proxy: mpl.deleteWarnings(recallingTask['user_proxy'], taskName)
                    elif status == "rejected":
                        msg = "The DDM request (ID: %d) has been rejected with this reason: %s" % (reqId, ddmRequest["data"][0]["reason"])
                        self.logger.info(msg + "\nSetting status of task %s to FAILED", taskName)
                        failTask(taskName, server, config.TaskWorker.restURInoAPI+'workflowdb', msg, self.logger, 'FAILED')

                else:
                    msg = "DDM request_id %d not found. Please report to experts" % reqId
                    self.logger.info(msg)
                    if user_proxy: mpl.uploadWarning(msg, recallingTask['user_proxy'], taskName)

        else:
            self.logger.info("No %s task retrieved.", tapeRecallStatus)


if __name__ == '__main__':
    """ Simple main to execute the action standalone. You just need to set the task worker environment and desired twconfig. """

    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(twconfig)

    trs = TapeRecallStatus(cfg.TaskWorker.logsDir)
    trs._execute(None, None, cfg, None)

