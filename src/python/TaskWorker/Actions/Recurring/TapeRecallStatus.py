from __future__ import division

import logging
import sys
import os
import time
import datetime

from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction
from TaskWorker.MasterWorker import MasterWorker
from TaskWorker.Actions.MyProxyLogon import MyProxyLogon
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import MAX_DAYS_FOR_TAPERECALL, getTimeFromTaskname
from RucioUtils import getNativeRucioClient
from TaskWorker.Worker import failTask
from rucio.common.exception import RuleNotFound

class TapeRecallStatus(BaseRecurringAction):
    pollingTime = 60*4 # minutes
    rucioClient = None

    def refreshSandbox(self, task):

        from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
        ufc = UserFileCache({'cert': task['user_proxy'], 'key': task['user_proxy'],
                             'endpoint': task['tm_cache_url'], "pycurl": True})
        sandbox = task['tm_user_sandbox'].replace(".tar.gz", "")
        debugFiles = task['tm_debug_files'].replace(".tar.gz", "")
        sandboxPath = os.path.join("/tmp", sandbox)
        debugFilesPath = os.path.join("/tmp", debugFiles)
        try:
            ufc.download(sandbox, sandboxPath, task['tm_username'])
            ufc.download(debugFiles, debugFilesPath, task['tm_username'])
            self.logger.info(
                "Successfully touched input and debug sandboxes (%s and %s) of task %s (frontend: %s) using the '%s' username (request_id = %s).",
                sandbox, debugFiles, task['tm_taskname'], task['tm_cache_url'], task['tm_username'], task['tm_DDM_reqid'])
        except Exception as ex:
            msg = "The CRAB3 server backend could not download the input and/or debug sandbox (%s and/or %s) " % (
            sandbox, debugFiles)
            msg += "of task %s from the frontend (%s) using the '%s' username (request_id = %s). " % \
                   (task['tm_taskname'], task['tm_cache_url'], task['tm_username'], task['tm_DDM_reqid'])
            msg += "\nThis could be a temporary glitch, will try again in next occurrence of the recurring action."
            msg += "Error reason:\n%s" % str(ex)
            self.logger.info(msg)
        finally:
            if os.path.exists(sandboxPath): os.remove(sandboxPath)
            if os.path.exists(debugFilesPath): os.remove(debugFilesPath)

    def _execute(self, resthost, resturi, config, task):

        # setup logger
        if not self.logger:
            self.logger = logging.getLogger(__name__)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        else:
        # do not use BaseRecurringAction logger but create a new logger
        # which writes to config.TaskWorker.logsDir/taks/recurring/TapeRecallStatus_YYMMDD-HHMM.log
            self.logger = logging.getLogger('TapeRecallStatus')
            logDir = config.TaskWorker.logsDir + '/tasks/recurring/'
            if not os.path.exists(logDir):
                os.makedirs(logDir)
            timeStamp = time.strftime('%y%m%d-%H%M', time.localtime())
            logFile = 'TapeRecallStatus_' + timeStamp + '.log'
            handler = logging.FileHandler(logDir + logFile)
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)


        mw = MasterWorker(config, logWarning=False, logDebug=False, sequential=True,
                          console=False, name='masterForTapeRecall')

        tapeRecallStatus = 'TAPERECALL'
        self.logger.info("Retrieving %s tasks", tapeRecallStatus)
        recallingTasks = mw.getWork(limit=999999, getstatus=tapeRecallStatus, ignoreTWName=True)
        if not recallingTasks:
            self.logger.info("No %s task retrieved.", tapeRecallStatus)
            return

        self.logger.info("Retrieved a total of %d %s tasks", len(recallingTasks), tapeRecallStatus)
        server = mw.server
        resturi = mw.restURInoAPI + '/info'
        for recallingTask in recallingTasks:
            taskName = recallingTask['tm_taskname']
            self.logger.info("Working on task %s", taskName)

            reqId = recallingTask['tm_DDM_reqid']
            if not reqId:
                self.logger.debug("tm_DDM_reqid' is not defined for task %s, skipping such task", taskName)
                continue
            else:
                msg = "Task points to Rucio RuleId:  %s " % reqId
                self.logger.info(msg)

            if (time.time() - getTimeFromTaskname(str(taskName))) > MAX_DAYS_FOR_TAPERECALL*24*60*60:
                self.logger.info("Task %s is older than %d days, setting its status to FAILED", taskName, MAX_DAYS_FOR_TAPERECALL)
                msg = "The disk replica request (ID: %s) for the input dataset did not complete in %d days." % (reqId, MAX_DAYS_FOR_TAPERECALL)
                failTask(taskName, server, resturi, msg, self.logger, 'FAILED')
                continue

            mpl = MyProxyLogon(config=config, server=server, resturi=resturi, myproxylen=self.pollingTime)
            user_proxy = True
            try:
                mpl.execute(task=recallingTask) # this adds 'user_proxy' to recallingTask
            except TaskWorkerException as twe:
                user_proxy = False
                self.logger.exception(twe)

            # Make sure the task sandbox in the crabcache is not deleted until the tape recall is completed
            if user_proxy:
                self.refreshSandbox(recallingTask)


            # Retrieve status of recall request
            if not self.rucioClient:
                self.rucioClient = getNativeRucioClient(config=config, logger=self.logger)
            try:
                ddmRequest = self.rucioClient.get_replication_rule(reqId)
            except RuleNotFound:
                msg = "Rucio rule id %s not found. Please report to experts" % reqId
                self.logger.error(msg)
                if user_proxy: mpl.uploadWarning(msg, recallingTask['user_proxy'], taskName)
            if ddmRequest['state'] == 'OK':
                self.logger.info("Request %s is completed, setting status of task %s to NEW", reqId, taskName)
                mw.updateWork(taskName, recallingTask['tm_task_command'], 'NEW')
                # Delete all task warnings (the tapeRecallStatus added a dataset warning which is no longer valid now)
                if user_proxy: mpl.deleteWarnings(recallingTask['user_proxy'], taskName)
            else:
                expiration = ddmRequest['expires_at'] # this is a datetime.datetime object
                if expiration < datetime.datetime.now():
                    # give up waiting
                    msg = ("Replication request %s for task %s expired. Setting its status to FAILED" % (reqId, taskName))
                    self.logger.info(msg)
                    failTask(taskName, server, resturi, msg, self.logger, 'FAILED')


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
