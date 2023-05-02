"""
Manages recurring actions for tape recall functionalities
1. for tasks in TAPERECALL check status of rule and set to NEW when rule is OK
2. for tasks in KILLRECALL delete rule and set to KILLED
"""
import logging
import sys
import os
import time
import datetime
import copy

from http.client import HTTPException
from urllib.parse import urlencode

from rucio.common.exception import RuleNotFound

from ServerUtilities import MAX_DAYS_FOR_TAPERECALL, getTimeFromTaskname
from RESTInteractions import CRABRest
from RucioUtils import getNativeRucioClient
from TaskWorker.MasterWorker import getRESTParams
from TaskWorker.Worker import failTask
from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

class TapeRecallManager(BaseRecurringAction):
    """ interface needed by the way TW deals with recurring actions
    must have a class which inherit from BaseRecurringAction
    which implements the _execute method (with the unused argument "task"...pff)
    name of this class, this file and the recurring action in TW config list
    must be the same """
    pollingTime = 60*4  # unit=minutes. Runs every 4 hours
    rucioClient = None
    privilegedRucioClient = None
    crabserver = None

    def _execute(self, config, task):  # pylint: disable=unused-argument
        """ this is what we do at every polling cycle """
        self.config = config
        # setup logger, crabserver client, rucio client
        self.init()
        # do the work
        self.handleRecall()
        self.handleKill()


    def handleKill(self):
        """ looks for tasks in KILLRECALL and deals with them """
        status = 'KILLRECALL'
        tasksToWorkOn = self.getTasks(status)

        for aTask in tasksToWorkOn:
            taskName = aTask['tm_taskname']
            self.logger.info("Working on task %s", taskName)

            reqId = aTask['tm_DDM_reqid']
            if not reqId:
                self.logger.debug("tm_DDM_reqid' is not defined for task %s, skipping it", taskName)
                continue
            msg = "Task points to Rucio RuleId:  %s " % reqId
            self.logger.info(msg)

            # delete rule and set task status to killed
            self.logger.info('Will delete rule %s', reqId)
            self.privilegedRucioClient.delete_replication_rule(reqId)
            self.updateTaskStatus(taskName, 'KILLED')
            # Clean up previous "dataset on tape" warnings
            self.deleteWarnings(taskName)
            self.logger.info("Done on this task")


    def handleRecall(self):
        """ looks for tasks in TAPERECALL and deals with them """
        status = 'TAPERECALL'
        tasksToWorkOn = self.getTasks(status)
        for aTask in tasksToWorkOn:
            taskName = aTask['tm_taskname']
            self.logger.info("Working on task %s", taskName)
            # 1.) check for "waited too long"
            if (time.time() - getTimeFromTaskname(str(taskName))) > MAX_DAYS_FOR_TAPERECALL*24*60*60:
                msg = "Disk replica request (ID: %s) for input data did not complete in %d days." %\
                      (reqId, MAX_DAYS_FOR_TAPERECALL)
                self.logger.info(msg)
                failTask(taskName, self.crabserver, msg, self.logger, 'FAILED')
                continue
            # 2.) integrity checks
            reqId = aTask['tm_DDM_reqid']
            if not reqId:
                self.logger.debug("tm_DDM_reqid' is not defined for task %s, skipping it", taskName)
                continue
            self.logger.info("Task points to Rucio RuleId:  %s ", reqId)
            try:
                rule = self.rucioClient.get_replication_rule(reqId)
            except RuleNotFound:
                msg = "Rucio rule id %s not found. Please report to experts" % reqId
                self.logger.error(msg)
                self.uploadWarning(msg, taskName)
                continue
            self.logger.info("Rule %s is %s", reqId, rule['state'])
            # 3.) check if rule completed
            if rule['state'] == 'OK':
                # all good kick off a new processing
                self.logger.info("Request %s is completed, proceed with submission", reqId)
                self.updateTaskStatus(taskName, 'NEW')
                # Clean up previous "dataset on tape" warnings
                self.deleteWarnings(taskName)
                # Make sure data will stay on disk
                self.logger.info("Extending rule lifetime to 30 days from now")
                newLifetime = 30 * 24 * 60 * 60  # 30 days in seconds
                self.privilegedRucioClient.update_replication_rule(reqId, {'lifetime': newLifetime})
            else:
                expiration = rule['expires_at']  # this is a datetime.datetime object
                # this condition could be met only if rule expiration is shorter then MAX_DAYS_FOR_TAPERECALL
                # which is more a mistake than a possibility, anyhow, let's cover all grounds
                if expiration < datetime.datetime.now():  # did we wait too much ?
                    msg = ("Replication request %s for task %s expired." % (reqId, taskName))
                    msg += " Setting its status to FAILED"
                    self.logger.info(msg)
                    failTask(taskName, self.crabserver, msg, self.logger, 'FAILED')
                else:
                    pass  # keep waiting
            self.logger.info("Done on this task")


    def init(self):
        """ setup logger, crabserver client and rucio client"""
        if not self.logger:
            # running interactively, setup logging to stdout
            self.logger = logging.getLogger(__name__)
            handler = logging.StreamHandler(sys.stdout)  # pylint: disable=redefined-outer-name
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")  # pylint: disable=redefined-outer-name
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        else:
        # do not use BaseRecurringAction logger but create a new logger
        # which writes to config.TaskWorker.logsDir/taks/recurring/TapeRecallManager_YYMMDD-HHMM.log
            self.logger = logging.getLogger('TapeRecallManager')
            logDir = self.config.TaskWorker.logsDir + '/tasks/recurring/'
            if not os.path.exists(logDir):
                os.makedirs(logDir)
            timeStamp = time.strftime('%y%m%d-%H%M', time.localtime())
            logFile = 'TapeRecallManager_' + timeStamp + '.log'
            handler = logging.FileHandler(logDir + logFile)
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # setup a crabserver REST client
        self.restHost, self.dbInstance = getRESTParams(self.config, self.logger)
        if not self.crabserver:
            self.crabserver = CRABRest(hostname=self.restHost, localcert=self.config.TaskWorker.cmscert,
                                       localkey=self.config.TaskWorker.cmskey,
                                       retry=2, userAgent='CRABTaskWorker')
            self.crabserver.setDbInstance(self.dbInstance)

        # setup a Rucio client
        if not self.rucioClient:
            self.rucioClient = getNativeRucioClient(config=self.config, logger=self.logger)

        # setup a Rucio client with the account which can edit our rules
        if not self.privilegedRucioClient:
            tapeRecallConfig = copy.copy(self.config)
            tapeRecallConfig.Services.Rucio_account = 'crab_tape_recall'
            self.privilegedRucioClient = getNativeRucioClient(tapeRecallConfig, self.logger)


    def getTasks(self, status):
        """retrieve from DB a list of tasks with given status"""
        #TODO this is also a candidate for TaskWorker/TaskUtils.py
        self.logger.info("Retrieving %s tasks", status)
        tasks = []
        configreq = {'limit': 1000, 'workername': self.config.TaskWorker.name, 'getstatus': status}
        try:
            data = urlencode(configreq)
            tasks = self.crabserver.get(api='workflowdb', data=data)[0]['result']
        except Exception:
            pass
        if not tasks:
            self.logger.info("No %s task retrieved.", status)
        else:
            self.logger.info("Retrieved a total of %d %s tasks", len(tasks), status)
        return tasks

    def uploadWarning(self, taskname, msg):
        """ Uploads a warning message to the Task DB so that crab status can show it """
        #TODO this is duplicated in TaskWorker/Actions/TaskAction.py but it is not possible
        # to import from there. Should probably create a TaskWorker/TaskUtils.py for such functions
        configreq = {'subresource': 'addwarning', 'workflow': taskname, 'warning': msg}
        try:
            self.crabserver.post(api='task', data=configreq)
        except HTTPException as hte:
            self.logger.error("Error uploading warning: %s", str(hte))
            self.logger.warning("Cannot add a warning to REST interface. Warning message: %s", msg)

    def deleteWarnings(self, taskname):
        """ Removes all warnings uploaded so fare for this task """
        #TODO this is also a candidate for TaskWorker/TaskUtils.py
        configreq = {'subresource': 'deletewarnings', 'workflow': taskname}
        data = urlencode(configreq)
        try:
            self.crabserver.post(api='task', data=data)
        except HTTPException as hte:
            self.logger.error("Error deleting warnings: %s", str(hte))
            self.logger.warning("Can not delete warnings from REST interface.")

    def updateTaskStatus(self, taskName, status):
        """ change task status in the DB """
        #TODO this is also a candidate for TaskWorker/TaskUtils.py
        self.logger.info('Will set to %s task %s', status, taskName)
        if status == 'NEW':
            command = 'SUBMIT'
        elif status == 'KILLED':
            command = 'KILL'
        else:
            self.logger.error('updateTaskStatus does not know how to handle status %s. Do nothing', status)
            return
        configreq = {'subresource': 'state', 'workflow': taskName, 'status': status, 'command': command}
        data = urlencode(configreq)
        self.crabserver.post(api='workflowdb', data=data)


if __name__ == '__main__':
    # Simple main to execute the action standalone for testing
    # You just need to set the task worker environment and desired twconfig.

    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(twconfig)

    trs = TapeRecallManager(cfg.TaskWorker.logsDir)
    trs._execute(cfg, None)  # pylint: disable=protected-access
