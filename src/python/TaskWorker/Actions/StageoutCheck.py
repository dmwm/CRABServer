import os
import re

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import isFailurePermanent
from ServerUtilities import getCheckWriteCommand, createDummyFile
from ServerUtilities import removeDummyFile, execute_command, isEnoughRucioQuota, getRucioAccountFromLFN
from RucioUtils import getWritePFN

class StageoutCheck(TaskAction):

    """
    Check if able to stageout dump file to destination site.
    Will not check this for ActivitiesToRunEverywhere - mainly HC usage or
    if stageout output and logs is false or
    if dryrun option True
    """
    def __init__(self, config, crabserver, procnum=-1, rucioClient=None):
        TaskAction.__init__(self, config, crabserver, procnum)
        self.rucioClient = rucioClient
        self.task = None
        self.proxy = None
        self.workflow = None

    def checkPermissions(self, Cmd):
        """
        Execute command and in case of permanent issue, raise error
        If issue unknown, upload warning message and return 1
        Return 0 otherwise
        """
        self.logger.info("Executing command: %s ", Cmd)
        out, err, exitcode = execute_command(Cmd)
        if exitcode != 0:
            isPermanent, failure, dummyExitCode = isFailurePermanent(err)
            if isPermanent:
                msg = "CRAB3 refuses to send jobs to grid scheduler for %s. Error message: %s" %(self.task['tm_taskname'], failure)
                msg += "\n" + out
                msg += "\n" + err
                self.logger.warning(msg)
                raise TaskWorkerException(msg)
            else:
                # Unknown error. Operators should check it from time to time and add failures if they are permanent.
                self.logger.warning("CRAB3 was not able to identify if failure is permanent. Err: %s Out: %s ExitCode: %s", err, out, exitcode)
                # Upload warning to user about not being able to check stageout
                msg = "The CRAB3 server got a non-critical error while checking stageout permissions. Please use checkwrite to check if everything is fine."
                self.uploadWarning(msg, self.task['user_proxy'], self.task['tm_taskname'])
                self.logger.info("UNKNOWN ERROR. Operator should check if it is permanent, but for now we go ahead and submit a task.")
                return 1
        return 0


    def execute(self, *args, **kw):
        """
        Main execute
        """
        self.task = kw['task']
        # Do not check it for HC
        # ActivitiesToRunEverywhere is used mainly for HC and there is no need to check for it.
        if hasattr(self.config.TaskWorker, 'ActivitiesToRunEverywhere') and \
                   self.task['tm_activity'] in self.config.TaskWorker.ActivitiesToRunEverywhere:
            self.logger.info("Will not check possibility to write to destination site because activity: %s is in ActivitiesToRunEverywhere", self.task['tm_activity'])
            return
        # If user specified no output and no logs transfer, there is also no need to check it.
        if self.task['tm_save_logs'] == 'F' and self.task['tm_transfer_outputs'] == 'F':
            self.logger.info("Will not check possibility to write to destination site because user specified not transfer any output/log files.")
            return
        # Do not need to check if it is dryrun.
        if self.task['tm_dry_run'] == 'T':
            self.logger.info("Will not check possibility to write to destination site. User specified dryrun option.")
            return
        self.workflow = self.task['tm_taskname']
        self.proxy = self.task['user_proxy']

        # In test machines this check is often only annoying
        if hasattr(self.config.TaskWorker, 'checkStageout') and not self.config.TaskWorker.checkStageout:
            self.logger.info("StageoutCheck disabled in this TaskWorker configuration. Skipping.")
            return

        # OK, we are interested in telling if output can be actually transferred to user destination
        # if user wants to user Rucio, we can only check quota, since transfer will be done
        # by Rucio robot without using user credentials
        if self.task['tm_output_lfn'].startswith('/store/user/rucio') or \
           self.task['tm_output_lfn'].startswith('/store/group/rucio'):
            rucioAccount = getRucioAccountFromLFN(self.task['tm_output_lfn'])
            self.logger.info(f"Checking Rucio quota from account {rucioAccount}.")
            quotaCheck = isEnoughRucioQuota(self.rucioClient, self.task['tm_asyncdest'], rucioAccount)
            if not quotaCheck['isEnough']:
                msg = f"Not enough Rucio quota at {self.task['tm_asyncdest']}:{self.task['tm_output_lfn']}."\
                      f" Remain quota: {quotaCheck['free']} GB."
                raise TaskWorkerException(msg)
            self.logger.info(f" Remain quota: {quotaCheck['free']} GB.")
            if quotaCheck['isQuotaWarning']:
                msg = 'Rucio Quota is very little and although CRAB will submit, stageout may fail.'
                self.logger.warning(msg)
                self.uploadWarning(msg, self.task['user_proxy'], self.task['tm_taskname'])

        # if not using Rucio, old code:
        else:
            cpCmd, rmCmd, append = getCheckWriteCommand(self.proxy, self.logger)
            if not cpCmd:
                self.logger.info("Can not check write permissions. No GFAL2 or LCG commands installed. Continuing")
                return
            self.logger.info("Will check stageout at %s", self.task['tm_asyncdest'])
            filename = re.sub("[:-_]", "", self.task['tm_taskname']) + '_crab3check.tmp'
            try:
                lfn = os.path.join(self.task['tm_output_lfn'], filename)
                # when checking the stageout, we are interested if we can use FTS for ASO.
                # therefore, we are interested in 'third_party_copy_write' only.
                # we should not get a PFN for 'write', which should be used for plain gfal.
                pfn = getWritePFN(self.rucioClient, siteName=self.task['tm_asyncdest'], lfn=lfn, operations=['third_party_copy_write'], logger=self.logger)
                cpCmd += append + os.path.abspath(filename) + " " + pfn
                rmCmd += " " + pfn
                createDummyFile(filename, self.logger)
                self.logger.info("Executing cp command: %s ", cpCmd)
                res = self.checkPermissions(cpCmd)
                if res == 0:
                    self.logger.info("Executing rm command: %s ", rmCmd)
                    self.checkPermissions(rmCmd)
            except IOError as er:
                raise TaskWorkerException("TaskWorker disk is full: %s" % er)
            finally:
                removeDummyFile(filename, self.logger)
            return
