import os
import re
from httplib import HTTPException

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException

from ServerUtilities import isFailurePermanent
from ServerUtilities import getCheckWriteCommand, createDummyFile
from ServerUtilities import removeDummyFile, executeCommand
from ServerUtilities import tempSetLogLevel
import logging

class StageoutCheck(TaskAction):

    """
    Check if able to stageout dump file to destination site.
    Will not check this for ActivitiesToRunEverywhere - mainly HC usage or
    if stageout output and logs is false or
    if dryrun option True
    """
    def __init__(self, *args, **kwargs):
        self.task = None
        self.proxy = None
        self.workflow = None
        TaskAction.__init__(self, *args, **kwargs)

    def getPFN(self, lfnsaddprefix, filename, sitename):
        from WMCore.Services.Rucio.Rucio import Rucio
        rucio_config_dict = {
            "phedexCompatible": True,
            "auth_type": "x509", "ca_cert": self.config.Services.Rucio_caPath,
            "logger": self.logger,
            "creds": {"client_cert": self.config.TaskWorker.cmscert, "client_key": self.config.TaskWorker.cmskey}
        }

        self.logger.info("Initializing Rucio client")
        # WMCore is awfully verbose
        try:
            with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
                rucioClient = Rucio(
                    self.config.Services.Rucio_account,
                    hostUrl=self.config.Services.Rucio_host,
                    authUrl=self.config.Services.Rucio_authUrl,
                    configDict=rucio_config_dict
                )
            rucioClient.whoAmI()
        except HTTPException as errormsg:
            self.logger.info('Error: Failed to contact Rucio')
            self.logger.info('Result: %s\nStatus :%s\nURL :%s', errormsg.result, errormsg.status, errormsg.url)
            raise HTTPException(errormsg)

        lfn = os.path.join(lfnsaddprefix, filename)
        try:
            pfnDict = rucioClient.getPFN(sitename, lfn)
            pfn = pfnDict[lfn]
        except Exception as ex:
            msg = 'lfn2pfn resolution with Rucio failed for site: %s  LFN: %s' % (sitename, lfn)
            msg += ' with exception :\n%s' % str(ex)
            raise TaskWorkerException(msg)

        return pfn

    def checkPermissions(self, Cmd):
        """
        Execute command and in case of permanent issue, raise error
        If issue unknown, upload warning message and return 1
        Return 0 otherwise
        """
        self.logger.info("Executing command: %s ", Cmd)
        out, err, exitcode = executeCommand(Cmd)
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
        if self.task['tm_output_lfn'].startswith('/store/user/rucio'):
            # to be filled with actual quota check, for the time being.. just go
            return
        # if not using Rucio, old code:
        else:
            cpCmd, rmCmd, append = getCheckWriteCommand(self.proxy, self.logger)
            if not cpCmd:
                self.logger.info("CRAB3 is not configured to check write permissions. There is no GFAL2 or LCG commands installed. Continuing")
                return
            filename = re.sub("[:-_]", "", self.task['tm_taskname']) + '_crab3check.tmp'
            try:
                with tempSetLogLevel(logger=self.logger, level=logging.ERROR):
                    pfn = self.getPFN(self.task['tm_output_lfn'], filename, self.task['tm_asyncdest'])
                cpCmd += append + os.path.abspath(filename) + " " + pfn
                rmCmd += " " + pfn
                createDummyFile(filename, self.logger)
                self.logger.info("Executing cp command: %s ", cpCmd)
                res = self.checkPermissions(cpCmd)
                if res==0:
                    self.logger.info("Executing rm command: %s ", rmCmd)
                    self.checkPermissions(rmCmd)
            except IOError as er:
                self.logger.info('IOError %s. CRAB3 backend disk is full. Please report to experts. Task will not be submitted', er)
                raise
            except HTTPException as er:
                self.logger.warning("CRAB3 is not able to get pfn from PhEDEx. Error %s", er)
            finally:
                removeDummyFile(filename, self.logger)
            return
