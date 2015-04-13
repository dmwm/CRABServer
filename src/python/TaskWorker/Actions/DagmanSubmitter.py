
"""
Submit a DAG directory created by the DagmanCreator component.
"""

import os
import time
import base64
import random
import urllib
import traceback
import subprocess

import HTCondorUtils
import HTCondorLocator

import TaskWorker.Actions.TaskAction as TaskAction
import TaskWorker.DataObjects.Result as Result

from TaskWorker.Actions.DagmanCreator import CRAB_HEADERS
from TaskWorker.WorkerExceptions import TaskWorkerException

from ApmonIf import ApmonIf

# Bootstrap either the native module or the BossAir variant.
try:
    import classad
    import htcondor
except ImportError, _:
    #pylint: disable=C0103
    classad = None
    htcondor = None

CRAB_META_HEADERS = \
"""
+CRAB_SplitAlgo = %(splitalgo)s
+CRAB_AlgoArgs = %(algoargs)s
+CRAB_ConfigDoc = %(configdoc)s
+CRAB_DBSUrl = %(dbsurl)s
+CRAB_LumiMask = %(lumimask)s
+CRAB_Publish = %(publication)s
+CRAB_PublishDBSUrl = %(publishdbsurl)s
"""

# NOTE: Changes here must be synchronized with the submitDirect function below
MASTER_DAG_SUBMIT_FILE = CRAB_HEADERS + CRAB_META_HEADERS + \
"""
+CRAB_Attempt = 0
+CRAB_Workflow = %(workflow)s
+CRAB_UserDN = %(userdn)s
universe = local
# Can't ever remember if this is quotes or not
+CRAB_ReqName = "%(requestname)s"
scratch = %(scratch)s
bindir = %(bindir)s
output = $(scratch)/request.out
error = $(scratch)/request.err
executable = $(bindir)/dag_bootstrap_startup.sh
arguments = $(bindir)/master_dag
transfer_input_files = %(inputFilesString)s
transfer_output_files = %(outputFilesString)s
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
on_exit_remove = ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))
+OtherJobRemoveRequirements = DAGManJobId =?= ClusterId
remove_kill_sig = SIGUSR1
+HoldKillSig = "SIGUSR1"
on_exit_hold = (ExitCode =!= UNDEFINED && ExitCode != 0)
+Environment= strcat("PATH=/usr/bin:/bin:/opt/glidecondor/bin CRAB3_VERSION=3.3.0-pre1 CONDOR_ID=", ClusterId, ".", ProcId, " %(additional_environment_options)s")
+RemoteCondorSetup = "%(remote_condor_setup)s"
+TaskType = "ROOT"
X509UserProxy = %(user_proxy)s
queue 1
"""

SUBMIT_INFO = [ \
            ##----- These are the CRAB_HEADERS ---------
            ('CRAB_ReqName', 'requestname'),
            ('CRAB_Workflow', 'workflow'),
            ('CRAB_JobType', 'jobtype'),
            ('CRAB_JobSW', 'jobsw'),
            ('CRAB_JobArch', 'jobarch'),
            ('CRAB_InputData', 'inputdata'),
            ('CRAB_PublishName', 'publishname'),
            ('CRAB_ISB', 'cacheurl'),
            ('CRAB_SiteBlacklist', 'siteblacklist'),
            ('CRAB_SiteWhitelist', 'sitewhitelist'),
            ('CRAB_SaveLogsFlag', 'savelogsflag'),
            ('CRAB_AdditionalOutputFiles', 'addoutputfiles'),
            ('CRAB_EDMOutputFiles', 'edmoutfiles'),
            ('CRAB_TFileOutputFiles', 'tfileoutfiles'),
            ('CRAB_TransferOutputs', 'saveoutput'),
            ('CRAB_UserDN', 'userdn'),
            ('CRAB_UserHN', 'userhn'),
            ('CRAB_AsyncDest', 'asyncdest'),
            ('CRAB_BlacklistT1', 'blacklistT1'),
            #('CRAB_StageoutPolicy', 'stageoutpolicy'),
            ('CRAB_UserRole', 'tm_user_role'),
            ('CRAB_UserGroup', 'tm_user_group'),
            ('CRAB_TaskWorker', 'worker_name'),
            ('CRAB_RetryOnASOFailures', 'retry_aso'),
            ('CRAB_ASOTimeout', 'aso_timeout'),
            ('CRAB_RestHost', 'resthost'),
            ('CRAB_RestURInoAPI', 'resturinoapi'),
            ##----- These are the CRAB_META_HEADERS ------
            ('CRAB_SplitAlgo', 'splitalgo'),
            ('CRAB_AlgoArgs', 'algoargs'),
            ('CRAB_DBSUrl', 'dbsurl'),
            ('CRAB_LumiMask', 'lumimask'),
            ('CRAB_Publish', 'publication'),
            ('CRAB_PublishDBSUrl', 'publishdbsurl'),
            ##--------------------------------------------
            ('CRAB_JobCount', 'jobcount'),
            ('CRAB_UserVO', 'tm_user_vo'),
            ('RequestMemory', 'tm_maxmemory'),
            ('RequestCpus', 'tm_numcores'),
            ('MaxWallTimeMins', 'tm_maxjobruntime'),
            ('JobPrio', 'tm_priority'),
            ('CRAB_ASOURL', 'tm_asourl'),
            ('CRAB_FailedNodeLimit', 'faillimit'),
            ('CRAB_DashboardTaskType', 'taskType'),
            ('CRAB_MaxPost', 'maxpost')]


def addCRABInfoToClassAd(ad, info):
    """
    Given a submit ClassAd, add in the appropriate CRAB_* attributes
    from the info directory
    """
    for adName, dictName in SUBMIT_INFO:
        if dictName in info and (info[dictName] != None):
            ad[adName] = classad.ExprTree(str(info[dictName]))
    if 'extra_jdl' in info and info['extra_jdl']:
        for jdl in info['extra_jdl'].split('\n'):
            adName, adVal = jdl.lstrip('+').split('=')
            ad[adName] = adVal

class DagmanSubmitter(TaskAction.TaskAction):

    """
    Submit a DAG to a HTCondor schedd
    """

    def execute(self, *args, **kw):
        if self.config.TaskWorker.max_retry == 0:
            try:
                return self.executeInternal(*args, **kw)
            except Exception, e:
                msg = "Failed to submit task %s; '%s'" % (kw['task']['tm_taskname'], str(e))
                self.logger.error(msg)
                configreq = {'workflow': kw['task']['tm_taskname'],
                             'status': "FAILED",
                             'subresource': 'failure',
                             'failure': base64.b64encode(msg)}
                self.server.post(self.resturi, data = urllib.urlencode(configreq))
                raise
        retry_issues = []
        for retry in range(self.config.TaskWorker.max_retry):
            self.logger.debug("Trying to submit task %s %s time." % (kw['task']['tm_taskname'], str(retry)))
            exec_int = ""
            try:
                exec_int = self.executeInternal(*args, **kw)
                return exec_int
            except Exception, e:
                msg = "Failed to submit task %s; '%s'" % (kw['task']['tm_taskname'], str(e))
                self.logger.error(msg)
                retry_issues.append(msg)
                self.logger.error("Will retry in %s seconds." % str(self.config.TaskWorker.retry_interval[retry]))
                time.sleep(self.config.TaskWorker.retry_interval[retry])
        msg = "The CRAB3 server backend could not submit your jobs to the Grid scheduler. This could be a temporary glitch, please retry again later and contact"+\
              " the experts if the error persist. The submission was retried %s times, these are the failures: %s" % (len(retry_issues), str(retry_issues))
        self.logger.error(msg)
        raise TaskWorkerException(msg)

    def duplicateCheck(self, task):
        """
        Look to see if the task we are about to submit is already in the schedd.
        If so, assume that this task in TaskWorker was run successfully, but killed
        before it could update the frontend.
        """
        workflow = task['tm_taskname']

        if task['tm_collector']:
            self.backendurls['htcondorPool'] = task['tm_collector']
        loc = HTCondorLocator.HTCondorLocator(self.backendurls)

        # If tm_schedd exist, this means task is submitted with new crabserver version
        # TODO remove it later when no old tasks will be left
        address = ""
        schedd = ""
        if task['tm_schedd']:
            schedd, address = loc.getScheddObjNew(task['tm_schedd'])
        else:
            schedd, address = loc.getScheddObj(workflow) #TODO wrap this with a try/except. Copy from HTCondorDataWf

        rootConst = 'TaskType =?= "ROOT" && CRAB_ReqName =?= %s && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)' % HTCondorUtils.quote(workflow)

        results = list(schedd.xquery(rootConst, []))

        if not results:
            # Task not already in schedd
            return None

        configreq = {'workflow': workflow,
                     'status': "SUBMITTED",
                     'jobset': "-1",
                     'subresource': 'success',
                    }
        self.logger.warning("Task %s already submitted to HTCondor; pushing information centrally: %s" % (workflow, str(configreq)))
        data = urllib.urlencode(configreq)
        self.server.post(self.resturi, data = data)

        # Note that we don't re-send Dashboard jobs; we assume this is a rare occurrance and
        # don't want to upset any info already in the Dashboard.

        return Result.Result(task=task, result=(-1))


    def executeInternal(self, *args, **kw):

        if not htcondor:
            raise Exception("Unable to import HTCondor module")

        task = kw['task']
        workflow = task['tm_taskname']
        tempDir = args[0][0]
        info = args[0][1]
        #self.logger.debug("Task input information: %s" % str(info))
        dashboard_params = args[0][2]
        inputFiles = args[0][3]

        dup = self.duplicateCheck(task)
        if dup != None:
            return dup

        cwd = os.getcwd()
        os.chdir(tempDir)

        info['inputFilesString'] = ", ".join(inputFiles)
        outputFiles = ["RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001"]
        info['outputFilesString'] = ", ".join(outputFiles)
        arg = "RunJobs.dag"

        info['resthost'] = '"%s"' % (self.server['host'])
        #info['resthost'] = self.config.TaskWorker.resturl
        info['resturinoapi'] = '"%s"' % (self.restURInoAPI)

        try:
            info['remote_condor_setup'] = ''
            if task['tm_collector']:
                self.backendurls['htcondorPool'] = task['tm_collector']
            loc = HTCondorLocator.HTCondorLocator(self.backendurls)
            # If tm_schedd exist, this means task is submitted with new crabserver version
            # TODO remove it later when no old tasks will be left
            address = ""
            schedd = ""
            if task['tm_schedd']:
                schedd, address = loc.getScheddObjNew(task['tm_schedd'])
            else:
                schedd, address = loc.getScheddObj(workflow) #TODO wrap this with a try/except. Copy from HTCondorDataWf

            #try to gsissh in order to create the home directory (and check if we can connect to the schedd)
            try:
                scheddAddress = loc.scheddAd['Machine']
            except:
                raise TaskWorkerException("Unable to get schedd address for task %s" % (task['tm_taskname']))
            #try to connect
            if hasattr(self.config.MyProxy, 'uisource'):
                ret = subprocess.call(["sh","-c","export X509_USER_PROXY=%s; source %s; gsissh -o ConnectTimeout=60 -o PasswordAuthentication=no %s pwd" %\
                                                    (task['user_proxy'], self.config.MyProxy.uisource, scheddAddress)])
            else:
                ret = subprocess.call(["sh","-c","export X509_USER_PROXY=%s; gsissh -o ConnectTimeout=60 -o PasswordAuthentication=no %s pwd" %\
                                                    (task['user_proxy'], scheddAddress)])
            if ret:
                raise TaskWorkerException("Cannot gsissh to %s. Taskname %s" % (scheddAddress, task['tm_taskname']))

            if address:
                self.submitDirect(schedd, 'dag_bootstrap_startup.sh', arg, info)
            else:
                jdl = MASTER_DAG_SUBMIT_FILE % info
                schedd.submitRaw(task['tm_taskname'], jdl, task['user_proxy'], inputFiles)
        finally:
            os.chdir(cwd)

        configreq = {'workflow': kw['task']['tm_taskname'],
                     'status': "SUBMITTED",
                     'jobset': "-1",
                     'subresource': 'success',}
        self.logger.debug("Pushing information centrally %s" %(str(configreq)))
        data = urllib.urlencode(configreq)
        self.server.post(self.resturi, data = data)

        self.sendDashboardJobs(dashboard_params, info['apmon'])

        return Result.Result(task=kw['task'], result=(-1))

    def submitDirect(self, schedd, cmd, arg, info): #pylint: disable=R0201
        """
        Submit directly to the schedd using the HTCondor module
        """
        dagAd = classad.ClassAd()
        addCRABInfoToClassAd(dagAd, info)

        # Set default task attributes:
        if 'RequestMemory' not in dagAd:
            dagAd['RequestMemory'] = 2000
        if 'RequestCpus' not in dagAd:
            dagAd['RequestCpus'] = 1
        if 'MaxWallTimeMins' not in dagAd:
            dagAd['MaxWallTimeMins'] = 1315
        if 'JobPrio' not in dagAd:
            dagAd['JobPrio'] = 10

        # NOTE: Changes here must be synchronized with the job_submit in DagmanCreator.py in CAFTaskWorker
        dagAd["Out"] = str(os.path.join(info['scratch'], "request.out"))
        dagAd["Err"] = str(os.path.join(info['scratch'], "request.err"))
        dagAd["CRAB_Attempt"] = 0
        # We switched from local to scheduler universe.  Why?  It seems there's no way in the
        # local universe to change the hold signal at runtime.  That's fairly important for our
        # resubmit implementation.
        #dagAd["JobUniverse"] = 12
        dagAd["JobUniverse"] = 7
        dagAd["HoldKillSig"] = "SIGUSR1"
        dagAd["Cmd"] = cmd
        dagAd['Args'] = arg
        dagAd["TransferInput"] = str(info['inputFilesString'])
        dagAd["LeaveJobInQueue"] = classad.ExprTree("(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")
        dagAd["PeriodicRemove"] = classad.ExprTree("(JobStatus == 5) && (time()-EnteredCurrentStatus > 30*86400)")
        dagAd["TransferOutput"] = info['outputFilesString']
        dagAd["OnExitRemove"] = classad.ExprTree("( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))")
        dagAd["OtherJobRemoveRequirements"] = classad.ExprTree("DAGManJobId =?= ClusterId")
        dagAd["RemoveKillSig"] = "SIGUSR1"
        dagAd["OnExitHold"] = classad.ExprTree("(ExitCode =!= UNDEFINED && ExitCode != 0)")
        dagAd["Environment"] = classad.ExprTree('strcat("PATH=/usr/bin:/bin CRAB3_VERSION=3.3.0-pre1 CONDOR_ID=", ClusterId, ".", ProcId," %s")' % " ".join(info['additional_environment_options'].split(";")))
        dagAd["RemoteCondorSetup"] = info['remote_condor_setup']
        dagAd["Requirements"] = classad.ExprTree('true || false')
        dagAd["TaskType"] = "ROOT"
        dagAd["X509UserProxy"] = info['user_proxy']

        with HTCondorUtils.AuthenticatedSubprocess(info['user_proxy']) as (parent, rpipe):
            if not parent:
                resultAds = []
                schedd.submit(dagAd, 1, True, resultAds)
                schedd.spool(resultAds)
                if resultAds:
                    id = "%s.%s" % (resultAds[0]['ClusterId'], resultAds[0]['ProcId'])
                    schedd.edit([id], "LeaveJobInQueue", classad.ExprTree("(JobStatus == 4) && (time()-EnteredCurrentStatus < 30*86400)"))
        results = rpipe.read()
        if results != "OK":
            raise TaskWorkerException("Failure when submitting task to scheduler. Error reason: '%s'" % results)


    def sendDashboardJobs(self, params, info):
        apmon = ApmonIf()
        for job in info:
            job.update(params)
            self.logger.debug("Dashboard job info: %s" % str(job))
            apmon.sendToML(job)
        apmon.free()



