
"""
Submit a DAG directory created by the DagmanCreator component.
"""

import os
import time
import urllib

import HTCondorUtils
import CMSGroupMapper
import HTCondorLocator

from httplib import HTTPException
from ServerUtilities import FEEDBACKMAIL
import TaskWorker.DataObjects.Result as Result
import TaskWorker.Actions.TaskAction as TaskAction
from TaskWorker.Actions.DagmanCreator import CRAB_HEADERS
from TaskWorker.WorkerExceptions import TaskWorkerException

from ApmonIf import ApmonIf

from RESTInteractions import HTTPRequests

# Bootstrap either the native module or the BossAir variant.
try:
    import classad
    import htcondor
except ImportError:
    #pylint: disable=C0103
    classad = None
    htcondor = None


CRAB_META_HEADERS = \
"""
+CRAB_SplitAlgo = %(splitalgo)s
+CRAB_AlgoArgs = %(algoargs)s
+CRAB_ConfigDoc = %(configdoc)s
+CRAB_LumiMask = %(lumimask)s
"""
## This is the fragment to be used as the JDL in the schedd.submitRaw() method.
## NOTE: Changes here must be synchronized with the submitDirect() function below.
MASTER_DAG_SUBMIT_FILE = CRAB_HEADERS + CRAB_META_HEADERS + \
"""
+CRAB_Attempt = 0
universe = local
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

## These are the CRAB attributes that we want to add to the job class ad when
## using the submitDirect() method.
SUBMIT_INFO = [ \
    ## CRAB_HEADERS
    ('CRAB_ReqName', 'requestname'),
    ('CRAB_Workflow', 'workflow'),
    ('CRAB_JobType', 'jobtype'),
    ('CRAB_JobSW', 'jobsw'),
    ('CRAB_JobArch', 'jobarch'),
    ('DESIRED_CMSDataset', 'inputdata'),
    ('CRAB_DBSURL', 'dbsurl'),
    ('CRAB_PublishName', 'publishname'),
    ('CRAB_PublishGroupName', 'publishgroupname'),
    ('CRAB_Publish', 'publication'),
    ('CRAB_PublishDBSURL', 'publishdbsurl'),
    ('CRAB_ISB', 'cacheurl'),
    ('CRAB_SaveLogsFlag', 'savelogsflag'),
    ('CRAB_AdditionalOutputFiles', 'addoutputfiles'),
    ('CRAB_EDMOutputFiles', 'edmoutfiles'),
    ('CRAB_TFileOutputFiles', 'tfileoutfiles'),
    ('CRAB_TransferOutputs', 'saveoutput'),
    ('CRAB_UserDN', 'userdn'),
    ('CRAB_UserHN', 'userhn'),
    ('CRAB_AsyncDest', 'asyncdest'),
    #('CRAB_StageoutPolicy', 'stageoutpolicy'),
    ('CRAB_UserRole', 'tm_user_role'),
    ('CRAB_UserGroup', 'tm_user_group'),
    ('CRAB_TaskWorker', 'worker_name'),
    ('CRAB_RetryOnASOFailures', 'retry_aso'),
    ('CRAB_ASOTimeout', 'aso_timeout'),
    ('CRAB_RestHost', 'resthost'),
    ('CRAB_RestURInoAPI', 'resturinoapi'),
    ('CRAB_NumAutomJobRetries', 'numautomjobretries'),
    ## CRAB_META_HEADERS
    ('CRAB_SplitAlgo', 'splitalgo'),
    ('CRAB_AlgoArgs', 'algoargs'),
    ('CRAB_LumiMask', 'lumimask'),
    ## Additional CRAB attributes (since these are not part of CRAB_HEADERS or
    ## CRAB_META_HEADERS, they are not added by defaul to the class ad if using the
    ## schedd.submitRaw() method).
    ('CRAB_JobCount', 'jobcount'),
    ('CRAB_UserVO', 'tm_user_vo'),
    ('CRAB_SiteBlacklist', 'siteblacklist'),
    ('CRAB_SiteWhitelist', 'sitewhitelist'),
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

    def execute(self, *args, **kwargs):
        userServer = HTTPRequests(self.server['host'], kwargs['task']['user_proxy'], kwargs['task']['user_proxy'], retry=2, logger=self.logger)
        retryIssuesBySchedd = {}
        goodSchedulers = []
        try:
            goodSchedulers = self.server.get(self.restURInoAPI + '/info', data={'subresource': 'backendurls'})[0]['result'][0]['htcondorSchedds']
            goodSchedulers = list(set(goodSchedulers)) #we do not care about
        except HTTPException as hte:
            self.logger.error(hte.headers)
            self.logger.warning("Unable to contact cmsweb. Will use only on schedulers which was chosen by CRAB3 frontend.")
        self.logger.info("Good schedulers list got from crabserver: %s ", goodSchedulers)
        if kwargs['task']['tm_schedd'] not in goodSchedulers:
            self.logger.info("Scheduler which is chosen is not in crabserver output %s.", goodSchedulers)
            self.logger.info("No late binding of schedd. Will use %s for submission.", kwargs['task']['tm_schedd'])
            goodSchedulers = [kwargs['task']['tm_schedd']]
        else:
            #Make sure that first scheduler is used which is chosen by HTCondorLocator
            try:
                goodSchedulers.remove(kwargs['task']['tm_schedd'])
            except ValueError:
                pass
            goodSchedulers.insert(0, kwargs['task']['tm_schedd'])
        self.logger.info("Final good schedulers list after shuffle: %s ", goodSchedulers)

        #Check memory and walltime and if user requires too much:
        # upload warning back to crabserver
        # change walltime to max 47h Issue: #4742
        stdmaxjobruntime = 2750
        stdmaxmemory = 2500
        if kwargs['task']['tm_maxjobruntime'] > stdmaxjobruntime:
            msg = "Task requests %s minutes of runtime, but only %s minutes are guaranteed to be available." % (kwargs['task']['tm_maxjobruntime'], stdmaxjobruntime)
            msg += " Jobs may not find a site where to run."
            msg += " CRAB has changed this value to %s minutes." % (stdmaxjobruntime)
            self.logger.warning(msg)
            args[0][1]['tm_maxjobruntime'] = str(stdmaxjobruntime)
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
        if kwargs['task']['tm_maxmemory'] > stdmaxmemory:
            msg = "Task requests %s bytes of memory, but only %s bytes are guaranteed to be available." % (kwargs['task']['tm_maxmemory'], stdmaxmemory)
            msg += " Jobs may not find a site where to run and stay idle forever."
            self.logger.warning(msg)
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])

        for schedd in goodSchedulers:
            #If submission failure is true, trying to change a scheduler
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'subresource': 'updateschedd',
                         'scheddname': schedd}
            try:
                userServer.post(self.restURInoAPI + '/task', data=urllib.urlencode(configreq))
                kwargs['task']['tm_schedd'] = schedd
            except HTTPException as hte:
                msg = "Unable to contact cmsweb and update scheduler on which task will be submitted. Error msg: %s" % hte.headers
                self.logger.warning(msg)
                time.sleep(20)
                retryIssuesBySchedd[schedd] = [msg]
                continue
            retryIssues = []
            for retry in range(self.config.TaskWorker.max_retry + 1): #max_retry can be 0
                self.logger.debug("Trying to submit task %s %s time.", kwargs['task']['tm_taskname'], str(retry))
                try:
                    execInt = self.executeInternal(*args, **kwargs)
                    return execInt
                except Exception as ex:
                    msg = "Failed to submit task %s; '%s'"% (kwargs['task']['tm_taskname'], str(ex))
                    self.logger.exception(msg)
                    retryIssues.append(msg)
                    if retry < self.config.TaskWorker.max_retry: #do not sleep on the last retry
                        self.logger.error("Will retry in %s seconds.", self.config.TaskWorker.retry_interval[retry])
                        time.sleep(self.config.TaskWorker.retry_interval[retry])
            ## All the submission retries to the current schedd have failed. Record the
            ## failures.
            retryIssuesBySchedd[schedd] = retryIssues
        ## All the submission retries to all possible schedds have failed.
        msg = "The CRAB server backend was not able to submit the jobs to the Grid schedulers."
        msg += " This could be a temporary glitch. Please try again later."
        msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
        msg += " The submission was retried %s times on %s schedulers." % (sum(map(len, retryIssuesBySchedd.values())), len(retryIssuesBySchedd))
        msg += " These are the failures per Grid scheduler: %s" % (str(retryIssuesBySchedd))
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

        schedd = ""
        try:
            self.logger.debug("Duplicate check is getting the schedd obj. Collector is: %s", task['tm_collector'])
            schedd, _address = loc.getScheddObjNew(task['tm_schedd'])
            self.logger.debug("Got schedd obj for %s ", task['tm_schedd'])
        except Exception as exp:
            msg = "The CRAB server backend was not able to contact the Grid scheduler."
            msg += " Please try again later."
            msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
            msg += " Message from the scheduler: %s" % (str(exp))
            self.logger.exception("%s: %s", workflow, msg)
            raise TaskWorkerException(msg)

        rootConst = 'TaskType =?= "ROOT" && CRAB_ReqName =?= %s && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)' % HTCondorUtils.quote(workflow)

        self.logger.debug("Duplicate check is querying the schedd: %s", rootConst)
        results = list(schedd.xquery(rootConst, []))
        self.logger.debug("Schedd queried %s", results)

        if not results:
            # Task not already in schedd
            return None

        configreq = {'workflow': workflow,
                     'status': "SUBMITTED",
                     'jobset': "-1",
                     'subresource': 'success',
                    }
        self.logger.warning("Task %s already submitted to HTCondor; pushing information centrally: %s", workflow, str(configreq))
        data = urllib.urlencode(configreq)
        self.server.post(self.resturi, data=data)

        # Note that we don't re-send Dashboard jobs; we assume this is a rare occurrance and
        # don't want to upset any info already in the Dashboard.

        return Result.Result(task=task, result=(-1))


    def executeInternal(self, *args, **kwargs):
        """Internal execution to submit to selected scheduler
           Before submission it does duplicate check to see if
           task was not submitted by previous time"""
        if not htcondor:
            raise Exception("Unable to import HTCondor module")

        task = kwargs['task']
        workflow = task['tm_taskname']
        tempDir = args[0][0]
        info = args[0][1]
        #self.logger.debug("Task input information: %s" % str(info))
        dashboardParams = args[0][2]
        inputFiles = args[0][3]

        self.logger.debug("Starting duplicate check")
        dup = self.duplicateCheck(task)
        self.logger.debug("Duplicate check finished %s", dup)
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
            address = ""
            schedd = ""
            try:
                self.logger.debug("Getting schedd object")
                schedd, address = loc.getScheddObjNew(task['tm_schedd'])
                self.logger.debug("Got schedd object")
            except Exception as exp:
                msg = "The CRAB server backend was not able to contact the Grid scheduler."
                msg += " Please try again later."
                msg += " Message from the scheduler: %s" % (str(exp))
                self.logger.exception("%s: %s", workflow, msg)
                raise TaskWorkerException(msg)

            try:
                _address = loc.scheddAd['Machine']
            except:
                raise TaskWorkerException("Unable to get schedd address for task %s" % (task['tm_taskname']))

            # Get location of schedd-specific environment script from schedd ad.
            info['remote_condor_setup'] = loc.scheddAd.get("RemoteCondorSetup", "")

            self.logger.debug("Finally submitting to the schedd")
            if address:
                self.submitDirect(schedd, 'dag_bootstrap_startup.sh', arg, info)
            else:
                raise TaskWorkerException("Not able to get schedd address.")
            self.logger.debug("Submission finished")
        finally:
            os.chdir(cwd)

        configreq = {'workflow': kwargs['task']['tm_taskname'],
                     'status': "SUBMITTED",
                     'jobset': "-1",
                     'subresource': 'success',}
        self.logger.debug("Pushing information centrally %s", configreq)
        data = urllib.urlencode(configreq)
        self.server.post(self.resturi, data=data)

        self.sendDashboardJobs(dashboardParams, info['apmon'])

        return Result.Result(task=kwargs['task'], result=(-1))


    def submitDirect(self, schedd, cmd, arg, info): #pylint: disable=R0201
        """
        Submit directly to the schedd using the HTCondor module
        """
        dagAd = classad.ClassAd()
        addCRABInfoToClassAd(dagAd, info)

        groups = CMSGroupMapper.map_user_to_groups(dagAd["CRAB_UserHN"])
        if groups:
            dagAd["CMSGroups"] = groups

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
        """Send dashboard information about jobs of task"""
        apmon = ApmonIf()
        for job in info:
            job.update(params)
            self.logger.debug("Dashboard job info: %s", job)
            apmon.sendToML(job)
        apmon.free()



