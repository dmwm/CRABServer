
"""
Submit a DAG directory created by the DagmanCreator component.
"""

import os
import copy
import json
import time
import pickle
import urllib

from httplib import HTTPException

import HTCondorUtils
import CMSGroupMapper
import HTCondorLocator

from ServerUtilities import FEEDBACKMAIL
from ServerUtilities import TASKLIFETIME

import TaskWorker.DataObjects.Result as Result
import TaskWorker.Actions.TaskAction as TaskAction
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


## These are the CRAB attributes that we want to add to the job class ad when
## using the submitDirect() method.
SUBMIT_INFO = [ \
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
    ('CRAB_PrimaryDataset', 'primarydataset'),
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
    ('CRAB_SplitAlgo', 'splitalgo'),
    ('CRAB_AlgoArgs', 'algoargs'),
    ('CRAB_LumiMask', 'lumimask'),
    ('CRAB_JobCount', 'jobcount'),
    ('CRAB_UserVO', 'tm_user_vo'),
    ('CRAB_SiteBlacklist', 'siteblacklist'),
    ('CRAB_SiteWhitelist', 'sitewhitelist'),
    ('RequestMemory', 'tm_maxmemory'),
    ('RequestCpus', 'tm_numcores'),
    ('MaxWallTimeMins', 'tm_maxjobruntime'),
    ('JobPrio', 'tm_priority'),
    ('CRAB_ASOURL', 'tm_asourl'),
    ('CRAB_ASODB', 'tm_asodb'),
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
            adName, adVal = jdl.lstrip('+').split('=',1)
            ad[adName] = adVal


class ScheddStats(dict):
    def __init__(self):
        self.procnum = None
        self.schedd = None
        self.clusterId = None
        self.taskErrors = {}
        self.pid = os.getpid()
        dict.__init__(self)

    def success(self, schedd, clusterId):
        self[schedd]["successes"] = self.setdefault(schedd, {}).get("successes", 0) + 1
        #in case of success store schedd and clusterid so they can be printed
        self.schedd = schedd
        self.clusterId = clusterId

    def failure(self, schedd):
        self[schedd]["failures"] = self.setdefault(schedd, {}).get("failures", 0) + 1
        #in case of failure clear schedd and clusterId
        self.schedd = None
        self.clusterId = None

    def taskError(self, schedd, msg):
        self.taskErrors.setdefault(schedd, []).append(msg)

    def resetTaskInfo(self):
        self.taskErrors = {}

    def __str__(self):
        res = "Summary of schedd failures/successes for slave process %s (PID %s):\n" % (self.procnum, self.pid)
        for schedd in self:
            res += "\t" + schedd + ":\n"
            res += "\t\t%s %s\n" % ("Number of successes: ", self[schedd].get("successes", 0))
            res += "\t\t%s %s\n" % ("Number of failures: ", self[schedd].get("failures", 0))

        if self.schedd and self.clusterId:
            res += "Last successful submission: ClusterId %s submitted to schedd %s\n" % (self.clusterId, self.schedd)

        return res
scheddStats = ScheddStats()


class DagmanSubmitter(TaskAction.TaskAction):

    """
    Submit a DAG to a HTCondor schedd
    """

    def __init__(self, *args, **kwargs):
        TaskAction.TaskAction.__init__(self, *args, **kwargs)
        scheddStats.procnum = kwargs['procnum']
        scheddStats.resetTaskInfo()
        self.clusterId = None


    def sendScheddToREST(self, task, schedd):
        """ Try to set the schedd to the oracle database in the REST interface
            Raises TaskWorkerException in case of failure
        """
        task['tm_schedd'] = schedd
        userServer = HTTPRequests(self.server['host'], task['user_proxy'], task['user_proxy'], retry=20, logger=self.logger)
        configreq = {'workflow':task['tm_taskname'], 'subresource':'updateschedd',
            'scheddname':schedd}
        try:
            userServer.post(self.restURInoAPI + '/task', data=urllib.urlencode(configreq))
        except HTTPException as hte:
            msg = "Unable to contact cmsweb and update scheduler on which task will be submitted. Error msg: %s" % hte.headers
            self.logger.warning(msg)
            time.sleep(20)
            raise TaskWorkerException(msg) #we already tried 20 times, give up


    def checkMemoryWalltime(self, info, task):
        """ Check memory and walltime and if user requires too much:
            - upload warning back to crabserver
            - change walltime to max 47h Issue: #4742
        """

        stdmaxjobruntime = 2750
        stdmaxmemory = 2500
        if task['tm_maxjobruntime'] > stdmaxjobruntime:
            msg = "Task requests %s minutes of runtime, but only %s minutes are guaranteed to be available." % (task['tm_maxjobruntime'], stdmaxjobruntime)
            msg += " Jobs may not find a site where to run."
            msg += " CRAB has changed this value to %s minutes." % (stdmaxjobruntime)
            self.logger.warning(msg)
            info['tm_maxjobruntime'] = str(stdmaxjobruntime)
            self.uploadWarning(msg, task['user_proxy'], task['tm_taskname'])
        if task['tm_maxmemory'] > stdmaxmemory:
            msg = "Task requests %s MB of memory, but only %s MB are guaranteed to be available." % (task['tm_maxmemory'], stdmaxmemory)
            msg += " Jobs may not find a site where to run and stay idle forever."
            self.logger.warning(msg)
            self.uploadWarning(msg, task['user_proxy'], task['tm_taskname'])


    def pickAndSetSchedd(self, task):
        """ Pick up a schedd using the correct formula
            Send it to the REST

            If we can't send the schedd to the REST this is considered a permanent error

            Return the name of the schedd selected of None if there are no schedd left
        """
        self.logger.debug("Picking up the scheduler using %s", self.config.TaskWorker.scheddPickerFunction)
        #copy the list of schedd from REST external configuration. They are loaded when action is created
        restSchedulers = self.backendurls['htcondorSchedds']
        alreadyTriedSchedds = scheddStats.taskErrors.keys() #keys in the taskerrors are schedd

        currentBackendurls = copy.deepcopy(self.backendurls)
        currentBackendurls['htcondorSchedds'] = dict([(s,restSchedulers[s]) for s in restSchedulers if s not in alreadyTriedSchedds])
        if len(currentBackendurls['htcondorSchedds']) == 0:
            return None
        loc = HTCondorLocator.HTCondorLocator(currentBackendurls, self.logger)
        if hasattr(self.config.TaskWorker, 'scheddPickerFunction'):
            schedd = loc.getSchedd(chooserFunction=self.config.TaskWorker.scheddPickerFunction)
        else:
            schedd = loc.getSchedd() #uses the default memory stuff
        self.logger.debug("Finished picking up scheduler. Sending schedd (%s) to rest", schedd)
        self.sendScheddToREST(task, schedd)

        return schedd


    def execute(self, *args, **kwargs):
        """ Execute is the core method that submit the task to the schedd.
            The schedd can be defined in the tm_schedd task parameter if the user selected a schedd or it is a retry, otherwise it is empty.
                If it is empty the method will choose one
                If it contains a schedd it will do the duplicate check and try to submit the task to it
                In case of multiple failures is will set a new schedd and return back to the asction handler for retries.
        """
        task =  kwargs['task']
        schedd = task['tm_schedd']
        info = args[0][0]
        dashboardParams = args[0][1]
        inputFiles = args[0][2]

        self.checkMemoryWalltime(info, task)

        if not schedd:
            schedd = self.pickAndSetSchedd(task)

        self.logger.debug("Starting duplicate check")
        dupRes = self.duplicateCheck(task)
        self.logger.debug("Duplicate check finished with result %s", dupRes)
        if dupRes != None:
            return dupRes


        for retry in range(self.config.TaskWorker.max_retry + 1): #max_retry can be 0
            self.logger.debug("Trying to submit task %s to schedd %s for the %s time.", task['tm_taskname'], schedd, str(retry))
            try:
                execInt = self.executeInternal(info, dashboardParams, inputFiles, **kwargs)
                scheddStats.success(schedd, self.clusterId)
                return execInt
            except Exception as ex: #pylint: disable=broad-except
                scheddStats.failure(schedd)
                msg = "Failed to submit task %s; '%s'"% (task['tm_taskname'], str(ex))
                self.logger.exception(msg)
                scheddStats.taskError(schedd, msg)
                if retry < self.config.TaskWorker.max_retry: #do not sleep on the last retry
                    self.logger.error("Will retry in %s seconds on %s.", self.config.TaskWorker.retry_interval[retry], schedd)
                    time.sleep(self.config.TaskWorker.retry_interval[retry])
            finally:
                self.logger.info(scheddStats)
            ## All the submission retries to the current schedd have failed. Record the
            ## failures.

        ## Returning back to Handler.py for retries, and in case try on a new schedd
        self.logger.debug("Choosing a new schedd and then retrying")
        schedd = self.pickAndSetSchedd(task)

        ## All the submission retries to this schedd have failed.
        msg = "The CRAB server backend was not able to submit the jobs to the Grid schedulers."
        msg += " This could be a temporary glitch. Please try again later."
        msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
        msg += " The submission was retried %s times on %s schedulers." % (sum([len(x) for x in scheddStats.taskErrors.values()]), len(scheddStats.taskErrors))
        msg += " These are the failures per Grid scheduler: %s" % (str(scheddStats.taskErrors))

        raise TaskWorkerException(msg, retry=(schedd != None))


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
        oldx509 = os.environ.get("X509_USER_PROXY", None)
        try:
            self.logger.debug("Duplicate check is getting the schedd obj. Collector is: %s", task['tm_collector'])
            os.environ["X509_USER_PROXY"] = task['user_proxy']
            schedd, dummyAddress = loc.getScheddObjNew(task['tm_schedd'])
            self.logger.debug("Got schedd obj for %s ", task['tm_schedd'])

            rootConst = 'TaskType =?= "ROOT" && CRAB_ReqName =?= %s && (isUndefined(CRAB_Attempt) || '\
                        'CRAB_Attempt == 0)' % HTCondorUtils.quote(workflow.encode('ascii', 'ignore'))

            self.logger.debug("Duplicate check is querying the schedd: %s", rootConst)
            results = list(schedd.xquery(rootConst, []))
            self.logger.debug("Schedd queried %s", results)
        except Exception as exp:
            msg = "The CRAB server backend was not able to contact the Grid scheduler."
            msg += " Please try again later."
            msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
            msg += " Message from the scheduler: %s" % (str(exp))
            self.logger.exception("%s: %s", workflow, msg)
            raise TaskWorkerException(msg, retry=True)
        finally:
            if oldx509:
                os.environ["X509_USER_PROXY"] = oldx509
            else:
                del os.environ["X509_USER_PROXY"]

        if not results:
            # Task not already in schedd
            return None

        # Need to double check if JobStatus is 1(idle) or 2(running).
        # All other statuses means that task is not submitted or failed to be submitted.
        # There was issue with spooling files to scheduler and duplicate check found dagman on scheduler
        # but the filew were not correctly transferred.
        if results[0]['JobStatus'] not in [1,2]:
            # if the state of the dag is not idle or running then we raise exception and let
            # the dagman submitter retry later. hopefully after seven minutes the dag gets removed
            # from the schedd and the submission succeds
            retry = results[0]['JobStatus'] == 5 #5==Held
            msg = "Task %s already found on schedd %s " % (workflow, task['tm_schedd'])
            if retry:
                msg += "Going to retry submission later since the dag status is Held and the task should be removed on the schedd"
            else:
                msg += "Aborting submission since the task is in state %s" % results[0]['JobStatus']
            raise TaskWorkerException(msg, retry)
        else:
            self.logger.debug("Task seems to be submitted correctly. Classads got from scheduler: %s", results)

        configreq = {'workflow': workflow,
                     'status': "SUBMITTED",
                     'subresource': 'success',
                    }
        self.logger.warning("Task %s already submitted to HTCondor; pushing information centrally: %s", workflow, str(configreq))
        data = urllib.urlencode(configreq)
        self.server.post(self.resturi, data=data)

        # Note that we don't re-send Dashboard jobs; we assume this is a rare occurrance and
        # don't want to upset any info already in the Dashboard.

        return Result.Result(task=task, result=(-1))


    def executeInternal(self, info, dashboardParams, inputFiles, **kwargs):
        """Internal execution to submit to selected scheduler
           Before submission it does duplicate check to see if
           task was not submitted by previous time"""
        if not htcondor:
            raise Exception("Unable to import HTCondor module")

        task = kwargs['task']
        workflow = task['tm_taskname']

        cwd = os.getcwd()
        os.chdir(kwargs['tempDir'])

        info['start_time'] = task['tm_start_time']
        info['inputFilesString'] = ", ".join(inputFiles + ['subdag.ad'])
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
                raise TaskWorkerException(msg, retry=True)

            try:
                dummyAddress = loc.scheddAd['Machine']
            except:
                raise TaskWorkerException("Unable to get schedd address for task %s" % (task['tm_taskname']), retry=True)

            # Get location of schedd-specific environment script from schedd ad.
            info['remote_condor_setup'] = loc.scheddAd.get("RemoteCondorSetup", "")

            info["CMSGroups"] = set.union(CMSGroupMapper.map_user_to_groups(kwargs['task']['tm_username']), kwargs['task']['user_groups'])
            self.logger.info("User %s mapped to local groups %s.", kwargs['task']['tm_username'], info["CMSGroups"])

            self.logger.debug("Finally submitting to the schedd")
            if address:
                self.clusterId = self.submitDirect(schedd, 'dag_bootstrap_startup.sh', arg, info)
            else:
                raise TaskWorkerException("Not able to get schedd address.", retry=True)
            self.logger.debug("Submission finished")
        finally:
            os.chdir(cwd)

        configreq = {'workflow': kwargs['task']['tm_taskname'],
                     'status': "SUBMITTED",
                     'subresource': 'success',
                     'clusterid' : self.clusterId } #that's the condor cluster id of the dag (actually dag_bootstrap.sh that becomes that dag if everything goes well)
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

        if info["CMSGroups"]:
            dagAd["CMSGroups"] = ','.join(info["CMSGroups"])
        else:
            dagAd["CMSGroups"] = classad.Value.Undefined

        # NOTE: Changes here must be synchronized with the job_submit in DagmanCreator.py in CAFTaskWorker
        dagAd["CRAB_Attempt"] = 0
        # We switched from local to scheduler universe.  Why?  It seems there's no way in the
        # local universe to change the hold signal at runtime.  That's fairly important for our
        # resubmit implementation.
        #dagAd["JobUniverse"] = 12
        dagAd["JobUniverse"] = 7
        dagAd["HoldKillSig"] = "SIGUSR1"
        dagAd["X509UserProxy"] = info['user_proxy']
        dagAd["Requirements"] = classad.ExprTree('true || false')
        dagAd["TaskType"] = "ROOT"
        dagAd["Environment"] = classad.ExprTree('strcat("PATH=/usr/bin:/bin CRAB3_VERSION=3.3.0-pre1 CONDOR_ID=", ClusterId, ".", ProcId," %s")' % " ".join(info['additional_environment_options'].split(";")))
        dagAd["RemoteCondorSetup"] = info['remote_condor_setup']

        with open('subdag.ad' ,'w') as fd:
            for k, v in dagAd.items():
                if k == 'X509UserProxy':
                    v = os.path.basename(v)
                if isinstance(v, basestring):
                    value = classad.quote(v)
                elif isinstance(v, classad.ExprTree):
                    value = repr(v)
                elif isinstance(v, list):
                    value = "{{{0}}}".format(json.dumps(v)[1:-1])
                else:
                    value = v
                fd.write('+{0} = {1}\n'.format(k, value))

        dagAd["Out"] = str(os.path.join(info['scratch'], "request.out"))
        dagAd["Err"] = str(os.path.join(info['scratch'], "request.err"))
        dagAd["Cmd"] = cmd
        dagAd['Args'] = arg
        dagAd["TransferInput"] = str(info['inputFilesString'])
        dagAd["CRAB_TaskSubmitTime"] = classad.ExprTree("%s" % info["start_time"].encode('ascii', 'ignore'))
        # increasing the time for keeping the dagman into the queue with 10 days in order to
        # start a proper cleanup script after the task lifetime has expired
        extendedLifetime = TASKLIFETIME + 10*24*60*60
        # Putting JobStatus == 4 since LeaveJobInQueue is for completed jobs (probably redundant)
        LEAVE_JOB_IN_QUEUE_EXPR = "(JobStatus == 4) && ((time()-CRAB_TaskSubmitTime) < %s)" % extendedLifetime
        dagAd["LeaveJobInQueue"] = classad.ExprTree(LEAVE_JOB_IN_QUEUE_EXPR)
        # Removing a task after the expiration date no matter what its status is
        dagAd["PeriodicRemove"] = classad.ExprTree("((time()-CRAB_TaskSubmitTime) > %s)" % extendedLifetime)
        dagAd["TransferOutput"] = info['outputFilesString']
        dagAd["OnExitRemove"] = classad.ExprTree("( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))")
        dagAd["OtherJobRemoveRequirements"] = classad.ExprTree("DAGManJobId =?= ClusterId")
        dagAd["RemoveKillSig"] = "SIGUSR1"
        dagAd["OnExitHold"] = classad.ExprTree("(ExitCode =!= UNDEFINED && ExitCode != 0)")

        condorIdDict = {}
        with HTCondorUtils.AuthenticatedSubprocess(info['user_proxy'], pickleOut=True, outputObj=condorIdDict, logger=self.logger) as (parent, rpipe):
            if not parent:
                resultAds = []
                condorIdDict['ClusterId'] = schedd.submit(dagAd, 1, True, resultAds)
                schedd.spool(resultAds)
                # editing the LeaveJobInQueue since the remote submit overwrites it
                # see https://github.com/dmwm/CRABServer/pull/5212#issuecomment-216519749
                if resultAds:
                    id_ = "%s.%s" % (resultAds[0]['ClusterId'], resultAds[0]['ProcId'])
                    schedd.edit([id_], "LeaveJobInQueue", classad.ExprTree(LEAVE_JOB_IN_QUEUE_EXPR))

        try:
            results = pickle.load(rpipe)
        except EOFError:
            #Do not want to retry this since error may happen after submit (during edit for example).
            #And this can cause the task to be submitted twice (although we have a protection in the duplicatedCheck)
            raise TaskWorkerException("Timeout executing condor submit command.", retry=False)

        #notice that the clusterId might be set even if there was a failure. This is if the schedd.submit succeded, but the spool  call failed
        if 'ClusterId' in results.outputObj:
            self.logger.debug("Condor cluster ID just submitted is: %s", results.outputObj['ClusterId'])
        if results.outputMessage != "OK":
            self.logger.debug("Now printing the environment used for submission:\n" + "-"*70 + "\n" + results.environmentStr + "-"*70)
            raise TaskWorkerException("Failure when submitting task to scheduler. Error reason: '%s'" % results.outputMessage, retry=True)

        #if we don't raise exception above the id is here
        return results.outputObj['ClusterId']


    def sendDashboardJobs(self, params, info):
        """Send dashboard information about jobs of task"""
        apmon = ApmonIf()
        for job in info:
            job.update(params)
            apmon.sendToML(job)
        self.logger.debug("Dashboard job info submitted. Last job for example is: %s", job)
        apmon.free()



