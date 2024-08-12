
"""
Submit a DAG directory created by the DagmanCreator component.
"""

import os
import copy
import time
import sys

from http.client import HTTPException

import classad
import htcondor

import CMSGroupMapper
import HTCondorLocator

from ServerUtilities import FEEDBACKMAIL
from ServerUtilities import TASKLIFETIME
from ServerUtilities import MAX_MEMORY_PER_CORE, MAX_MEMORY_SINGLE_CORE

from TaskWorker.DataObjects import Result
from TaskWorker.Actions import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException, SubmissionRefusedException

if sys.version_info >= (3, 0):
    from urllib.parse import urlencode  # pylint: disable=no-name-in-module
if sys.version_info < (3, 0):
    from urllib import urlencode


## These are the CRAB attributes that we want to add to the job class ad when
## using the submitDirect() method.
# SB: do we really need all of this ? Most of them are in Job.submit created by
# DagmanCreator and are not needed to submit/run the DagMan.
SUBMIT_INFO = [ \
    ('+CRAB_ReqName', 'requestname'),
    ('+CRAB_Workflow', 'workflow'),
    ('+CMS_JobType', 'jobtype'),
    ('+CRAB_JobSW', 'jobsw'),
    ('+CRAB_JobArch', 'jobarch'),
    ('+DESIRED_CMSDataset', 'inputdata'),
    ('+CRAB_DBSURL', 'dbsurl'),
    ('+CRAB_PublishName', 'publishname'),
    ('+CRAB_Publish', 'publication'),
    ('+CRAB_PublishDBSURL', 'publishdbsurl'),
    ('+CRAB_PrimaryDataset', 'primarydataset'),
    ('+CRAB_ISB', 'cacheurl'),
    ('+CRAB_AdditionalOutputFiles', 'addoutputfiles'),
    ('+CRAB_EDMOutputFiles', 'edmoutfiles'),
    ('+CRAB_TFileOutputFiles', 'tfileoutfiles'),
    ('+CRAB_TransferOutputs', 'saveoutput'),
    ('+CRAB_SaveLogsFlag', 'savelogsflag'),
    ('+CRAB_UserDN', 'userdn'),
    ('+CRAB_UserHN', 'userhn'),
    ('+CRAB_AsyncDest', 'asyncdest'),
    #('+CRAB_StageoutPolicy', 'stageoutpolicy'),
    ('+CRAB_UserRole', 'tm_user_role'),
    ('+CRAB_UserGroup', 'tm_user_group'),
    ('+CRAB_TaskWorker', 'worker_name'),
    ('+CRAB_RetryOnASOFailures', 'retry_aso'),
    ('+CRAB_ASOTimeout', 'aso_timeout'),
    ('+CRAB_RestHost', 'resthost'),
    ('+CRAB_DbInstance', 'dbinstance'),
    ('+CRAB_NumAutomJobRetries', 'numautomjobretries'),
    ('+CRAB_SplitAlgo', 'splitalgo'),
    ('+CRAB_AlgoArgs', 'algoargs'),
    ('+CRAB_LumiMask', 'lumimask'),
    ('+CRAB_JobCount', 'jobcount'),
    ('+CRAB_UserVO', 'tm_user_vo'),
    ('+CRAB_SiteBlacklist', 'siteblacklist'),
    ('+CRAB_SiteWhitelist', 'sitewhitelist'),
    ('+CRAB_RequestedMemory', 'tm_maxmemory'),
    ('+CRAB_RequestedCores', 'tm_numcores'),
    ('+MaxWallTimeMins', 'tm_maxjobruntime'),
    ('+MaxWallTimeMinsRun', 'tm_maxjobruntime'),
    ('+MaxWallTimeMinsProbe', 'maxproberuntime'),
    ('+MaxWallTimeMinsTail', 'maxtailruntime'),
    ('+CRAB_FailedNodeLimit', 'faillimit'),
    ('+CRAB_DashboardTaskType', 'taskType'),
    ('+CRAB_MaxIdle', 'maxidle'),
    ('+CRAB_MaxPost', 'maxpost'),
    ('+CMS_Type', 'cms_type'),
    ('+CMS_WMTool', 'cms_wmtool'),
    ('+CMS_TaskType', 'cms_tasktype'),
              ]


def addCRABInfoToJobJDL(jdl, info):
    """
    given a submit objecty, add in the appropriate CRAB_& attributes
    from the info directory
    """
    for adName, dictName in SUBMIT_INFO:
        if dictName in info and info[dictName] is not None:
            jdl[adName] = str(info[dictName])
    # extra_jdl and accelerator_jdl are not listed in SUBMIT_INFO
    # and need ad-hoc handling, those are a string of `\n` separated k=v elements
    if 'extra_jdl' in info and info['extra_jdl']:
        for keyValue in info['extra_jdl'].split('\n'):
            adName, adVal = keyValue.split(sep='=', maxsplit=1)
            # remove any user-inserted spaces which would break schedd.submit #8420
            adName = adName.strip()
            adVal = adVal.strip()
            jdl[adName] = adVal
    if 'accelerator_jdl' in info and info['accelerator_jdl']:
        for keyValue in info['accelerator_jdl'].split('\n'):
            adName, adVal = keyValue.split(sep='=', maxsplit=1)
            # these are built in our code w/o extra spaces
            jdl[adName] = adVal


class ScheddStats(dict):
    """ collect statistics of submission success/failure at various schedulers """
    def __init__(self):
        self.procnum = None
        self.schedd = None
        self.clusterId = None
        self.taskErrors = {}
        self.pid = os.getpid()
        dict.__init__(self)

    def success(self, schedd, clusterId):
        """ doc string """
        self[schedd]["successes"] = self.setdefault(schedd, {}).get("successes", 0) + 1
        #in case of success store schedd and clusterid so they can be printed
        self.schedd = schedd
        self.clusterId = clusterId

    def failure(self, schedd):
        """ doc string """
        self[schedd]["failures"] = self.setdefault(schedd, {}).get("failures", 0) + 1
        #in case of failure clear schedd and clusterId
        self.schedd = None
        self.clusterId = None

    def taskError(self, schedd, msg):
        """ doc string """
        self.taskErrors.setdefault(schedd, []).append(msg)

    def resetTaskInfo(self):
        """ doc string """
        self.taskErrors = {}

    def __str__(self):
        """ formatter for cute printout """
        res = f"Summary of schedd failures/successes for slave process {self.procnum} (PID {self.pid}):\n"
        for schedd in self:
            res += "\t" + schedd + ":\n"
            res += f"\t\tNumber of successes: {self[schedd].get('successes', 0)}\n"
            res += f"\t\tNumber of failures:  {self[schedd].get('failures', 0)}\n"

        if self.schedd and self.clusterId:
            res += f"Last successful submission: ClusterId {self.clusterId} submitted to schedd {self.schedd}\n"

        return res
scheddStats = ScheddStats()


def checkMemoryWalltime(info, task, cmd, logger, warningUploader):
    """ Check memory and walltime and if user requires too much:
        - upload warning back to crabserver
        - change walltime to max 47h Issue: #4742
    """

    stdmaxjobruntime = 2750
    runtime = task[cmd+'_maxjobruntime']
    memory = task[cmd+'_maxmemory']
    ncores = task[cmd+'_numcores']
    if ncores is None:
        ncores = 1
    absmaxmemory = max(MAX_MEMORY_SINGLE_CORE, ncores*MAX_MEMORY_PER_CORE)
    if runtime is not None and runtime > stdmaxjobruntime:
        msg = f"Task requests {runtime} minutes of runtime, but only {stdmaxjobruntime} are guaranteed to be available."
        msg += " Jobs may not find a site where to run."
        msg += f" CRAB has changed this value to {stdmaxjobruntime} minutes."
        logger.warning(msg)
        if info is not None:
            info['tm_maxjobruntime'] = str(stdmaxjobruntime)
        # somehow TaskAction/uploadWaning wants the user proxy to make a POST to task DB
        warningUploader(msg, task['user_proxy'], task['tm_taskname'])
    if memory is not None and memory > absmaxmemory:
        msg = f"Task requests {memory} MB of memory, above the allowed maximum of {absmaxmemory}"
        msg += f" for a {ncores} core(s) job.\n"
        logger.error(msg)
        raise SubmissionRefusedException(msg)
    if memory is not None and memory > MAX_MEMORY_PER_CORE:
        if ncores is not None and ncores < 2:
            msg = f"Task requests {memory} MB of memory, but only {MAX_MEMORY_PER_CORE} are guaranteed to be available."
            msg += " Jobs may not find a site where to run and stay idle forever."
            logger.warning(msg)
            # somehow TaskAction/uploadWaning wants the user proxy to make a POST to task DB
            warningUploader(msg, task['user_proxy'], task['tm_taskname'])

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
        configreq = {'workflow':task['tm_taskname'],
                     'subresource':'updateschedd', 'scheddname':schedd}
        try:
            self.crabserver.post(api='task', data=urlencode(configreq))
        except HTTPException as hte:
            msg = f"Unable to contact cmsweb and update scheduler on which task will be submitted. Error msg: {hte.headers}"
            self.logger.warning(msg)
            time.sleep(20)
            raise TaskWorkerException(msg) from hte  # we already tried 20 times, give up


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
        currentBackendurls['htcondorSchedds'] = { k:v for (k,v) in restSchedulers.items() if k not in alreadyTriedSchedds}
        if not currentBackendurls['htcondorSchedds']:
            return None
        loc = HTCondorLocator.HTCondorLocator(currentBackendurls, self.logger)
        if hasattr(self.config.TaskWorker, 'scheddPickerFunction'):
            schedd = loc.getSchedd(chooserFunction=self.config.TaskWorker.scheddPickerFunction)
        else:
            schedd = loc.getSchedd() #uses the default memory stuff
        self.logger.debug("Finished picking up scheduler. Sending schedd name (%s) to REST", schedd)
        self.sendScheddToREST(task, schedd)

        return schedd


    def execute(self, *args, **kwargs):
        """ Execute is the core method that submit the task to the schedd.
            The schedd can be defined in the tm_schedd task parameter if the user selected a schedd or it is a retry, otherwise it is empty.
                If it is empty the method will choose one
                If it contains a schedd it will do the duplicate check and try to submit the task to it
                In case of multiple failures is will set a new schedd and return back to the asction handler for retries.
        """
        task = kwargs['task']
        schedd = task['tm_schedd']
        info = args[0][0]
        inputFiles = args[0][2]

        checkMemoryWalltime(info, task, 'tm', self.logger, self.uploadWarning)

        if not schedd:
            schedd = self.pickAndSetSchedd(task)

        self.logger.debug("Starting duplicate check")
        dupRes = self.duplicateCheck(task)
        self.logger.debug("Duplicate check finished with result %s", dupRes)
        if dupRes is not None:
            return dupRes


        for retry in range(self.config.TaskWorker.max_retry + 1): #max_retry can be 0
            self.logger.debug("Trying to submit task %s to schedd %s for the %s time.", task['tm_taskname'], schedd, str(retry))
            try:
                execInt = self.executeInternal(info, inputFiles, **kwargs)
                scheddStats.success(schedd, self.clusterId)
                return execInt
            except Exception as ex: #pylint: disable=broad-except
                scheddStats.failure(schedd)
                msg = f"Failed to submit task to: {schedd} . Task: {task['tm_taskname']};\n{ex}"
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
        msg += f" If the error persists send an e-mail to {FEEDBACKMAIL}."
        nTries = sum([len(x) for x in scheddStats.taskErrors.values()])
        nScheds = len(scheddStats.taskErrors)
        msg += f" The submission was retried {nTries} times on {nScheds} schedulers."
        msg += f" These are the failures per Grid scheduler:\n {scheddStats.taskErrors}"

        raise TaskWorkerException(msg, retry=(schedd is not None))


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

            rootConst = f'TaskType =?= "ROOT" && CRAB_ReqName =?= {classad.quote(workflow)}' \
                        '&& (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)'

            self.logger.debug("Duplicate check is querying the schedd: %s", rootConst)
            results = list(schedd.query(rootConst, []))
            self.logger.debug("Schedd queried %s", results)
        except Exception as exp:
            msg = "The CRAB server backend was not able to contact the Grid scheduler."
            msg += " Please try again later."
            msg += f" If the error persists send an e-mail to {FEEDBACKMAIL}."
            msg += f" Message from the scheduler: {exp}"
            self.logger.exception("%s: %s", workflow, msg)
            raise TaskWorkerException(msg, retry=True) from exp
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
        if results[0]['JobStatus'] not in [1, 2]:
            # if the state of the dag is not idle or running then we raise exception and let
            # the dagman submitter retry later. hopefully after seven minutes the dag gets removed
            # from the schedd and the submission succeds
            retry = results[0]['JobStatus'] == 5 #5==Held
            msg = f"Task {workflow} already found on schedd {task['tm_schedd']} "
            if retry:
                msg += "Going to retry submission later since the dag status is Held and the task should be removed on the schedd"
            else:
                msg += f"Aborting submission since the task is in state {results[0]['JobStatus']}"
            raise TaskWorkerException(msg, retry)
        self.logger.debug("Task seems to be submitted correctly. Classads got from scheduler: %s", results)

        configreq = {'workflow': workflow,
                     'status': "SUBMITTED",
                     'subresource': 'success',
                     'clusterid': results[0]['ClusterId']
                    }
        self.logger.warning("Task %s already submitted to HTCondor; pushing information centrally: %s", workflow, configreq)
        data = urlencode(configreq)
        self.crabserver.post(api='workflowdb', data=data)

        # Note that we don't re-send Dashboard jobs; we assume this is a rare occurrance and
        # don't want to upset any info already in the Dashboard.

        return Result.Result(task=task, result=(-1))


    def executeInternal(self, info, inputFiles, **kwargs):
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
        info['inputFilesString'] = ", ".join(inputFiles + ['subdag.jdl'])
        outputFiles = ["RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001"]
        info['outputFilesString'] = ", ".join(outputFiles)
        arg = "RunJobs.dag"

        # for uniformity with values prepared in DagmanCreator (in JSON format), add double quotes
        info['resthost'] = f"\"{self.crabserver.server['host']}\""
        info['dbinstance'] = f'"{self.crabserver.getDbInstance()}"'

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
                msg += f" Message from the scheduler: {exp}"
                self.logger.exception("%s: %s", workflow, msg)
                raise TaskWorkerException(msg, retry=True) from exp

            try:
                dummyAddress = loc.scheddAd['Machine']
            except Exception as ex:
                msg = f"Unable to get schedd address for task {task['tm_taskname']}"
                raise TaskWorkerException(msg, retry=True) from ex

            # Get location of schedd-specific environment script from schedd ad.
            info['remote_condor_setup'] = loc.scheddAd.get("RemoteCondorSetup", "")

            info["CMSGroups"] = set.union(CMSGroupMapper.map_user_to_groups(kwargs['task']['tm_username']), kwargs['task']['user_groups'])
            self.logger.info("User %s mapped to local groups %s.", kwargs['task']['tm_username'], info["CMSGroups"])
            if not info["CMSGroups"]:
                raise TaskWorkerException(f"CMSGroups can not be empty. Failing task {task['tm_taskname']}", retry=True)

            self.logger.debug("Finally submitting to the schedd")
            if address:
                try:
                    self.clusterId = self.submitDirect(schedd, 'dag_bootstrap_startup.sh', arg, info)
                except Exception as submissionError:
                    msg = f"Something went wrong: {submissionError} \n"
                    if self.clusterId:
                        msg += f"But a dagman_bootstrap was submitted with clusterId {self.clusterId}."
                    else:
                        msg += 'No clusterId was returned to DagmanSubmitter.'
                    msg += " Clean up condor queue before trying again."
                    self.logger.error(msg)
                    constrain = f"crab_reqname==\"{kwargs['task']['tm_taskname']}\""
                    constrain = str(constrain)  # beware unicode, it breaks htcondor binding
                    self.logger.error("Sending: condor_rm -constrain '%s'", constrain)
                    schedd.act(htcondor.JobAction.Remove, constrain)
                    # raise again to communicate failure upstream
                    raise submissionError from submissionError
            else:
                raise TaskWorkerException("Not able to get schedd address.", retry=True)
            self.logger.debug("Submission finished")
        finally:
            os.chdir(cwd)

        configreq = {'workflow': kwargs['task']['tm_taskname'],
                     'status': "SUBMITTED",
                     'subresource': 'success',
                     'clusterid' : self.clusterId} #that's the condor cluster id of the dag_bootstrap.sh
        self.logger.debug("Pushing information centrally %s", configreq)
        data = urlencode(configreq)
        self.crabserver.post(api='workflowdb', data=data)

        return Result.Result(task=kwargs['task'], result='OK')

    def submitDirect(self, schedd, cmd, arg, info): #pylint: disable=R0201
        """
        Submit directly to the schedd using the HTCondor module
        """
        jobJDL = htcondor.Submit()
        addCRABInfoToJobJDL(jobJDL, info)

        if info["CMSGroups"]:
            jobJDL["+CMSGroups"] = classad.quote(','.join(info["CMSGroups"]))
        else:
            jobJDL["+CMSGroups"] = classad.Value.Undefined

        # NOTE: Changes here must be synchronized with the job_submit in DagmanCreator.py in CAFTaskWorker
        jobJDL["+CRAB_Attempt"] = "0"
        jobJDL["+CMS_SubmissionTool"] = classad.quote("CRAB")
        # We switched from local to scheduler universe.  Why?  It seems there's no way in the
        # local universe to change the hold signal at runtime.  That's fairly important for our
        # resubmit implementation.
        #jobJDL["JobUniverse"] = "12"
        jobJDL["JobUniverse"] = "7"
        jobJDL["HoldKillSig"] = "SIGUSR1"
        jobJDL["X509UserProxy"] = str(info['user_proxy'])
        # submission command "priority" maps to jobAd "JobPrio" !
        jobJDL["priority"] = str(info['tm_priority'])
        jobJDL["Requirements"] = "TARGET.Cpus >= 1"  # see https://github.com/dmwm/CRABServer/issues/8456#issuecomment-2145887432
        jobJDL["Requirements"] = "True"
        environmentString = "PATH=/usr/bin:/bin CRAB3_VERSION=3.3.0-pre1"
        environmentString += " CONDOR_ID=$(ClusterId).$(ProcId)"
        environmentString += " " + " ".join(info['additional_environment_options'].split(';'))
        # Environment command in JDL requires proper quotes https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html#environment
        jobJDL["Environment"] = classad.quote(environmentString)
        jobJDL["+RemoteCondorSetup"] = classad.quote(info['remote_condor_setup'])
        jobJDL["+CRAB_TaskSubmitTime"] = str(info['start_time'])  # this is an int (seconds from epoch)
        jobJDL['+CRAB_TaskLifetimeDays'] = str(TASKLIFETIME // 24 // 60 // 60)
        jobJDL['+CRAB_TaskEndTime'] = str(int(info['start_time']) + TASKLIFETIME)
        #For task management info see https://github.com/dmwm/CRABServer/issues/4681#issuecomment-302336451
        jobJDL["LeaveJobInQueue"] = "True"
        jobJDL["PeriodicHold"] = "time() > CRAB_TaskEndTime"
        jobJDL["transfer_output_files"] = str(info['outputFilesString'])
        jobJDL["OnExitHold"] = "(ExitCode =!= UNDEFINED && ExitCode != 0)"
        jobJDL["OnExitRemove"] = "( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))"
        # jobJDL["OtherJobRemoveRequirements"] = "DAGManJobId =?= ClusterId"  # appears unused SB
        jobJDL["RemoveKillSig"] = "SIGUSR1"

        # prepare a jobJDL fragment to be used when running in the scheduler
        # to create subdags for automatic splitting. A crucial change is location of the proxy
        subdagJDL = htcondor.Submit()  # submit object does not have a copy method
        for k,v in jobJDL.items():     # so we have to create a new object and
            subdagJDL[k] = v           # fill it one element at a time
        subdagJDL['X509UserProxy'] = os.path.basename(jobJDL['X509UserProxy'])  # proxy in scheduler will be in cwd

        # make sure that there is no "queue" statement in subdagJDL "jdl fragment" (introduced in v2 HTC bindings)
        # since condor_submit_dag will add one anyhow
        subdag = str(subdagJDL)  # print subdagJDL into a string
        subdag = subdag.rstrip('queue\n')  # strip "queue" from last line
        # save to the file
        with open('subdag.jdl', 'w', encoding='utf-8') as fd:
            print(subdag, file=fd)

        jobJDL["+TaskType"] = classad.quote("ROOT")  # we want the ad value to be "ROOT", not ROOT
        jobJDL["output"] = os.path.join(info['scratch'], "request.out")
        jobJDL["error"] = os.path.join(info['scratch'], "request.err")
        jobJDL["Cmd"] = cmd
        jobJDL['Args'] = arg
        jobJDL["transfer_input_files"] = str(info['inputFilesString'])

        htcondor.param['DELEGATE_FULL_JOB_GSI_CREDENTIALS'] = 'true'
        htcondor.param['DELEGATE_JOB_GSI_CREDENTIALS_LIFETIME'] = '0'
        try:
            submitResult = schedd.submit(description=jobJDL, count=1, spool=True)
            clusterId = submitResult.cluster()
            numProcs = submitResult.num_procs()
            # firstProc = submitResult.first_proc()
            # htcId = f"{clusterId}.{firstProc}"  # out cluster has only 1 job
            # resultAds = submitResult.clusterad()
            myjobs = jobJDL.jobs(count=numProcs, clusterid=clusterId)
            schedd.spool(list(myjobs))
        except  htcondor.HTCondorException as hte:
            raise TaskWorkerException(f"Submission failed with:\n{hte}") from hte

        self.logger.debug("Condor cluster ID returned from submit is: %s", clusterId)

        return clusterId
