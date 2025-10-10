
"""
Submit a DAG directory created by the DagmanCreator component.
"""

import os
import copy
import time

from http.client import HTTPException
from urllib.parse import urlencode

import HTCondorLocator

from ServerUtilities import FEEDBACKMAIL
from ServerUtilities import TASKLIFETIME
from ServerUtilities import MAX_MEMORY_PER_CORE, MAX_MEMORY_SINGLE_CORE, MAX_MEMORY_SINGLE_CORE_ON_RESUBMIT

from TaskWorker.DataObjects import Result
from TaskWorker.Actions import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException, SubmissionRefusedException

import htcondor2 as htcondor
import classad2 as classad


def addJobSubmitInfoToDagJobJDL(dagJdl, jobSubmit):
    """
    given an htondor.Submit objecty, add the appropriate information
    from the jobSubmit prepared by DagmanCreator
    """
    # These are the CRAB attributes that we want to add to the job class ad when
    # using the submitDirect() method. I.e. to submit the dagman boostrap job to the scheduler (AP)
    # These ads are the way to communivate variable values to the scripts which run in the scheduler,
    # Why classAds rather than e.g. a JSON or `setenv` file ? It started this way, and it stuck.
    adsToPort = [
        # used in bootstrap script (in other places too !) and useful to lookup things with condor_q
        'My.CRAB_RestHost',
        'My.CRAB_DbInstance',
        'My.CRAB_ReqName',
        'My.CRAB_Workflow',
        'My.CRAB_UserDN',
        'My.CRAB_UserHN',
        # these are used in Pre/Post scripts
        'My.CMS_JobType',
        'My.CRAB_JobSW',
        'My.CRAB_JobArch',
        'My.DESIRED_CMSDataset',
        'My.CRAB_MaxIdle',
        'My.CRAB_MaxPost',
        'My.CRAB_FailedNodeLimit',
        'My.CRAB_SaveLogsFlag',
        'My.CRAB_TransferOutputs',
        'My.CRAB_SiteBlacklist',
        'My.CRAB_SiteWhitelist',
        'My.CRAB_JobReleaseTimeout',
        'My.CRAB_RequestedMemory',
        'My.CRAB_RequestedCores',
        'My.MaxWallTimeMins',
        'My.MaxWallTimeMinsRun',
        'My.MaxWallTimeMinsProbe',
        'My.MaxWallTimeMinsTail',
        # these are used in PostJob only
        'My.CRAB_UserRole',
        'My.CRAB_UserGroup',
        'My.CRAB_DBSURL',
        'My.CRAB_PrimaryDataset',
        'My.CRAB_AdditionalOutputFiles',
        'My.CRAB_EDMOutputFiles',
        'My.CRAB_TFileOutputFiles',
        'My.CRAB_RetryOnASOFailures',
        'My.CRAB_AsyncDest',
        'My.CRAB_ASOTimeout',
        'My.CRAB_PublishName',
        'My.CRAB_Publish',
        # usefulness of these is to be be determined
        'My.CRAB_PublishDBSURL',  # not used anywhere in our code in GH
        'My.CRAB_TaskWorker',
        'My.CRAB_NumAutomJobRetries',
        'My.CRAB_SplitAlgo',
        'My.CRAB_AlgoArgs',
        'My.CRAB_LumiMask',
        'My.CRAB_JobCount',
        'My.CRAB_UserVO',
        'My.CRAB_DashboardTaskType',
        'My.CMS_Type',
        'My.CMS_WMTool',
        'My.CMS_TaskType',
    ]

    for adName in adsToPort:
        if adName in jobSubmit:
            dagJdl[adName] = jobSubmit[adName]


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


def checkMemoryWalltime(task, cmd, logger, warningUploader):
    """ Check memory and walltime and if user requires too much:
        - upload warning back to crabserver
        - change walltime to max 47h Issue: #4742
        NOTE: this is used also in DagmanResubmitter.
        Arguments:
            task : dictionary : the Task info from taskdb
            cmd: string : "tm" if called by D.Submitter, "resubmit" if callead by D.Resubmitter
            logger: logging object : the current logger
            warningUploader : should remove this as arg. and use CRABUtils.TaskUtils.uploadWarning
                                  must remove from TW/Actions/TaskAction and cleanup of TW/Actions code
    """

    stdmaxjobruntime = 2750
    runtime = task[cmd+'_maxjobruntime']
    memory = task[cmd+'_maxmemory']
    ncores = task['tm_numcores']
    if ncores is None:
        ncores = 1

    if cmd == 'tm':  # means this was called by DagmanSubmitter, not DagmanResubmitter
        absmaxmemory = max(MAX_MEMORY_SINGLE_CORE, ncores*MAX_MEMORY_PER_CORE)
    else:  # it is a resubmission
        absmaxmemory = max(MAX_MEMORY_SINGLE_CORE_ON_RESUBMIT, ncores*MAX_MEMORY_PER_CORE)
    if runtime is not None and runtime > stdmaxjobruntime:
        msg = f"Task requests {runtime} minutes of runtime, but only {stdmaxjobruntime} are guaranteed to be available."
        msg += " Jobs may not find a site where to run."
        msg += f" CRAB has changed this value to {stdmaxjobruntime} minutes."
        logger.warning(msg)
        if cmd == 'tm':  # means this was called by DagmanSubmitter, not DagmanResubmitter
            task['tm_maxjobruntime'] = str(stdmaxjobruntime)
        # somehow TaskAction/uploadWaning wants the user proxy to make a POST to task DB
        warningUploader(msg, task['user_proxy'], task['tm_taskname'])
    if memory is not None and memory > absmaxmemory:
        msg = f"Task requests {memory} MB of memory, above the allowed maximum of {absmaxmemory}"
        msg += f" for a {ncores} core(s) job.\nPlease see https://cmssi.docs.cern.ch/policies/memory/ \n"
        logger.error(msg)
        if cmd == 'resubmit':
            raise TaskWorkerException(msg)
        else:
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
            Set it in the task{} dictionary

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
        self.sendScheddToREST(task, schedd)  # this will raise if it fails
        task['tm_schedd'] = schedd

        return schedd


    def execute(self, *args, **kwargs):
        """ Execute is the core method that submit the task to the schedd.
            The schedd can be defined in the tm_schedd task parameter if the user selected a schedd or it is a retry, otherwise it is empty.
                If it is empty the method will choose one
                If it contains a schedd it will do the duplicate check and try to submit the task to it
                In case of multiple failures is will set a new schedd and return back to the asction handler for retries.
        """
        task = kwargs['task']
        schedName = task['tm_schedd']
        # jobSubmit from DagmanCreator is an htcondor.Submit() object with same content as Job.submit file
        # with the addition of : CRAB_MaxIdle, CRAB_MaxPost, CRAB_FailedNodeLimit which only make sense for DAG
        jobSubmit = args[0][0]
        # DagmanCreated also created the list of files to be transferred to the scheduler host
        filesForScheduler = args[0][2]

        cwd = os.getcwd()

        checkMemoryWalltime(task, 'tm', self.logger, self.uploadWarning)

        if not schedName:
            self.pickAndSetSchedd(task)
            schedName = task['tm_schedd']

        self.logger.debug("Starting duplicate check")
        dupRes = self.duplicateCheck(task)
        self.logger.debug("Duplicate check finished with result %s", dupRes)
        if dupRes is not None:
            return dupRes

        for retry in range(self.config.TaskWorker.max_retry + 1): #max_retry can be 0
            self.logger.debug("Try to submit %s to %s for the %s time.", task['tm_taskname'], schedName, str(retry+1))
            os.chdir(kwargs['tempDir'])
            try:
                execInt = self.executeInternal(jobSubmit, filesForScheduler, task)
                scheddStats.success(schedName, self.clusterId)
                os.chdir(cwd)
                return execInt
            except Exception as ex: #pylint: disable=broad-except
                scheddStats.failure(schedName)
                msg = f"Failed to submit task to: {schedName} . Task: {task['tm_taskname']};\n{ex}"
                self.logger.exception(msg)
                scheddStats.taskError(schedName, msg)
                if retry < self.config.TaskWorker.max_retry: #do not sleep on the last retry
                    self.logger.error("Will retry in %s seconds on %s.",
                                      self.config.TaskWorker.retry_interval[retry], schedName)
                    time.sleep(self.config.TaskWorker.retry_interval[retry])
            finally:
                self.logger.info(scheddStats)
                os.chdir(cwd)
            ## All the submission retries to the current schedd have failed. Record the
            ## failures.

        ## Returning back to Handler.py for retries, and in case try on a new schedd
        self.logger.debug("Choosing a new schedd and retry")
        self.pickAndSetSchedd(task)
        schedName = task['tm_schedd']

        ## All the submission retries to this schedd have failed.
        msg = "The CRAB server backend was not able to submit the jobs to the Grid schedulers."
        msg += " This could be a temporary glitch. Please try again later."
        msg += f" If the error persists send an e-mail to {FEEDBACKMAIL}."
        nTries = sum([len(x) for x in scheddStats.taskErrors.values()])
        nScheds = len(scheddStats.taskErrors)
        msg += f" The submission was retried {nTries} times on {nScheds} schedulers."
        msg += f" These are the failures per Grid scheduler:\n {scheddStats.taskErrors}"

        raise TaskWorkerException(msg, retry=schedName is not None)


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

            rootConst = f'CRAB_DAGType =?= "BASE" && CRAB_ReqName =?= {classad.quote(workflow)}'

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

        return Result.Result(task=task, result=-1)


    def executeInternal(self, jobSubmit, filesForScheduler, task):
        """Internal execution to submit to selected scheduler
           Before submission it does duplicate check to see if
           task was not submitted by previous time
           Arguments:
               jobSubmit : htcondor.Submit object: the content of Job.submit file
               filesForScheduler : list : list of files to be spooled for dagman job submission
               task : dictionary : the Task infor from taskdb
            Returns:
                if all ok , returns a TaskWorker.DataObjects.Result oject
                    Result(task=task, result='OK')
                otherwise, raise TaskWorkerException
            Side effects:
                HTC job is submitted to remote schedulere (AP) to execute dagman_bootstrap_startup.sh
                DAGJob.jdl file is written to tmp directory with the JDL used to submit that
                subdag.jdl file is written to tmp directory with the a JDL fragment to be included in DAG
                            description files by PreDag.py when creating subdags for automatic splitting
                            this cointains a (very large) subset of DAGJob.jdl
           """

        # start preparing the JDL which will be used for submitting the Dagman bootstrap job
        dagJobJDL = htcondor.Submit()
        addJobSubmitInfoToDagJobJDL(dagJobJDL, jobSubmit)  # start with the Job.submit from DagmanCreator

        dagJobJDL['My.CRAB_TaskSubmitTime'] = str(task['tm_start_time'])
        dagJobJDL['transfer_input_files'] = ", ".join(filesForScheduler + ['subdag.jdl'])
        outputFiles = ["RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001"]
        dagJobJDL['transfer_output_files'] = ", ".join(outputFiles)

        try:
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
                self.logger.exception("%s: %s", task['tm_taskname'], msg)
                raise TaskWorkerException(msg, retry=True) from exp

            try:
                dummyAddress = loc.scheddAd['Machine']
            except Exception as ex:
                msg = f"Unable to get schedd address for task {task['tm_taskname']}"
                raise TaskWorkerException(msg, retry=True) from ex

            # Get location of schedd-specific environment script from schedd ad.
            dagJobJDL['My.RemoteCondorSetup'] = loc.scheddAd.get("RemoteCondorSetup", "")

            self.logger.debug("Finally submitting to the schedd")
            if address:
                cmd = 'dag_bootstrap_startup.sh'
                arg = "RunJobs.dag"
                try:
                    self.clusterId = self.submitDirect(schedd, cmd, arg, dagJobJDL, task)
                except Exception as submissionError:
                    msg = f"Something went wrong: {submissionError} \n"
                    if self.clusterId:
                        msg += f"But a dagman_bootstrap was submitted with clusterId {self.clusterId}."
                    else:
                        msg += 'No clusterId was returned to DagmanSubmitter.'
                    msg += " Clean up condor queue before trying again."
                    self.logger.error(msg)
                    constrain = f"crab_reqname==\"{task['tm_taskname']}\""
                    constrain = str(constrain)  # beware unicode, it breaks htcondor binding
                    self.logger.error("Sending: condor_rm -constrain '%s'", constrain)
                    schedd.act(htcondor.JobAction.Remove, constrain)
                    # raise again to communicate failure upstream
                    raise submissionError from submissionError
            else:
                raise TaskWorkerException("Not able to get schedd address.", retry=True)
            self.logger.debug("Submission finished")
        finally:
            pass

        configreq = {'workflow': task['tm_taskname'],
                     'status': "SUBMITTED",
                     'subresource': 'success',
                     'clusterid' : self.clusterId} #that's the condor cluster id of the dag_bootstrap.sh
        self.logger.debug("Pushing information centrally %s", configreq)
        data = urlencode(configreq)
        self.crabserver.post(api='workflowdb', data=data)

        return Result.Result(task=task, result='OK')

    def submitDirect(self, schedd, cmd, arg, dagJobJDL, task):
        """
        Submit directly to the schedd using the HTCondor module
        """

        # NOTE: Changes here must be synchronized with the job_submit in DagmanCreator.py in CAFTaskWorker
        dagJobJDL["My.CMS_SubmissionTool"] = classad.quote("CRAB")
        # We switched from local to scheduler universe.  Why?  It seems there's no way in the
        # local universe to change the hold signal at runtime.  That's fairly important for our
        # resubmit implementation.
        dagJobJDL["JobUniverse"] = "7"
        dagJobJDL["HoldKillSig"] = "SIGUSR1"
        dagJobJDL["X509UserProxy"] = task['user_proxy']
        # submission command "priority" maps to jobAd "JobPrio" !
        dagJobJDL["priority"] = str(task['tm_priority'])
        dagJobJDL["Requirements"] = "TARGET.Cpus >= 1"  # see https://github.com/dmwm/CRABServer/issues/8456#issuecomment-2145887432
        dagJobJDL["Requirements"] = "True"
        environmentString = "PATH=/usr/bin:/bin CRAB3_VERSION=3.3.0-pre1"
        environmentString += " CONDOR_ID=$(ClusterId).$(ProcId)"
        environmentString += " CRAB_RUNTIME_TARBALL=local CRAB_TASKMANAGER_TARBALL=local"
        ##SB environmentString += " " + " ".join(task['additional_environment_options'].split(';'))
        # Environment command in JDL requires proper quotes https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html#environment
        dagJobJDL["Environment"] = classad.quote(environmentString)
        dagJobJDL['My.CRAB_TaskLifetimeDays'] = str(TASKLIFETIME // 24 // 60 // 60)
        dagJobJDL['My.CRAB_TaskEndTime'] = str(int(task['tm_start_time']) + TASKLIFETIME)
        #For task management info see https://github.com/dmwm/CRABServer/issues/4681#issuecomment-302336451
        dagJobJDL["LeaveJobInQueue"] = "True"
        dagJobJDL["PeriodicRemove"] = "time() > CRAB_TaskEndTime"
        dagJobJDL["OnExitHold"] = "(ExitCode =!= UNDEFINED && ExitCode != 0)"
        dagJobJDL["OnExitRemove"] = "( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))"
        # dagJobJDL["OtherJobRemoveRequirements"] = "DAGManJobId =?= ClusterId"  # appears unused SB
        dagJobJDL["RemoveKillSig"] = "SIGUSR1"

        # prepare a dagJobJDL fragment to be used when running in the scheduler
        # to create subdags for automatic splitting. A crucial change is location of the proxy
        subdagJDL = htcondor.Submit()  # submit object does not have a copy method
        for k,v in dagJobJDL.items():     # so we have to create a new object and
            subdagJDL[k] = v           # fill it one element at a time
        subdagJDL['X509UserProxy'] = os.path.basename(dagJobJDL['X509UserProxy'])  # proxy in scheduler will be in cwd

        # make sure that there is no "queue" statement in subdagJDL "jdl fragment" (introduced in v2 HTC bindings)
        # since condor_submit_dag will add one anyhow
        subdag = str(subdagJDL)  # print subdagJDL into a string
        # strip "queue" from last line
        subdag = subdag.rstrip('queue\n')  # pylint: disable=bad-str-strip-call
        # save to the file
        with open('subdag.jdl', 'w', encoding='utf-8') as fd:
            print(subdag, file=fd)

        dagJobJDL["My.CRAB_DAGType"] = classad.quote("BASE")  # we want the ad value to be "BASE", not BASE
        dagJobJDL["output"] = os.path.join(task['scratch'], "request.out")
        dagJobJDL["error"] = os.path.join(task['scratch'], "request.err")
        dagJobJDL["Executable"] = cmd
        dagJobJDL['Arguments'] = arg

        # for debugging purpose
        with open('DAGJob.jdl', 'w', encoding='utf-8') as fd:
            print(dagJobJDL, file=fd)

        htcondor.param['DELEGATE_FULL_JOB_GSI_CREDENTIALS'] = 'true'
        htcondor.param['DELEGATE_JOB_GSI_CREDENTIALS_LIFETIME'] = '0'
        try:
            submitResult = schedd.submit(description=dagJobJDL, count=1, spool=True)
            clusterId = submitResult.cluster()
            schedd.spool(submitResult)

        except Exception as hte:
            raise TaskWorkerException(f"Submission failed with:\n{hte}") from hte

        self.logger.debug("Condor cluster ID returned from submit is: %s", clusterId)

        return clusterId
