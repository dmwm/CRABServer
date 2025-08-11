"""
Create a set of files for a DAG submission.

Generates the condor submit files and the master DAG.
"""
# pylint:  disable=invalid-name  # have a lot of snake_case varaibles here from "old times"
# also.. yeah.. this is a long and somehow complex file, but we are not going to break it now
# pylint: disable=too-many-locals, too-many-branches, too-many-statements, too-many-lines, too-many-arguments
# there just one very long line in the HTCondor JDL template
# pylint: disable=line-too-long

import os
import re
import json
import shutil
import pickle
import random
import tarfile
import hashlib
import tempfile
from ast import literal_eval
from urllib.parse import urlencode

from ServerUtilities import MAX_DISK_SPACE, MAX_IDLE_JOBS, MAX_POST_JOBS, TASKLIFETIME
from ServerUtilities import getLock, checkS3Object, getColumn, pythonListToClassAdExprTree

import TaskWorker.DataObjects.Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.Actions.Splitter import SplittingSummary
from TaskWorker.WorkerExceptions import TaskWorkerException, SubmissionRefusedException
from RucioUtils import getWritePFN
from CMSGroupMapper import get_egroup_users, map_user_to_groups

from WMCore import Lexicon
from WMCore.WMRuntime.Tools.Scram import ARCH_TO_OS, SCRAM_TO_ARCH

import htcondor2 as htcondor
import classad2 as classad

DAG_HEADER = """

NODE_STATUS_FILE node_state{nodestate} 120 ALWAYS-UPDATE

# NOTE: a file must be present, but 'noop' makes it not be read.
#FINAL FinalCleanup Job.1.submit NOOP
#SCRIPT PRE FinalCleanup dag_bootstrap.sh FINAL $DAG_STATUS $FAILED_COUNT {resthost}

"""


DAG_FRAGMENT = """
JOB Job{count} Job.{count}.submit
SCRIPT {prescriptDefer} PRE  Job{count} dag_bootstrap.sh PREJOB $RETRY {count} {taskname} {backend} {stage}
SCRIPT DEFER 4 1800 POST Job{count} dag_bootstrap.sh POSTJOB $JOBID $RETURN $RETRY $MAX_RETRIES {taskname} {count} {tempDest} {outputDest} cmsRun_{count}.log.tar.gz {stage} {remoteOutputFiles}
#PRE_SKIP Job{count} 3
RETRY Job{count} {maxretries} UNLESS-EXIT 2
VARS Job{count} count="{count}"
# following 3 classAds could possibly be moved to Job.submit but as they are job-dependent
# would need to be done in the PreJob... doing it here is a bit ugly, but simpler
VARS Job{count} My.CRAB_localOutputFiles="\\"{localOutputFiles}\\""
VARS Job{count} My.CRAB_DataBlock="\\"{block}\\""
VARS Job{count} My.CRAB_Destination="\\"{destination}\\""
ABORT-DAG-ON Job{count} 3
"""


SUBDAG_FRAGMENT = """
SUBDAG EXTERNAL Job{count}SubJobs RunJobs{count}.subdag NOOP
SCRIPT DEFER 4 300 PRE Job{count}SubJobs dag_bootstrap.sh PREDAG {stage} {completion} {count}
"""


SPLIT_ARG_MAP = {"Automatic": "minutes_per_job",
                 "LumiBased": "lumis_per_job",
                 "EventBased": "events_per_job",
                 "FileBased": "files_per_job",
                 "EventAwareLumiBased": "events_per_job",}


def getCreateTimestamp(taskname):
    """ name says it all """
    return "_".join(taskname.split(":")[:1])


def makeLFNPrefixes(task):
    """
    create LFN's for output files both on /store/temp and on final destination
    Once we don't care anymore about backward compatibility with crab server < 3.3.1511
    we can uncomment the 1st line below and remove the next 6 lines.
    """
    #primaryds = task['tm_primary_dataset']
    if task['tm_primary_dataset']:
        primaryds = task['tm_primary_dataset']
    elif task['tm_input_dataset']:
        primaryds = task['tm_input_dataset'].split('/')[1]
    else:
        primaryds = task['tm_publish_name'].rsplit('-', 1)[0]
    hash_input = task['tm_user_dn']
    if 'tm_user_group' in task and task['tm_user_group']:
        hash_input += "," + task['tm_user_group']
    if 'tm_user_role' in task and task['tm_user_role']:
        hash_input += "," + task['tm_user_role']
    lfn = task['tm_output_lfn']
    hash_input = hash_input.encode('utf-8')
    pset_hash = hashlib.sha1(hash_input).hexdigest()
    user = task['tm_username']
    tmp_user = f"{user}.{pset_hash}"
    publish_info = task['tm_publish_name'].rsplit('-', 1) #publish_info[0] is the publishname or the taskname
    timestamp = getCreateTimestamp(task['tm_taskname'])
    splitlfn = lfn.split('/')
    if splitlfn[2] == 'user':
        #join:                    /       store    /temp   /user  /mmascher.1234    /lfn          /GENSYM    /publishname     /120414_1634
        temp_dest = os.path.join('/', splitlfn[1], 'temp', 'user', tmp_user, *(splitlfn[4:] + [primaryds, publish_info[0], timestamp]))
    else:
        temp_dest = os.path.join('/', splitlfn[1], 'temp', 'user', tmp_user, *(splitlfn[3:] + [primaryds, publish_info[0], timestamp]))
    dest = os.path.join(lfn, primaryds, publish_info[0], timestamp)

    return temp_dest, dest


def validateLFNs(path, outputFiles):
    """
    validate against standard Lexicon the LFN's that this task will try to publish in DBS
    :param path: string: the path part of the LFN's, w/o the directory counter
    :param outputFiles: list of strings: the filenames to be published (w/o the jobId, i.e. out.root not out_1.root)
    :return: nothing if all OK. If LFN is not valid Lexicon raises an AssertionError exception
    """
    # fake values to get proper LFN length, actual numbers chance job by job
    jobId = '10000'       # current max is 10k jobs per task
    dirCounter = '0001'   # need to be same length as 'counter' used later in makeDagSpecs

    for origFile in outputFiles:
        nameParts = origFile.rsplit(".", 1)
        if len(nameParts) == 2:    # filename ends with .<something>, put jobId before the dot
            fileName = f"{nameParts[0]}_{jobId}.{nameParts[1]}"
        else:
            fileName = f"{origFile}_{jobId}"
        testLfn = os.path.join(path, dirCounter, fileName)
        Lexicon.lfn(testLfn)  # will raise if testLfn is not a valid lfn
        # since Lexicon does not have lenght check, do it manually here.
        if len(testLfn) > 500:
            msg = f"\nYour task specifies an output LFN {len(testLfn)}-char long "
            msg += "\n which exceeds maximum length of 500"
            msg += "\n and therefore can not be handled in our DataBase"
            raise SubmissionRefusedException(msg)


def validateUserLFNs(path, outputFiles):
    """
    validate against standard Lexicon a user-defined LFN which will not go in DBS, but still needs to be sane
    :param path: string: the path part of the LFN's, w/o the directory counter
    :param outputFiles: list of strings: the filenames to be published (w/o the jobId, i.e. out.root not out_1.root)
    :return: nothing if all OK. If LFN is not valid Lexicon raises an AssertionError exception
    """
    # fake values to get proper LFN length, actual numbers chance job by job
    jobId = '10000'       # current max is 10k jobs per task
    dirCounter = '0001'   # need to be same length as 'counter' used later in makeDagSpecs

    for origFile in outputFiles:
        nameParts = origFile.rsplit(".", 1)
        if len(nameParts) == 2:    # filename ends with .<something>, put jobId before the dot
            fileName = f"{nameParts[0]}_{jobId}.{nameParts[1]}"
        else:
            fileName = f"{origFile}_{jobId}"
        testLfn = os.path.join(path, dirCounter, fileName)
        Lexicon.userLfn(testLfn)  # will raise if testLfn is not a valid lfn
        # since Lexicon does not have lenght check, do it manually here.
        if len(testLfn) > 500:
            msg = f"\nYour task specifies an output LFN {len(testLfn)}-char long "
            msg += "\n which exceeds maximum length of 500"
            msg += "\n and therefore can not be handled in our DataBase"
            raise SubmissionRefusedException(msg)


def getLocation(default_name):
    """ Get the location of the runtime code (job wrapper, postjob, anything executed on the schedd
        and on the worker node)

        First check if the files are present in the current working directory (as it happens
        when DagmanCreator is run in the scheduler by PreDag for automatic splitting)
        Then check if CRABTASKWORKER_ROOT is in the environment and use that location (that viariable is
            set by the taskworker init script. In the prod source script we use "export CRABTASKWORKER_ROOT")
            which is the case when DagmanCreator runs in the TaskWorker container, where the build process
            places files to be transferred in the "data" directory
    """
    loc = default_name
    if not os.path.exists(loc):
        if 'CRABTASKWORKER_ROOT' in os.environ:
            for path in ['xdata', 'data']:
                fname = os.path.join(os.environ['CRABTASKWORKER_ROOT'], path, loc)
                if os.path.exists(fname):
                    return fname
        raise Exception(f"Unable to locate {loc}")  # pylint: disable=broad-exception-raised
    loc = os.path.abspath(loc)
    return loc


class DagmanCreator(TaskAction):
    """
    Given a task definition, create the corresponding DAG files for submission
    into HTCondor
    """

    def __init__(self, config, crabserver, procnum=-1, rucioClient=None):
        """ need a comment line here """
        TaskAction.__init__(self, config, crabserver, procnum)
        self.rucioClient = rucioClient
        self.runningInTW = crabserver is not None

    def populateGlideinMatching(self, task):
        """ actually simply set the required arch and microarch
            arguments:
                task : dictionary : the standard task info from taskdb
            returns:
                matchinfo : dictionary : keys:
                            required_arch (string)
                            required_minimum_microarch (int)
        """
        matchInfo = {}
        scram_arch = task['tm_job_arch']
        # required_arch, set default
        matchInfo['required_arch'] = "X86_64"
        # The following regex matches a scram arch into four groups
        # for example el9_amd64_gcc10 is matched as (el)(9)_(amd64)_(gcc10)
        # later, only the third group is returned, the one corresponding to the arch.
        m = re.match(r"([a-z]+)(\d+)_(\w+)_(\w+)", scram_arch)
        if m:
            _, _, arch, _ = m.groups()
            if arch not in SCRAM_TO_ARCH:
                msg = f"Job configured for non-supported ScramArch '{arch}'"
                raise SubmissionRefusedException(msg)
            matchInfo['required_arch'] = SCRAM_TO_ARCH.get(arch)

        # required minimum micro_arch may need to be handled differently in the future (arm, risc, ...)
        # and may need different classAd(s) in the JDL, so try to be general here
        # ref:  https://github.com/dmwm/WMCore/issues/12168#issuecomment-2539233761 and comments below
        min_micro_arch = task['tm_job_min_microarch']
        if not min_micro_arch:
            matchInfo['required_minimum_microarch'] = 2  # the current default for CMSSW
            return matchInfo
        if min_micro_arch == 'any':
            matchInfo['required_minimum_microarch'] = 0
            return matchInfo
        if min_micro_arch.startswith('x86-64-v'):
            matchInfo['required_minimum_microarch'] = int(min_micro_arch.split('v')[-1])
            return matchInfo
        self.logger.error(f"Not supported microarch: {min_micro_arch}. Ignore it")
        matchInfo['required_minimum_microarch'] = 0

        return matchInfo

    def getDashboardTaskType(self, task):
        """ Get the dashboard activity name for the task.
        """
        if task['tm_activity'] in (None, ''):
            return getattr(self.config.TaskWorker, 'dashboardTaskType', 'analysistest')
        return task['tm_activity']

    def isHammerCloud(self, task):
        " name says it all "
        return task['tm_activity'] and 'HC' in task['tm_activity'].upper()

    def setCMS_WMTool(self, task):
        " for reporting to MONIT "
        if self.isHammerCloud(task):
            WMTool = 'HammerCloud'
        else:
            WMTool = 'User'
        return WMTool

    def setCMS_TaskType(self, task):
        " for reporting to MONIT "
        if self.isHammerCloud(task):
            taskType = task['tm_activity']
        else:
            if task['tm_scriptexe']:
                taskType = 'script'
            else:
                taskType = 'cmsRun'
        return taskType

    def setCMS_Type(self, task):
        " for reporting to MONIT "
        if self.isHammerCloud(task):
            cms_type = 'Test'
        else:
            cms_type = 'Analysis'
        return cms_type

    def isGlobalBlacklistIgnored(self, kwargs):
        """ Determine wether the user wants to ignore the globalblacklist
        """

        return kwargs['task']['tm_ignore_global_blacklist'] == 'T'

    def makeJobSubmit(self, task):
        """
        Prepare an HTCondor Submit object which will serve as template for
         all job submissions. It will be persisted in Job.submiy JDL file and
         customized by PreJob.py for each job using info from DAG
        Differences between the jobs are taken care of in the makeDagSpecs and
         propagated to the scheduler via the DAG description files
        """

        jobSubmit = htcondor.Submit()

        if os.path.exists("Job.submit"):
            jobSubmit['jobcount'] = str(task['jobcount'])
            return jobSubmit

        # these are classAds that we want to be added to each grid job
        # in the assignement the RHS has to be a string which will be
        # internally resolved in the HTC python APIU to a "classAd primitieve type" i.e.
        # integer or double-quoted string. If we put a simple string like
        # ad = something, HTCondor will look for a variable named something.
        # Therefore 3 types are acceptable RHS here:
        #   - for int variables, use a simple str: jobSubmit[] = str(myInt)
        #   - for string variable, need to quote: jobSubmit[] = classad.quote(myString)
        #   - if we "type the literal", we can simply use: jobSubmit[]="3" for
        #      a classAd that should resolve to an int and jobSubmit[]='"sometext"'
        #      for a classAd that should resolve to a string. In this latter case
        #      we can also (and better) user jobSubmit[]=classad.quote('sometext')
        # Note that argument to classad.quote can only be string or None
        #  we prefer to use classad.quote to using f'"{myString}"' as it is more clear and robust
        #   e.g. it handles cases where myString contains '"' by inserting proper escaping

        jobSubmit['My.CRAB_Reqname'] = classad.quote(task['tm_taskname'])
        jobSubmit['My.CRAB_Workflow'] = classad.quote(task['tm_taskname'])
        jobSubmit['My.CMS_JobType'] = classad.quote('Analysis')
        jobSubmit['My.CRAB_JobSW'] = classad.quote(task['tm_job_sw'])
        jobSubmit['My.CRAB_JobArch'] = classad.quote(task['tm_job_arch'])
        # Note: next ad must always be 0 for probe jobs, this is taken care of in PreJob.py
        jobSubmit['My.CRAB_SaveLogsFlag'] = "1" if task['tm_save_logs'] == 'T' else "0"
        jobSubmit['My.CRAB_DBSURL'] = classad.quote(task['tm_dbs_url'])
        jobSubmit['My.CRAB_PostJobStatus'] = classad.quote("NOT RUN")
        jobSubmit['My.CRAB_PostJobLastUpdate'] = "0"
        jobSubmit['My.CRAB_PublishName'] = classad.quote(task['tm_publish_name'])
        jobSubmit['My.CRAB_Publish'] =  "1" if task['tm_publication'] == 'T' else "0"
        jobSubmit['My.CRAB_PublishDBSURL'] = classad.quote(task['tm_publish_dbs_url'])
        jobSubmit['My.CRAB_ISB'] = classad.quote(task['tm_cache_url'])


        # note about Lists
        # in the JDL everything is a string, we can't use the simple classAd[name]=somelist
        # but need the ExprTree format (what classAd.lookup() would return)
        jobSubmit['My.CRAB_SiteBlacklist'] = pythonListToClassAdExprTree(list(task['tm_site_blacklist']))
        jobSubmit['My.CRAB_SiteWhitelist'] =  pythonListToClassAdExprTree(list(task['tm_site_whitelist']))
        jobSubmit['My.CRAB_AdditionalOutputFiles'] = pythonListToClassAdExprTree(task['tm_outfiles'])
        jobSubmit['My.CRAB_EDMOutputFiles'] =  pythonListToClassAdExprTree(task['tm_edm_outfiles'])
        jobSubmit['My.CRAB_TFileOutputFiles'] = pythonListToClassAdExprTree(task['tm_outfiles'])

        jobSubmit['My.CRAB_UserDN'] = classad.quote(task['tm_user_dn'])
        jobSubmit['My.CRAB_UserHN'] = classad.quote(task['tm_username'])
        jobSubmit['My.CRAB_AsyncDest'] = classad.quote(task['tm_asyncdest'])
        jobSubmit['My.CRAB_StageoutPolicy'] = classad.quote(task['stageoutpolicy'])
        # for VOMS role and group, PostJob and RenewRemoteProxies code want the undefined value, not "
        userRole = task['tm_user_role']
        jobSubmit['My.CRAB_UserRole'] = classad.quote(userRole) if userRole else 'undefined'
        userGroup = task['tm_user_group']
        jobSubmit['My.CRAB_UserGroup'] = classad.quote(userGroup) if userGroup else 'undefined'
        jobSubmit['My.CRAB_TaskWorker'] = classad.quote(getattr(self.config.TaskWorker, 'name', 'unknown'))
        retry_aso = "1" if getattr(self.config.TaskWorker, 'retryOnASOFailures', True) else "0"
        jobSubmit['My.CRAB_RetryOnASOFailures'] = retry_aso
        if task['tm_output_lfn'].startswith('/store/user/rucio') or \
                task['tm_output_lfn'].startswith('/store/group/rucio'):
            jobSubmit['My.CRAB_ASOTimeout'] = str(2 * TASKLIFETIME)  # effectivley infinite
        else:
            jobSubmit['My.CRAB_ASOTimeout'] = str(getattr(self.config.TaskWorker, 'ASOTimeout', 0))
        jobSubmit['My.CRAB_RestHost'] = classad.quote(task['resthost'])
        jobSubmit['My.CRAB_DbInstance'] = classad.quote(task['dbinstance'])
        jobSubmit['My.CRAB_NumAutomJobRetries'] = str(task['numautomjobretries'])
        jobSubmit['My.CRAB_Id'] = classad.quote("$(count)")  # count macro will be defined in VARS line in the DAG file
        jobSubmit['My.CRAB_JobCount'] = str(task['jobcount'])
        temp_dest, dest = makeLFNPrefixes(task)
        jobSubmit['My.CRAB_OutTempLFNDir'] = classad.quote(temp_dest)
        jobSubmit['My.CRAB_OutLFNDir'] = classad.quote(dest)
        oneEventMode = "1" if task['tm_one_event_mode'] == 'T' else "0"
        jobSubmit['My.CRAB_oneEventMode'] = oneEventMode
        jobSubmit['My.CRAB_PrimaryDataset'] = classad.quote(task['tm_primary_dataset'])
        jobSubmit['My.CRAB_DAGType'] = classad.quote("Job")
        jobSubmit['My.CRAB_SubmitterIpAddr'] = classad.quote(task['tm_submitter_ip_addr'])
        jobSubmit['My.CRAB_TaskLifetimeDays'] = str(TASKLIFETIME // 24 // 60 // 60)
        jobSubmit['My.CRAB_TaskEndTime'] = str(int(task["tm_start_time"]) + TASKLIFETIME)
        jobSubmit['My.CRAB_SplitAlgo'] = classad.quote(task['tm_split_algo'])
        jobSubmit['My.CRAB_AlgoArgs'] = classad.quote(str(task['tm_split_args']))  # from dict to str before quoting
        jobSubmit['My.CMS_WMTool'] =  classad.quote(self.setCMS_WMTool(task))
        jobSubmit['My.CMS_TaskType'] = classad.quote(self.setCMS_TaskType(task))
        jobSubmit['My.CMS_SubmissionTool'] = classad.quote("CRAB")
        jobSubmit['My.CMS_Type'] = classad.quote(self.setCMS_Type(task))
        transferOutputs = "1" if task['tm_transfer_outputs'] == 'T' else "0" # Note: this must always be 0 for probe jobs, is taken care of in PreJob.py
        jobSubmit['My.CRAB_TransferOutputs'] = transferOutputs

        # These attributes help gWMS decide what platforms this job can run on; see https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsMatchArchitecture
        matchInfo = self.populateGlideinMatching(task)
        jobSubmit['My.REQUIRED_ARCH'] = classad.quote(matchInfo['required_arch'])
        jobSubmit['My.REQUIRED_MINIMUM_MICROARCH'] = str(matchInfo['required_minimum_microarch'])
        jobSubmit['My.DESIRED_CMSDataset'] = classad.quote(task['tm_input_dataset'])

        # extra requirements that user may set when submitting
        jobSubmit['My.CRAB_RequestedMemory'] = str(task['tm_maxmemory'])
        jobSubmit['My.CRAB_RequestedCores'] = str(task['tm_numcores'])
        jobSubmit['My.MaxWallTimeMins'] = str(task['tm_maxjobruntime'])  # this will be used in gWms matching
        jobSubmit['My.MaxWallTimeMinsRun'] = str(task['tm_maxjobruntime'])  # this will be used in PeriodicRemove
        ## Add group information (local groups in SITECONF via CMSGroupMapper, VOMS groups via task info in DB)
        groups = set.union(map_user_to_groups(task['tm_username']), task['user_groups'])
        groups = ','.join(groups)  # from the set {'g1','g2'...,'gN'} to the string 'g1,g2,..gN'
        jobSubmit['My.CMSGroups'] = classad.quote(groups)

        # do we really need this ?  i.e. or can replace it with GLIDEIN_CMSSite where it is used in the code ?
        jobSubmit['My.JOBGLIDEIN_CMSSite'] = classad.quote('$$([ifThenElse(GLIDEIN_CMSSite is undefined, "Unknown", GLIDEIN_CMSSite)])')

        #
        # now actual HTC Job Submission commands, here right hand side can be simply strings
        #

        egroups = getattr(self.config.TaskWorker, 'highPrioEgroups', [])
        if egroups and task['tm_username'] in self.getHighPrioUsers(egroups):
            jobSubmit['accounting_group'] = 'highprio'
        else:
            jobSubmit['accounting_group'] = 'analysis'
        jobSubmit['accounting_group_user'] = task['tm_username']

        jobSubmit['job_ad_information_attrs'] = "MATCH_EXP_JOBGLIDEIN_CMSSite, JOBGLIDEIN_CMSSite, RemoteSysCpu, RemoteUserCpu"

        # Recover job output and logs on eviction events; make sure they aren't spooled
        # This allows us to return stdout to users when they hit memory limits (which triggers PeriodicRemove).
        jobSubmit['WhenToTransferOutput'] = "ON_EXIT_OR_EVICT"
        # old code had this line in Job.submit, but I can't find it used nor documented anywhere
        # +SpoolOnEvict = false

        # Keep job in the queue upon completion long enough for the postJob to run,
        # allowing the monitoring script to fetch the postJob status and job exit-code updated by the postJob
        jobSubmit['LeaveJobInQueue'] = "ifThenElse((JobStatus=?=4 || JobStatus=?=3) " + \
                                        "&& (time() - EnteredCurrentStatus < 30 * 60*60), true, false)"

        jobSubmit['universe'] = "vanilla"
        jobSubmit['Executable'] = "gWMS-CMSRunAnalysis.sh"
        jobSubmit['Output'] = "job_out.$(count)"
        jobSubmit['Error'] = "job_err.$(count)"
        jobSubmit['Log'] = "job_log"

        # from https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html#submit-description-file-commands
        # The entire string representing the command line arguments is surrounded by double quote marks
        jobSubmit['Arguments'] = classad.quote("--jobId=$(count)")
        jobSubmit['transfer_output_files'] = "jobReport.json.$(count)"

        additional_input_file = ""
        additional_environment_options = ""
        if os.path.exists("CMSRunAnalysis.tar.gz"):
            additional_environment_options += ' CRAB_RUNTIME_TARBALL=local'
            additional_input_file += ", CMSRunAnalysis.tar.gz"
        else:
            raise TaskWorkerException(f"Cannot find CMSRunAnalysis.tar.gz inside the cwd: {os.getcwd()}")
        if os.path.exists("TaskManagerRun.tar.gz"):
            additional_environment_options += ' CRAB_TASKMANAGER_TARBALL=local'
        else:
            raise TaskWorkerException(f"Cannot find TaskManagerRun.tar.gz inside the cwd: {os.getcwd()}")
        additional_input_file += ", sandbox.tar.gz"  # it will be present on SPOOL_DIR after dab_bootstrap
        additional_input_file += ", input_args.json"
        additional_input_file += ", run_and_lumis.tar.gz"
        additional_input_file += ", input_files.tar.gz"
        additional_input_file += ", submit_env.sh"
        additional_input_file += ", cmscp.sh"
        jobSubmit['transfer_input_files'] = f"CMSRunAnalysis.sh, cmscp.py{additional_input_file}"
        # make sure coredump (if any) is not added to output files ref: https://lists.cs.wisc.edu/archive/htcondor-users/2022-September/msg00052.shtml
        jobSubmit['coresize'] = "0"

        # we should fold this into the config file instead of hardcoding things.
        jobSubmit['Environment'] = classad.quote(f"SCRAM_ARCH=$(CRAB_JobArch){additional_environment_options}")
        jobSubmit['should_transfer_files'] = "YES"
        jobSubmit['use_x509userproxy'] = "true"

        arch = task['tm_job_arch'].split("_")[0]  # extracts "slc7" from "slc7_amd64_gcc10"
        required_os_list = ARCH_TO_OS.get(arch)
        if not required_os_list:
            raise SubmissionRefusedException(f"Unsupported architecture {arch}")
        # ARCH_TO_OS.get("slc7") gives a list with one item only: ['rhel7']
        jobSubmit['My.REQUIRED_OS'] = classad.quote(required_os_list[0])
        jobSubmit['Requirements'] = "stringListMember(TARGET.Arch, REQUIRED_ARCH)"

        # Ref: https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html#HoldReasonCode
        jobSubmit['periodic_release'] = "(HoldReasonCode == 28) || (HoldReasonCode == 30) " + \
                                         "|| (HoldReasonCode == 13) || (HoldReasonCode == 6)"

        # Remove if
        # a) job is in the 'held' status for more than 7 minutes
        # b) job is idle more than 7 days
        # c) job is running and one of:
        #    1) Over memory use
        #    2) Over wall clock limit
        #    3) Over disk usage of N GB, which is set in ServerUtilities
        # d) the taks EndTime has been reached
        # e) job is idle and users proxy expired 1 day ago.
        # (P.S. why 1 day ago? because there is recurring action which is updating user proxy and lifetime.)
        # ** If New periodic remove expression is added, also it should have Periodic Remove Reason. **
        # ** Otherwise message will not be clear and it is hard to debug **
        periodicRemove = "( (JobStatus =?= 5) && (time() - EnteredCurrentStatus > 7*60) )"  # a)
        periodicRemove += "|| ( (JobStatus =?= 1) && (time() - EnteredCurrentStatus > 7*24*60*60) )"  # b)
        periodicRemove += "|| ( (JobStatus =?= 2) && ( "  # c)
        periodicRemove += "(MemoryUsage =!= UNDEFINED && MemoryUsage > RequestMemory)"  # c) 1)
        periodicRemove += "|| (MaxWallTimeMinsRun * 60 < (time() - EnteredCurrentStatus))"  # c) 2)
        periodicRemove += f"|| (DiskUsage > {MAX_DISK_SPACE})"  # c) 3)
        periodicRemove += "))"  # these parentheses close the "if running" condition, i.e. JobStatus==2
        periodicRemove += "|| (time() > CRAB_TaskEndTime)"  # d)
        periodicRemove += "|| ( (JobStatus =?= 1) && (time() > (x509UserProxyExpiration + 86400)))"""  # e)
        jobSubmit['periodic_remove'] = periodicRemove

        # remove reasons are "ordered" in the following big IF starting from the less-conditial ones
        # order is relevant and getting it right is "an art"
        periodicRemoveReason = "ifThenElse("
        periodicRemoveReason += "((time() - EnteredCurrentStatus) > (7 * 24 * 60 * 60)) && isUndefined(MemoryUsage),"
        periodicRemoveReason += "\"Removed due to idle time limit\","  # set this reasons. Else
        periodicRemoveReason += "ifThenElse(time() > x509UserProxyExpiration, \"Removed job due to proxy expiration\","
        periodicRemoveReason += "ifThenElse(MemoryUsage > RequestMemory, \"Removed due to memory use\","
        periodicRemoveReason += "ifThenElse(MaxWallTimeMinsRun * 60 < (time() - EnteredCurrentStatus), \"Removed due to wall clock limit\","
        periodicRemoveReason += f"ifThenElse(DiskUsage > {MAX_DISK_SPACE}, \"Removed due to disk usage\","
        periodicRemoveReason += "ifThenElse(time() > CRAB_TaskEndTime, \"Removed due to reached CRAB_TaskEndTime\","
        periodicRemoveReason += "\"Removed due to job being held\"))))))"  # one closed ")" for each "ifThenElse("
        jobSubmit['My.PeriodicRemoveReason'] = periodicRemoveReason

        # tm_extrajdl contains a list of k=v assignements to be turned each into a classAds
        # also special handling is needed because is retrieved from DB not as a python list, but as a string
        # with format "['a=b','c=d'...]"  (a change in RESTWorkerWorkflow would be needed to get a python list
        # so we use here the same literal_eval trick which is used in RESTWorkerWorkflow.py )
        for extraJdl in literal_eval(task['tm_extrajdl']):
            name,value = extraJdl.split('=',1)
            if name.startswith('+'):
                name = name.replace('+', 'My.', 1)  # make sure JDL line starts with My. instead of +
            jobSubmit[name] = value

        if task['tm_user_config']['requireaccelerator']:
            # hardcoding accelerator to GPU (SI currently only have nvidia GPU)
            jobSubmit['My.RequiresGPU'] = "1"
            jobSubmit['request_GPUs'] = "1"
            if task['tm_user_config']['acceleratorparams']:
                gpuMemoryMB = task['tm_user_config']['acceleratorparams'].get('GPUMemoryMB', None)
                cudaCapabilities = task['tm_user_config']['acceleratorparams'].get('CUDACapabilities', None)
                cudaRuntime = task['tm_user_config']['acceleratorparams'].get('CUDARuntime', None)
                if gpuMemoryMB:
                    jobSubmit['My.GPUMemoryMB'] = classad.quote(gpuMemoryMB)
                if cudaCapabilities:
                    cudaCapability = ','.join(sorted(cudaCapabilities))
                    jobSubmit['My.CUDACapability'] = classad.quote(cudaCapability)
                if cudaRuntime:
                    jobSubmit['My.CUDARuntime'] = classad.quote(cudaRuntime)

        with open("Job.submit", "w", encoding='utf-8') as fd:
            print(jobSubmit, file=fd)

        return jobSubmit

    def getPreScriptDefer(self, task, jobid):
        """ Return the string to be used for deferring prejobs
            If the extrajdl CRAB_JobReleaseTimeout is not set in the client it returns
            an empty string, otherwise it return a string to defer the prejob by
            jobid * CRAB_JobReleaseTimeout seconds
        """
        slowJobRelease = False
        extrajdls = literal_eval(task['tm_extrajdl'])  # from "['a','b',...]" to a python list
        for ej in extrajdls:
            if ej.startswith('+'):
                ej = ej.replace('+', 'My.', 1)
            if ej.startswith('My.CRAB_JobReleaseTimeout'):
                slowJobRelease = True
                releaseTimeout = int(ej.split('=')[1])

        if slowJobRelease:
            prescriptDeferString = f"DEFER 4 {jobid * releaseTimeout}"
        else:
            prescriptDeferString = ''
        return prescriptDeferString

    def makeDagSpecs(self, task, siteinfo, jobgroup, block, availablesites, datasites,
                     outfiles, startjobid, parentDag=None, stage='conventional'):
        """
        prepares the information for the lines for a DAG description file
        as a list of `dagspec` objects. Each object is used to set value in the
        DAG_FRAGMENT string and to extract values for JSON object which passes
        arguments to CMSRunAnalysis.py (store in input_args.json), see prepareJobArguments below
        """
        dagSpecs = []
        i = startjobid
        temp_dest, dest = makeLFNPrefixes(task)
        try:
            # validate LFN's. Check both dest and temp_dest. See https://github.com/dmwm/CRABServer/issues/6871
            if task['tm_publication'] == 'T':
                validateLFNs(dest, outfiles)
                validateLFNs(temp_dest, outfiles)
            else:
                validateUserLFNs(dest, outfiles)
                validateUserLFNs(temp_dest, outfiles)
        except AssertionError as ex:
            msg = "\nYour task specifies an output LFN which fails validation in"
            msg += "\n WMCore/Lexicon and therefore can not be handled in our DataBase"
            msg += f"\nError detail: {ex}"
            raise SubmissionRefusedException(msg) from ex
        groupid = len(siteinfo['group_sites'])
        siteinfo['group_sites'][groupid] = list(availablesites)
        siteinfo['group_datasites'][groupid] = list(datasites)
        lastDirectDest = None
        lastDirectPfn = None
        for job in jobgroup.getJobs():
            if task['tm_use_parent'] == 1:
                inputFiles = json.dumps([
                    {
                        'lfn': inputfile['lfn'],
                        'parents': [{'lfn': parentfile} for parentfile in inputfile['parents']]
                    }
                    for inputfile in job['input_files']
                ])
            else:
                inputFiles = json.dumps([inputfile['lfn'] for inputfile in job['input_files']])
            runAndLumiMask = json.dumps(job['mask']['runAndLumis'])
            firstEvent = str(job['mask']['FirstEvent'])
            lastEvent = str(job['mask']['LastEvent'])
            firstLumi = str(job['mask']['FirstLumi'])
            firstRun = str(job['mask']['FirstRun'])
            i = int(i) + 1
            if parentDag is None or parentDag == "":
                count = str(i)
            else:
                count = f"{parentDag}-{i}"
            siteinfo[count] = groupid
            remoteOutputFiles = []
            localOutputFiles = []
            for origFile in outfiles:
                nameParts = origFile.rsplit(".", 1)
                if len(nameParts) == 2:  # filename ends with .<something>, put jobId before the dot
                    fileName = f"{nameParts[0]}_{count}.{nameParts[1]}"
                else:
                    fileName = f"{origFile}_{count}"
                remoteOutputFiles.append(fileName)
                localOutputFiles.append(f"{origFile}={fileName}")
            remoteOutputFilesStr = " ".join(remoteOutputFiles)
            localOutputFiles = ", ".join(localOutputFiles)
            counter = f"{(i // 1000):04d}"  # counter=0000 for i<999, 1 for 1000<i<1999 etc.
            tempDest = os.path.join(temp_dest, counter)
            directDest = os.path.join(dest, counter)
            if lastDirectDest != directDest:
                # Since we want to compute the PFN for the direct stageout,
                # we only care about 'write', which should be used with plain gfal.
                # there is no need to get the PFN for 'third_party_copy_write',
                # which should be used with FTS.
                lastDirectPfn = getWritePFN(self.rucioClient, siteName=task['tm_asyncdest'], lfn=directDest,
                                            operations=['write'], logger=self.logger)
                lastDirectDest = directDest
            pfns = [f"log/cmsRun_{count}.log.tar.gz"] + remoteOutputFiles
            pfns = ", ".join([f"{lastDirectPfn}/{pfn}" for pfn in pfns])
            prescriptDeferString = self.getPreScriptDefer(task, i)

            nodeSpec = {'count': count,
                        'prescriptDefer' : prescriptDeferString,
                        'maxretries': task['numautomjobretries'],
                        'taskname': task['tm_taskname'],
                        'backend': os.environ.get('HOSTNAME', ''),
                        'tempDest': tempDest,
                        'outputDest': os.path.join(dest, counter),
                        'remoteOutputFiles': remoteOutputFilesStr,
                        'runAndLumiMask': runAndLumiMask,
                        'inputFiles': inputFiles,
                        'localOutputFiles': localOutputFiles,
                        'asyncDest': task['tm_asyncdest'],
                        'firstEvent': firstEvent,
                        'lastEvent': lastEvent,
                        'firstLumi': firstLumi,
                        'firstRun': firstRun,
                        'seeding': 'AutomaticSeeding',
                        'lheInputFiles': 'tm_generator' in task and task['tm_generator'] == 'lhe',
                        'eventsPerLumi': task['tm_events_per_lumi'],
                        'maxRuntime': task['max_runtime'] * 60,  # the job script takes seconds, internal units are minutes
                        'sw': task['tm_job_sw'],
                        'block': block,
                        'destination': pfns,
                        'scriptExe': task['tm_scriptexe'],
                        'scriptArgs': json.dumps(task['tm_scriptargs']).replace('"', r'\"\"'),
                        'stage': stage,
                       }
            dagSpecs.append(nodeSpec)

        return dagSpecs, i

    def prepareJobArguments(self, dagSpecs, task):
        """ Prepare an object with all the input parameters of each jobs. It is a list
            with a dictionary for each job. The dictionary key/value pairs are the variables needed in CMSRunAnalysis.py
            This will be save in "input_args.json" by the caller, adding to existing list in ther if any
            Inputs:
                dagSpecs : list of dictionaries with information for each DAG job
                task: dictionary, the "standard" task dictionary with info from the DataBase TASK table
            Returns:
                argdicts : list of dictionaries, one per job, with the args needeed by CMSRunAnalysis.py
        """

        argdicts = []
        for dagspec in dagSpecs:
            argDict = {}
            argDict['inputFileList'] = f"job_input_file_list_{dagspec['count']}.txt"  #'job_input_file_list_1.txt'
            argDict['runAndLumis'] = f"job_lumis_{dagspec['count']}.json"
            # TODO: should find a way to use only CRAB_Id and get rid or jobNumber
            argDict['jobNumber'] = dagspec['count'] # '1' or '0-3' etc
            argDict['CRAB_Id'] = dagspec['count'] # '1' or '0-3' etc
            argDict['lheInputFiles'] = dagspec['lheInputFiles']  # False
            argDict['firstEvent'] = dagspec['firstEvent']  # 'None'
            argDict['lastEvent'] = dagspec['lastEvent']  # 'None'
            argDict['firstLumi'] = dagspec['firstLumi']  # 'None'
            argDict['firstRun'] = dagspec['firstRun']  # 'None'
            argDict['userSandbox'] = task['tm_user_sandbox']  #SB we could simply hardocode 'sandbox.tar.gz'
            argDict['cmsswVersion'] = task['tm_job_sw']  # 'CMSSW_9_2_5'
            argDict['scramArch'] = task['tm_job_arch']  # 'slc6_amd64_gcc530'
            argDict['seeding'] = 'AutomaticSeeding'
            argDict['scriptExe'] = task['tm_scriptexe']  #
            argDict['eventsPerLumi'] = task['tm_events_per_lumi']  #
            argDict['maxRuntime'] = dagspec['maxRuntime']  # -1
            argDict['scriptArgs'] = task['tm_scriptargs']

            # The following two are for fixing up job.submit files
            # SB argDict['CRAB_localOutputFiles'] = dagspec['localOutputFiles']
            # SB argDict['CRAB_Destination'] = dagspec['destination']
            argdicts.append(argDict)
        return argdicts

    def prepareTarballForSched(self, filesForSched, subdags):
        """ prepare a single "InputFiles.tar.gz" file with all the files to be moved
            from the TW to the schedd.
            This file will also be used by by the client preparelocal command.
        """

        with tarfile.open('InputFiles.tar.gz', mode='w:gz') as tf:
            for ifname in filesForSched + subdags:
                tf.add(ifname)

    def createSubdag(self, splitterResult, **kwargs):
        """ beware the "Sub" in the name ! This is used also for Main DAG
        Does the actual DAG file creation and writes out relevant files
        Handles both conventional tasks (only one DAG created in the TW) and
         automatic splitting (multiple subdags which will be added in the scheduler by
         the PreDag.py script which calls this DagmanCreator
        Returns:
            jobSubmit : HTCondor submit object : passes the Job.submit template to next action (DagmanSubmitter)
            splitterResult : object : this is the output of previous action (Splitter) and is part of input
                             arguments to DagmanCreator ! As far as Stefano can tell returning it
                             here is a "perverse" way to pass it also to DagmanSubmitter
            subdags : list : list of subdags files created which will need to be sent to scheduler
                               Stefano does not understans why it is needed since the subdags will be
                               overwritten by PreDag in the scheduler, maybe DAGMAN requires that the subdag
                               file indicated in the DAG exists, even if empty and eventually filled by the PreDag
                               e.g. the probe stage DAG ends with:
                                SUBDAG EXTERNAL Job0SubJobs RunJobs0.subdag NOOP
                                SCRIPT DEFER 4 300 PRE Job0SubJobs dag_bootstrap.sh PREDAG processing 5 0

        Side effects - writes these files to cwd:
          RunJobs.dag : the initial DAGMAN which will be submitted by DagmanSubmitter
          RunJobs{0,1,2,3}.subdag : the DAGMAN description for processing and tail subdags
          input_args.json : the arguments needed by CMSRunAnalysis.py for each job
          datadiscovery.pkl : the object returned in output from DataDiscovery action, PreDag will need it
                             in order to call Splitter and create the correct subdags
          taskinformation.pkl : the content of the task dictionary
          taskworkerconfig.pkl : the content of the TaskWorkerConfig object
          site.ad.json : sites assigned to jobs in each job group (info from Splitter)
        """

        # SB should have a createDagSpects() methos to move away a lot of following code

        task = kwargs['task']
        startjobid = kwargs.get('startjobid', 0)
        # parentDag below is only used during automatic splitting
        # possible values in each stage are:  probe: 0, tail: 1/2/3, processing: None
        parentDag = kwargs.get('parent', None)
        stage = kwargs.get('stage', 'conventional')
        self.logger.debug('starting createSubdag, kwargs are:')
        self.logger.debug(str(kwargs))
        dagSpecs = []  # there will be a dagSpec for every node to be written in the DAG file
        subdags = []  # in automatic splitting a DAG may contain a list of one or more subdags to start

        # disable direct stageout for rucio tasks
        if kwargs['task']['tm_output_lfn'].startswith('/store/user/rucio') or \
           kwargs['task']['tm_output_lfn'].startswith('/store/group/rucio'):
            kwargs['task']['stageoutpolicy'] = "local"
        elif hasattr(self.config.TaskWorker, 'stageoutPolicy'):
            kwargs['task']['stageoutpolicy'] = ",".join(self.config.TaskWorker.stageoutPolicy)
        else:
            kwargs['task']['stageoutpolicy'] = "local,remote"

        ## In the future this parameter may be set by the user in the CRAB configuration
        ## file and we would take it from the Task DB.
        kwargs['task']['numautomjobretries'] = getattr(self.config.TaskWorker, 'numAutomJobRetries', 2)

        runtime = kwargs['task']['tm_split_args'].get('minutes_per_job', -1)

        proberuntime = getattr(self.config.TaskWorker, 'automaticProbeRuntimeMins', 15)
        tailruntime = int(max(
            getattr(self.config.TaskWorker, 'automaticTailRuntimeMinimumMins', 45),
            getattr(self.config.TaskWorker, 'automaticTailRuntimeFraction', 0.2) * runtime
        ))

        overhead = getattr(self.config.TaskWorker, 'automaticProcessingOverheadMins', 60)

        kwargs['task']['max_runtime'] = runtime
        # include a factor of 4 as a buffer
        kwargs['task']['maxproberuntime'] = proberuntime * 4
        kwargs['task']['maxtailruntime'] = tailruntime * 5
        if kwargs['task']['tm_split_algo'] == 'Automatic':
            if stage == 'conventional':
                kwargs['task']['max_runtime'] = proberuntime
                outfiles = []
                stage = 'probe'
                parentDag = 0
            elif stage == 'processing':
                # include a buffer of one hour for overhead beyond the time
                # given to CMSSW
                kwargs['task']['tm_maxjobruntime'] = min(runtime + overhead, kwargs['task']['tm_maxjobruntime'])
            elif stage == 'tail':
                kwargs['task']['max_runtime'] = -1

        outfiles = kwargs['task']['tm_outfiles'] + kwargs['task']['tm_tfile_outfiles'] + kwargs['task']['tm_edm_outfiles']

        os.chmod("CMSRunAnalysis.sh", 0o755)

        # This config setting acts as a global black list
        global_blacklist = set(self.loadJSONFromFileInScratchDir('blacklistedSites.txt'))
        self.logger.debug("CRAB site blacklist: %s", list(global_blacklist))
        globalBlacklistUrl = self.config.Sites.DashboardURL

        # Get accleratorsites from GetAcceleratorSite recurring action.
        acceleratorsites = set(self.loadJSONFromFileInScratchDir('acceleratorSites.json'))
        self.logger.debug("Accelerator site from pilot pool: %s", list(acceleratorsites))

        # This is needed for Site Metrics
        # It should not block any site for Site Metrics and if needed for other activities
        # self.config.TaskWorker.ActivitiesToRunEverywhere = ['hctest', 'hcdev']
        # The other case where the blacklist is ignored is if the user sset this explicitly in his configuration
        if self.isGlobalBlacklistIgnored(kwargs) or (hasattr(self.config.TaskWorker, 'ActivitiesToRunEverywhere') and \
                   kwargs['task']['tm_activity'] in self.config.TaskWorker.ActivitiesToRunEverywhere):
            global_blacklist = set()
            self.logger.debug("Ignoring the CRAB site blacklist.")

        # Create site-ad and info.  DagmanCreator should only be run in
        # succession, never parallel for a task!
        if os.path.exists("site.ad.json"):
            with open("site.ad.json", encoding='utf-8') as fd:
                siteinfo = json.load(fd)
        else:
            siteinfo = {'group_sites': {}, 'group_datasites': {}}

        blocksWithNoLocations = set()
        blocksWithBannedLocations = set()
        allblocks = set()

        siteWhitelist = set(kwargs['task']['tm_site_whitelist'])
        siteBlacklist = set(kwargs['task']['tm_site_blacklist'])

        ignoreLocality = kwargs['task']['tm_ignore_locality'] == 'T'
        self.logger.debug("Ignore locality: %s", ignoreLocality)

        for jobgroup in splitterResult[0]:
            jobs = jobgroup.getJobs()

            jgblocks = set() #job group blocks
            for job in jobs:
                if stage == 'probe':
                    random.shuffle(job['input_files'])
                for inputfile in job['input_files']:
                    jgblocks.add(inputfile['block'])
                    allblocks.add(inputfile['block'])
            self.logger.debug("Blocks: %s", list(jgblocks))

            if not jobs:
                locations = set()
            else:
                locations = set(jobs[0]['input_files'][0]['locations'])
            self.logger.debug("Locations: %s", list(locations))

            ## Discard the jgblocks that have no locations. This can happen when a block is
            ## still open in PhEDEx. Newly created datasets from T0 (at least) have a large
            ## chance of having some block which is closed in DBS but not in PhEDEx.
            ## Open jgblocks in PhEDEx can have a location; it is WMCore who is returning no
            ## location.
            ## This is how a block is constructed during data taking:
            ## 1) an open block in T0 is injected in PhEDEx;
            ## 2) files are being added to the block in T0;
            ## 3) data are transferred by PhEDEx if a subscription is present;
            ## 4) once the block is finished:
            ##   a) the block is inserted into DBS as a closed block (before this, DBS has
            ##      no knowledge about the block);
            ##   b) block is closed in PhEDEx.
            if not locations and not ignoreLocality:
                blocksWithNoLocations = blocksWithNoLocations.union(jgblocks)
                continue

            if ignoreLocality:
                availablesites = set(kwargs['task']['all_possible_processing_sites'])
            else:
                availablesites = locations - global_blacklist

            ## At this point 'available sites' should never be empty.
            self.logger.debug("Available sites: %s", list(availablesites))

            # Special activities' white list overrides any other location
            if kwargs['task']['tm_activity'] in self.config.TaskWorker.ActivitiesToRunEverywhere and siteWhitelist:
                availablesites = siteWhitelist

            ## See https://github.com/dmwm/CRABServer/issues/5241
            ## for a discussion about blocksWithBannedLocations
            if not availablesites:
                blocksWithBannedLocations = blocksWithBannedLocations.union(jgblocks)
                continue

            # Intersect with sites that only have accelerator (currently we only have nvidia GPU)
            if kwargs['task']['tm_user_config']['requireaccelerator']:
                availablesites &= acceleratorsites
                if availablesites:
                    msg = "Site.requireAccelerator is True. CRAB will restrict sites to run the jobs to %s."
                    msg = msg % (list(availablesites), )
                    self.logger.warning(msg)
                    self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
                else:
                    blocksWithBannedLocations = blocksWithBannedLocations.union(jgblocks)
                    continue

            # NOTE: User can still shoot themselves in the foot with the resubmit blacklist
            # However, this is the last chance we have to warn the users about an impossible task at submit time.
            available = set(availablesites)
            if siteWhitelist:
                available &= siteWhitelist
                if not available:
                    msg = '%s block(s) %s present at %s will be skipped because those sites are not in user white list'
                    trimmedList = sorted(list(jgblocks))[:3] + ['...']
                    msg = msg % (len(list(jgblocks)), trimmedList, list(availablesites))
                    self.logger.warning(msg)
                    self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
                    blocksWithBannedLocations = blocksWithBannedLocations.union(jgblocks)
                    continue
            available -= (siteBlacklist - siteWhitelist)
            if not available:
                msg = '%s block(s) %s present at %s will be skipped because those sites are in user black list'
                trimmedList = sorted(list(jgblocks))[:3] + ['...']
                msg = msg % (len(list(jgblocks)), trimmedList, list(availablesites))
                self.logger.warning(msg)
                self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
                blocksWithBannedLocations = blocksWithBannedLocations.union(jgblocks)
                continue

            availablesites = [str(i) for i in availablesites]
            datasites = jobs[0]['input_files'][0]['locations']
            self.logger.info("Resulting available sites: %s", list(availablesites))

            if siteWhitelist or siteBlacklist:
                msg = "The site whitelist and blacklist will be applied by the pre-job."
                msg += f" This is expected to result in DESIRED_SITES = {list(available)}"
                self.logger.debug(msg)

            jobgroupDagSpecs, startjobid = self.makeDagSpecs(
                kwargs['task'], siteinfo, jobgroup, list(jgblocks)[0],
                availablesites, datasites, outfiles, startjobid,
                parentDag=parentDag, stage=stage)
            dagSpecs += jobgroupDagSpecs

        def getBlacklistMsg():
            tmp = ""
            if len(global_blacklist) != 0:
                t12 = [s for s in global_blacklist if 'T1' in s or 'T2' in s]
                t3 = [s for s in global_blacklist if 'T3' in s]
                tmp += f"\nGlobal blacklist contain these T1/T2: {t12} and {len(t3)} T3's\n"
                tmp += f" Full list at {globalBlacklistUrl}\n"
            if len(siteBlacklist) != 0:
                tmp += f" User blacklist is {list(siteBlacklist)}.\n"
            if len(siteWhitelist) != 0:
                tmp += f" User whitelist is {list(siteWhitelist)}.\n"
            return tmp

        if not dagSpecs:
            msg = f"No jobs created for task {kwargs['task']['tm_taskname']}."
            if blocksWithNoLocations or blocksWithBannedLocations:
                msg = "The CRAB server backend refuses to send jobs to the Grid scheduler. "
                msg += f"No locations found for dataset {kwargs['task']['tm_input_dataset']}. "
                msg += "(or at least for what passed the lumi-mask and/or run-range selection).\n"
                msg += "Use `crab checkdataset` command to find block locations\n"
                msg += "and compare with your black/white list\n"
            if blocksWithBannedLocations:
                msg += f" Found {len(blocksWithBannedLocations)} (out of {len(allblocks)}) blocks present\n"
                msg += " only at blacklisted not-whitelisted, and/or non-accelerator sites."
                msg += getBlacklistMsg()
            raise SubmissionRefusedException(msg)
        msg = f"Some blocks from dataset {kwargs['task']['tm_input_dataset']} were skipped"
        if blocksWithNoLocations:
            msgBlocklist = sorted(list(blocksWithNoLocations)[:5]) + ['...']
            msg += f" because they have no locations.\n List is (first 5 elements only): {msgBlocklist}\n"
        if blocksWithBannedLocations:
            msg += " because they are only present at blacklisted, not-whitelisted, and/or non-accelerator sites.\n"
            msgBlocklist = sorted(list(blocksWithBannedLocations)[:5]) + ['...']
            msg += f" List is (first 5 elements only): {msgBlocklist}\n"
            msg += getBlacklistMsg()
        if blocksWithNoLocations or blocksWithBannedLocations:
            msg += f" Dataset processing will be incomplete because {len(blocksWithNoLocations) + len(blocksWithBannedLocations)} (out of {len(allblocks)}) blocks"
            msg += " are only present at blacklisted and/or not whitelisted site(s)"
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
            self.logger.warning(msg)

        ## Write down the DAG as needed by DAGMan.
        restHostForSchedd = kwargs['task']['resthost']  # SB better to use self.crabserver.server['host']
        dag = DAG_HEADER.format(nodestate=f".{parentDag}" if parentDag else ('.0' if stage == 'processing' else ''),
                                resthost=restHostForSchedd)
        if stage == 'probe':
            dagSpecs = dagSpecs[:getattr(self.config.TaskWorker, 'numAutomaticProbes', 5)]
        for dagSpec in dagSpecs:
            dag += DAG_FRAGMENT.format(**dagSpec)
        if stage in ('probe', 'processing'):
            # default for probe DAG: only one processing DAG after 100% of the probe jobs have completed
            subdagCompletions = [100]
            nextStage = {'probe': 'processing', 'processing': 'tail'}[stage]

            if stage == 'processing' and len(dagSpecs) > getattr(self.config.TaskWorker, 'minAutomaticTailSize', 100):
                subdagCompletions = getattr(self.config.TaskWorker, 'minAutomaticTailTriggers', [50, 80, 100])

            for n, percent in enumerate(subdagCompletions, 0 if stage == 'probe' else 1):
                subdagSpec = {
                    'count': n,
                    'stage': nextStage,
                    'completion': (len(dagSpecs) * percent) // 100
                }
                dag += SUBDAG_FRAGMENT.format(**subdagSpec)
                subdag = f"RunJobs{subdagSpec['count']}.subdag"
                with open(subdag, "w", encoding='utf-8') as fd:
                    fd.write("")
                subdags.append(subdag)

        ## Create a tarball with all the job lumi files.
        # SB: I do not understand the "First iteration" comments here
        #     but am wary of changing, keep it as is for now
        with getLock('splitting_data'):
            self.logger.debug("Acquired lock on run and lumi tarball")

            try:
                tempDir = tempfile.mkdtemp()
                tempDir2 = tempfile.mkdtemp()

                try:
                    tfd = tarfile.open('run_and_lumis.tar.gz', 'r:gz')
                    tfd.extractall(tempDir)
                    tfd.close()
                except (tarfile.ReadError, IOError):
                    self.logger.debug("First iteration: creating run and lumi from scratch")
                try:
                    tfd2 = tarfile.open('input_files.tar.gz', 'r:gz')
                    tfd2.extractall(tempDir2)
                    tfd2.close()
                except (tarfile.ReadError, IOError):
                    self.logger.debug("First iteration: creating inputfiles from scratch")
                tfd = tarfile.open('run_and_lumis.tar.gz', 'w:gz')
                tfd2 = tarfile.open('input_files.tar.gz', 'w:gz')
                for dagSpec in dagSpecs:
                    job_lumis_file = os.path.join(tempDir, 'job_lumis_'+ str(dagSpec['count']) +'.json')
                    with open(job_lumis_file, "w", encoding='utf-8') as fd:
                        fd.write(str(dagSpec['runAndLumiMask']))
                    ## Also creating a tarball with the dataset input files.
                    ## Each .txt file in the tarball contains a list of dataset files to be used for the job.
                    job_input_file_list = os.path.join(tempDir2, 'job_input_file_list_'+ str(dagSpec['count']) +'.txt')
                    with open(job_input_file_list, "w", encoding='utf-8') as fd2:
                        fd2.write(str(dagSpec['inputFiles']))
            finally:
                tfd.add(tempDir, arcname='')
                tfd.close()
                shutil.rmtree(tempDir)
                tfd2.add(tempDir2, arcname='')
                tfd2.close()
                shutil.rmtree(tempDir2)

        # pick the name for the DAG file that we will create
        if stage in ('probe', 'conventional'):
            # this stages means that we run in TW, so let's also save as pickles
            # some data that will be needed to be able to run in the scheduler
            dagFileName = "RunJobs.dag"
            with open("datadiscovery.pkl", "wb") as fd:
                pickle.dump(splitterResult[1], fd)  # Cache data discovery
            with open("taskinformation.pkl", "wb") as fd:
                pickle.dump(kwargs['task'], fd)  # Cache task information
            with open("taskworkerconfig.pkl", "wb") as fd:
                pickle.dump(self.config, fd)  # Cache TaskWorker configuration
        elif stage == 'processing':
            dagFileName = "RunJobs0.subdag"
        else:
            dagFileName = f"RunJobs{parentDag}.subdag"
        argFileName = "input_args.json"

        ## Cache site information
        with open("site.ad.json", "w", encoding='utf-8') as fd:
            json.dump(siteinfo, fd)

        ## Save the DAG into a file.
        with open(dagFileName, "w", encoding='utf-8') as fd:
            fd.write(dag)

        kwargs['task']['jobcount'] = len(dagSpecs)

        jobSubmit = self.makeJobSubmit(kwargs['task'])

        # list of input arguments needed for each jobs
        argdicts = self.prepareJobArguments(dagSpecs, kwargs['task'])
        # save the input arguments to each job's CMSRunAnalysis.py in input_args.json file
        if stage in ['processing', 'tail']:  # add to argument list from previous stage
            with open(argFileName, 'r', encoding='utf-8') as fd:
                oldArgs = json.load(fd)
            argdicts = oldArgs + argdicts
        # no worry of overwriting, even in automatic splitting multiple DagmanCreator
        # is executed inside PreDag which is wrapped with a lock
        with open(argFileName, 'w', encoding='utf-8') as fd:
            json.dump(argdicts, fd)

        # add maxidle, maxpost and faillimit to the object passed to DagmanSubmitter
        # first two be used in the DAG submission and the latter the PostJob
        maxidle = getattr(self.config.TaskWorker, 'maxIdle', MAX_IDLE_JOBS)
        if maxidle == -1:
            maxidle = kwargs['task']['jobcount']
        elif maxidle == 0:
            maxidle = int(max(MAX_IDLE_JOBS, kwargs['task']['jobcount']*.1))
        jobSubmit['My.CRAB_MaxIdle'] = str(maxidle)

        maxpost = getattr(self.config.TaskWorker, 'maxPost', MAX_POST_JOBS)
        if maxpost == -1:
            maxpost = kwargs['task']['jobcount']
        elif maxpost == 0:
            maxpost = int(max(MAX_POST_JOBS, kwargs['task']['jobcount']*.1))
        jobSubmit['My.CRAB_MaxPost'] = str(maxpost)

        if not 'My.CRAB_FailedNodeLimit' in jobSubmit:
            jobSubmit['My.CRAB_FailedNodeLimit'] = "-1"
        elif int(jobSubmit['My.CRAB_FailedNodeLimit']) < 0:
            jobSubmit['My.CRAB_FailedNodeLimit'] = "-1"

        # last thing, update number of jobs in this task in Task table
        # using the numer of jobs in this (sub)DAGta
        self.reportNumJobToDB(task, len(dagSpecs))

        return jobSubmit, splitterResult, subdags

    def getHighPrioUsers(self, egroups):
        """ get the list of high priority users """

        highPrioUsers = set()
        try:
            for egroup in egroups:
                highPrioUsers.update(get_egroup_users(egroup))
        except Exception as ex:  # pylint: disable=broad-except
            msg = "Error when getting the high priority users list." \
                  " Will ignore the high priority list and continue normally." \
                  f" Error reason: {ex}"
            self.logger.error(msg)
            return []
        return highPrioUsers

    def reportNumJobToDB(self, task, nJobs):
        # update numner of jobs in this task in task table.
        numJobsHere = nJobs
        taskname = task['tm_taskname']
        if self.runningInTW:
            # things are easy
            self.logger.info('Reporting numJobs from TW: %s', numJobsHere)
            data = {'subresource': 'edit', 'column': 'tm_num_jobs',
                    'value': numJobsHere, 'workflow': taskname}
            self.crabserver.post(api='task', data=urlencode(data))
        else:
            # running in HTC AP host, i.e. automatic splitting, running inside PreDAG
            # in the HTC AP there is no crabserver from "the TW slave process" so create one now
            self.logger.info('Reporting numJobs from AP (preDag)')
            with open(os.environ['_CONDOR_JOB_AD'], 'r', encoding='utf-8') as fd:
                ad = classad.parseOne(fd)
            host = ad['CRAB_RestHost']
            dbInstance = ad['CRAB_DbInstance']
            cert = ad['X509UserProxy']
            reqname = ad['CRAB_Reqname']
            self.logger.info(f"host {host} dbInstance {dbInstance} cert {cert}")
            self.logger.info(f"taskname {taskname}  reqname {reqname}")
            from RESTInteractions import CRABRest  # pylint: disable=import-outside-toplevel
            crabserver = CRABRest(host, localcert=cert, localkey=cert, retry=3, userAgent='CRABSchedd')
            crabserver.setDbInstance(dbInstance)
            # fetch previous value from DB and  add to current value
            data = {'subresource': 'search', 'workflow': taskname}
            taskDict, _, _ = crabserver.get(api='task', data=data)
            previousNumJobs = int(getColumn(taskDict, 'tm_num_jobs'))
            self.logger.info(f"previousNumJobs {previousNumJobs}")
            self.logger.info(f"numJobsHere {numJobsHere}")
            numJobs = previousNumJobs + numJobsHere
            # report
            data = {'subresource': 'edit', 'column': 'tm_num_jobs',
                    'value': numJobs, 'workflow': taskname}
            R = crabserver.post(api='task', data=urlencode(data))
            self.logger.info(f"HTTP POST returned {R}")

    def executeInternal(self, *args, **kw):
        """ all real work is done here """

        # put in current directory all files that we need to move around
        transform_location = getLocation('CMSRunAnalysis.sh')
        cmscp_location = getLocation('cmscp.py')
        cmscpsh_location = getLocation('cmscp.sh')
        gwms_location = getLocation('gWMS-CMSRunAnalysis.sh')
        env_location = getLocation('submit_env.sh')
        dag_bootstrap_location = getLocation('dag_bootstrap_startup.sh')
        bootstrap_location = getLocation("dag_bootstrap.sh")
        adjust_location = getLocation("AdjustSites.py")

        shutil.copy(transform_location, '.')
        shutil.copy(cmscp_location, '.')
        shutil.copy(cmscpsh_location, '.')
        shutil.copy(gwms_location, '.')
        shutil.copy(env_location, '.')
        shutil.copy(dag_bootstrap_location, '.')
        shutil.copy(bootstrap_location, '.')
        shutil.copy(adjust_location, '.')

        # make sure we have InputSandBox
        sandboxTarBall = 'sandbox.tar.gz'  # SB only used once 10 lines below ! no need for a variable !

        # Bootstrap the ISB if we are running in the TW
        # SB: I do not like this overwriting of kw['task']['tm_user_sandbox'] nor
        # understand yet why it is being done.
        if self.runningInTW:
            username = kw['task']['tm_username']
            taskname = kw['task']['tm_taskname']
            sandboxName = kw['task']['tm_user_sandbox']
            self.logger.debug(f"Checking if sandbox file is available: {sandboxName}")
            try:
                checkS3Object(crabserver=self.crabserver, objecttype='sandbox', taskname=taskname,
                              username=username, tarballname=sandboxName, logger=self.logger)
                kw['task']['tm_user_sandbox'] = sandboxTarBall  # SB no need for this either ! it is only there to reuse same name in prepareArguments. A variable in self would be much cleaner, of even one constant to ensure same name in AdjustSites and CRABClient
            except Exception as ex:
                raise TaskWorkerException("The CRAB server backend could not find the input sandbox with your code " + \
                                  "from S3.\nThis could be a temporary glitch; please try to submit a new task later " + \
                                  "(resubmit will not work) and contact the experts if the error persists." + \
                                  f"\nError reason: {ex}") from ex

        # Bootstrap the runtime if it is available.
        # SB I presume these lines can be move up toghether with the other getLocations and copy
        job_runtime = getLocation('CMSRunAnalysis.tar.gz',)
        shutil.copy(job_runtime, '.')
        task_runtime = getLocation('TaskManagerRun.tar.gz')
        shutil.copy(task_runtime, '.')

        kw['task']['resthost'] = self.crabserver.server['host']  # SB most likely not needed, see Line 1027
        kw['task']['dbinstance'] = self.crabserver.getDbInstance()  # SB never used in here nor in DagmanSubmitter ??
        params = {}

        # files to be transferred to remove WN's via Job.submit, could pack most in a tarball
        filesForWN = ['submit_env.sh', 'CMSRunAnalysis.sh', 'cmscp.py', 'cmscp.sh', 'CMSRunAnalysis.tar.gz',
                      'run_and_lumis.tar.gz', 'input_files.tar.gz', 'input_args.json']
        # files to be transferred to the scheduler by fDagmanSubmitter (these will all be placed in InputFiles.tar.gz)
        filesForSched = filesForWN + \
            ['gWMS-CMSRunAnalysis.sh', 'RunJobs.dag', 'Job.submit', 'dag_bootstrap.sh',
             'AdjustSites.py', 'site.ad.json', 'TaskManagerRun.tar.gz',
             'datadiscovery.pkl', 'taskinformation.pkl', 'taskworkerconfig.pkl',]

        if kw['task']['tm_input_dataset']:
            filesForSched.append("input_dataset_lumis.json")
            filesForSched.append("input_dataset_duplicate_lumis.json")

        # now create the DAG. According to wheter this runs in the TW or in one PreDag, it will
        # create the MAIN DAG or one of the subdags for automatic splitting, hence it was called "createsubdag"
        jobSubmit, splitterResult, subdags = self.createSubdag(*args, **kw)

        # as splitter summary is useful for dryrun, let's add it to the InputFiles tarball
        jobGroups = splitterResult[0]  # the first returned value of Splitter action is the splitterFactory output
        splittingSummary = SplittingSummary(kw['task']['tm_split_algo'])
        for jobgroup in jobGroups:
            jobs = jobgroup.getJobs()
            splittingSummary.addJobs(jobs)
        splittingSummary.dump('splitting-summary.json')
        filesForSched.append('splitting-summary.json')

        self.prepareTarballForSched(filesForSched, subdags)

        return jobSubmit, params, ["InputFiles.tar.gz"], splitterResult

    def execute(self, *args, **kw):
        """ entry point called by Hanlder """
        cwd = os.getcwd()
        try:
            os.chdir(kw['tempDir'])
            jobSubmit, params, inputFiles, splitterResult = self.executeInternal(*args, **kw)
            return TaskWorker.DataObjects.Result.Result(task=kw['task'], result=(jobSubmit, params, inputFiles, splitterResult))
        finally:
            os.chdir(cwd)
