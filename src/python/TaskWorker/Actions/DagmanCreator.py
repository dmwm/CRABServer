"""
Create a set of files for a DAG submission.

Generates the condor submit files and the master DAG.
"""

import os
import re
import json
import shutil
import string
import pickle
import random
import tarfile
import hashlib
import tempfile
import commands
from ast import literal_eval
from httplib import HTTPException

from ServerUtilities import getLock
from ServerUtilities import TASKLIFETIME

import TaskWorker.WorkerExceptions
import TaskWorker.DataObjects.Result
import TaskWorker.Actions.TaskAction as TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import insertJobIdSid, MAX_DISK_SPACE
from CMSGroupMapper import get_egroup_users

import WMCore.WMSpec.WMTask
import WMCore.Services.PhEDEx.PhEDEx as PhEDEx
import WMCore.Services.SiteDB.SiteDB as SiteDB

import classad

try:
    from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
except ImportError:
    UserFileCache = None

from ApmonIf import ApmonIf

DAG_HEADER = """

NODE_STATUS_FILE node_state{nodestate} 30 ALWAYS-UPDATE

# NOTE: a file must be present, but 'noop' makes it not be read.
#FINAL FinalCleanup Job.1.submit NOOP
#SCRIPT PRE FinalCleanup dag_bootstrap.sh FINAL $DAG_STATUS $FAILED_COUNT {resthost} {resturiwfdb}

"""

DAG_FRAGMENT = """
JOB Job{count} Job.{count}.submit
SCRIPT {prescriptDefer} PRE  Job{count} dag_bootstrap.sh PREJOB $RETRY {count} {parent} {taskname} {backend} {stage}
SCRIPT DEFER 4 1800 POST Job{count} dag_bootstrap.sh POSTJOB $JOBID $RETURN $RETRY $MAX_RETRIES {taskname} {count} {tempDest} {outputDest} cmsRun_{count}.log.tar.gz {stage} {remoteOutputFiles}
#PRE_SKIP Job{count} 3
RETRY Job{count} {maxretries} UNLESS-EXIT 2
VARS Job{count} count="{count}" parent="{parent}" runAndLumiMask="job_lumis_{count}.json" lheInputFiles="{lheInputFiles}" firstEvent="{firstEvent}" firstLumi="{firstLumi}" lastEvent="{lastEvent}" firstRun="{firstRun}" maxRuntime="{maxRuntime}" eventsPerLumi="{eventsPerLumi}" seeding="{seeding}" inputFiles="job_input_file_list_{count}.txt" scriptExe="{scriptExe}" scriptArgs="{scriptArgs}" +CRAB_localOutputFiles="\\"{localOutputFiles}\\"" +CRAB_DataBlock="\\"{block}\\"" +CRAB_Destination="\\"{destination}\\""
ABORT-DAG-ON Job{count} 3
"""

SUBDAG_FRAGMENT = """
SUBDAG EXTERNAL Job{count}SubJobs RunJobs{count}.subdag
PARENT Job{count} CHILD Job{count}SubJobs
"""

JOB_SUBMIT = \
"""
+CRAB_ReqName = %(requestname)s
+CRAB_Workflow = %(workflow)s
+CRAB_JobType = %(jobtype)s
+CRAB_JobSW = %(jobsw)s
+CRAB_JobArch = %(jobarch)s
+CRAB_DBSURL = %(dbsurl)s
+CRAB_PublishName = %(publishname)s
+CRAB_PublishGroupName = %(publishgroupname)s
+CRAB_Publish = %(publication)s
+CRAB_PublishDBSURL = %(publishdbsurl)s
+CRAB_ISB = %(cacheurl)s
+CRAB_SaveLogsFlag = %(savelogsflag)s
+CRAB_AdditionalOutputFiles = %(addoutputfiles)s
+CRAB_EDMOutputFiles = %(edmoutfiles)s
+CRAB_TFileOutputFiles = %(tfileoutfiles)s
+CRAB_TransferOutputs = %(saveoutput)s
+CRAB_UserDN = %(userdn)s
+CRAB_UserHN = %(userhn)s
+CRAB_AsyncDest = %(asyncdest)s
+CRAB_StageoutPolicy = %(stageoutpolicy)s
+CRAB_UserRole = %(tm_user_role)s
+CRAB_UserGroup = %(tm_user_group)s
+CRAB_TaskWorker = %(worker_name)s
+CRAB_RetryOnASOFailures = %(retry_aso)s
+CRAB_ASOTimeout = %(aso_timeout)s
+CRAB_RestHost = %(resthost)s
+CRAB_RestURInoAPI = %(resturinoapi)s
+CRAB_NumAutomJobRetries = %(numautomjobretries)s
CRAB_Attempt = %(attempt)d
CRAB_ISB = %(cacheurl_flatten)s
CRAB_AdditionalOutputFiles = %(addoutputfiles_flatten)s
CRAB_JobSW = %(jobsw_flatten)s
CRAB_JobArch = %(jobarch_flatten)s
CRAB_Archive = %(cachefilename_flatten)s
CRAB_Id = $(count)
+CRAB_Id = "$(count)"
+CRAB_ParentId = "$(parent)"
+CRAB_OutTempLFNDir = "%(temp_dest)s"
+CRAB_OutLFNDir = "%(output_dest)s"
+CRAB_oneEventMode = %(oneEventMode)s
+CRAB_ASOURL = %(tm_asourl)s
+CRAB_ASODB = %(tm_asodb)s
+CRAB_PrimaryDataset = %(primarydataset)s
+TaskType = "Job"
+AccountingGroup = %(accounting_group)s
+CRAB_SubmitterIpAddr = %(submitter_ip_addr)s
+CRAB_TaskLifetimeDays = %(task_lifetime_days)s
+CRAB_TaskEndTime = %(task_endtime)s

# These attributes help gWMS decide what platforms this job can run on; see https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsMatchArchitecture
+DESIRED_OpSyses = %(desired_opsys)s
+DESIRED_OpSysMajorVers = %(desired_opsysvers)s
+DESIRED_Archs = %(desired_arch)s
+DESIRED_CMSDataset = %(inputdata)s

+JOBGLIDEIN_CMSSite = "$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\"Unknown\\", GLIDEIN_CMSSite)])"
job_ad_information_attrs = MATCH_EXP_JOBGLIDEIN_CMSSite, JOBGLIDEIN_CMSSite, RemoteSysCpu, RemoteUserCpu

# Recover job output and logs on eviction events; make sure they aren't spooled
# This requires 8.1.6 or later (https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=4292)
# This allows us to return stdout to users when they hit memory limits (which triggers PeriodicRemove).
+WhenToTransferOutput = "ON_EXIT_OR_EVICT"
+SpoolOnEvict = false

universe = vanilla
Executable = gWMS-CMSRunAnalysis.sh
Output = job_out.$(CRAB_Id)
Error = job_err.$(CRAB_Id)
Log = job_log
# args changed...

Arguments = "-a $(CRAB_Archive) --sourceURL=$(CRAB_ISB) --jobNumber=$(CRAB_Id) --cmsswVersion=$(CRAB_JobSW) --scramArch=$(CRAB_JobArch) '--inputFile=$(inputFiles)' '--runAndLumis=$(runAndLumiMask)' --lheInputFiles=$(lheInputFiles) --firstEvent=$(firstEvent) --firstLumi=$(firstLumi) --lastEvent=$(lastEvent) --firstRun=$(firstRun) --seeding=$(seeding) --scriptExe=$(scriptExe) --eventsPerLumi=$(eventsPerLumi) --maxRuntime=$(maxRuntime) '--scriptArgs=$(scriptArgs)' -o $(CRAB_AdditionalOutputFiles)"

transfer_input_files = CMSRunAnalysis.sh, cmscp.py%(additional_input_file)s
transfer_output_files = jobReport.json.$(count), WMArchiveReport.json.$(count)
# TODO: fold this into the config file instead of hardcoding things.
Environment = "SCRAM_ARCH=$(CRAB_JobArch) %(additional_environment_options)s"
should_transfer_files = YES
#x509userproxy = %(x509up_file)s
use_x509userproxy = true
%(opsys_req)s
Requirements = ((target.IS_GLIDEIN =!= TRUE) || (target.GLIDEIN_CMSSite =!= UNDEFINED))
periodic_release = (HoldReasonCode == 28) || (HoldReasonCode == 30) || (HoldReasonCode == 13) || (HoldReasonCode == 6)
# Remove if
# a) job is in the 'held' status for more than 7 minutes
# b) job is idle more than 7 days
# c) job is running and one of:
#    1) Over memory use
#    2) Over wall clock limit
#    3) Over disk usage of N GB, which is set in ServerUtilities
# d) job is idle and users proxy expired 1 day ago. (P.S. why 1 day ago? because there is recurring action which is updating user proxy and lifetime.)
# == If New periodic remove expression is added, also it should have Periodic Remove Reason. Otherwise message will not be clear and it is hard to debug
periodic_remove = ((JobStatus =?= 5) && (time() - EnteredCurrentStatus > 7*60)) || \
                  ((JobStatus =?= 1) && (time() - EnteredCurrentStatus > 7*24*60*60)) || \
                  ((JobStatus =?= 2) && ( \
                     (MemoryUsage > RequestMemory) || \
                     (MaxWallTimeMins*60 < time() - EnteredCurrentStatus) || \
                     (DiskUsage > %(max_disk_space)s))) || \
                     (time() > CRAB_TaskEndTime) || \
                  ((JobStatus =?= 1) && (time() > (x509UserProxyExpiration + 86400)))
+PeriodicRemoveReason = ifThenElse(time() - EnteredCurrentStatus > 7*24*60*60 && isUndefined(MemoryUsage), "Removed due to idle time limit", \
                          ifThenElse(time() > x509UserProxyExpiration, "Removed job due to proxy expiration", \
                            ifThenElse(MemoryUsage > RequestMemory, "Removed due to memory use", \
                              ifThenElse(MaxWallTimeMins*60 < time() - EnteredCurrentStatus, "Removed due to wall clock limit", \
                                ifThenElse(DiskUsage >  %(max_disk_space)s, "Removed due to disk usage", \
                                  ifThenElse(time() > CRAB_TaskEndTime, "Removed due to reached CRAB_TaskEndTime", \
                                  "Removed due to job being held")))))
%(extra_jdl)s
queue
"""

SPLIT_ARG_MAP = { "Automatic": "seconds_per_job",
                  "LumiBased": "lumis_per_job",
                  "EventBased": "events_per_job",
                  "FileBased": "files_per_job",
                  "EventAwareLumiBased": "events_per_job",}


def getCreateTimestamp(taskname):
    return "_".join(taskname.split(":")[:1])


def makeLFNPrefixes(task):
    ## Once we don't care anymore about backward compatibility with crab server < 3.3.1511
    ## we can uncomment the 1st line below and remove the next 6 lines.
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
    hash = hashlib.sha1(hash_input).hexdigest()
    user = task['tm_username']
    tmp_user = "%s.%s" % (user, hash)
    publish_info = task['tm_publish_name'].rsplit('-', 1) #publish_info[0] is the publishname or the taskname
    timestamp = getCreateTimestamp(task['tm_taskname'])
    splitlfn = lfn.split('/')
    if splitlfn[2] == 'user':
        #join:                    /       store    /temp   /user  /mmascher.1234    /lfn          /GENSYM    /publishname     /120414_1634
        temp_dest = os.path.join('/', splitlfn[1], 'temp', 'user', tmp_user, *( splitlfn[4:] + [primaryds, publish_info[0], timestamp] ))
    else:
        temp_dest = os.path.join('/', splitlfn[1], 'temp', 'user', tmp_user, *( splitlfn[3:] + [primaryds, publish_info[0], timestamp] ))
    dest = os.path.join(lfn, primaryds, publish_info[0], timestamp)

    return temp_dest, dest

def validateLFNs(path, outputFiles):
    """
    validate against standard Lexicon the LFN's that this task will try to publish in DBS
    :param path: string: the path part of the LFN's, w/o the directory counter
    :param outputFiles: list of strings: the filenames to be published (w/o the jobId, i.e. out.root not out_1.root)
    :return: nothing if all OK. If LFN is not valid Lexicon raises an AssertionError exception
    """
    from WMCore import Lexicon
    # fake values to get proper LFN length, actual numbers chance job by job
    jobId = '10000'       # current max is 10k jobs per task
    dirCounter = '0001'   # need to be same length as 'counter' used later in makeDagSpecs

    for origFile in outputFiles:
        info = origFile.rsplit(".", 1)
        if len(info) == 2:    # filename ends with .<something>, put jobId before the dot
            fileName = "%s_%s.%s" % (info[0], jobId, info[1])
        else:
            fileName = "%s_%s" % (origFile, jobId)
        testLfn = os.path.join(path, dirCounter, fileName)
        Lexicon.lfn(testLfn)  # will raise if testLfn is not a valid lfn
    return

def transform_strings(input):
    """
    Converts the arguments in the input dictionary to the arguments necessary
    for the job submit file string.
    """
    info = {}
    for var in 'workflow', 'jobtype', 'jobsw', 'jobarch', 'inputdata', 'primarydataset', 'splitalgo', 'algoargs', \
               'cachefilename', 'cacheurl', 'userhn', 'publishname', 'asyncdest', 'dbsurl', 'publishdbsurl', \
               'userdn', 'requestname', 'oneEventMode', 'tm_user_vo', 'tm_user_role', 'tm_user_group', \
               'tm_maxmemory', 'tm_numcores', 'tm_maxjobruntime', 'tm_priority', 'tm_asourl', 'tm_asodb', \
               'stageoutpolicy', 'taskType', 'worker_name', 'desired_opsys', 'desired_opsysvers', \
               'desired_arch', 'accounting_group', 'resthost', 'resturinoapi', 'submitter_ip_addr', \
               'task_lifetime_days', 'task_endtime', 'maxproberuntime', 'maxtailruntime':
        val = input.get(var, None)
        if val == None:
            info[var] = 'undefined'
        else:
            info[var] = json.dumps(val)

    for var in 'savelogsflag', 'blacklistT1', 'retry_aso', 'aso_timeout', 'publication', 'saveoutput', 'numautomjobretries', 'publishgroupname':
        info[var] = int(input[var])

    for var in 'siteblacklist', 'sitewhitelist', 'addoutputfiles', 'tfileoutfiles', 'edmoutfiles':
        val = input[var]
        if val == None:
            info[var] = "{}"
        else:
            info[var] = "{" + json.dumps(val)[1:-1] + "}"

    info['lumimask'] = '"' + json.dumps(WMCore.WMSpec.WMTask.buildLumiMask(input['runs'], input['lumis'])).replace(r'"', r'\"') + '"'

    splitArgName = SPLIT_ARG_MAP[input['splitalgo']]
    info['algoargs'] = '"' + json.dumps({'halt_job_on_file_boundaries': False, 'splitOnRun': False, splitArgName : input['algoargs']}).replace('"', r'\"') + '"'
    info['attempt'] = 0

    for var in ["cacheurl", "jobsw", "jobarch", "cachefilename", "asyncdest", "requestname"]:
        info[var+"_flatten"] = input[var]

    # TODO: PanDA wrapper wants some sort of dictionary.
    info["addoutputfiles_flatten"] = '{}'

    temp_dest, dest = makeLFNPrefixes(input)
    info["temp_dest"] = temp_dest
    info["output_dest"] = dest
    info['x509up_file'] = os.path.split(input['user_proxy'])[-1]
    info['user_proxy'] = input['user_proxy']
    info['scratch'] = input['scratch']

    return info


def getLocation(default_name, checkout_location):
    """ Get the location of the runtime code (job wrapper, postjob, anything executed on the schedd
        and on the worker node)

        First check if the files are present in the current working directory
        Then check if CRABTASKWORKER_ROOT is in the environment and use that location (that viariable is
            set by the taskworker init script. In the prod source script we use "export CRABTASKWORKER_ROOT")
        Finally, check if the CRAB3_CHECKOUT variable is set. That option is interesting for developer who
            can use this to point to their github repository. (Marco: we need to check this)
    """
    loc = default_name
    if not os.path.exists(loc):
        if 'CRABTASKWORKER_ROOT' in os.environ:
            for path in ['xdata', 'data']:
                fname = os.path.join(os.environ['CRABTASKWORKER_ROOT'], path, loc)
                if os.path.exists(fname):
                    return fname
        if 'CRAB3_CHECKOUT' not in os.environ:
            raise Exception("Unable to locate %s" % loc)
        loc = os.path.join(os.environ['CRAB3_CHECKOUT'], checkout_location, loc)
    loc = os.path.abspath(loc)
    return loc


class DagmanCreator(TaskAction.TaskAction):
    """
    Given a task definition, create the corresponding DAG files for submission
    into HTCondor
    """


    def __init__(self, *args, **kwargs):
        TaskAction.TaskAction.__init__(self, *args, **kwargs)
        self.phedex = PhEDEx.PhEDEx() #TODO use config certs!


    def buildDashboardInfo(self):
        taskType = self.getDashboardTaskType()

        params = {'tool': 'crab3',
                  'SubmissionType':'crab3',
                  'JSToolVersion': '3.3.0',
                  'tool_ui': os.environ.get('HOSTNAME', ''),
                  'scheduler': 'GLIDEIN',
                  'GridName': self.task['tm_user_dn'],
                  'ApplicationVersion': self.task['tm_job_sw'],
                  'taskType': taskType,
                  'vo': 'cms',
                  'CMSUser': self.task['tm_username'],
                  'user': self.task['tm_username'],
                  'taskId': self.task['tm_taskname'],
                  'datasetFull': self.task['tm_input_dataset'],
                  'resubmitter': self.task['tm_username'],
                  'exe': 'cmsRun' }
        return params


    def sendDashboardTask(self):
        apmon = ApmonIf()
        params = self.buildDashboardInfo()
        params_copy = dict(params)
        params_copy['jobId'] = 'TaskMeta'
        self.logger.debug("Dashboard task info: %s" % str(params_copy))
        apmon.sendToML(params_copy)
        apmon.free()
        return params


    def resolvePFNs(self, dest_site, dest_dir):
        """
        Given a directory and destination, resolve the directory to a srmv2 PFN
        """
        lfns = [dest_dir]
        dest_sites_ = [dest_site]
        try:
            pfn_info = self.phedex.getPFN(nodes=dest_sites_, lfns=lfns)
        except HTTPException as ex:
            self.logger.error(ex.headers)
            raise TaskWorker.WorkerExceptions.TaskWorkerException("The CRAB3 server backend could not contact phedex to do the site+lfn=>pfn translation.\n"+\
                                "This is could be a temporary phedex glitch, please try to submit a new task (resubmit will not work)"+\
                                " and contact the experts if the error persists.\nError reason: %s" % str(ex))
        results = []
        for lfn in lfns:
            found_lfn = False
            if (dest_site, lfn) in pfn_info:
                results.append(pfn_info[dest_site, lfn])
                found_lfn = True
                break
            if not found_lfn:
                raise TaskWorker.WorkerExceptions.NoAvailableSite("The CRAB3 server backend could not map LFN %s at site %s" % (lfn, dest_site)+\
                                "This is a fatal error. Please, contact the experts")
        return results[0]


    def populateGlideinMatching(self, info):
        scram_arch = info['tm_job_arch']
        # Set defaults
        info['desired_opsys'] = "LINUX"
        info['desired_opsysvers'] = "5,6"
        info['desired_arch'] = "X86_64"
        m = re.match("([a-z]+)(\d+)_(\w+)_(\w+)", scram_arch)
        # At the time of writing, for opsys and arch, the only supported variant
        # is the default variant; we actually parse this information so the future maintainer
        # can see what is needed.  For OpSys version, users can support 5,6 or 6.
        if m:
            os, ver, arch, _ = m.groups()
            if os == "slc":
                info['desired_opsys'] = "LINUX"
            if ver == "5":
                info['desired_opsysvers'] = "5,6"
            elif ver == "6":
                info['desired_opsysvers'] = "6"
            if arch == "amd64":
                info['desired_arch'] = "X86_64"


    def getDashboardTaskType(self):
        """ Get the dashboard activity name for the task.
        """
        if self.task['tm_activity'] in (None, ''):
            return getattr(self.config.TaskWorker, 'dashboardTaskType', 'analysistest')
        return self.task['tm_activity']


    def isGlobalBlacklistIgnored(self, kwargs):
        """ Determine wether the user wants to ignore the globalblacklist
        """
        extrajdls = literal_eval(kwargs['task']['tm_extrajdl'])
        for ej in extrajdls:
            if ej.find('CRAB_IgnoreGlobalBlacklist') in [0, 1]: #there might be a + before
                return True

        return kwargs['task']['tm_ignore_global_blacklist'] == 'T'


    def makeJobSubmit(self, task):
        """
        Create the submit file.  This is reused by all jobs in the task; differences
        between the jobs are taken care of in the makeDagSpecs.
        """

        if os.path.exists("Job.submit"):
            return {}

        # From here on out, we convert from tm_* names to the DataWorkflow names
        info = dict(task)
        info['workflow'] = task['tm_taskname']
        info['jobtype'] = 'analysis'
        info['jobsw'] = info['tm_job_sw']
        info['jobarch'] = info['tm_job_arch']
        info['inputdata'] = info['tm_input_dataset']
        ## The 1st line below is for backward compatibility with entries in the TaskDB
        ## made by CRAB server < 3.3.1511, where tm_primary_dataset = null
        ## and tm_input_dataset always contains at least one '/'. Once we don't
        ## care about backward compatibility anymore, remove the 1st line and
        ## uncomment the 2nd line.
        info['primarydataset'] = info['tm_primary_dataset'] if info['tm_primary_dataset'] else info['tm_input_dataset'].split('/')[1]
        #info['primarydataset'] = info['tm_primary_dataset']
        info['splitalgo'] = info['tm_split_algo']
        info['algoargs'] = info['tm_split_args']
        info['cachefilename'] = info['tm_user_sandbox']
        info['cacheurl'] = info['tm_cache_url']
        info['userhn'] = info['tm_username']
        info['publishname'] = info['tm_publish_name']
        info['publishgroupname'] = 1 if info['tm_publish_groupname'] == 'T' else 0
        info['asyncdest'] = info['tm_asyncdest']
        info['dbsurl'] = info['tm_dbs_url']
        info['publishdbsurl'] = info['tm_publish_dbs_url']
        info['publication'] = 1 if info['tm_publication'] == 'T' else 0
        info['userdn'] = info['tm_user_dn']
        info['requestname'] = string.replace(task['tm_taskname'], '"', '')
        info['savelogsflag'] = 1 if info['tm_save_logs'] == 'T' else 0
        info['blacklistT1'] = 0
        info['siteblacklist'] = task['tm_site_blacklist']
        info['sitewhitelist'] = task['tm_site_whitelist']
        info['addoutputfiles'] = task['tm_outfiles']
        info['tfileoutfiles'] = task['tm_tfile_outfiles']
        info['edmoutfiles'] = task['tm_edm_outfiles']
        info['oneEventMode'] = 1 if info['tm_one_event_mode'] == 'T' else 0
        info['ASOURL'] = task['tm_asourl']
        asodb = task.get('tm_asodb', 'asynctransfer') or 'asynctransfer'
        info['ASODB'] = asodb
        info['taskType'] = self.getDashboardTaskType()
        info['worker_name'] = getattr(self.config.TaskWorker, 'name', 'unknown')
        info['retry_aso'] = 1 if getattr(self.config.TaskWorker, 'retryOnASOFailures', True) else 0
        info['aso_timeout'] = getattr(self.config.TaskWorker, 'ASOTimeout', 0)
        info['submitter_ip_addr'] = task['tm_submitter_ip_addr']

        #Classads for task lifetime management, see https://github.com/dmwm/CRABServer/issues/5505
        info['task_lifetime_days'] = TASKLIFETIME // 24 // 60 // 60
        info['task_endtime'] = int(task["tm_start_time"]) + TASKLIFETIME

        self.populateGlideinMatching(info)

        # TODO: pass through these correctly.
        info['runs'] = []
        info['lumis'] = []
        info['saveoutput'] = 1 if info['tm_transfer_outputs'] == 'T' else 0
        egroups = getattr(self.config.TaskWorker, 'highPrioEgroups', [])
        if egroups and info['userhn'] in self.getHighPrioUsers(info['user_proxy'], info['workflow'], egroups):
            info['accounting_group'] = 'highprio.%s' % info['userhn']
        else:
            info['accounting_group'] = 'analysis.%s' % info['userhn']
        info = transform_strings(info)
        info['faillimit'] = task['tm_fail_limit']
        info['extra_jdl'] = '\n'.join(literal_eval(task['tm_extrajdl']))
        if info['jobarch_flatten'].startswith("slc6_"):
            info['opsys_req'] = 'REQUIRED_OS="rhel6"'
        if info['jobarch_flatten'].startswith("slc7_"):
            info['opsys_req'] = 'REQUIRED_OS="rhel7"'
        else:
            info['opsys_req'] = 'REQUIRED_OS="any"'

        info.setdefault("additional_environment_options", '')
        info.setdefault("additional_input_file", "")
        if os.path.exists("CMSRunAnalysis.tar.gz"):
            info['additional_environment_options'] += 'CRAB_RUNTIME_TARBALL=local'
            info['additional_input_file'] += ", CMSRunAnalysis.tar.gz"
        else:
            raise TaskWorkerException("Cannot find CMSRunAnalysis.tar.gz inside the cwd: %s" % os.getcwd())
        if os.path.exists("TaskManagerRun.tar.gz"):
            info['additional_environment_options'] += ' CRAB_TASKMANAGER_TARBALL=local'
        else:
            raise TaskWorkerException("Cannot find TaskManagerRun.tar.gz inside the cwd: %s" % os.getcwd())
        if os.path.exists("sandbox.tar.gz"):
            info['additional_input_file'] += ", sandbox.tar.gz"
        info['additional_input_file'] += ", run_and_lumis.tar.gz"
        info['additional_input_file'] += ", input_files.tar.gz"

        info['max_disk_space'] = MAX_DISK_SPACE

        with open("Job.submit", "w") as fd:
            fd.write(JOB_SUBMIT % info)

        return info


    def getPreScriptDefer(self, task, jobid):
        """ Return the string to be used for deferring prejobs
            If the extrajdl CRAB_JobReleaseTimeout is not set in the client it returns
            an empty string, otherwise it return a string to defer the prejob by
            jobid * CRAB_JobReleaseTimeout seconds
        """
        slowJobRelease = False
        extrajdls = literal_eval(task['tm_extrajdl'])
        for ej in extrajdls:
            if ej.find('CRAB_JobReleaseTimeout') in [0, 1]: #there might be a + before
                slowJobRelease = True
                releaseTimeout = int(ej.split('=')[1])

        if slowJobRelease:
            prescriptDeferString = 'DEFER 4 %s' % (jobid * releaseTimeout)
        else:
            prescriptDeferString = ''
        return prescriptDeferString


    def makeDagSpecs(self, task, sitead, siteinfo, jobgroup, block, availablesites, datasites, outfiles, startjobid, subjob=None, stage='conventional'):
        dagSpecs = []
        i = startjobid
        temp_dest, dest = makeLFNPrefixes(task)
	if task['tm_publication'] == 'T' :
            try:
                validateLFNs(dest,outfiles)
            except AssertionError as ex:
                msg  = "\nYour task speficies an output LFN which fails validation in"
                msg += "\n WMCore/Lexicon and therefore can not be published in DBS"
                msg += "\nError detail: %s" % (str(ex))
                raise TaskWorker.WorkerExceptions.TaskWorkerException(msg)
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
            if subjob is None:
                i = int(i) + 1
                count = str(i)
            else:
                if isinstance(i, basestring):
                    i = int(i.split('-', 1)[0])
                subjob += 1
                count = '{parent}-{subjob}'.format(parent=i, subjob=subjob)
            sitead['Job{0}'.format(count)] = list(availablesites)
            siteinfo[count] = groupid
            remoteOutputFiles = []
            localOutputFiles = []
            if stage != 'probe':
                for origFile in outfiles:
                    info = origFile.rsplit(".", 1)
                    if len(info) == 2:
                        fileName = "%s_%s.%s" % (info[0], count, info[1])
                    else:
                        fileName = "%s_%s" % (origFile, count)
                    remoteOutputFiles.append("%s" % fileName)
                    localOutputFiles.append("%s=%s" % (origFile, fileName))
            remoteOutputFilesStr = " ".join(remoteOutputFiles)
            localOutputFiles = ", ".join(localOutputFiles)
            counter = "%04d" % (i / 1000)
            tempDest = os.path.join(temp_dest, counter)
            directDest = os.path.join(dest, counter)
            if lastDirectDest != directDest:
                lastDirectPfn = self.resolvePFNs(task['tm_asyncdest'], directDest)
                lastDirectDest = directDest
            pfns = ["log/cmsRun_{0}.log.tar.gz".format(count)] + remoteOutputFiles
            pfns = ", ".join(["%s/%s" % (lastDirectPfn, pfn) for pfn in pfns])
            prescriptDeferString = self.getPreScriptDefer(task, i)
            nodeSpec = {'count': count,
                        'parent': str(i),
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
                        'maxRuntime' : task['max_runtime'],
                        'sw': task['tm_job_sw'],
                        'block': block,
                        'destination': pfns,
                        'scriptExe': task['tm_scriptexe'],
                        'scriptArgs': json.dumps(task['tm_scriptargs']).replace('"', r'\"\"'),
                        'stage': stage,
                       }
            dagSpecs.append(nodeSpec)

        return dagSpecs, i


    def createSubdag(self, splitterResult, **kwargs):

        startjobid = kwargs.get('startjobid', 0)
        subjob = kwargs.get('subjob', None)
        stage = kwargs.get('stage', 'conventional')
        self.logger.debug('starting createSubdag, kwargs are:')
        self.logger.debug(str(kwargs))
        dagSpecs = []
        subdags = []

        if hasattr(self.config.TaskWorker, 'stageoutPolicy'):
            kwargs['task']['stageoutpolicy'] = ",".join(self.config.TaskWorker.stageoutPolicy)
        else:
            kwargs['task']['stageoutpolicy'] = "local,remote"

        ## In the future this parameter may be set by the user in the CRAB configuration
        ## file and we would take it from the Task DB.
        kwargs['task']['numautomjobretries'] = getattr(self.config.TaskWorker, 'numAutomJobRetries', 2)

        proberuntime = getattr(self.config.TaskWorker, 'splittingPilotRuntime', 15 * 60)
        tailruntime = getattr(self.config.TaskWorker, 'splittingTailRuntime', 45 * 60)  # Not used yet

        kwargs['task']['max_runtime'] = kwargs['task']['tm_split_args'].get('seconds_per_job', -1)
        # include a factor of 4 as a buffer
        kwargs['task']['maxproberuntime'] = (proberuntime * 4) // 60
        kwargs['task']['maxtailruntime'] = (tailruntime * 4) // 60
        if kwargs['task']['tm_split_algo'] == 'Automatic' and stage == 'conventional':
            kwargs['task']['max_runtime'] = proberuntime
            kwargs['task']['completion_jobs'] = getattr(self.config.TaskWorker, 'completionJobs', False)
            outfiles = []
            stage = 'probe'
        if stage == 'process' and not kwargs['task']['completion_jobs']:
            kwargs['task']['max_runtime'] = -1

        if stage == 'probe':
            parent = None
            startjobid = -1
        else:
            parent = startjobid

        info = self.makeJobSubmit(kwargs['task'])

        outfiles = kwargs['task']['tm_outfiles'] + kwargs['task']['tm_tfile_outfiles'] + kwargs['task']['tm_edm_outfiles']

        os.chmod("CMSRunAnalysis.sh", 0o755)

        # This config setting acts as a global black list
        global_blacklist = set(self.getBlacklistedSites())
        self.logger.debug("CRAB site blacklist: %s" % (list(global_blacklist)))

        # This is needed for Site Metrics
        # It should not block any site for Site Metrics and if needed for other activities
        # self.config.TaskWorker.ActivitiesToRunEverywhere = ['hctest', 'hcdev']
        # The other case where the blacklist is ignored is if the user sset this explicitly in his configuration
        if self.isGlobalBlacklistIgnored(kwargs) or (hasattr(self.config.TaskWorker, 'ActivitiesToRunEverywhere') and \
                   kwargs['task']['tm_activity'] in self.config.TaskWorker.ActivitiesToRunEverywhere):
            global_blacklist = set()
            self.logger.debug("Ignoring the CRAB site blacklist.")

        sitead = classad.ClassAd()
        siteinfo = {'group_sites': {}, 'group_datasites': {}}

        blocksWithNoLocations = set()
        blocksWithBannedLocations = set()
        allblocks = set()

        siteWhitelist = set(kwargs['task']['tm_site_whitelist'])
        siteBlacklist = set(kwargs['task']['tm_site_blacklist'])
        self.logger.debug("Site whitelist: %s" % (list(siteWhitelist)))
        self.logger.debug("Site blacklist: %s" % (list(siteBlacklist)))

        if siteWhitelist & global_blacklist:
            msg  = "The following sites from the user site whitelist are blacklisted by the CRAB server: %s." % (list(siteWhitelist & global_blacklist))
            msg += " Since the CRAB server blacklist has precedence, these sites are not considered in the user whitelist."
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
            self.logger.warning(msg)

        if siteBlacklist & siteWhitelist:
            msg  = "The following sites appear in both the user site blacklist and whitelist: %s." % (list(siteBlacklist & siteWhitelist))
            msg += " Since the whitelist has precedence, these sites are not considered in the blacklist."
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
            self.logger.warning(msg)

        ignoreLocality = kwargs['task']['tm_ignore_locality'] == 'T'
        self.logger.debug("Ignore locality: %s" % (ignoreLocality))

        for jobgroup in splitterResult[0]:
            jobs = jobgroup.getJobs()

            jgblocks = set() #job group blocks
            for job in jobs:
                if stage == 'probe':
                    random.shuffle(job['input_files'])
                for inputfile in job['input_files']:
                    jgblocks.add(inputfile['block'])
                    allblocks.add(inputfile['block'])
            self.logger.debug("Blocks: %s" % list(jgblocks))

            if not jobs:
                locations = set()
            else:
                locations = set(jobs[0]['input_files'][0]['locations'])
            self.logger.debug("Locations: %s" % (list(locations)))

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
                sbj = SiteDB.SiteDBJSON({"key": self.config.TaskWorker.cmskey,
                                         "cert": self.config.TaskWorker.cmscert})
                try:
                    possiblesites = set(sbj.getAllCMSNames())
                except Exception as ex:
                    msg  = "The CRAB3 server backend could not contact SiteDB to get the list of all CMS sites."
                    msg += " This could be a temporary SiteDB glitch."
                    msg += " Please try to submit a new task (resubmit will not work)"
                    msg += " and contact the experts if the error persists."
                    msg += "\nError reason: %s" % (str(ex)) #TODO add the sitedb url so the user can check themselves!
                    raise TaskWorker.WorkerExceptions.TaskWorkerException(msg)
            else:
                possiblesites = locations
            ## At this point 'possiblesites' should never be empty.
            self.logger.debug("Possible sites: %s" % (list(possiblesites)))

            ## Apply the global site blacklist.
            availablesites = possiblesites - global_blacklist

            ## See https://github.com/dmwm/CRABServer/issues/5241
            ## for a discussion about blocksWithBannedLocations
            if not availablesites:
                blocksWithBannedLocations = blocksWithBannedLocations.union(jgblocks)
                continue

            # NOTE: User can still shoot themselves in the foot with the resubmit blacklist
            # However, this is the last chance we have to warn the users about an impossible task at submit time.
            available = set(availablesites)
            if siteWhitelist:
                available &= siteWhitelist
                if not available:
                    blocksWithBannedLocations = blocksWithBannedLocations.union(jgblocks)
            available -= (siteBlacklist - siteWhitelist)
            if not available:
                blocksWithBannedLocations = blocksWithBannedLocations.union(jgblocks)
                continue

            availablesites = [str(i) for i in availablesites]
            datasites = jobs[0]['input_files'][0]['locations']
            self.logger.info("Resulting available sites: %s" % (list(availablesites)))

            if siteWhitelist or siteBlacklist:
                msg  = "The site whitelist and blacklist will be applied by the pre-job."
                msg += " This is expected to result in DESIRED_SITES = %s" % (list(available))
                self.logger.debug(msg)

            jobgroupDagSpecs, startjobid = self.makeDagSpecs(kwargs['task'], sitead, siteinfo, jobgroup, list(jgblocks)[0], availablesites, datasites, outfiles, startjobid, subjob=subjob, stage=stage)
            dagSpecs += jobgroupDagSpecs

        def getBlacklistMsg():
            tmp = ""
            if len(global_blacklist)!=0:
                tmp += " Global CRAB3 blacklist is %s.\n" % global_blacklist
            if len(siteBlacklist)!=0:
                tmp += " User blacklist is %s.\n" % siteBlacklist
            if len(siteWhitelist)!=0:
                tmp += " User whitelist is %s.\n" % siteWhitelist
            return tmp

        if not dagSpecs:
            msg = "No jobs created for task %s." % (kwargs['task']['tm_taskname'])
            if blocksWithNoLocations or blocksWithBannedLocations:
                msg  = "The CRAB server backend refuses to send jobs to the Grid scheduler. "
                msg += "No locations found for dataset '%s'. " % (kwargs['task']['tm_input_dataset'])
                msg += "(or at least for the part of the dataset that passed the lumi-mask and/or run-range selection).\n"
            if blocksWithBannedLocations:
                msg += " Found %s (out of %s) blocks present only at blacklisted sites." % (len(blocksWithBannedLocations), len(allblocks))
                msg += getBlacklistMsg()
            raise TaskWorker.WorkerExceptions.NoAvailableSite(msg)
        msg = "Some blocks from dataset '%s' were skipped " % (kwargs['task']['tm_input_dataset'])
        if blocksWithNoLocations:
            msgBlocklist = sorted(list(blocksWithNoLocations[:10])) + ['...']
            msg += " because they have no locations.\n List is (first 10 elements only): %s.\n" % msgBlocklist
        if blocksWithBannedLocations:
            msg += " because they are only present at blacklisted sites.\n List is: %s.\n" % (sorted(list(blocksWithBannedLocations)))
            msg += getBlacklistMsg()
        if blocksWithNoLocations or blocksWithBannedLocations:
            msg += (" Dataset processing will be incomplete because %s (out of %s) blocks are only present at blacklisted site(s)" %
                (len(blocksWithNoLocations)+len(blocksWithBannedLocations), len(allblocks)))
            self.uploadWarning(msg, kwargs['task']['user_proxy'], kwargs['task']['tm_taskname'])
            self.logger.warning(msg)

        ## Write down the DAG as needed by DAGMan.
        dag = DAG_HEADER.format(
                nodestate='' if not parent else '.{0}'.format(parent),
                resthost=kwargs['task']['resthost'],
                resturiwfdb=kwargs['task']['resturinoapi'] + '/workflowdb')
        if stage == 'probe':
            # We want only one probe job
            dagSpecs = dagSpecs[:1]
        for dagSpec in dagSpecs:
            dag += DAG_FRAGMENT.format(**dagSpec)
            if stage == 'probe' or (stage == 'process' and kwargs['task']['completion_jobs']):
                dag += SUBDAG_FRAGMENT.format(**dagSpec)
                subdag = "RunJobs{0}.subdag".format(dagSpec['count'])
                with open(subdag, "w") as fd:
                    fd.write("")
                subdags.append(subdag)

        ## Create a tarball with all the job lumi files.
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
                    with open(job_lumis_file, "w") as fd:
                        fd.write(str(dagSpec['runAndLumiMask']))
                    ## Also creating a tarball with the dataset input files.
                    ## Each .txt file in the tarball contains a list of dataset files to be used for the job.
                    job_input_file_list = os.path.join(tempDir2, 'job_input_file_list_'+ str(dagSpec['count']) +'.txt')
                    with open(job_input_file_list, "w") as fd2:
                        fd2.write(str(dagSpec['inputFiles']))
            finally:
                tfd.add(tempDir, arcname='')
                tfd.close()
                shutil.rmtree(tempDir)
                tfd2.add(tempDir2, arcname='')
                tfd2.close()
                shutil.rmtree(tempDir2)

        if stage in ('probe', 'conventional'):
            name = "RunJobs.dag"
            ## Cache data discovery
            with open("datadiscovery.pkl", "wb") as fd:
                pickle.dump(splitterResult[1], fd)

            ## Cache task information
            with open("taskinformation.pkl", "wb") as fd:
                pickle.dump(kwargs['task'], fd)
        else:
            name = "RunJobs{0}.subdag".format(parent)

        if stage != 'tail':
            ## Cache site information
            with open("site.ad", "w") as fd:
                fd.write(str(sitead))

            with open("site.ad.json", "w") as fd:
                json.dump(siteinfo, fd)

        ## Save the DAG into a file.
        with open(name, "w") as fd:
            fd.write(dag)

        task_name = kwargs['task'].get('CRAB_ReqName', kwargs['task'].get('tm_taskname', ''))
        userdn = kwargs['task'].get('CRAB_UserDN', kwargs['task'].get('tm_user_dn', ''))

        info["jobcount"] = len(dagSpecs)
        maxpost = getattr(self.config.TaskWorker, 'maxPost', 20)
        if maxpost == -1:
            maxpost = info['jobcount']
        elif maxpost == 0:
            maxpost = int(max(20, info['jobcount']*.1))
        info['maxpost'] = maxpost

        if info.get('faillimit') == None:
            info['faillimit'] = -1
            #if info['jobcount'] > 200
            #    info['faillimit'] = 100
            #else:
            #    info['faillimit'] = -1
        elif info.get('faillimit') < 0:
            info['faillimit'] = -1

        # Info for ML:
        target_se = ''
        max_len_target_se = 900
        for site in map(str, availablesites):
            if len(target_se) > max_len_target_se:
                target_se += ',Many_More'
                break
            if len(target_se):
                target_se += ','
            target_se += site
        ml_info = info.setdefault('apmon', [])
        shift = 0 if stage == 'probe' else 1
        for idx in range(shift, info['jobcount']+shift):
            taskid = kwargs['task']['tm_taskname']
            jinfo = {'broker': os.environ.get('HOSTNAME', ''),
                     'bossId': str(idx),
                     'TargetSE': target_se,
                     'localId': '',
                     'StatusValue': 'pending',
                    }
            insertJobIdSid(jinfo, idx, taskid, 0)
            ml_info.append(jinfo)

        # When running in standalone mode, we want to record the number of jobs in the task
        if ('CRAB_ReqName' in kwargs['task']) and ('CRAB_UserDN' in kwargs['task']):
            const = 'TaskType =?= \"ROOT\" && CRAB_ReqName =?= "%s" && CRAB_UserDN =?= "%s"' % (task_name, userdn)
            cmd = "condor_qedit -const '%s' CRAB_JobCount %d" % (const, len(dagSpecs))
            self.logger.debug("+ %s" % cmd)
            status, output = commands.getstatusoutput(cmd)
            if status:
                self.logger.error(output)
                self.logger.error("Failed to record the number of jobs.")
                return 1

        return info, splitterResult, subdags


    def extractMonitorFiles(self, inputFiles, **kw):
        """
        Ops mon needs access to some files from the debug_files.tar.gz or sandbox.tar.gz.
        tarball.
        If an older client is used, the files are in the sandbox, the newer client (3.3.1607)
        separates them into a debug_file tarball to allow sandbox recycling and not break the ops mon.
        The files are extracted here to the debug folder to be later sent to the schedd.

        Modifies inputFiles list by appending the debug folder if the extraction succeeds.
        """
        tarFileName = 'sandbox.tar.gz' if not os.path.isfile('debug_files.tar.gz') else 'debug_files.tar.gz'
        try:
            debugTar = tarfile.open(tarFileName)
            debugTar.extract('debug/crabConfig.py')
            debugTar.extract('debug/originalPSet.py')
            scriptExeName = kw['task'].get('tm_scriptexe')
            if scriptExeName != None:
                debugTar.extract(scriptExeName)
                shutil.copy(scriptExeName, 'debug/' + scriptExeName)
            debugTar.close()

            inputFiles.append('debug')

            # Change permissions of extracted files to allow Ops mon to read them.
            for _, _, filenames in os.walk('debug'):
                for f in filenames:
                    os.chmod('debug/' + f, 0o644)
        except Exception as ex:
            self.logger.exception(ex)
            self.uploadWarning("Extracting files from %s failed, ops monitor will not work." % tarFileName, \
                    kw['task']['user_proxy'], kw['task']['tm_taskname'])

        return


    def getHighPrioUsers(self, userProxy, workflow, egroups):
        # Import needed because the DagmanCreator module is also imported in the schedd,
        # where there is no ldap available. This function however is only called
        # in the TW (where ldap is installed) during submission.
        from ldap import LDAPError

        highPrioUsers = set()
        try:
            for egroup in egroups:
                highPrioUsers.update(get_egroup_users(egroup))
        except LDAPError as le:
            msg = "Error when getting the high priority users list." \
                  " Will ignore the high priority list and continue normally." \
                  " Error reason: %s" % str(le)
            self.uploadWarning(msg, userProxy, workflow)
            return []
        return highPrioUsers


    def executeInternal(self, *args, **kw):
        # FIXME: In PanDA, we provided the executable as a URL.
        # So, the filename becomes http:// -- and doesn't really work.  Hardcoding the analysis wrapper.
        #transform_location = getLocation(kw['task']['tm_transformation'], 'CAFUtilities/src/python/transformation/CMSRunAnalysis/')
        transform_location = getLocation('CMSRunAnalysis.sh', 'CRABServer/scripts/')
        cmscp_location = getLocation('cmscp.py', 'CRABServer/scripts/')
        gwms_location = getLocation('gWMS-CMSRunAnalysis.sh', 'CRABServer/scripts/')
        dag_bootstrap_location = getLocation('dag_bootstrap_startup.sh', 'CRABServer/scripts/')
        bootstrap_location = getLocation("dag_bootstrap.sh", "CRABServer/scripts/")
        adjust_location = getLocation("AdjustSites.py", "CRABServer/scripts/")

        shutil.copy(transform_location, '.')
        shutil.copy(cmscp_location, '.')
        shutil.copy(gwms_location, '.')
        shutil.copy(dag_bootstrap_location, '.')
        shutil.copy(bootstrap_location, '.')
        shutil.copy(adjust_location, '.')

        # Bootstrap the ISB if we are using UFC
        if UserFileCache and kw['task']['tm_cache_url'].find('/crabcache')!=-1:
            ufc = UserFileCache(mydict={'cert': kw['task']['user_proxy'], 'key': kw['task']['user_proxy'], 'endpoint' : kw['task']['tm_cache_url']})
            try:
                ufc.download(hashkey=kw['task']['tm_user_sandbox'].split(".")[0], output="sandbox.tar.gz")
            except Exception as ex:
                self.logger.exception(ex)
                raise TaskWorkerException("The CRAB3 server backend could not download the input sandbox with your code "+\
                                    "from the frontend (crabcache component).\nThis could be a temporary glitch; please try to submit a new task later "+\
                                    "(resubmit will not work) and contact the experts if the error persists.\nError reason: %s" % str(ex)) #TODO url!?
            kw['task']['tm_user_sandbox'] = 'sandbox.tar.gz'

            # For an older client (<3.3.1607) this field will be empty and the file will not exist.
            if kw['task']['tm_debug_files']:
                try:
                    ufc.download(hashkey=kw['task']['tm_debug_files'].split(".")[0], output="debug_files.tar.gz")
                except Exception as ex:
                    self.logger.exception(ex)

        # Bootstrap the runtime if it is available.
        job_runtime = getLocation('CMSRunAnalysis.tar.gz', 'CRABServer/')
        shutil.copy(job_runtime, '.')
        task_runtime = getLocation('TaskManagerRun.tar.gz', 'CRABServer/')
        shutil.copy(task_runtime, '.')

        kw['task']['resthost'] = self.server['host']
        kw['task']['resturinoapi'] = self.restURInoAPI
        self.task = kw['task']

        params = {}
        if kw['task']['tm_dry_run'] == 'F':
            params = self.sendDashboardTask()

        inputFiles = ['gWMS-CMSRunAnalysis.sh', 'CMSRunAnalysis.sh', 'cmscp.py', 'RunJobs.dag', 'Job.submit', 'dag_bootstrap.sh',
                      'AdjustSites.py', 'site.ad', 'site.ad.json', 'datadiscovery.pkl', 'taskinformation.pkl', 'run_and_lumis.tar.gz', 'input_files.tar.gz']

        self.extractMonitorFiles(inputFiles, **kw)

        if kw['task'].get('tm_user_sandbox') == 'sandbox.tar.gz':
            inputFiles.append('sandbox.tar.gz')
        if os.path.exists("CMSRunAnalysis.tar.gz"):
            inputFiles.append("CMSRunAnalysis.tar.gz")
        if os.path.exists("TaskManagerRun.tar.gz"):
            inputFiles.append("TaskManagerRun.tar.gz")
        if kw['task']['tm_input_dataset']:
            inputFiles.append("input_dataset_lumis.json")
            inputFiles.append("input_dataset_duplicate_lumis.json")
        if kw['task']['tm_debug_files']:
            inputFiles.append("debug_files.tar.gz")

        info, splitterResult, subdags = self.createSubdag(*args, **kw)

        return info, params, inputFiles + subdags, splitterResult


    def execute(self, *args, **kw):
        cwd = os.getcwd()
        try:
            os.chdir(kw['tempDir'])
            info, params, inputFiles, splitterResult = self.executeInternal(*args, **kw)
            return TaskWorker.DataObjects.Result.Result(task = kw['task'], result = (info, params, inputFiles, splitterResult))
        finally:
            os.chdir(cwd)

