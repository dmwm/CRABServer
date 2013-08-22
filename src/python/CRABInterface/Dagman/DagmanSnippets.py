"""
    DagmanSnippets.py - Stores the little bits of classad we need all over the place
"""
import re

CRAB_META_HEADERS = \
"""
+CRAB_SplitAlgo = %(splitalgo)s
+CRAB_AlgoArgs = %(algoargs)s
+CRAB_ConfigDoc = %(configdoc)s
+CRAB_PublishName = %(publishname)s
+CRAB_DBSUrl = %(dbsurl)s
+CRAB_PublishDBSUrl = %(publishdbsurl)s
+CRAB_LumiMask = %(lumimask)s
"""

CRAB_HEADERS = \
"""
+CRAB_ReqName = "%(requestname)s"
+CRAB_Workflow = %(workflow)s
+CRAB_JobType = %(jobtype)s
+CRAB_JobSW = %(jobsw)s
+CRAB_JobArch = %(jobarch)s
+CRAB_InputData = %(inputdata)s
+CRAB_OutputData = %(publishname)s
+CRAB_ISB = %(cacheurl)s
+CRAB_SiteBlacklist = %(siteblacklist)s
+CRAB_SiteWhitelist = %(sitewhitelist)s
+CRAB_AdditionalOutputFiles = %(addoutputfiles)s
+CRAB_EDMOutputFiles = %(edmoutfiles)s
+CRAB_TFileOutputFiles = %(tfileoutfiles)s
+CRAB_SaveLogsFlag = %(savelogsflag)s
+CRAB_UserDN = %(userdn)s
+CRAB_UserHN = %(userhn)s
+CRAB_AsyncDest = %(asyncdest)s
+CRAB_BlacklistT1 = %(blacklistT1)s
should_transfer_files = YES
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
should_transfer_files = YES
transfer_input_files = %(inputFilesString)s
transfer_output_files = %(outputFilesString)s
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
on_exit_remove = ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))
+OtherJobRemoveRequirements = DAGManJobId =?= ClusterId
remove_kill_sig = SIGUSR1
+HoldKillSig = "SIGUSR1"
on_exit_hold = (ExitCode =!= UNDEFINED && ExitCode != 0)
+Environment= strcat("PATH=/usr/bin:/bin:/opt/glidecondor/bin CONDOR_ID=", ClusterId, ".", ProcId, " %(additional_environment_options)s")
+RemoteCondorSetup = "%(remote_condor_setup)s"
+TaskType = "ROOT"
X509UserProxy = %(userproxy)s
queue 1
"""

SUBMIT_INFO = [ \
            ('CRAB_Workflow', 'workflow'),
            ('CRAB_ReqName', 'requestname'),
            ('CRAB_JobType', 'jobtype'),
            ('CRAB_JobSW', 'jobsw'),
            ('CRAB_JobArch', 'jobarch'),
            ('CRAB_InputData', 'inputdata'),
            ('CRAB_ISB', 'cacheurl'),
            ('CRAB_SiteBlacklist', 'siteblacklist'),
            ('CRAB_SiteWhitelist', 'sitewhitelist'),
            ('CRAB_AdditionalOutputFiles', 'addoutputfiles'),
            ('CRAB_EDMOutputFiles', 'edmoutfiles'),
            ('CRAB_TFileOutputFiles', 'tfileoutfiles'),
            ('CRAB_SaveLogsFlag', 'savelogsflag'),
            ('CRAB_UserDN', 'userdn'),
            ('CRAB_UserHN', 'userhn'),
            ('CRAB_AsyncDest', 'asyncdest'),
            ('CRAB_BlacklistT1', 'blacklistT1'),
            ('CRAB_SplitAlgo', 'splitalgo'),
            ('CRAB_AlgoArgs', 'algoargs'),
            ('CRAB_PublishName', 'publishname'),
            ('CRAB_DBSUrl', 'dbsurl'),
            ('CRAB_PublishDBSUrl', 'publishdbsurl'),
            ('CRAB_LumiMask', 'lumimask')]


DAG_FRAGMENT = """
JOB Job%(count)d Job.submit
#SCRIPT PRE  Job%(count)d dag_bootstrap.sh PREJOB $RETRY $JOB
#SCRIPT POST Job%(count)d dag_bootstrap.sh POSTJOB $RETRY $JOB
#PRE_SKIP Job%(count)d 3
#TODO: Disable retries for now - fail fast to help debug
#RETRY Job%(count)d 3
VARS Job%(count)d count="%(count)d" runAndLumiMask="%(runAndLumiMask)s" inputFiles="%(inputFiles)s" +DESIRED_Sites="\\"%(desiredSites)s\\"" +CRAB_localOutputFiles="\\"%(localOutputFiles)s\\""

JOB ASO%(count)d ASO.submit
VARS ASO%(count)d count="%(count)d" outputFiles="%(remoteOutputFiles)s"
RETRY ASO%(count)d 3

PARENT Job%(count)d CHILD ASO%(count)d
"""

DAG_FRAGMENT_WORKAROUND = """
JOB Job%(count)d Job.submit.%(count)d
#SCRIPT PRE  Job%(count)d dag_bootstrap.sh PREJOB $RETRY $JOB
#SCRIPT POST Job%(count)d dag_bootstrap.sh POSTJOB $RETRY $JOB
#PRE_SKIP Job%(count)d 3
#TODO: Disable retries for now - fail fast to help debug
#RETRY Job%(count)d 3
VARS Job%(count)d count="%(count)d" runAndLumiMask="%(runAndLumiMask)s" inputFiles="%(inputFiles)s"

JOB ASO%(count)d ASO.submit
VARS ASO%(count)d count="%(count)d" outputFiles="%(remoteOutputFiles)s"
RETRY ASO%(count)d 3

PARENT Job%(count)d CHILD ASO%(count)d
"""

# NOTE: keep Arugments in sync with PanDAInjection.py.  ASO is very picky about argument order.
JOB_SUBMIT = CRAB_HEADERS + \
"""
CRAB_Attempt = %(attempt)d
CRAB_ISB = %(cacheurl_flatten)s
CRAB_AdditionalOutputFiles = %(addoutputfiles_flatten)s
CRAB_JobSW = %(jobsw_flatten)s
CRAB_JobArch = %(jobarch_flatten)s
CRAB_Archive = %(cachefilename_flatten)s
+CRAB_ReqName = "%(requestname)s"
#CRAB_ReqName = %(requestname_flatten)s
CRAB_DBSURL = %(dbsurl_flatten)s
CRAB_PublishDBSURL = %(publishdbsurl_flatten)s
CRAB_Publish = %(publication)s
CRAB_AvailableSites = %(available_sites_flatten)s
CRAB_Id = $(count)
+CRAB_Id = $(count)
+CRAB_Dest = "cms://%(temp_dest)s"
+TaskType = "Job"

+JOBGLIDEIN_CMSSite = "$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\"Unknown\\", GLIDEIN_CMSSite)])"
job_ad_information_attrs = MATCH_EXP_JOBGLIDEIN_CMSSite, JOBGLIDEIN_CMSSite

universe = vanilla
Executable = gWMS-CMSRunAnalysis.sh
Output = job_out.$(CRAB_Id)
Error = job_err.$(CRAB_Id)
Log = job_log.$(CRAB_Id)
# args changed...

Arguments = "-a $(CRAB_Archive) --sourceURL=$(CRAB_ISB) --jobNumber=$(CRAB_Id) --cmsswVersion=$(CRAB_JobSW) --scramArch=$(CRAB_JobArch) '--inputFile=$(inputFiles)' '--runAndLumis=$(runAndLumiMask)' -o $(CRAB_AdditionalOutputFiles)"

transfer_input_files = CMSRunAnalysis.sh, cmscp.py%(additional_input_files)s
transfer_output_files = jobReport.json.$(count)
Environment = SCRAM_ARCH=$(CRAB_JobArch);%(additional_environment_options)s
should_transfer_files = YES
#x509userproxy = %(x509up_file)s
use_x509userproxy = true
# TODO: Uncomment this when we get out of testing mode
Requirements = (target.IS_GLIDEIN =!= TRUE) || (target.GLIDEIN_CMSSite =!= UNDEFINED)
#Requirements = ((target.IS_GLIDEIN =!= TRUE) || ((target.GLIDEIN_CMSSite =!= UNDEFINED) && (stringListIMember(target.GLIDEIN_CMSSite, DESIRED_SEs) )))
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
queue
"""

ASYNC_SUBMIT = CRAB_HEADERS + \
"""
+TaskType = "ASO"
+CRAB_Id = $(count)
CRAB_AsyncDest = %(asyncdest_flatten)s

universe = local
Executable = dag_bootstrap.sh
Arguments = "ASO $(CRAB_AsyncDest) %(temp_dest)s %(output_dest)s $(count) $(Cluster).$(Process) cmsRun_$(count).log.tar.gz $(outputFiles)"
Output = aso.$(count).out
transfer_input_files = job_log.$(count), jobReport.json.$(count)%(additional_input_files)s
+TransferOutput = ""
Error = aso.$(count).err
Environment = PATH=/usr/bin:/bin;%(additional_environment_options)s
use_x509userproxy true
#x509userproxy = %(x509up_file)s
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
queue
"""

SPLIT_ARG_MAP = { "LumiBased" : "lumis_per_job",
                  "FileBased" : "files_per_job",}

HTCONDOR_78_WORKAROUND = True

LOGGER = None


MASTER_DAG_FILE = \
"""
JOB DBSDiscovery DBSDiscovery.submit
JOB JobSplitting JobSplitting.submit
SUBDAG EXTERNAL RunJobs RunJobs.dag

PARENT DBSDiscovery CHILD JobSplitting
PARENT JobSplitting CHILD RunJobs
"""

CRAB_META_HEADERS = \
"""
+CRAB_SplitAlgo = %(splitalgo)s
+CRAB_AlgoArgs = %(algoargs)s
+CRAB_PublishName = %(publishname)s
+CRAB_DBSUrl = %(dbsurl)s
+CRAB_PublishDBSUrl = %(publishdbsurl)s
+CRAB_LumiMask = %(lumimask)s
"""

JOB_SPLITTING_SUBMIT_FILE = CRAB_HEADERS + CRAB_META_HEADERS + \
"""
+TaskType = "SPLIT"
universe = local
Executable = dag_bootstrap.sh
Output = job_splitting.out
Error = job_splitting.err
Args = SPLIT dbs_results job_splitting_results
transfer_input = dbs_results%(additional_input_files)s
transfer_output_files = splitting_results
# TODO - need to clean this bit up
#Environment = PATH=/usr/bin:/bin
Environment = PATH=/usr/bin:/bin;%(additional_environment_options)s
#x509userproxy = %(x509up_file)s
use_x509userproxy = true
queue
"""

DBS_DISCOVERY_SUBMIT_FILE = CRAB_HEADERS + CRAB_META_HEADERS + \
"""
+TaskType = "DBS"
universe = local
Executable = dag_bootstrap.sh
Output = dbs_discovery.out
Error = dbs_discovery.err
Args = DBS None dbs_results
transfer_input_files = cmscp.py%(additional_input_files)s
transfer_output_files = dbs_results
Environment = PATH=/usr/bin:/bin;%(additional_environment_options)s
use_x509userproxy = true # %(x509up_file)s
queue
"""

ASYNC_SUBMIT = CRAB_HEADERS + \
"""
+TaskType = "ASO"
+CRAB_Id = $(count)
CRAB_AsyncDest = %(asyncdest_flatten)s

universe = local
Executable = dag_bootstrap.sh
Arguments = "ASO $(CRAB_AsyncDest) %(temp_dest)s %(output_dest)s $(count) $(Cluster).$(Process) cmsRun_$(count).log.tar.gz $(outputFiles)"
Output = aso.$(count).out
transfer_inputFiles = job_log.$(count), jobReport.json.$(count)%(additional_input_files)s
+TransferOutput = ""
Error = aso.$(count).err
#Environment = PATH=/usr/bin:/bin
Environment = %(additional_environment_options)s
#x509userproxy = %(x509up_file)s
use_x509userproxy = true
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
queue
"""

WORKFLOW_RE = re.compile("[a-z0-9_]+")


