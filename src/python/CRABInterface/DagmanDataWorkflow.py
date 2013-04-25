
import os
import re
import json
import time
import random
import traceback

try:
    import classad
    import htcondor
except ImportError:
    classad = None
    htcondor = None
try:
    import WMCore.BossAir.Plugins.RemoteCondorPlugin as RemoteCondorPlugin
except ImportError:
    if not htcondor:
        raise
#classad = None
#htcondor = None

from WMCore.WMSpec.WMTask import buildLumiMask
from WMCore.Configuration import Configuration
from WMCore.REST.Error import InvalidParameter

import WMCore.REST.Error as Error
from CRABInterface.Utils import retriveUserCert

import DataWorkflow

master_dag_file = \
"""
JOB DBSDiscovery DBSDiscovery.submit
JOB JobSplitting JobSplitting.submit
SUBDAG EXTERNAL RunJobs RunJobs.dag

PARENT DBSDiscovery CHILD JobSplitting
PARENT JobSplitting CHILD RunJobs
"""

resubmit_dag_file = \
"""
JOB JobRecovery JobRecovery.submit
SUBDAG EXTERNAL RecreateJobs RecreateJobs.dag

PARENT JobRecovery CHILD RecreateJobs
"""

crab_headers = \
"""
+CRAB_ReqName = %(requestname)s
+CRAB_Workflow = %(workflow)s
+CRAB_JobType = %(jobtype)s
+CRAB_JobSW = %(jobsw)s
+CRAB_JobArch = %(jobarch)s
+CRAB_InputData = %(inputdata)s
+CRAB_ISB = %(userisburl)s
+CRAB_SiteBlacklist = %(siteblacklist)s
+CRAB_SiteWhitelist = %(sitewhitelist)s
+CRAB_AdditionalUserFiles = %(adduserfiles)s
+CRAB_AdditionalOutputFiles = %(addoutputfiles)s
+CRAB_EDMOutputFiles = %(edmoutfiles)s
+CRAB_TFileOutputFiles = %(tfileoutfiles)s
+CRAB_SaveLogsFlag = %(savelogsflag)s
+CRAB_UserDN = %(userdn)s
+CRAB_UserHN = %(userhn)s
+CRAB_AsyncDest = %(asyncdest)s
+CRAB_Campaign = %(campaign)s
+CRAB_BlacklistT1 = %(blacklistT1)s
"""

crab_meta_headers = \
"""
+CRAB_SplitAlgo = %(splitalgo)s
+CRAB_AlgoArgs = %(algoargs)s
+CRAB_ConfigDoc = %(configdoc)s
+CRAB_PublishName = %(publishname)s
+CRAB_DBSUrl = %(dbsurl)s
+CRAB_PublishDBSUrl = %(publishdbsurl)s
+CRAB_LumiMask = %(lumimask)s
"""

# NOTE: Changes here must be synchronized with the submitDirect function below
master_dag_submit_file = crab_headers + crab_meta_headers + \
"""
+CRAB_Attempt = 0
+CRAB_Workflow = %(workflow)s
+CRAB_UserDN = %(userdn)s
universe = vanilla
+CRAB_ReqName = %(requestname)s
scratch = %(scratch)s
bindir = %(bindir)s
output = $(scratch)/request.out
error = $(scratch)/request.err
executable = $(bindir)/dag_bootstrap_startup.sh
transfer_input_files = $(bindir)/dag_bootstrap.sh, $(scratch)/master_dag, $(scratch)/DBSDiscovery.submit, $(scratch)/JobSplitting.submit, $(scratch)/Job.submit, $(scratch)/ASO.submit, %(transform_location)s
transfer_output_files = master_dag.dagman.out, master_dag.rescue.001, RunJobs.dag, RunJobs.dag.dagman.out, RunJobs.dag.rescue.001, dbs_discovery.err, dbs_discovery.out, job_splitting.err, job_splitting.out
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
on_exit_remove = ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))
+OtherJobRemoveRequirements = DAGManJobId =?= ClusterId
remove_kill_sig = SIGUSR1
+HoldKillSig = "SIGUSR1"
on_exit_hold = (ExitCode =!= UNDEFINED && ExitCode != 0)
+Environment= strcat("PATH=/usr/bin:/bin CONDOR_ID=", ClusterId, ".", ProcId)
+RemoteCondorSetup = %(remote_condor_setup)s
+TaskType = "ROOT"
X509UserProxy = %(userproxy)s
queue 1
"""

resubmit_dag_submit_file = crab_headers + crab_meta_headers + \
"""
+CRAB_Attempt = %(attempt)d
+CRAB_UserDN = %(userdn)s
universe = vanilla
+CRAB_ReqName = %(requestname)s
scratch = %(scratch)s
bindir = %(bindir)s
output = $(scratch)/request.out
error = $(scratch)/request.err
executable = $(bindir)/dag_bootstrap_startup.sh
transfer_input_files = $(bindir)/dag_bootstrap.sh, $(scratch)/resubmit_dag, $(scratch)/JobRecovery.submit, $(scratch)/Job.submit, $(scratch)/ASO.submit, %(transform_location)s
transfer_output_files = resubmit_dag.dagman.out, resubmit_dag.rescue.001, RunJobs.dag, RunJobs.dag.dagman.out, RunJobs.dag.rescue.001, job_recovery.err, job_recovery.out
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
on_exit_remove = ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))
+OtherJobRemoveRequirements = DAGManJobId =?= ClusterId
remove_kill_sig = SIGUSR1
+Environment= strcat("PATH=/usr/bin:/bin CONDOR_ID=", ClusterId, ".", ProcId)
+RemoteCondorSetup = %(remote_condor_setup)s
+TaskType = "ROOT"
X509UserProxy = %(userproxy)s
queue 1
"""

job_splitting_submit_file = crab_headers + crab_meta_headers + \
"""
+TaskType = "SPLIT"
universe = local
Executable = dag_bootstrap.sh
Output = job_splitting.out
Error = job_splitting.err
Args = SPLIT dbs_results job_splitting_results
transfer_input = dbs_results
transfer_output_files = splitting_results
Environment = PATH=/usr/bin:/bin
x509userproxy = %(x509up_file)s
queue
"""

dbs_discovery_submit_file = crab_headers + crab_meta_headers + \
"""
+TaskType = "DBS"
universe = local
Executable = dag_bootstrap.sh
Output = dbs_discovery.out
Error = dbs_discovery.err
Args = DBS None dbs_results
transfer_output_files = dbs_results
Environment = PATH=/usr/bin:/bin
x509userproxy = %(x509up_file)s
queue
"""

job_recovery_submit_file = crab_headers + crab_meta_headers + \
"""
"""

job_submit = crab_headers + \
"""
CRAB_Attempt = %(attempt)d
CRAB_ISB = %(userisburl_flatten)s
CRAB_AdditionalUserFiles = %(adduserfiles_flatten)s
CRAB_AdditionalOutputFiles = %(addoutputfiles_flatten)s
CRAB_JobSW = %(jobsw_flatten)s
CRAB_JobArch = %(jobarch_flatten)s
CRAB_Archive = %(cachefilename_flatten)s
CRAB_Id = $(count)
+CRAB_Id = $(count)
+CRAB_Dest = "cms://%(temp_dest)s"
+TaskType = "Job"

+JOBGLIDEIN_CMSSite = "$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\"Unknown\\", GLIDEIN_CMSSite)])"
job_ad_information_attrs = MATCH_EXP_JOBGLIDEIN_CMSSite, JOBGLIDEIN_CMSSite

universe = vanilla
Executable = gWMS-CMSRunAnaly.sh
Output = job_out.$(CRAB_Id)
Error = job_err.$(CRAB_Id)
Log = job_log.$(CRAB_Id)
Arguments = "-o $(CRAB_AdditionalOutputFiles) -a $(CRAB_Archive) --sourceURL=$(CRAB_ISB) '--inputFile=$(inputFiles)' '--lumiMask=$(runAndLumiMask)' --cmsswVersion=$(CRAB_JobSW) --scramArch=$(CRAB_JobArch) --jobNumber=$(CRAB_Id)"
transfer_input_files = CMSRunAnaly.sh, cmscp.py
transfer_output_files = jobReport.json.$(count)
Environment = SCRAM_ARCH=$(CRAB_JobArch)
should_transfer_files = YES
x509userproxy = %(x509up_file)s
# TODO: Uncomment this when we get out of testing mode
Requirements = ((target.IS_GLIDEIN =!= TRUE) || ((target.GLIDEIN_CMSSite =!= UNDEFINED) && (stringListIMember(target.GLIDEIN_CMSSite, desiredSites) )))
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
queue
"""

async_submit = crab_headers + \
"""
+TaskType = "ASO"
+CRAB_Id = $(count)
CRAB_AsyncDest = %(asyncdest_flatten)s

universe = local
Executable = dag_bootstrap.sh
Arguments = "ASO $(CRAB_AsyncDest) %(temp_dest)s %(output_dest)s $(count) $(Cluster).$(Process) cmsRun_$(count).log.tar.gz $(outputFiles)"
Output = aso.$(count).out
transfer_input_files = job_log.$(count), jobReport.json.$(count)
+TransferOutput = ""
Error = aso.$(count).err
Environment = PATH=/usr/bin:/bin
x509userproxy = %(x509up_file)s
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
queue
"""

WORKFLOW_RE = re.compile("[a-z0-9_]+")

SUBMIT_INFO = [ \
            ('CRAB_Workflow', 'workflow'),
            ('CRAB_ReqName', 'requestname'),
            ('CRAB_JobType', 'jobtype'),
            ('CRAB_JobSW', 'jobsw'),
            ('CRAB_JobArch', 'jobarch'),
            ('CRAB_InputData', 'inputdata'),
            ('CRAB_ISB', 'userisburl'),
            ('CRAB_SiteBlacklist', 'siteblacklist'),
            ('CRAB_SiteWhitelist', 'sitewhitelist'),
            ('CRAB_AdditionalUserFiles', 'adduserfiles'),
            ('CRAB_AdditionalOutputFiles', 'addoutputfiles'),
            ('CRAB_EDMOutputFiles', 'edmoutfiles'),
            ('CRAB_TFileOutputFiles', 'tfileoutfiles'),
            ('CRAB_SaveLogsFlag', 'savelogsflag'),
            ('CRAB_UserDN', 'userdn'),
            ('CRAB_UserHN', 'userhn'),
            ('CRAB_AsyncDest', 'asyncdest'),
            ('CRAB_Campaign', 'campaign'),
            ('CRAB_BlacklistT1', 'blacklistT1'),
            ('CRAB_SplitAlgo', 'splitalgo'),
            ('CRAB_AlgoArgs', 'algoargs'),
            ('CRAB_ConfigDoc', 'configdoc'),
            ('CRAB_PublishName', 'publishname'),
            ('CRAB_DBSUrl', 'dbsurl'),
            ('CRAB_PublishDBSUrl', 'publishdbsurl'),
            ('CRAB_LumiMask', 'lumimask')]

def addCRABInfoToClassAd(ad, info):
    for ad_name, dict_name in SUBMIT_INFO:
        ad[ad_name] = classad.ExprTree(str(info[dict_name]))

def getCRABInfoFromClassAd(ad):
    info = {}
    for ad_name, dict_name in SUBMIT_INFO:
        pass

class DagmanDataWorkflow(DataWorkflow.DataWorkflow):
    """A specialization of the DataWorkflow for submitting to HTCondor DAGMan instead of
       PanDA
    """

    def __init__(self, **kwargs):
        super(DagmanDataWorkflow, self).__init__()
        self.config = None
        if 'config' in kwargs:
            self.config = kwargs['config']
        else:
            self.config = Configuration()
        self.config.section_("BossAir")
        self.config.section_("General")
        if not hasattr(self.config.BossAir, "remoteUserHost"):
            self.config.BossAir.remoteUserHost = "cmssubmit-r1.t2.ucsd.edu"


    def getSchedd(self):
        """
        Determine a schedd to use for this task.
        """
        if not htcondor:
            return self.config.BossAir.remoteUserHost
        collector = None
        if self.config and hasattr(self.config.General, 'condorPool'):
            collector = self.config.General.condorPool
        schedd = "localhost"
        if self.config and hasattr(self.config.General, 'condorScheddList'):
            random.shuffle(self.config.General.condorScheddList)
            schedd = self.config.General.condorScheddList[0]
        if collector:
            return "%s:%s" % (schedd, collector)
        return schedd


    def getScheddObj(self, name):
        if htcondor:
            if name == "localhost":
                schedd = htcondor.Schedd()
                with open(htcondor.param['SCHEDD_ADDRESS_FILE']) as fd:
                    address = fd.read().split("\n")[0]
            else:
                info = name.split(":")
                pool = "localhost"
                if len(info) == 2:
                    pool = info[1]
                coll = htcondor.Collector(self.getCollector(pool))
                schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, info[0])
                address = schedd_ad['MyAddress']
                schedd = htcondor.Schedd(schedd_ad)
            return schedd, address
        else:
            return RemoteCondorPlugin.RemoteCondorPlugin(self.config, logger=self.logger), None


    def getCollector(self, name="localhost"):
        if self.config and hasattr(self.config.General, 'condorPool'):
            return self.config.General.condorPool
        return name


    def getScratchDir(self):
        """
        Returns a scratch dir for working files.
        """
        return "/tmp/crab3"


    def getBinDir(self):
        """
        Returns the directory of pithy shell scripts
        """
        # TODO: Nuke this with the rest of the dev hooks
        dir = os.path.expanduser("~/projects/CRABServer/bin")
        if self.config and hasattr(self.config.General, 'binDir'):
            dir = self.config.General.binDir
        if 'CRAB3_BASEPATH' in os.environ:
            dir = os.path.join(os.environ["CRAB3_BASEPATH"], "bin")
        return os.path.expanduser(dir)


    def getTransformLocation(self):
        """
        Returns the location of the PanDA job transform
        """
        # TODO: Nuke this with the rest of the dev hooks
        dir = "~/projects/CAFUtilities/src/python/transformation"
        if self.config and hasattr(self.config.General, 'transformDir'):
            dir = self.config.General.transformDir
        if 'CRAB3_BASEPATH' in os.environ:
            dir = os.path.join(os.environ["CRAB3_BASEPATH"], "bin")
        return os.path.join(os.path.expanduser(dir), "CMSRunAnaly.sh")


    def getRemoteCondorSetup(self):
        """
        Returns the environment setup file for the remote schedd.
        """
        return ""


    def submitRaw(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, cachefilename, cacheurl, adduserfiles, addoutputfiles, savelogsflag,
               userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, vorole, vogroup, publishdbsurl, tfileoutfiles, edmoutfiles, userdn,
               runs, lumis, **kwargs): #TODO delete unused parameters
        """Perform the workflow injection into the reqmgr + couch

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str list blockwhitelist: selective list of input iblock from the specified input dataset;
           :arg str list blockblacklist:  input blocks to be excluded from the specified input dataset;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str configdoc: URL of the configuration object ot be used;
           :arg str userisburl: URL of the input sandbox file;
           :arg str list adduserfiles: list of additional input files;
           :arg str list addoutputfiles: list of additional output files;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg str userdn: DN of user doing the request;
           :arg str userhn: hyper new name of the user doing the request;
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg str campaign: needed just in case the workflow has to be appended to an existing campaign;
           :arg int blacklistT1: flag enabling or disabling the black listing of Tier-1 sites;
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :arg str list runs: list of run numbers
           :arg str list lumis: list of lumi section numbers
           :returns: a dict which contaians details of the request"""

        self.logger.debug("""workflow %s, jobtype %s, jobsw %s, jobarch %s, inputdata %s, siteblacklist %s, sitewhitelist %s, blockwhitelist %s,
               blockblacklist %s, splitalgo %s, algoargs %s, configdoc %s, userisburl %s, cachefilename %s, cacheurl %s, adduserfiles %s, addoutputfiles %s, savelogsflag %s,
               userhn %s, publishname %s, asyncdest %s, campaign %s, blacklistT1 %s, dbsurl %s, publishdbsurl %s, tfileoutfiles %s, edmoutfiles %s, userdn %s,
               runs %s, lumis %s"""%(workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,\
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, cachefilename, cacheurl, adduserfiles, addoutputfiles, savelogsflag,\
               userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, publishdbsurl, tfileoutfiles, edmoutfiles, userdn,\
               runs, lumis))
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
        schedd = self.getSchedd()
        requestname = '%s_%s_%s_%s' % (self.getSchedd(), timestamp, userhn, workflow)

        scratch = self.getScratchDir()
        scratch = os.path.join(scratch, requestname)
        os.makedirs(scratch)

        # Poor-man's string escaping.  We do this as classad module isn't guaranteed to be present.
        info = {}
        for var in 'workflow', 'jobtype', 'jobsw', 'jobarch', 'inputdata', 'splitalgo', 'algoargs', 'configdoc', 'userisburl', \
               'cachefilename', 'cacheurl', 'userhn', 'publishname', 'asyncdest', 'campaign', 'dbsurl', 'publishdbsurl', \
               'userdn', 'requestname':
            val = locals()[var]
            if val == None:
                info[var] = 'undefined'
            else:
                info[var] = json.dumps(val)

        for var in 'savelogsflag', 'blacklistT1':
            info[var] = int(locals()[var])

        for var in 'siteblacklist', 'sitewhitelist', 'blockwhitelist', 'blockblacklist', 'adduserfiles', 'addoutputfiles', \
               'tfileoutfiles', 'edmoutfiles':
            val = locals()[var]
            if val == None:
                info[var] = "{}"
            else:
                info[var] = "{" + json.dumps(val)[1:-1] + "}"

        #TODO: We don't handle user-specified lumi masks correctly.
        info['lumimask'] = '"' + json.dumps(buildLumiMask(runs, lumis)).replace(r'"', r'\"') + '"'
        splitArgName = self.splitArgMap[splitalgo]
        info['algoargs'] = '"' + json.dumps({'halt_job_on_file_boundaries': False, 'splitOnRun': False, splitArgName : algoargs}).replace('"', r'\"') + '"'
        info['attempt'] = 0

        for var in ["userisburl", "jobsw", "jobarch", "cachefilename", "asyncdest"]:
            info[var+"_flatten"] = locals()[var]
        info["adduserfiles_flatten"] = json.dumps(adduserfiles)

        # TODO: PanDA wrapper wants some sort of dictionary.
        info["addoutputfiles_flatten"] = '{}'

        info["output_dest"] = os.path.join("/store/user", userhn, workflow, publishname)
        info["temp_dest"] = os.path.join("/store/temp/user", userhn, workflow, publishname)
        info['x509up_file'] = os.path.split(kwargs['userproxy'])[-1]
        info['userproxy'] = kwargs['userproxy']
        info['remote_condor_setup'] = self.getRemoteCondorSetup()
        info['bindir'] = self.getBinDir()
        info['scratch'] = scratch
        info['transform_location'] = self.getTransformLocation()

        schedd_name = self.getSchedd()
        schedd, address = self.getScheddObj(schedd_name)

        with open(os.path.join(scratch, "master_dag"), "w") as fd:
            fd.write(master_dag_file % info)
        with open(os.path.join(scratch, "DBSDiscovery.submit"), "w") as fd:
            fd.write(dbs_discovery_submit_file % info)
        with open(os.path.join(scratch, "JobSplitting.submit"), "w") as fd:
            fd.write(job_splitting_submit_file % info)
        with open(os.path.join(scratch, "Job.submit"), "w") as fd:
            fd.write(job_submit % info)
        with open(os.path.join(scratch, 'ASO.submit'), 'w') as fd:
            fd.write(async_submit % info)

        input_files = [os.path.join(self.getBinDir(), "dag_bootstrap.sh"),
                       self.getTransformLocation(),
                       os.path.join(self.getBinDir(), "cmscp.py")]
        scratch_files = ['master_dag', 'DBSDiscovery.submit', 'JobSplitting.submit', 'master_dag', 'Job.submit', 'ASO.submit']
        input_files.extend([os.path.join(scratch, i) for i in scratch_files])

        if address:
            self.submitDirect(schedd, kwargs['userproxy'], scratch, input_files, info)
        else:
            jdl = master_dag_submit_file % info
            schedd.submitRaw(requestname, jdl, kwargs['userproxy'], input_files)

        return [{'RequestName': requestname}]
    submit = retriveUserCert(clean=False)(submitRaw)

    def submitDirect(self, schedd, userproxy, scratch, input_files, info):
        """
        Submit directly to the schedd using the HTCondor module
        """
        dag_ad = classad.ClassAd()
        addCRABInfoToClassAd(dag_ad, info)

        # NOTE: Changes here must be synchronized with the remote-submit variety at the top of this file.
        dag_ad["CRAB_Attempt"] = 0
        dag_ad["JobUniverse"] = 12
        dag_ad["HoldKillSig"] = "SIGUSR1"
        dag_ad["Out"] = os.path.join(scratch, "request.out")
        dag_ad["Err"] = os.path.join(scratch, "request.err")
        dag_ad["Cmd"] = os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh")
        dag_ad["TransferInput"] = ", ".join(input_files)
        dag_ad["LeaveJobInQueue"] = classad.ExprTree("(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")
        dag_ad["TransferOutput"] = "master_dag.dagman.out, master_dag.rescue.001, RunJobs.dag, RunJobs.dag.dagman.out, RunJobs.dag.rescue.001, dbs_discovery.err, dbs_discovery.out, job_splitting.err, job_splitting.out"
        dag_ad["OnExitRemove"] = classad.ExprTree("( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))")
        dag_ad["OtherJobRemoveRequirements"] = classad.ExprTree("DAGManJobId =?= ClusterId")
        dag_ad["RemoveKillSig"] = "SIGUSR1"
        dag_ad["Environment"] = classad.ExprTree('strcat("PATH=/usr/bin:/bin CONDOR_ID=", ClusterId, ".", ProcId)')
        dag_ad["RemoteCondorSetup"] = self.getRemoteCondorSetup()
        dag_ad["Requirements"] = classad.ExprTree('true || false')
        dag_ad["TaskType"] = "ROOT"
        dag_ad["X509UserProxy"] = userproxy

        r, w = os.pipe()
        rpipe = os.fdopen(r, 'r')
        wpipe = os.fdopen(w, 'w')
        if os.fork() == 0:
            try:
                rpipe.close()
                try:
                    result_ads = []
                    htcondor.SecMan().invalidateAllSessions()
                    os.environ['X509_USER_PROXY'] = userproxy
                    cluster = schedd.submit(dag_ad, 1, True, result_ads)
                    schedd.spool(result_ads)
                    wpipe.write("OK")
                    wpipe.close()
                    os._exit(0)
                except Exception, e:
                    wpipe.write(str(traceback.format_exc()))
            finally:
                os._exit(1)
        wpipe.close()
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when submitting HTCondor task: %s" % results)

        schedd.reschedule()


    @retriveUserCert(clean=False)
    def kill(self, workflow, force, userdn, **kwargs):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""

        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)

        userproxy = kwargs['userproxy']
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        schedd_name = self.getSchedd()
        schedd, address = self.getScheddObj(schedd_name)

        const = 'TaskType =?= \"ROOT\" && CRAB_ReqName =?= "%s" && CRAB_UserDN =?= "%s"' % (workflow, userdn)
        if address:
            r, w = os.pipe()
            rpipe = os.fdopen(r, 'r')
            wpipe = os.fdopen(w, 'w')
            if os.fork() == 0:
                try:
                    rpipe.close()
                    try:
                        htcondor.SecMan().invalidateAllSessions()
                        os.environ['X509_USER_PROXY'] = userproxy
                        schedd.act(htcondor.JobAction.Hold, const)
                        wpipe.write("OK")
                        wpipe.close()
                        os._exit(0)
                    except Exception, e:
                        wpipe.write(str(traceback.format_exc()))
                finally:
                    os._exit(1)
            wpipe.close()
            results = rpipe.read()
            if results != "OK":
                raise Exception("Failure when submitting to HTCondor: %s" % results)
        else:
            schedd.hold(const)

        # Search for and hold the sub-dag
        root_const = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= \"%s\" && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)" % workflow
        root_attr_list = ["ClusterId"]
        if address:
            results = schedd.query(root_const, root_attr_list)
        else:
            results = schedd.getClassAds(root_const, root_attr_list)

        if not results:
            return

        sub_dag_const = "DAGManJobId =?= %s && DAGParentNodeNames =?= \"JobSplitting\"" % results[0]["ClusterId"]
        if address:
            sub_dag_results = schedd.query(sub_dag_const, root_attr_list)
        else:
            sub_dag_results = schedd.getClassAds(sub_dag_const, root_attr_list)

        if not sub_dag_results:
            return
        finished_job_const = "DAGManJobId =?= %s && ExitCode =?= 0" % sub_dag_results[0]["ClusterId"]

        if address:
            r, w = os.pipe()
            rpipe = os.fdopen(r, 'r')
            wpipe = os.fdopen(w, 'w')
            if os.fork() == 0:
                try:
                    rpipe.close()
                    try:
                        htcondor.SecMan().invalidateAllSessions()
                        os.environ['X509_USER_PROXY'] = userproxy
                        schedd.edit(sub_dag_const, "HoldKillSig", "\"SIGUSR1\"")
                        schedd.act(htcondor.JobAction.Hold, sub_dag_const)
                        schedd.edit(finished_job_const, "DAGManJobId", "-1")
                        wpipe.write("OK")
                        wpipe.close()
                        os._exit(0)
                    except Exception, e:
                        wpipe.write(str(traceback.format_exc()))
                finally:
                    os._exit(1)
            wpipe.close()
            results = rpipe.read()
            if results != "OK":
                raise Exception("Failure when killing job: %s" % results)
        else:
            schedd.edit(sub_dag_const, "HoldKillSig", "SIGUSR1")
            schedd.hold(const)



    def status(self, workflow, userdn):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        self.logger.info("Getting status for workflow %s" % workflow)

        name = workflow.split("_")[0]

        schedd, address = self.getScheddObj(name)

        root_const = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= \"%s\" && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)" % workflow
        root_attr_list = ["JobStatus", "ExitCode", 'CRAB_JobCount']
        if address:
            results = schedd.query(root_const, root_attr_list)
        else:
            results = schedd.getClassAds(root_const, root_attr_list)

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)

        jobsPerStatus = {}
        jobStatus = {}
        jobList = []
        taskStatusCode = results[0]['JobStatus']
        taskJobCount = results[0].get('CRAB_JobCount', 0)
        codes = {1: 'Idle', 2: 'Running', 4: 'Completed (Success)', 5: 'Killed'}
        retval = {"status": codes.get(taskStatusCode, 'Unknown'), "taskFailureMsg": "", "jobSetID": workflow,
            "jobsPerStatus" : jobsPerStatus, "jobList": jobList}

        job_const = "TaskType =?= \"Job\" && CRAB_ReqName =?= \"%s\"" % workflow
        job_list = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id']
        if address:
            results = schedd.query(job_const, job_list)
        else:
            results = schedd.getClassAds(job_const, job_list)
        failedJobs = []
        for result in results:
            jobState = int(result['JobStatus'])
            if result['CRAB_Id'] in failedJobs:
                failedJobs.remove(result['CRAB_Id'])
            if (jobState == 4) and ('ExitCode' in result) and (result['ExitCode']):
                failedJobs.append(result['CRAB_Id'])
                statusName = "Failed (%s)" % result['ExitCode']
            else:
                statusName = codes.get(jobState, 'Unknown')
            jobStatus[result['CRAB_Id']] = statusName

        job_const = "TaskType =?= \"ASO\" && CRAB_ReqName =?= \"%s\"" % workflow
        job_list = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id']
        if address:
            results = schedd.query(job_const, job_list)
        else:
            results = schedd.getClassAds(job_const, job_list)
        aso_codes = {1: 'ASO Queued', 2: 'ASO Running', 4: 'Stageout Complete (Success)'}
        for result in results:
            if result['CRAB_Id'] in failedJobs:
                failedJobs.remove(result['CRAB_Id'])
            jobState = int(result['JobStatus'])
            if (jobState == 4) and ('ExitCode' in result) and (result['ExitCode']):
                failedJobs.append(result['CRAB_Id'])
                statusName = "Failed Stage-Out (%s)" % result['ExitCode']
            else:
                statusName = aso_codes.get(jobState, 'Unknown')
            jobStatus[result['CRAB_Id']] = statusName

        for i in range(1, taskJobCount+1):
            if i not in jobStatus:
                if taskStatusCode == 5:
                    jobStatus[i] = 'Killed'
                else:
                    jobStatus[i] = 'Unsubmitted'

        for job, status in jobStatus.items():
            jobsPerStatus.setdefault(status, 0)
            jobsPerStatus[status] += 1
            jobList.append((status, job))

        retval["failedJobdefs"] = len(failedJobs)
        retval["totalJobdefs"] = len(jobStatus)

        if len(jobStatus) == 0 and taskJobCount == 0 and taskStatusCode == 2:
            retval['status'] = 'Running (jobs not submitted)'

        self.logger.info("Status result for workflow %s: %s" % (workflow, retval))
        #print "Status result for workflow %s: %s" % (workflow, retval)

        return retval


    def outputLocation(self, workflow, max, pandaids):
        """
        Retrieves the output LFN from async stage out

        :arg str workflow: the unique workflow name
        :arg int max: the maximum number of output files to retrieve
        :return: the result of the view as it is."""
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        name = workflow.split("_")[0]
        schedd, address = self.getScheddObj(name)

        job_const = 'TaskType =?= \"ASO\"&& CRAB_ReqName =?= \"%s\"' % workflow
        job_list = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'GlobalJobId', 'OutputSizes', 'OutputPFNs']

        if address:
            results = schedd.query(job_const, job_list)
        else:
            results = schedd.getClassAds(job_const, job_list)
        files = []
        for result in results:
            try:
                outputSizes = [int(i.strip()) for i in result.get("OutputSizes", "").split(",") if i]
            except ValueError:
                self.logger.info("Invalid OutputSizes (%s) for workflow %s" % (result.get("OutputSizes", ""), workflow))
                raise InvalidParameter("Internal state had invalid OutputSize.")
            outputFiles = [i.strip() for i in result.get("OutputPFNs", "").split(",")]
            for idx in range(min(len(outputSizes), len(outputFiles))):
                files.append({'pfn': outputFiles[idx], 'size': outputSizes[idx]})

        if max > 0:
            return {'result': files[:max]}
        return {'result': files}

    @retriveUserCert(clean=False)
    def resubmit(self, workflow, siteblacklist, sitewhitelist, userdn, **kwargs):
        # TODO: In order to take advantage of the updated white/black list, we need
        # to sneak those into the resubmitted DAG.

        self.logger.info("About to resubmit workflow: %s." % workflow)

        userproxy = kwargs['userproxy']
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        schedd_name = self.getSchedd()
        schedd, address = self.getScheddObj(schedd_name)

        # Search for and hold the sub-dag
        root_const = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= \"%s\" && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)" % workflow
        root_attr_list = ["ClusterId"]
        if address:
            results = schedd.query(root_const, root_attr_list)
        else:
            results = schedd.getClassAds(root_const, root_attr_list)

        if not results:
            return

        sub_dag_const = "DAGManJobId =?= %s && DAGParentNodeNames =?= \"JobSplitting\"" % results[0]["ClusterId"]
        if address:
            r, w = os.pipe()
            rpipe = os.fdopen(r, 'r')
            wpipe = os.fdopen(w, 'w')
            if os.fork() == 0:
                try:
                    rpipe.close()
                    try:
                        htcondor.SecMan().invalidateAllSessions()
                        os.environ['X509_USER_PROXY'] = userproxy
                        schedd.act(htcondor.JobAction.Release, sub_dag_const)
                        schedd.act(htcondor.JobAction.Release, root_const)
                        wpipe.write("OK")
                        wpipe.close()
                        os._exit(0)
                    except Exception, e:
                        wpipe.write(str(traceback.format_exc()))
                finally:
                    os._exit(1)
            wpipe.close()
            results = rpipe.read()
            if results != "OK":
                raise Exception("Failure when killing job: %s" % results)
        else:
            schedd.release(sub_dag_const)
            schedd.release(root_const)
        

# Hold onto this for later -- we'll want to resubmit a separate DAG if we
# want to resplit
"""
    def resubmit(self, workflow, siteblacklist, sitewhitelist, userdn):

        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")
        self.logger.info("Resubmitting workflow %s" % workflow)
        name = workflow.split("_")[0]
        schedd, address = self.getScheddObj(name)

        # Retrieve the original task ClassAd
        root_const = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= \"%s\"" % workflow
        root_attr_list = []
        if address:
            results = schedd.query(root_const, root_attr_list)
        else:
            results = schedd.getClassAds(root_const, root_attr_list)
        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)
        attempt = len(results)
        original_task = [i for i in results if str(i.get('CRAB_Attempt', '-1')) == '0']
        if not original_task:
            raise InvalidParameter("Unable to determine original task for workflow.")
        original_task = original_task[0]

        # Reconstruct the info dictionary from the ClassAd.
        info = getCRABInfoFromClassAd(ad)
        for var in ["userisburl", "jobsw", "jobarch", "cachefilename", "asyncdest"]:
            info[var+"_flatten"] = locals()[var]
        info["adduserfiles_flatten"] = json.dumps(adduserfiles)
        info["addoutputfiles_flatten"] = '{}'
        info["output_dest"] = os.path.join("/store/user", userhn, workflow, publishname)
        info["temp_dest"] = os.path.join("/store/temp/user", userhn, workflow, publishname)
        info['x509up_file'] = os.path.split(kwargs['userproxy'])[-1]
        info['userproxy'] = kwargs['userproxy']
        info['remote_condor_setup'] = self.getRemoteCondorSetup()
        info['bindir'] = self.getBinDir()
        info['scratch'] = scratch
        info['transform_location'] = self.getTransformLocation()

        # Submit
        with open(os.path.join(scratch, "resubmit_dag"), "w") as fd:
            fd.write(master_dag_file % info)
        with open(os.path.join(scratch, "JobRecovery.submit"), "w") as fd:
            fd.write(job_recovery_file % info)
        with open(os.path.join(scratch, "Job.submit"), "w") as fd:
            fd.write(job_submit % info)
        with open(os.path.join(scratch, 'ASO.submit'), 'w') as fd:
            fd.write(async_submit % info)

        input_files = [os.path.join(self.getBinDir(), "dag_bootstrap.sh"),
                       self.getTransformLocation(),
                       os.path.join(self.getBinDir(), "cmscp.py")]
        scratch_files = ['JobRecovery.submit', 'resubmit_dag', 'Job.submit', 'ASO.submit']
        input_files.extend([os.path.join(scratch, i) for i in scratch_files])

        if address:
            self.resubmitDirect(schedd, kwargs['userproxy'], scratch, input_files, info)
        else:
            jdl = resubmit_dag_submit_file % info
            schedd.submitRaw(requestname, jdl, kwargs['userproxy'], input_files)

        return [{'RequestName': requestname}]
    submit = retriveUserCert(clean=False)(submitRaw)

    def resubmitDirect(self, schedd, userproxy, scratch, input_files):
        "\
        Resubmit directly to the schedd using the HTCondor module\
        "
        dag_ad = classad.ClassAd()
        addCRABInfoToClassAd(dag_ad, info)
        dag_ad["CRAB_Attempt"] = 0
        dag_ad["CRAB_UserDN"] = userdn
        dag_ad["JobUniverse"] = 12
        dag_ad["CRAB_ReqName"] = requestname
        dag_ad["Out"] = os.path.join(scratch, "request.out")
        dag_ad["Err"] = os.path.join(scratch, "request.err")
        dag_ad["Cmd"] = os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh")
        dag_ad["TransferInput"] = ", ".join(input_files)
        dag_ad["LeaveJobInQueue"] = classad.ExprTree("(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")
        dag_ad["TransferOutput"] = "master_dag.dagman.out, master_dag.rescue.001, RunJobs.dag, RunJobs.dag.dagman.out, RunJobs.dag.rescue.001, dbs_discovery.err, dbs_discovery.out, job_splitting.err, job_splitting.out"
        dag_ad["OnExitRemove"] = classad.ExprTree("( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))")
        dag_ad["OtherJobRemoveRequirements"] = classad.ExprTree("DAGManJobId =?= ClusterId")
        dag_ad["RemoveKillSig"] = "SIGUSR1"
        dag_ad["Environment"] = classad.ExprTree('strcat("PATH=/usr/bin:/bin CONDOR_ID=", ClusterId, ".", ProcId)')
        dag_ad["RemoteCondorSetup"] = self.getRemoteCondorSetup()
        dag_ad["Requirements"] = classad.ExprTree('true || false')
        dag_ad["TaskType"] = "ROOT"
        dag_ad["X509UserProxy"] = userproxy

        r, w = os.pipe()
        rpipe = os.fdopen(r, 'r')
        wpipe = os.fdopen(w, 'w')
        if os.fork() == 0:
            try:
                rpipe.close()
                try:
                    result_ads = []
                    htcondor.SecMan().invalidateAllSessions()
                    os.environ['X509_USER_PROXY'] = userproxy
                    cluster = schedd.submit(dag_ad, 1, True, result_ads)
                    schedd.spool(result_ads)
                    wpipe.write("OK")
                    wpipe.close()
                    os._exit(0)
                except Exception, e:
                    wpipe.write(str(traceback.format_exc()))
            finally:
                os._exit(1)
        wpipe.close()
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing HTCondor task: %s" % results)

        schedd.reschedule()
"""

def main():
    dag = DagmanDataWorkflow()
    workflow = 'bbockelm'
    jobtype = 'analysis'
    jobsw = 'CMSSW_5_3_7'
    jobarch = 'slc5_amd64_gcc462'
    inputdata = '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO'
    siteblacklist = []
    sitewhitelist = ['T2_US_Nebraska']
    blockwhitelist = []
    blockblacklist = []
    splitalgo = "LumiBased"
    algoargs = 40
    configdoc = ''
    userisburl = 'https://voatlas178.cern.ch:25443'
    cachefilename = 'default.tgz'
    cacheurl = 'https://voatlas178.cern.ch:25443'
    adduserfiles = []
    addoutputfiles = []
    savelogsflag = False
    userhn = 'bbockelm'
    publishname = ''
    asyncdest = 'T2_US_Nebraska'
    campaign = ''
    blacklistT1 = True
    dbsurl = ''
    vorole = 'cmsuser'
    vogroup = ''
    publishdbsurl = ''
    tfileoutfiles = []
    edmoutfiles = []
    userdn = '/CN=Brian Bockelman'
    runs = []
    lumis = []
    dag.submitRaw(workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, cachefilename, cacheurl, adduserfiles, addoutputfiles, savelogsflag,
               userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, vorole, vogroup, publishdbsurl, tfileoutfiles, edmoutfiles, userdn,
               runs, lumis, userproxy = '/tmp/x509up_u%d' % os.geteuid()) #TODO delete unused parameters


if __name__ == "__main__":
    main()

