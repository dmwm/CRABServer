
import os
import json
import time

import classad
import htcondor

import WMCore.REST.Error as Error
from CRABInterface.Utils import retriveUserCert

import DataWorkflow

master_dag_submit_file = \
"""
JOB DBSDiscovery DBSDiscovery.submit
JOB JobSplitting JobSplitting.submit
SUBDAG EXTERNAL RunJobs RunJobs.dag

PARENT DBSDiscovery CHILD JobSplitting
PARENT JobSplitting CHILD RunJobs
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
+CRAB_Runs = %(runs)s
+CRAB_Lumis = %(lumis)s
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
queue
"""

job_submit = crab_headers + \
"""
CRAB_ISB = %(userisburl_flatten)s
CRAB_AdditionalUserFiles = %(adduserfiles_flatten)s
CRAB_AdditionalOutputFiles = %(addoutputfiles_flatten)s
CRAB_JobSW = %(jobsw_flatten)s
CRAB_JobArch = %(jobarch_flatten)s
CRAB_Archive = %(cachefilename_flatten)s
CRAB_Id = $(count)
+TaskType = "Job"

+JOBGLIDEIN_CMSSite = "$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\"Unknown\\", GLIDEIN_CMSSite)])"
job_ad_information_attrs = MATCH_EXP_JOBGLIDEIN_CMSSite, JOBGLIDEIN_CMSSite

universe = vanilla
Executable = CMSRunAnaly.sh
Output = job_out.$(CRAB_Id)
Error = job_err.$(CRAB_Id)
Log = job_log.$(CRAB_Id)
Arguments = "-o $(CRAB_AdditionalOutputFiles) -a $(CRAB_Archive) --sourceURL=$(CRAB_ISB) '--inputFile=$(inputFiles)' '--lumiMask=$(runAndLumiMask)' --cmsswVersion=$(CRAB_JobSW) --scramArch=$(CRAB_JobArch) --jobNumber=$(CRAB_Id)"
#transfer_input = $(CRAB_ISB)
transfer_output_files = cmsRun-stderr.log?compressCount=3&remoteName=cmsRun_$(count).log, cmsRun-stdout.log?compressCount=3&remoteName=cmsRun_$(count).log, FrameworkJobReport.xml?compressCount=3&remoteName=cmsRun_$(count).log, $(localOutputFiles)
output_destination = cms://%(temp_dest)s
Environment = SCRAM_ARCH=$(CRAB_JobArch)
should_transfer_files = YES
x509userproxy = %(x509up_file)s
# TODO: Uncomment this when we get out of testing mode
# Requirements = GLIDEIN_CMSSite isnt undefined
queue
"""

async_submit = crab_headers + \
"""
CRAB_AsyncDest = %(asyncdest_flatten)s

universe = local
Executable = dag_bootstrap.sh
Arguments = "ASO $(CRAB_AsyncDest) %(temp_dest)s %(output_dest)s $(count) cmsRun_$(count).log.tar.gz $(outputFiles)"
Output = aso.$(count).out
transfer_input_files = job_log.$(count)
Error = aso.$(count).err
Environment = PATH=/usr/bin:/bin
x509userproxy = %(x509up_file)s
queue
"""

#def globalinit(serverkey, servercert, serverdn, uisource, credpath):
    

class DagmanDataWorkflow(DataWorkflow.DataWorkflow):
    """A specialization of the DataWorkflow for submitting to HTCondor DAGMan instead of
       PanDA
    """

    def __init__(self, **kwargs):
        super(DagmanDataWorkflow, self).__init__()
        self.config = None
        if 'config' in kwargs:
            self.config = kwargs['config']

    def getSchedd(self):
        """
        Determine a schedd to use for this task.
        """
        if self.config and hasattr(self.config.General, 'condorScheddList'):
            return random.shuffle(self.config.General.condorScheddList, 1)
        return "localhost"


    def getScheddObj(self, name):
        if name == "localhost":
            schedd = htcondor.Schedd()
            with open(htcondor.param['SCHEDD_ADDRESS_FILE']) as fd:
                address = fd.read()
        else:
            coll = htcondor.Collector(self.getCollector())
            schedd_ad = coll.locate(htcondor.DaemonTypes.Collector, name)
            address = fd.read()
            schedd = htcondor.Schedd(schedd_ad)
        return schedd, address


    def getCollector(self):
        if self.config and hasattr(self.config.General, 'condorPool'):
            return self.config.General.condorPool
        return "localhost"


    def getScratchDir(self):
        """
        Returns a scratch dir for working files.
        """
        return "/tmp/crab3"


    def getBinDir(self):
        """
        Returns the directory of pithy shell scripts
        """
        return os.path.expanduser("~/projects/CRABServer/bin")


    def getTransformLocation(self):
        """
        Returns the location of the PanDA job transform
        """
        return os.path.expanduser("~/projects/CAFUtilities/src/python/transformation/CMSRunAnaly.sh")


    def getRemoteCondorSetup(self):
        """
        Returns the environment setup file for the remote schedd.
        """
        return ""


    @retriveUserCert(clean=False)
    def submit(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
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
        #add the user in the reqmgr database
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
        schedd = self.getSchedd()
        requestname = '%s_%s_%s_%s' % (self.getSchedd(), timestamp, userhn, workflow)

        scratch = self.getScratchDir()
        scratch = os.path.join(scratch, requestname)
        os.makedirs(scratch)

        classad_info = classad.ClassAd()
        for var in 'workflow', 'jobtype', 'jobsw', 'jobarch', 'inputdata', 'siteblacklist', 'sitewhitelist', 'blockwhitelist',\
               'blockblacklist', 'splitalgo', 'algoargs', 'configdoc', 'userisburl', 'cachefilename', 'cacheurl', 'adduserfiles', 'addoutputfiles', 'savelogsflag',\
               'userhn', 'publishname', 'asyncdest', 'campaign', 'blacklistT1', 'dbsurl', 'publishdbsurl', 'tfileoutfiles', 'edmoutfiles', 'userdn',\
               'runs', 'lumis':
            val = locals()[var]
            if val == None:
                classad_info[var] = classad.Value.Undefined
            else:
                classad_info[var] = locals()[var]
        classad_info['requestname'] = requestname

        splitArgName = self.splitArgMap[splitalgo]
        classad_info['algoargs'] = json.dumps({'halt_job_on_file_boundaries': False, 'splitOnRun': False, splitArgName : algoargs})

        info = {}
        for key in classad_info:
            info[key] = classad_info.lookup(key).__repr__()
        for key in ["userisburl", "jobsw", "jobarch", "cachefilename", "asyncdest"]:
            info[key+"_flatten"] = classad_info[key]
        info["adduserfiles_flatten"] = json.dumps(classad_info["adduserfiles"].eval())
        #info["addoutputfiles_flatten"] = json.dumps(classad_info["addoutputfiles"].eval())
        # TODO: PanDA wrapper wants some sort of dictionary.
        info["addoutputfiles_flatten"] = '{}'

        info["output_dest"] = os.path.join("/store/user", userhn, workflow, publishname)
        info["temp_dest"] = os.path.join("/store/temp/user", userhn, workflow, publishname)
        info['x509up_file'] = os.path.split(kwargs['userproxy'])[-1]

        schedd_name = self.getSchedd()
        schedd, address = self.getScheddObj(schedd_name)

        with open(os.path.join(scratch, "master_dag"), "w") as fd:
            fd.write(master_dag_submit_file % info)
        with open(os.path.join(scratch, "DBSDiscovery.submit"), "w") as fd:
            fd.write(dbs_discovery_submit_file % info)
        with open(os.path.join(scratch, "JobSplitting.submit"), "w") as fd:
            fd.write(job_splitting_submit_file % info)
        with open(os.path.join(scratch, "Job.submit"), "w") as fd:
            fd.write(job_submit % info)
        with open(os.path.join(scratch, 'ASO.submit'), 'w') as fd:
            fd.write(async_submit % info)

        dag_ad = classad.ClassAd()
        dag_ad["CRAB_Workflow"] = workflow
        dag_ad["CRAB_UserDN"] = userdn
        dag_ad["JobUniverse"] = 12
        dag_ad["RequestName"] = requestname
        dag_ad["Out"] = os.path.join(scratch, "request.out")
        dag_ad["Err"] = os.path.join(scratch, "request.err")
        dag_ad["Cmd"] = os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh")
        dag_ad["TransferInput"] = ", ".join([os.path.join(self.getBinDir(), "dag_bootstrap.sh"),
            os.path.join(scratch, "master_dag"),
            os.path.join(scratch, "DBSDiscovery.submit"),
            os.path.join(scratch, "JobSplitting.submit"),
            os.path.join(scratch, "Job.submit"),
            os.path.join(scratch, "ASO.submit"),
            self.getTransformLocation(),
        ])
        dag_ad["LeaveJobInQueue"] = classad.ExprTree("(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")
        dag_ad["TransferOutput"] = ""
        dag_ad["OnExitRemove"] = classad.ExprTree("( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))")
        dag_ad["OtherJobRemoveRequirements"] = classad.ExprTree("DAGManJobId =?= ClusterId")
        dag_ad["RemoveKillSig"] = "SIGUSR1"
        dag_ad["Environment"] = classad.ExprTree('strcat("PATH=/usr/bin:/bin CONDOR_ID=", ClusterId, ".", ProcId)')
        dag_ad["RemoteCondorSetup"] = self.getRemoteCondorSetup()
        dag_ad["Requirements"] = classad.ExprTree('true || false')
        dag_ad["TaskType"] = "ROOT"
        dag_ad["X509UserProxy"] = kwargs['userproxy']

        r, w = os.pipe()
        rpipe = os.fdopen(r, 'r')
        wpipe = os.fdopen(w, 'w')
        if os.fork():
            try:
                rpipe.close()
                try:
                    result_ads = []
                    htcondor.SecMan().invalidateAllSessions()
                    cluster = schedd.submit(dag_ad, 1, True, result_ads)
                    schedd.spool(result_ads)
                    wpipe.write("OK")
                    wpipe.close()
                    os._exit(0)
                except Exception, e:
                    wpipe.write(str(e))
            finally:
                os._exit(1)
        wpipe.close()
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when submitting to HTCondor: %s" % results)

        schedd.reschedule()

        return [{'RequestName': requestname}]


    def kill(self, workflow, force, userdn):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""

        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)

        schedd_name = self.getSchedd()
        schedd, address = self.getScheddObj(schedd_name)

        schedd.act(htcondor.JobAction.Remove, 'CRAB_Workflow =?= "%s" && CRAB_UserDN =?= "%s"' % (workflow, userdn))


    def status(self, workflow, userdn):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        workflow = str(workflow)

        self.logger.info("Getting status for workflow %s" % workflow)

        name = workflow.split("_")[0]

        schedd, _ = self.getScheddObj(name)
        ad = classad.ClassAd()
        ad["foo"] = workflow
        results = schedd.query("TaskType =?= \"ROOT\" && RequestName =?= %s" % ad.lookup("foo"), ["JobStatus", 'ExitCode'])

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)

        jobsPerStatus = {}
        jobList = []
        retval = {"status": results[0]['JobStatus'], "taskFailureMsg": "", "jobSetID": "",
            "jobsPerStatus" : jobsPerStatus, "jobList": jobList}
        if results[0]['JobStatus'] == 4: # Job is completed.
            retval["ExitCode"] = results[0]["ExitCode"]

        results = schedd.query("TaskType =?= \"Job\" && RequestName =?= %s" % ad.lookup("foo"), ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID'])
        totalJobdefs = 0
        failedJobdefs = 0
        for result in results:
            totalJobdefs += 1
            if (result['JobStatus'] == 4) and ('ExitCode' in result) and (result['ExitCode']):
                failedJobdefs += 1
            jobsPerStatus[result['JobStatus']] = jobsPerStatus.setdefault(result['JobStatus'], 0) + 1
            jobList.append((result['JobStatus'], result['GlobalJobID']))

        self.logger.debug("Status result for workflow %s: %s" % (workflow, retval))

        return retval

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
    algoargs = 400
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
    dag.submit(workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, cachefilename, cacheurl, adduserfiles, addoutputfiles, savelogsflag,
               userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, vorole, vogroup, publishdbsurl, tfileoutfiles, edmoutfiles, userdn,
               runs, lumis) #TODO delete unused parameters


if __name__ == "__main__":
    main()

