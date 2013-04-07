
import time

try:
    import classad
    import htcondor
except:
    pass

import WMCore.REST.Error as Error

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
+CRAB.ReqName = %{requestname}s
+CRAB.Workflow = %{workflow}s
+CRAB.JobType = %{jobtype}s
+CRAB.JobSW = %{jobsw}s
+CRAB.JobArch = %{jobarch}s
+CRAB.InputData = %{inputdata}s
+CRAB.ISB = %{userisburl}s
+CRAB.SiteBlacklist = %{siteblacklist}s
+CRAB.SiteWhitelist = %{sitewhitelist}s
+CRAB.AdditionalUserFiles = %{adduserfiles}s
+CRAB.AdditionalOutputFiles = %{addoutputfiles}s
+CRAB.SaveLogsFlag = %{savelogsflag}d
+CRAB.UserDN = %{userdn}s
+CRAB.UserHN = %{userhn}s
+CRAB.AsyncDest = %{asyncdest}s
+CRAB.Campaign = %{campaign}s
+CRAB.BlacklistT1 = %{blacklistT1}d
"""

crab_meta_headers = \
"""
+CRAB.SplitAlgo = %{splitalgo}s
+CRAB.AlgoArgs = %{algoargs}s
+CRAB.ConfigDoc = %{configdoc}s
+CRAB.PublishName = %{publishname}s
+CRAB.DBSUrl = %{dbsurl}s
+CRAB.PublishDBSUrl = %{publishdbsurl}s
+CRAB.Runs = %{runs}s
+CRAB.Lumis = %{lumis}s
"""

job_splitting_submit_file = crab_headers + crab_meta_headers + \
"""
universe = local
Executable = dag_bootstrap.sh
Output = job_splitting.out
Error = job_splitting.err
Args = SPLIT dbs_results job_splitting_results
transfer_input = dbs_results
transfer_output = splitting_results
"""

dbs_discovery_submit_file = crab_headers + crab_meta_headers + \
"""
universe = local
Executable = dag_bootstrap.sh
Output = dbs_discovery.out
Error = dbs_discovery.err
Args = DBS None dbs_results
transfer_output = dbs_results
"""

job_submit = crab_headers + \
"""
CRAB.Id = $(count)d
universe = vanilla
Executable = CMSRunAnaly.sh
Output = $(CRAB.Workflow)/out.$(CRAB.Id)
Error = $(CRAB.Workflow)/err.$(CRAB.Id)
Args = -o $(CRAB.AdditionalOutputFiles) --sourceURL $(CRAB.ISB) --inputFile $(CRAB.AdditionalUserFiles) --lumiMask lumimask.$(CRAB.Id) --cmsswVersion $(CRAB.JobSW) --scramArch $(CRAB.JobArch) --jobNumber $(CRAB.Id)
transfer_input = $(CRAB.ISB), $(CRAB.Workflow)/lumimask.$(CRAB.Id), http://common-analysis-framework.cern.ch/CMSRunAnaly.tgz
x509userproxy = x509up
"""

async_submit = crab_headers + \
"""
universe = local
Executable = dag_bootstrap.sh
Args = ASO $(count)
Output = aso.$(count).out
Error = aso.$(count).err
"""

class DagmanDataWorkflow(DataWorkflow.DataWorkflow):
    """A specialization of the DataWorkflow for submitting to HTCondor DAGMan instead of
       PanDA
    """

    def getSchedd():
        """
        Determine a schedd to use for this task.
        """
        return "localhost"


    def getScheddObj(name):
        if name == "localhost":
            schedd = htcondor.Schedd()
        else:
            coll = htcondor.Collector(self.getCollector())
            schedd_ad = coll.locate(htcondor.DaemonTypes.Collector, name)
            schedd = htcondor.Schedd(schedd_ad)
        return schedd


    def getCollector():
        return "localhost"


    def getScratchDir():
        """
        Returns a scratch dir for working files.
        """
        return "/tmp/crab3"


    def getBinDir():
        """
        Returns the directory of pithy shell scripts
        """
        return "/User/bbockelm/projects/CRABServer/bin"


    def getRemoteCondorSetup():
        """
        Returns the environment setup file for the remote schedd.
        """
        return ""


    @retriveUserCert(clean=False)
    def submit(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, blockwhitelist,
               blockblacklist, splitalgo, algoargs, configdoc, userisburl, cachefilename, cacheurl, adduserfiles, addoutputfiles, savelogsflag,
               userhn, publishname, asyncdest, campaign, blacklistT1, dbsurl, vorole, vogroup, publishdbsurl, tfileoutfiles, edmoutfiles, userdn,
               runs, lumis): #TODO delete unused parameters
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
        os.path.mkdirs(scratch)

        info = {}
        for var in 'workflow', 'jobtype', 'jobsw', 'jobarch', 'inputdata', 'siteblacklist', 'sitewhitelist', 'blockwhitelist',\
               'blockblacklist', 'splitalgo', 'algoargs', 'configdoc', 'userisburl', 'cachefilename', 'cacheurl', 'adduserfiles', 'addoutputfiles', 'savelogsflag',\
               'userhn', 'publishname', 'asyncdest', 'campaign', 'blacklistT1', 'dbsurl', 'publishdbsurl', 'tfileoutfiles', 'edmoutfiles', 'userdn',\
               'runs', 'lumis':
            info[var] = classad.ExprTree(locals()['var'])

        with open(os.path.join(scratch, "master_dag")) as fd:
            fd.write(master_dag_submit_file % info)
        with open(os.path.join(scratch, "DBSDiscovery.submit")) as fd:
            fd.write(dbs_discovery_submit_file % info)
        with open(os.path.join(scratch, "JobSplitting.submit")) as fd:
            fd.write(job_splitting_submit_file % info)
        with open(os.path.join(scratch, "Job.submit")) as fd:
            fd.write(job_submit % info)

        dag_ad = classad.ClassAd()
        dag_ad["RequestName"] = requestname
        dag_ad["Out"] = os.path.join(scratch, "request.out")
        dag_ad["Err"] = os.path.join(scratch, "request.err")
        dag_ad["Executable"] = os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh")
        dag_ad["TransferInput"] = ", ".join([os.path.join(self.getBinDir(), "dag_bootstrap.sh"),
            os.path.join(scratch, "master_dag"),
            os.path.join(scratch, "DBSDiscovery.submit"),
            os.path.join(scratch, "JobSplitting.submit"),
            os.path.join(scratch, "Job.submit")])
        dag_ad["LeaveJobInQueue"] = classad.ExprTree("(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")
        dag_ad["TransferOutput"] = ""
        dag_ad["RemoteCondorSetup"] = self.getRemoteCondorSetup()

        schedd = self.getScheddObj()

        result_ads = []
        cluster = schedd.submit(dag_ad, 1, result_ads)
        schedd.spool(result_ads)

        return [{'RequestName': requestname}]


    def status(self, workflow, userdn):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        workflow = str(workflow)

        self.logger.info("Getting status for workflow %s" % workflow)

        name = workflow.split("_")[0]

        schedd = self.getScheddObj(name)
        ad = classad.ClassAd()
        ad["foo"] = workflow
        results = schedd.query("RequestName =?= %s" % ad.lookup("foo"), ["JobStatus"])

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)

        if 'JobStatus' not in results:
            self.logger.info("JobStatus is unknown for workflow %s" % workflow)
            raise Error.MissingObject("JobStatus is unknown for workflow %s" % workflow)

        status = results['JobStatus']
        self.logger.debug("Status result for workflow %s: %d" % (workflow, status))

        return status

def main():
    dag = DagmanDataWorkflow()
    workflow = 'bbockelm'
    jobtype = 'analysis'
    jobsw = 'CMSSW_5_3_7'
    jobarch = 'slc5_amd64_gcc434'
    inputdata = '/SingleMu/Run2012D-PromptReco-v1/AOD'
    siteblacklist = []
    sitewhitelist = ['T2_US_Nebraska']
    blockwhitelist = []
    blockblacklist = []
    splitalgo = None
    algoargs = None
    configdoc = ''
    userisburl = ''
    cachefilename = ''
    cacheurl = ''
    adduserfiles = []
    addoutputfiles = []
    savelogsflag = ''
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
               runs, lumis): #TODO delete unused parameters


if __name__ == "__main__":
    main()

