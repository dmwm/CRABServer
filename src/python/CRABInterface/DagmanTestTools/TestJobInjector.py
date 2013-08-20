"""
    TestJobInjector - Handles the bits needed to handle fake/testing condor
                      jobs
"""
import logging
import classad
import htcondor
logging.basicConfig(level=logging.DEBUG)
from TaskWorker.Actions.DagmanSubmitter import DagmanSubmitter
from CRABInterface.DagmanDataWorkflow import DagmanDataWorkflow

# Global list of workflows that were created for testing
testingWorkflows = []
dbsString = """+CRAB_ReqName = "localhost_130818_212153_meloam_gwms_killing"
+CRAB_Workflow = "gwms_killing"
+CRAB_JobType = "Analysis"
+CRAB_JobSW = "CMSSW_5_3_2"
+CRAB_JobArch = "slc5_amd64_gcc462"
+CRAB_InputData = "/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO"
+CRAB_OutputData = "MyReskimForTwo-c6bdfb160b2ee3ff6d8128351c7e995c"
+CRAB_ISB = "https://voatlas294.cern.ch:25443"
+CRAB_SiteBlacklist = {}
+CRAB_SiteWhitelist = {}
+CRAB_AdditionalOutputFiles = {}
+CRAB_EDMOutputFiles = {"dumper.root"}
+CRAB_TFileOutputFiles = {}
+CRAB_SaveLogsFlag = 1 
+CRAB_UserDN = "/DC=org/DC=doegrids/OU=People/CN=Andrew Malone Melo 788499"
+CRAB_UserHN = "meloam"
+CRAB_AsyncDest = "T2_US_Nebraska"
+CRAB_BlacklistT1 = 1 
+CRAB_SplitAlgo = "LumiBased"
+CRAB_AlgoArgs = "{\"splitOnRun\": false, \"halt_job_on_file_boundaries\": false, \"lumis_per_job\": 500}"
+CRAB_PublishName = "MyReskimForTwo-c6bdfb160b2ee3ff6d8128351c7e995c"
+CRAB_DBSUrl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
+CRAB_PublishDBSUrl = ""
+CRAB_LumiMask = "{}"
+TaskType = "DBS"
should_transfer_files = YES
universe = local
Executable = dag_bootstrap.sh
Output = dbs_discovery.out
Error = dbs_discovery.err
Args = DBS None dbs_results
transfer_input_files = cmscp.py
transfer_output_files = dbs_results
Environment = PATH=/usr/bin:/bin;DUMMY=""
use_x509userproxy = true # x509up_u500
"""

class TestJobInjector:
    def __init__(self, workflowString, config, tempdir):
        global testingWorkflows
        self.workflowString = workflowString
        testingWorkflows.append(workflowString)
        self.tempdir = tempdir
        self.config = config

    def getDummyArgs(self):
        # 'taskManagerCodeLocation': '/home/vagrant/sync' 'taskManagerTarball': 'local'
        return {'runs': [], 'workflow': self.workflowString, 'cacheurl': 'https://voatlas294.cern.ch:25443', 'publishname': 'MyReskimForTwo-c6bdfb160b2ee3ff6d8128351c7e995c', 'savelogsflag': 1, 'userisburl': 'https://voatlas294.cern.ch:25443', 'vogroup': '/cms', 'userdn': '/DC=org/DC=doegrids/OU=People/CN=Andrew Malone Melo 788499', 'vorole': 'cmsuser', 'tfileoutfiles': [], 'asyncdest': 'T2_US_Nebraska', 'algoargs': 500, 'lumis': [], 'sitewhitelist': [], 'totalunits': 0, 'cachefilename': '8b578f03-677e-4808-81e4-3d44cd60bdd2default.tgz', 'siteblacklist': [], 'jobarch': 'slc5_amd64_gcc462', 'blockwhitelist': [], 'publication': 0, 'blockblacklist': [], 'blacklistT1': 1, 'splitalgo': 'LumiBased', 'publishdbsurl': '', 'jobsw': 'CMSSW_5_3_2', 'dbsurl': 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet', 'addoutputfiles': [], 'edmoutfiles': ['dumper.root'], 'adduserfiles': [], 'userhn': 'meloam', 'inputdata': '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO', 'jobtype': 'Analysis', 'testSleepMode' : True }

    def makeRootObject(self):
        subObj = DagmanDataWorkflow(config = self.config,
                                    requestarea = self.tempdir)
        args = self.getDummyArgs()
        return subObj.submit(**args)

    def makeDBSObject(self):
        dbsClassad = classad.parseOld(dbsString)
        schedd = htcondor.Schedd()
        retval = schedd.submit(dbsClassad)
        return dbsClassad
    
    @staticmethod
    def tearDown():
        # TODO: implement blowing up the test jobs
        pass
