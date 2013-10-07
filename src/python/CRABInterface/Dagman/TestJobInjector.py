"""
    TestJobInjector - Handles the bits needed to handle fake/testing condor
                      jobs
"""
import logging
import classad
import htcondor
logging.basicConfig(level=logging.DEBUG)
from CRABInterface.Dagman.DagmanSubmitter import DagmanSubmitter
from CRABInterface.DagmanDataWorkflow import DagmanDataWorkflow

# Global list of workflows that were created for testing
testingWorkflows = []

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

    def makeRootObject(self, **extraArgs):
        subObj = DagmanDataWorkflow(config = self.config,
                                    requestarea = self.tempdir)
        args = self.getDummyArgs()
        args.update(extraArgs)
        return subObj.submit(**args)

    def makeDBSObject(self):
        return self.makeRootObject
        dbsClassad  = "xkd"
        schedd = htcondor.Schedd()
        retval = schedd.submit(dbsClassad)
        return dbsClassad
    
    @staticmethod
    def tearDown():
        # TODO: implement blowing up the test jobs
        pass
