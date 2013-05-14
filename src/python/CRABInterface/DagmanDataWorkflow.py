
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

from TaskWorker.Actions.DagmanCreator import CRAB_HEADERS, job_submit, async_submit, escape_strings_to_classads
from TaskWorker.Actions.DagmanSubmitter import addCRABInfoToClassAd, MASTER_DAG_SUBMIT_FILE, CRAB_HEADERS, CRAB_META_HEADERS

import DataWorkflow

master_dag_file = \
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
+CRAB_ConfigDoc = %(configdoc)s
+CRAB_PublishName = %(publishname)s
+CRAB_DBSUrl = %(dbsurl)s
+CRAB_PublishDBSUrl = %(publishdbsurl)s
+CRAB_LumiMask = %(lumimask)s
"""

job_splitting_submit_file = CRAB_HEADERS + CRAB_META_HEADERS + \
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

dbs_discovery_submit_file = CRAB_HEADERS + CRAB_META_HEADERS + \
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

async_submit = CRAB_HEADERS + \
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

        userproxy = kwargs['userproxy']

        # Poor-man's string escaping.  We do this as classad module isn't guaranteed to be present.
        info = escape_strings_to_classads(locals())
        info['remote_condor_setup'] = self.getRemoteCondorSetup()
        info['bindir'] = self.getBinDir()
        info['transform_location'] = self.getTransformLocation()

        dagmanSubmitter = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        schedd_name = dagmanSubmitter.getSchedd()
        schedd, address = dagmanSubmitter.getScheddObj(schedd_name)

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
        info['inputFilesString'] = ", ".join(input_files)

        outputFiles = ["master_dag.dagman.out", "master_dag.rescue.001", "RunJobs.dag",
            "RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001", "dbs_discovery.err",
            "dbs_discovery.out", "job_splitting.err", "job_splitting.out"]
        info['outputFilesString'] = ", ".join(outputFiles)

        if address:
            dagmanSubmitter.submitDirect(schedd,
                os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh"), 'master_dag',
                info)
        else:
            jdl = MASTER_DAG_SUBMIT_FILE % info
            schedd.submitRaw(requestname, jdl, kwargs['userproxy'], input_files)

        return [{'RequestName': requestname}]
    submit = retriveUserCert(clean=False)(submitRaw)


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

