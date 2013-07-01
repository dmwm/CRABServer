
"""
DagmanDataWorkflow,

A module providing HTCondor querying capabilities to the CRABServer
"""
# First, see if we have python condor libraries
import os
try:
    import htcondor
    # Strip out condor libraries from base path -- breaks voms-proxy-*
    if 'LD_LIBRARY_PATH' in os.environ:
        crab3_basepath = os.environ.get("CRAB3_BASEPATH", "")
        crab3_dev_base = os.environ.get("CRAB_DEV_BASE", "")
        paths = []
        for path in os.environ['LD_LIBRARY_PATH'].split(":"):
            if crab3_basepath and path.startswith(crab3_basepath):
                continue
            if crab3_dev_base and path.startswith(crab3_dev_base):
                continue
            paths.append(path)
        os.environ['LD_LIBRARY_PATH'] = ":".join(paths)
except ImportError:
    htcondor = None #pylint: disable=C0103
try:
    import WMCore.BossAir.Plugins.RemoteCondorPlugin as RemoteCondorPlugin
except ImportError:
    if not htcondor:
        raise

import re
import glob
import json
import time
import errno
import logging
import traceback
import subprocess
import pprint
import sys
import TaskWorker
import CRABInterface.DataWorkflow
import TaskWorker.Actions.DagmanSubmitter

from WMCore.Configuration import Configuration
from WMCore.REST.Error import InvalidParameter
from CRABInterface.Utils import retrieveUserCert
from CRABInterface.CRABServerBase import getCRABServerBase
from Databases.CAFUtilitiesBase import getCAFUtilitiesBase
from TaskWorker.Actions.DagmanCreator import CRAB_HEADERS, JOB_SUBMIT, ASYNC_SUBMIT, escape_strings_to_classads
from TaskWorker.Actions.DagmanSubmitter import MASTER_DAG_SUBMIT_FILE, CRAB_HEADERS, CRAB_META_HEADERS, SUBMIT_INFO

VERSION_RE = re.compile("([a-zA-Z0-9_.\-]+)")

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
Environment = PATH=/usr/bin:/bin;CRAB3_VERSION=%(crab3_version)s;%(additional_environment_options)s
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
Environment = PATH=/usr/bin:/bin;CRAB3_VERSION=%(crab3_version)s;%(additional_environment_options)s
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
#Environment = PATH=/usr/bin:/bin;CRAB3_VERSION=%(crab3_version)s
Environment = %(additional_environment_options)s
#x509userproxy = %(x509up_file)s
use_x509userproxy = true
#leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
leave_in_queue = (JobStatus == 4)
queue
"""

WORKFLOW_RE = re.compile("[a-z0-9_]+")

def getCRABInfoFromClassAd(ad):
    return
    #info = {}
    #for adName, dictName in SUBMIT_INFO:
    #    pass

class DagmanDataWorkflow(CRABInterface.DataWorkflow.DataWorkflow):
    """A specialization of the DataWorkflow for submitting to HTCondor DAGMan instead of
       PanDA
    """

    def __init__(self, **kwargs):
        super(DagmanDataWorkflow, self).__init__()
        self.config = None
        if 'config' in kwargs:
            self.logger.error("A config wasn't passed to DagmanDataWorkflow")
            self.config = kwargs['config']
        else:
            self.config = Configuration()
        self.config.section_("BossAir")
        self.config.section_("General")
        if not hasattr(self.config.BossAir, "remoteUserHost"):
            # Not really the best place for a default, I think...
            # TODO: Also, this default should pull from somewhere smarter
            self.config.BossAir.remoteUserHost = "submit-5.t2.ucsd.edu"
        if 'requestarea' in kwargs:
            self.requestarea = kwargs['requestarea']
        else:
            self.requestarea = "/tmp/crab3"


    def getBinDir(self):
        """
        Returns the directory of pithy shell scripts
        TODO this is definitely a thing that needs to be fixed for an RPM-deploy
        """
        # TODO: Nuke this with the rest of the dev hooks
        binDir = os.path.join(getCRABServerBase(), "bin")
        if self.config and hasattr(self.config.General, 'binDir'): #pylint: disable=E1103
            binDir = self.config.General.binDir #pylint: disable=E1103
        if 'CRAB3_BASEPATH' in os.environ:
            binDir = os.path.join(os.environ["CRAB3_BASEPATH"], "bin")
        return os.path.expanduser(binDir)


    def __getversion__(self):
        version = os.environ.get("CRAB3_VERSION", "devel")
        if not VERSION_RE.match(version):
            version = "unknown"
        return version

    def getTransformLocation(self):
        """
        Returns the location of the PanDA job transform
        """
        # TODO: Nuke this with the rest of the dev hooks
        tDir = os.path.join(getCAFUtilitiesBase(), "src", "python", \
                    "transformation", "CMSRunAnalysis")
        if self.config and hasattr(self.config.General, 'transformDir'): #pylint: disable=E1103
            tDir = self.config.General.transformDir #pylint: disable=E1103
        if 'CRAB3_BASEPATH' in os.environ:
            tDir = os.path.join(os.environ["CRAB3_BASEPATH"], "bin")
        return os.path.join(os.path.expanduser(tDir), "CMSRunAnalysis.sh")

    def getRemoteCondorSetup(self):
        """
        Returns the environment setup file for the remote schedd.
        """
        return ""

    def submitRaw(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles, \
               userhn, userdn, savelogsflag, publication, publishname, asyncdest, blacklistT1, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles, runs, lumis, userproxy=None, **kwargs):

        """Perform the workflow injection into HTCondor

           :arg str workflow: workflow name requested by the user;
           :arg str jobtype: job type of the workflow, usually Analysis;
           :arg str jobsw: software requirement;
           :arg str jobarch: software architecture (=SCRAM_ARCH);
           :arg str inputdata: input dataset;
           :arg str list siteblacklist: black list of sites, with CMS name;
           :arg str list sitewhitelist: white list of sites, with CMS name;
           :arg str list blockblacklist:  input blocks to be excluded from the specified input dataset;
           :arg str splitalgo: algorithm to be used for the workflow splitting;
           :arg str algoargs: argument to be used by the splitting algorithm;
           :arg str list addoutputfiles: list of additional output files;
           :arg int savelogsflag: archive the log files? 0 no, everything else yes;
           :arg str userdn: DN of user doing the request;
           :arg str userhn: hyper new name of the user doing the request;
           :arg str publishname: name to use for data publication;
           :arg str asyncdest: CMS site name for storage destination of the output files;
           :arg int blacklistT1: flag enabling or disabling the black listing of Tier-1 sites;
           :arg str dbsurl: dbs url where the input dataset is published;
           :arg str publishdbsurl: dbs url where the output data has to be published;
           :arg str list runs: list of run numbers
           :arg str list lumis: list of lumi section numbers
           :returns: a dict which contaians details of the request"""

        self.logger.debug("""workflow %s, jobtype %s, jobsw %s, jobarch %s, inputdata %s, siteblacklist %s, sitewhitelist %s, 
               splitalgo %s, algoargs %s, cachefilename %s, cacheurl %s, addoutputfiles %s, savelogsflag %s,
               userhn %s, publishname %s, asyncdest %s, blacklistT1 %s, dbsurl %s, publishdbsurl %s, tfileoutfiles %s, edmoutfiles %s, userdn %s,
               runs %s, lumis %s"""%(workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, \
               splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles, savelogsflag, \
               userhn, publishname, asyncdest, blacklistT1, dbsurl, publishdbsurl, tfileoutfiles, edmoutfiles, userdn, \
               runs, lumis))
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())

        dagmanSubmitter = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        scheddName = dagmanSubmitter.getSchedd()

        requestname = '%s_%s_%s_%s' % (scheddName, timestamp, userhn, workflow)

        scratch = os.path.join(self.requestarea, "." + requestname)
        os.makedirs(scratch)

        # Poor-man's string escaping.  We do this as classad module isn't guaranteed to be present.
        # FIXME: Available sites aren't known ahead of time for us.
        available_sites = []
        info = escape_strings_to_classads(locals())
        # Condor will barf if the requesname is quoted .. for some reason
        #info['requestname'] = info['requestname'].replace('"','')
        info['remote_condor_setup'] = self.getRemoteCondorSetup()
        info['bindir'] = self.getBinDir()
        info['transform_location'] = self.getTransformLocation()
        
        if kwargs.get('taskManagerTarball', None) and \
                kwargs.get('taskManagerCodeLocation', None) and \
                kwargs['taskManagerTarball'] != 'local':
            raise RuntimeError, "Debug.taskManagerTarball must be 'local' if you provide a code location"

        if kwargs.get('taskManagerCodeLocation', None):
            kwargs['taskManagerTarball'] = 'local'

        if kwargs.get('taskManagerTarball', None):
            info['additional_environment_options'] = 'CRAB_TASKMANAGER_TARBALL=%s' % kwargs['taskManagerTarball']
        else:
            # need to have  a blank variable to keep the separaters right
            info['additional_environment_options'] = 'DUMMY=""'
        info['additional_input_files'] = ''
        if kwargs.get('taskManagerTarball') == 'local':
            if not kwargs.get('taskManagerCodeLocation', None):
                raise RuntimeError, "Tarball was set to local, but not location was set"
            # we need to generate the tarball to ship along with the jobs
            #  CAFTaskWorker/bin/dagman_make_runtime.sh
            self.logger.info('Packing up tarball from local source tree')
            runtimePath = os.path.expanduser( os.path.join( 
                                        kwargs['taskManagerCodeLocation'],
                                        'CAFTaskWorker',
                                        'bin',
                                        'dagman_make_runtime.sh') )
            tarMaker = subprocess.Popen([ runtimePath, kwargs['taskManagerCodeLocation']])
            tarMaker.communicate()
            info['additional_input_files'] = ', TaskManagerRun.tar.gz'
                                          

        info['crab3_version'] = self.__getversion__()

        schedd, address = dagmanSubmitter.getScheddObj(scheddName)

        with open(os.path.join(scratch, "master_dag"), "w") as fd:
            fd.write(MASTER_DAG_FILE % info)
        with open(os.path.join(scratch, "DBSDiscovery.submit"), "w") as fd:
            fd.write(DBS_DISCOVERY_SUBMIT_FILE % info)
        with open(os.path.join(scratch, "JobSplitting.submit"), "w") as fd:
            fd.write(JOB_SPLITTING_SUBMIT_FILE % info)
        with open(os.path.join(scratch, "Job.submit"), "w") as fd:
            fd.write(JOB_SUBMIT % info)
        with open(os.path.join(scratch, 'ASO.submit'), 'w') as fd:
            fd.write(ASYNC_SUBMIT % info)

        inputFiles = [os.path.join(self.getBinDir(), "dag_bootstrap.sh"),
		       os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh"),
                       self.getTransformLocation(),
                       os.path.join(self.getBinDir(), "cmscp.py")]
        scratch_files = ['master_dag', 'DBSDiscovery.submit', 'JobSplitting.submit', 'master_dag', 'Job.submit', 'ASO.submit']
        inputFiles.extend([os.path.join(scratch, i) for i in scratch_files])
        if kwargs.get('taskManagerTarball', None) == 'local':
            inputFiles.append('TaskManagerRun.tar.gz')

        outputFiles = ["master_dag.dagman.out", "master_dag.rescue.001", "RunJobs.dag",
            "RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001", "dbs_discovery.err",
            "dbs_discovery.out", "job_splitting.err", "job_splitting.out"]

        if address:
            # Submit directly to a scheduler
            info['outputFilesString'] = ", ".join(outputFiles)
            info['inputFilesString'] = ", ".join(inputFiles)

            dagmanSubmitter.submitDirect(schedd,
                os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh"), 'master_dag',
                info)
        else:
            # Submit over Gsissh
            # testing getting the right directory
            requestname = info['requestname'].replace('"','')
            requestname_bak = requestname
            requestname = './'
            info['outputFilesString'] = ", ".join([os.path.basename(x) for x in outputFiles])
            info['inputFilesString']  = ", ".join([os.path.basename(x) for x in inputFiles])
            info['iwd'] = '%s/' % requestname_bak
            info['scratch'] = '%s/' % requestname
            info['bindir'] = '%s/' % requestname
            info['transform_location'] = os.path.basename(info['transform_location'])
            info['x509up_file'] = '%s/user.proxy' % requestname
            info['userproxy'] = '%s/user.proxy' % requestname
            info['configdoc'] = 'CONFIGDOCSUP'
            jdl = MASTER_DAG_SUBMIT_FILE % info
            with open(os.path.join(scratch, 'submit.jdl'), 'w') as fd:
                fd.write(jdl)
            requestname = requestname_bak
            #schedd.submitRaw(requestname, os.path.join(scratch, 'submit.jdl'), info['userproxy'], inputFiles)
            schedd.submitRaw(requestname, os.path.join(scratch, 'submit.jdl'), userproxy, inputFiles)
        return [{'RequestName': requestname}]
    submit = retrieveUserCert(submitRaw)


    @retrieveUserCert
    def kill(self, workflow, force, userdn, **kwargs):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""

        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)

        userproxy = kwargs['userproxy']
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        dag = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        scheddName = dag.getSchedd()
        schedd, address = dag.getScheddObj(scheddName)

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
                        os._exit(0) #pylint: disable=W0212
                    except Exception:
                        wpipe.write(str(traceback.format_exc()))
                finally:
                    os._exit(1) #pylint: disable=W0212
            wpipe.close()
            results = rpipe.read()
            if results != "OK":
                raise Exception("Failure when submitting to HTCondor: %s" % results)
        else:
            # Use the remoteCondor plugin 
            schedd.hold(const) #pylint: disable=E1103

        # Search for and hold the sub-dag
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= \"%s\" && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)" % workflow
        rootAttrList = ["ClusterId"]
        if address:
            results = schedd.query(rootConst, rootAttrList)
        else:
            results = schedd.getClassAds(rootConst, rootAttrList)

        if not results:
            return

        subDagConst = "DAGManJobId =?= %s && DAGParentNodeNames =?= \"JobSplitting\"" % results[0]["ClusterId"]
        if address:
            subDagResults = schedd.query(subDagConst, rootAttrList)
        else:
            subDagResults = schedd.getClassAds(subDagConst, rootAttrList)

        if not subDagResults:
            return
        finished_jobConst = "DAGManJobId =?= %s && ExitCode =?= 0" % subDagResults[0]["ClusterId"]

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
                        schedd.edit(subDagConst, "HoldKillSig", "\"SIGUSR1\"")
                        schedd.act(htcondor.JobAction.Hold, subDagConst)
                        schedd.edit(finished_jobConst, "DAGManJobId", "-1")
                        wpipe.write("OK")
                        wpipe.close()
                        os._exit(0) #pylint: disable=W0212
                    except Exception, e:
                        wpipe.write(str(traceback.format_exc()))
                finally:
                    os._exit(1) #pylint: disable=W0212
            wpipe.close()
            results = rpipe.read()
            if results != "OK":
                raise Exception("Failure when killing job: %s" % results)
        else:
            # Use the remoteCondor plugin
            schedd.edit(subDagConst, "HoldKillSig", "SIGUSR1")
            schedd.hold(const) #pylint: disable=E1103

    def getScheddAndAddress(self, scheddName=None):
        dag = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        if not scheddName:
            scheddName = dag.getSchedd()
        return  dag.getScheddObj(scheddName)

    def __getroottasks__(self, address, workflow, schedd):
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= \"%s\" && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)" % workflow
        rootAttrList = ["JobStatus", "ExitCode", 'CRAB_JobCount', 'CRAB_LumiMask']
        #print "trying to get %s" % workflow
        #print "Using rootConst: %s" % rootConst
        _, address = self.getScheddAndAddress()
        if address:
            results = schedd.query(rootConst, rootAttrList)
        else:
            results = schedd.getClassAds(rootConst, rootAttrList)

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            self.logger.info("Tried to read from address %s" % address)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)
        return results


    def __getasojobs__(self, address, workflow, schedd):
        jobConst = "TaskType =?= \"ASO\" && CRAB_ReqName =?= \"%s\"" % workflow
        jobList = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id']
        _, address = self.getScheddAndAddress()
        if address:
            results = schedd.query(jobConst, jobList)
        else:
            results = schedd.getClassAds(jobConst, jobList)
        return results


    def __getjobs__(self, address, workflow, schedd):
        jobConst = "TaskType =?= \"Job\" && CRAB_ReqName =?= \"%s\"" % workflow
        jobList = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id']
        if address:
            results = schedd.query(jobConst, jobList)
        else:
            results = schedd.getClassAds(jobConst, jobList)
        return results


    def __getjobdir__(self, workflow, subdir):
        jobdir = os.path.join(self.requestarea, "."+workflow, subdir)
        jobdir = os.path.abspath(jobdir)
        requestarea = os.path.abspath(self.requestarea)
        assert(jobdir.startswith(requestarea))
        try:
            os.makedirs(jobdir)
        except OSError, oe:
            if oe.errno != errno.EEXIST:
                raise
        return jobdir


    def __serializefinishedjobs__(self, workflow, userdn, userproxy=None):
        if not userproxy:
            return
        jobdir = self.__getjobdir__(workflow, "finished_condor_jobs")

        name = workflow.split("_")[0]
        dag = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        schedd, address = dag.getScheddObj(name)

        finished_jobs = {}

        jobs = self.__getjobs__(address, workflow, schedd)
        jobs.extend(self.__getasojobs__(address, workflow, schedd))
        for job in jobs:
            if 'CRAB_Id' not in job:
                continue
            #if (int(job.get("JobStatus", "1")) == 4) and (task_is_finished or (int(job.get("ExitCode", -1)) == 0)):
            if (int(job.get("JobStatus", "1")) == 4):
                finished_jobs["%s.%s" % (job['ClusterID'], job['ProcID'])] = job

        # TODO: consider POSIX data integrity
        for jobId, job in finished_jobs.items():
            with open(os.path.join(jobdir, jobId), "w") as fd:
                json.dump(dict(job), fd)

        if address:
            schedd.act(htcondor.JobAction.Remove, finished_jobs.keys())
        else:
            schedd.remove(finished_jobs.keys())


    def __getreports__(self, workflow, userproxy):
        if not userproxy:
            return

        jobdir = self.__getjobdir__(workflow, "job_results")

        name = workflow.split("_")[0]
        dag = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        schedd, address = dag.getScheddObj(name)

        jobIDs = {}
        jobIDsConstraint = []
        for job in self.__getjobs__(address, workflow, schedd):
            if 'CRAB_Id' not in job:
                continue
            if os.path.exists(os.path.join(jobdir, "jobReport.json.%s" % job['CRAB_Id'])):
                continue
            if int(job.get("JobStatus", "1")) == 4:
                jobIDs[int(job['CRAB_Id'])] = "%s.%s" % (job['ClusterID'], job['ProcID'])
                jobIDsConstraint.append((job['ClusterID'], job['ProcID']))

        if not jobIDs:
            return

        if address:
            schedd.edit(jobIDs.values(), "TransferOutputRemaps", 'strcat("jobReport.json.", CRAB_Id, "=%s/jobReport.json.", CRAB_Id, ";job_out.", CRAB_Id, "=/dev/null;job_err.", CRAB_Id, "=/dev/null")' % jobdir)
            const = " || ".join(["(ClusterID == %s && ProcID == %s)" % i for i in jobIDsConstraint])
            schedd.retrieve(const)


    def status(self, workflow, userdn, userproxy=None):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        print "Getting status for workflow %s" % workflow

        name = workflow.split("_")[0]
        dag = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        schedd, address = dag.getScheddObj(name)

        results = self.__getroottasks__(address, workflow, schedd)
        #print "Got root tasks: %s" % results
        jobsPerStatus = {}
        jobStatus = {}
        jobList = []
        taskStatusCode = int(results[-1]['JobStatus'])
        #print "got status code %s" % taskStatusCode
        taskJobCount = int(results[-1].get('CRAB_JobCount', 0))
        codes = {1: 'Idle', 2: 'Running', 4: 'Completed (Success)', 5: 'Killed'}
        taskExitCode = int(results[-1].get("ExitCode", 0))
        retval = {"status": codes.get(taskStatusCode, 'Unknown'), "taskFailureMsg": "", "jobSetID": workflow,
            "jobsPerStatus" : jobsPerStatus, "jobList": jobList}
        if taskStatusCode == 4 and taskExitCode:
            retval['status'] = "Completed (Failed %s)" % str(taskExitCode)

        failedJobs = []
        allJobs = self.__getjobs__(address, workflow, schedd)
        #print "found %s jobs" % len(allJobs)
        for result in allJobs:
            # print "Examining one job: %s" % result
            jobState = int(result['JobStatus'])
            if result['CRAB_Id'] in failedJobs:
                failedJobs.remove(result['CRAB_Id'])
            if (jobState == 4) and ('ExitCode' in result) and (int(result['ExitCode'])):
                failedJobs.append(result['CRAB_Id'])
                statusName = "Failed (%s)" % result['ExitCode']
            else:
                statusName = codes.get(jobState, 'Unknown')
            jobStatus[int(result['CRAB_Id'])] = statusName

        aso_codes = {1: 'ASO Queued', 2: 'ASO Running', 4: 'Stageout Complete (Success)'}
        for result in self.__getasojobs__(address, workflow, schedd):
            if result['CRAB_Id'] in failedJobs:
                failedJobs.remove(result['CRAB_Id'])
            jobState = int(result['JobStatus'])
            if (jobState == 4) and ('ExitCode' in result) and (int(result['ExitCode'])):
                failedJobs.append(result['CRAB_Id'])
                statusName = "Failed Stage-Out (%s)" % result['ExitCode']
            else:
                statusName = aso_codes.get(jobState, 'Unknown')
            jobStatus[int(result['CRAB_Id'])] = statusName

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

        retval['jobdefErrors'] = []

        if taskStatusCode == 4 and taskExitCode and retval["totalJobdefs"] == 0:
            retval['status'] = 'Task failed before any jobs were created'

        self.logger.info("Status result for workflow %s: %s" % (workflow, retval))
        #print "Status result for workflow %s: %s" % (workflow, retval)

        return retval


    def outputLocation(self, workflow, maxNum, _):
        """
        Retrieves the output LFN from async stage out

        :arg str workflow: the unique workflow name
        :arg int maxNum: the maximum number of output files to retrieve
        :return: the result of the view as it is."""
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        name = workflow.split("_")[0]
        dag = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        schedd, address = dag.getScheddObj(name)

        jobConst = 'TaskType =?= \"ASO\"&& CRAB_ReqName =?= \"%s\"' % workflow
        jobList = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'GlobalJobId', 'OutputSizes', 'OutputPFNs']

        if address:
            results = schedd.query(jobConst, jobList)
        else:
            results = schedd.getClassAds(jobConst, jobList)
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

        if maxNum > 0:
            return {'result': files[:maxNum]}
        return {'result': files}

    @retrieveUserCert
    def resubmit(self, workflow, siteblacklist, sitewhitelist, userdn, **kwargs):
        # TODO: In order to take advantage of the updated white/black list, we need
        # to sneak those into the resubmitted DAG.

        self.logger.info("About to resubmit workflow: %s." % workflow)

        userproxy = kwargs['userproxy']
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        dag = TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(self.config)
        scheddName = dag.getSchedd()
        schedd, address = dag.getScheddObj(scheddName)

        # Search for and hold the sub-dag
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= \"%s\" && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)" % workflow
        rootAttrList = ["ClusterId"]
        if address:
            results = schedd.query(rootConst, rootAttrList)
        else:
            results = schedd.getClassAds(rootConst, rootAttrList)

        if not results:
            return

        subDagConst = "DAGManJobId =?= %s && DAGParentNodeNames =?= \"JobSplitting\"" % results[0]["ClusterId"]
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
                        schedd.act(htcondor.JobAction.Release, subDagConst)
                        schedd.act(htcondor.JobAction.Release, rootConst)
                        wpipe.write("OK")
                        wpipe.close()
                        os._exit(0) #pylint: disable=W0212
                    except Exception:
                        wpipe.write(str(traceback.format_exc()))
                finally:
                    os._exit(1) #pylint: disable=W0212
            wpipe.close()
            results = rpipe.read()
            if results != "OK":
                raise Exception("Failure when killing job: %s" % results)
        else:
            schedd.release(subDagConst) #pylint: disable=E1103
            schedd.release(rootConst) #pylint: disable=E1103


    def report(self, workflow, userdn, userproxy=None):
        #if not userdn:
        #    raise Exception("Unable to determine DN for report")

        self.__getreports__(workflow, userproxy)
        jobdir = self.__getjobdir__(workflow, "job_results")
        name = workflow.split("_")[0]
        schedd, address = self.getScheddAndAddress(name)

        #load the lumimas
        results = self.__getroottasks__(address, workflow, schedd)
        if not results:
            raise Exception("Unable to determine task lumimask for workflow %s." % workflow)

        res = {}
        res['lumiMask'] = json.loads(results[0]['CRAB_LumiMask'])

        res['runsAndLumis'] = {}
        fileRe = re.compile("jobReport\.json\.([0-9]+)")
	for filename in glob.glob(os.path.join(jobdir, "jobReport.json.*")):
            shortFilename = os.path.split(filename)[1]
            m = fileRe.match(shortFilename)
            if not m:
                continue

            report = {}
            with open(filename, "r") as fd:
                try:
                    report = json.load(fd)
                except ValueError:
                    continue

            for outputModule in report['steps']['cmsRun']['output'].values():
                if not outputModule:
                    continue
                outputModule = outputModule[0]
                if u'ouput_module_class' not in outputModule:
                    continue
                if outputModule['ouput_module_class'] == 'PoolOutputModule':
                    # TODO: this results in the wrong numbers for multi-output jobs.
                    res['runsAndLumis'][m.groups()[0]] = {'parents': json.dumps(outputModule['input']), 'runlumi': json.dumps(outputModule['runs']), 'events': outputModule['events']}

        yield res


def main():
    dag = DagmanDataWorkflow()
    workflow = 'bbockelm'
    jobtype = 'analysis'
    jobsw = 'CMSSW_5_3_7'
    jobarch = 'slc5_amd64_gcc462'
    inputdata = '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO'
    siteblacklist = []
    sitewhitelist = ['T2_US_Nebraska']
    splitalgo = "LumiBased"
    algoargs = 40
    cachefilename = 'default.tgz'
    cacheurl = 'https://voatlas178.cern.ch:25443'
    addoutputfiles = []
    savelogsflag = False
    userhn = 'bbockelm'
    publishname = ''
    asyncdest = 'T2_US_Nebraska'
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
    dag.submitRaw(workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, 
               splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles,
               userhn, userdn, savelogsflag, publishname, asyncdest, blacklistT1, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles,
               runs, lumis, userproxy = '/tmp/x509up_u%d' % os.geteuid()) #TODO delete unused parameters


if __name__ == "__main__":
    main()

