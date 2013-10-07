
"""
DagmanDataWorkflow

A module providing HTCondor integration to CRABServer. Can also be directly
called for serverless operation
"""
# First, see if we have python condor libraries
try:
    import htcondor
except ImportError:
    htcondor = None #pylint: disable=C0103
try:
    import WMCore.BossAir.Plugins.RemoteCondorPlugin as RemoteCondorPlugin
except ImportError:
    RemoteCondorPlugin = None #pylint: disable=C0103

import os
import os.path
import subprocess
import re
import copy
import json
import time
import errno
import logging
import shutil
import traceback
import pprint
import sys
import TaskWorker
import CRABInterface.DataWorkflow
import classad
import warnings

from WMCore.Configuration import Configuration
from WMCore.REST.Error import InvalidParameter
from CRABInterface.Utils import retrieveUserCert
from CRABInterface.CRABServerBase import getCRABServerBase
from Databases.CAFUtilitiesBase import getCAFUtilitiesBase
# Sorry for the wildcard
from CRABInterface.Dagman.DagmanSnippets import *
from CRABInterface.Dagman.Fork import ReadFork
from CRABInterface.Dagman.DagmanSubmitter import DagmanSubmitter
import WMCore.WMSpec.WMTask
def getCRABInfoFromClassAd(ad):
    info = {}
    for adName, dictName in SUBMIT_INFO:
        pass

def addCRABInfoToClassAd(ad, info):
    """
    Given a submit ClassAd, add in the appropriate CRAB_* attributes
    from the info directory
    """
    for adName, dictName in SUBMIT_INFO:
        if type(info[dictName]) == type(''):
            ad[adName] = info[dictName]
        else:
            try:
                ad[adName] = classad.ExprTree(str(info[dictName]))
            except SyntaxError:
                raise SyntaxError, "Couldn't parse this into a ClassAd (type %s): %s" % (type(info[dictName]), info[dictName])


class DagmanDataWorkflow(CRABInterface.DataWorkflow.DataWorkflow):
    """A specialization of the DataWorkflow for submitting to HTCondor DAGMan instead of
       PanDA
    """
    JOB_KILLED_HOLD_REASON='Killed by CRAB3 client'
    JOB_RESTART_HOLD_REASON='Restarted by CRAB3 client'

    def __init__(self, **kwargs):
        if not htcondor and not RemoteCondorPlugin:
            raise RuntimeError, "You need to have at least one of htcondor or RemoteCondorPlugin"
        super(DagmanDataWorkflow, self).__init__()
        self.config = None
        if 'config' in kwargs:
            self.config = kwargs['config']
        else:
            self.logger.error("A config wasn't passed to DagmanDataWorkflow")
            self.config = Configuration()
        
        self.config.section_("BossAir")
        self.config.section_("General")
        if not hasattr(self.config.BossAir, "remoteUserHost"):
            # Not really the best place for a default, I think...
            # TODO: Also, this default should pull from somewhere smarter
            self.config.BossAir.remoteUserHost = "submit-5.t2.ucsd.edu"
        self.logger.debug("Got kwargs %s " % kwargs)
        if 'requestarea' in kwargs:
            self.requestarea = kwargs['requestarea']
        else:
            self.requestarea = "/tmp/crab3"
        self.logger.debug("Setting request area to %s" % self.requestarea)


    def pullLocalsIntoClassAd(self, input):
        """
        Poor-man's string escaping for ClassAds.  We do this as classad module isn't guaranteed to be present.

        NO WAY, we're requiring classad is present. This blows otherwise

        Converts the arguments in the input dictionary to the arguments necessary
        for the job submit file string.

        FIXME: This can be cleaned up since we're not actually doing any string escaping
        """
        info = {}
        for var in 'workflow', 'jobtype', 'jobsw', 'jobarch', 'inputdata', 'splitalgo', 'algoargs', \
               'cachefilename', 'cacheurl', 'userhn', 'publishname', 'asyncdest', 'dbsurl', 'publishdbsurl', \
               'userdn', 'requestname', 'publication':
            val = input.get(var, None)
            if val == None:
                info[var] = 'undefined'
            else:
                # do I need to escape somehow? I feel like no.
                info[var] = val

        for var in 'savelogsflag', 'blacklistT1':
            info[var] = int(input[var])

        for var in 'siteblacklist', 'sitewhitelist', 'addoutputfiles', \
               'tfileoutfiles', 'edmoutfiles':
            val = input[var]
            if val == None:
                info[var] = "{}"
            else:
                info[var] = "{" + json.dumps(val)[1:-1] + "}"

        #TODO: We don't handle user-specified lumi masks correctly.
        lumiJson = WMCore.WMSpec.WMTask.buildLumiMask(input['runs'], input['lumis'])
        info['lumimask'] = json.dumps(lumiJson)
        splitArgName = SPLIT_ARG_MAP[input['splitalgo']]
        info['algoargs'] = json.dumps({'halt_job_on_file_boundaries': False,
                                       'splitOnRun': False, 
                                       splitArgName : input['algoargs']})
        info['attempt'] = 0
        #FIXME: brian, where were you going with this? - Melo
        if input.get('available_sites', None):
            info['available_sites'] = input['available_sites']

        info["output_dest"] = os.path.join("/store/user", input['userhn'], input['workflow'], input['publishname'])
        info["temp_dest"] = os.path.join("/store/temp/user", input['userhn'], input['workflow'], input['publishname'])
        if input['userproxy']:
            info['x509up_file'] = os.path.split(input['userproxy'])[-1]
        else:
            info['x509up_file'] = os.path.split(os.environ['X509_USER_PROXY'])[-1]
        info['userproxy'] = input['userproxy']
        #info['scratch'] = input['scratch']

        return info

    def getMasterDag(self, info):
        return MASTER_DAG_FILE
    
   
    CRAB_META_HEADERS = ( ('CRAB_SplitAlgo', 'splitalgo'),
                            ('CRAB_AlgoArgs', 'algoargs'),
                            ('CRAB_PublishName', 'publishname'),
                            ('CRAB_DBSUrl', 'dbsurl'),
                            ('CRAB_Lumimask', 'lumimask'), )
    CRAB_HEADERS = ( ('CRAB_ReqName', 'requestname'),
                        ('CRAB_Workflow', 'workflow'),
                        ('CRAB_JobType', 'jobtype'),
                        ('CRAB_JobSW', 'jobsw'),
                        ('CRAB_JobArch', 'jobarch'),
                        ('CRAB_InputData', 'inputdata'),
                        ('CRAB_OutputData', 'publishname'),
                        ('CRAB_ISB', 'cacheurl'),
                        ('CRAB_SiteBlackList', 'siteblacklist'),
                        ('CRAB_SiteWhiteList', 'sitewhitelist'),
                        ('CRAB_AdditionalOutputFiles', 'addoutputfiles'),
                        ('CRAB_EDMOutputFiles', 'edmoutfiles'),
                        ('CRAB_TFileOutputFiles', 'tfileoutfiles'),
                        ('CRAB_SaveLogsFlag', 'savelogsflag'),
                        ('CRAB_UserDN', 'userdn'),
                        ('CRAB_UserHN', 'userhn'),
                        ('CRAB_AsyncDest', 'asyncdest'),
                        ('CRAB_BlacklistT1', 'blacklistT1'), )
    def addCRABHeadersToClassAd(self, ad, info):
        return self.addDictsToClassAd(ad, info, self.CRAB_HEADERS)
    
    def addCRABMetaHeadersToClassAd(self, ad, info):
        return self.addDictsToClassAd(ad, info, self.CRAB_META_HEADERS)

    def addDictsToClassAd(self, ad, info, configList):
        for k,v in configList:
            if not v in info:
                continue
            ad[k] = info[v]
        return ad
    
    def getDBSDiscoveryClassAd(self, info):
        retval = classad.ClassAd()
        retval = self.addCRABMetaHeadersToClassAd(retval, info)
        retval = self.addCRABHeadersToClassAd(retval, info)
        retval['TaskType'] = "DBS"
        retval['universe'] = classad.ExprTree('local')
        retval['Output'] = classad.ExprTree('dbs_discovery.out')
        retval['Error']  = classad.ExprTree('dbs_discovery.err')
        retval['transfer_output_files'] = classad.ExprTree('dbs_results')
        retval['Environment'] = 'PATH=/usr/bin:/bin %s' % info['additional_environment_options']
        retval['use_x509userproxy'] = 'True'
        retval['Executable'] = classad.ExprTree(info.get('dbs_executable', 'dag_bootstrap.sh'))
        retval['Args'] = info.get('dbs_args', 'DBS None dbs_results')
        return retval

    def getJobSplittingClassAd(self, info):
        retval = classad.ClassAd()
        retval = self.addCRABMetaHeadersToClassAd(retval, info)
        retval = self.addCRABHeadersToClassAd(retval, info)
        retval['TaskType'] = "SPLIT"
        retval['universe'] = classad.ExprTree('local')
        retval['Output'] = classad.ExprTree('job_splitting.out')
        retval['Error']  = classad.ExprTree('job_splitting.err')
        retval['transfer_output_files'] = 'dbs_splitting_results'
        retval['Environment'] = 'PATH=/usr/bin:/bin %s' % info['additional_environment_options']
        retval['use_x509userproxy'] = 'True'
        retval['Executable'] = classad.ExprTree(info.get('split_executable', 'dag_bootstrap.sh'))
        retval['Args'] = info.get('split_args', 'SPLIT dbs_results job_splitting_results')
        return retval

    def getASOClassAd(self, info):
        retval = classad.ClassAd()
        retval = self.addCRABHeadersToClassAd(retval, info)
        retval['TaskType'] = "ASO"
        retval['CRAB_Id']  = "$(count)"
        retval['universe'] = classad.ExprTree('local')
        retval['Output'] = 'aso.$(count).out'
        retval['Error']  = 'aso.$(count).err'
        retval['Log']    = 'aso.$(count).log'
        retval['transfer_output_files'] = 'dbs_splitting_results'
        retval['Environment'] = 'PATH=/usr/bin:/bin %s' % info['additional_environment_options']
        retval['use_x509userproxy'] = 'True'
        retval['Executable'] = classad.ExprTree(info.get('aso_executable', 'dag_bootstrap.sh'))
        retval['Args'] = info.get('aso_args', 'ASO $(CRAB_AsyncDest) %(temp_dest)s %(output_dest)s $(count) $(Cluster).$(Process) cmsRun_$(count).log.tar.gz $(outputFiles)')
        retval['leave_in_queue'] = '(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)'
        return retval

    def getJobClassAd(self, info):
        retval = classad.ClassAd()
        retval = self.addCRABHeadersToClassAd(retval, info)
        retval['TaskType'] = "Job"
        retval['CRAB_Id']  = "$(count)"
        retval['CRAB_Attempt'] = info['attempt']
        retval['CRAB_Archive'] = info['cachefilename']
        retval['CRAB_ReqName'] = info['requestname']
        retval['CRAB_DBSURL'] = info['dbsurl']
        retval['CRAB_PublishDBSURL'] = info['publishdbsurl']
        retval['CRAB_Publish'] = info['publication']
        retval['CRAB_Dest'] = "cms://%s" % info['temp_dest']
        # FIXME: Is this needed? I thought DBS was GoodEnough
        if info.get('available_sites'):
            retval['AvailableSites'] = info['available_sites']
        if info.get('one_event_mode'):
            retval['CRAB_oneEventMode'] = 1
        retval['CRAB_Id'] = '$(count)'
        retval['CRAB_InputFiles'] = '$(inputFiles)'
        retval['CRAB_RunAndLumiMask'] ='$(runAndLumiMask)'
        retval['JOBGLIDEIN_CMSSITE'] = '$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\"Unknown\\", GLIDEIN_CMSSite)])'
        retval['Requirements'] = classad.ExprTree('(target.IS_GLIDEIN =!= TRUE) || (target.GLIDEIN_CMSSite =!= UNDEFINED)')
        retval['universe'] = classad.ExprTree('vanilla') #5 # vanilla
        retval['Output'] = 'job_out.$(count)'
        retval['Error']  = 'job_err.$(count)'
        retval['Log']    = 'job_log.$(count)'
        inputFiles = ['CMSRunAnalysis.sh', 'cmscp.py']
        inputFiles.extend(info['additional_input_files'])
        retval['transfer_input_files']  = ",".join([os.path.basename(x) for x in inputFiles])
        retval['transfer_output_files'] = "jobReport.json.$(count)"
        retval['should_transfer_files'] = classad.ExprTree('YES')
        retval['Environment'] = 'SCRAM_ARCH=$(CRAB_JobArch) %s' % info['additional_environment_options']
        retval['use_x509userproxy'] = classad.ExprTree('True')
        retval['Executable'] = info.get('job_executable', 'gWMS-CMSRunAnalysis.sh')
        retval['Args'] = info.get('job_args', "'--inputFile=$(inputFiles)' '--runAndLumis=$(runAndLumiMask)'")
        retval['leave_in_queue'] = '(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)'
        return retval

    def makeMasterClassAd(self, info, cmd, arg):
        """
        Produces the classad that wraps around the dagman process
        """
        dagAd = classad.ClassAd()
        addCRABInfoToClassAd(dagAd, info)

        # NOTE: Changes here must be synchronized with the job_submit in DagmanCreator.py in CAFTaskWorker
        dagAd["Out"] = str(os.path.join(info['scratch'], "request.out"))
        dagAd["Err"] = str(os.path.join(info['scratch'], "request.err"))
        dagAd["CRAB_Attempt"] = 0
        dagAd["JobUniverse"] = 12
        dagAd["HoldKillSig"] = "SIGUSR1"
        dagAd["Cmd"] = cmd
        dagAd['Args'] = arg
        dagAd["TransferInput"] = ",".join(info['inputFiles'])
        dagAd["LeaveJobInQueue"] = classad.ExprTree("(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")
        dagAd["TransferOutput"] = ",".join(info['outputFiles'])
        #dagAd["OnExitRemove"] = classad.ExprTree("( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))")
        dagAd["OtherJobRemoveRequirements"] = classad.ExprTree("DAGManJobId =?= ClusterId")
        dagAd["RemoveKillSig"] = "SIGUSR1"
        dagAd["Environment"] = classad.ExprTree('strcat("PATH=/usr/bin:/bin CONDOR_ID=", ClusterId, ".", ProcId," %s")' % info['additional_environment_options'])
        dagAd["RemoteCondorSetup"] = info['remote_condor_setup']
        dagAd["Requirements"] = classad.ExprTree('true || false')
        dagAd["TaskType"] = "ROOT"
        dagAd["X509UserProxy"] = str(info['userproxy'])
        dagAd["use_x509userproxy"] = classad.ExprTree('true')
        # Not entirely sure why this is needed
        dagAd['CRAB_ReqName'] = info['requestname']
        dagAd['should_transfer_files'] = classad.ExprTree('YES')
        if info['userproxy'] == None:
            raise RuntimeError, "The userproxy was None. That is bad"
        # hook testing functions
        if info.get('testReturnStdoutStderr', False):
            dagAd
        return dagAd


    def stripQuotesFromSomeAttributes(self, input):
        """
        I blame brian for this. When converting from new to old classads
        there's some ambiguity on if things should be strings or bare
        expressions. Classad could use about 10 million less parsing rules
        """
        unquoteAttributes = ['transferoutput',
                             'output',
                             'error',
                             'log',
                             'transferinput',
                             'universe',
                             'executable',
                             'transfer_output_files',
                             'transfer_input_files',
                             'use_x509userproxy']
        unquoteList = '|'.join([re.escape(x) for x in unquoteAttributes])
        matchRE = re.compile(r'^(%s)\s*=\s*"+(.*)"+\s*$' % unquoteList, re.IGNORECASE | re.MULTILINE)
        print "Matching with pattern %s" % matchRE.pattern
        retval = re.sub(matchRE, r'\1 = \2', input)
        return retval

    def putPlusMacrosOnTop(self, var):
        top = []
        bottom = []
        for line in var.split('\n'):
            print "examining %s" % line
            if len(line) and line[0] == '+':
                top.append(line)
            else:
                bottom.append(line)
        top.extend(bottom)
        return "\n".join(top)

    def makeDagSubmitFiles(self, scratch, info):
        # TODO: make this use the classad library
        replaceStringRE = re.compile('^CRAB_', re.MULTILINE)
        with open(os.path.join(scratch, "master_dag"), "w") as fd:
            fd.write(self.getMasterDag(info))
        with open(os.path.join(scratch, "DBSDiscovery.submit"), "w") as fd:
            oldStr = self.getDBSDiscoveryClassAd(info).printOld()
            oldStr = re.sub(replaceStringRE, '+CRAB_', oldStr)
            oldStr = self.stripQuotesFromSomeAttributes(oldStr)
            fd.write(oldStr)
            fd.write("\nqueue 1\n")
        with open(os.path.join(scratch, "JobSplitting.submit"), "w") as fd:
            oldStr = self.getJobSplittingClassAd(info).printOld()
            oldStr = re.sub(replaceStringRE, '+CRAB_', oldStr)
            oldStr = self.stripQuotesFromSomeAttributes(oldStr)
            fd.write(oldStr)
            fd.write("\nqueue 1\n")
        with open(os.path.join(scratch, "Job.submit"), "w") as fd:
            oldStr = self.getJobClassAd(info).printOld()
            oldStr = re.sub(replaceStringRE, '+CRAB_', oldStr)
            oldStr = self.putPlusMacrosOnTop(oldStr)
            oldStr = self.stripQuotesFromSomeAttributes(oldStr)
            fd.write(oldStr)
            fd.write("\nqueue 1\n")
        with open(os.path.join(scratch, 'ASO.submit'), 'w') as fd:
            oldStr = self.getASOClassAd(info).printOld()
            oldStr = re.sub(replaceStringRE, '+CRAB_', oldStr)
            oldStr = self.putPlusMacrosOnTop(oldStr)
            oldStr = self.stripQuotesFromSomeAttributes(oldStr)
            fd.write(oldStr)
            fd.write("\nqueue 1\n")


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

    def getTransformLocation(self):
        """
        Returns the location of the PanDA job transform
        """
        # TODO: Nuke this with the rest of the dev hooks
        tDir = os.path.join(getCAFUtilitiesBase(), "src", "python", \
                    "transformation")
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

    def validateKWArgParty(self, args):
        """
        To submit, the client/server just pass in a gigantic call with 30+
        arguments. This function will read through the arguments and try
        to at least be somewhat sensible
        """
        requiredArgs = ["workflow", "jobtype", "jobsw", "jobarch", "inputdata",
                        "siteblacklist", "sitewhitelist", "splitalgo",
                        "algoargs", "cachefilename", "cacheurl",
                        "addoutputfiles", "userhn", "userdn", "savelogsflag",
                        "publishname", "asyncdest", "blacklistT1", "dbsurl",
                        "publishdbsurl", "vorole", "vogroup", "tfileoutfiles",
                        "edmoutfiles", "runs", "lumis"]

        optionalArgs = ["testSleepMode", "oneEventMode", 
                        "taskManagerCodeLocation", "taskManagerTarball",
                        "blockblacklist", "blockwhitelist", "userisburl",
                        "publication", "adduserfiles", "totalunits",
                        "userproxy"]

        for k in requiredArgs:
            if not k in args:
                raise RuntimeError, "Required argument \"%s\" wasn't received" % k
        # FIXME: What's the right idiom for this? I want to iterate over both
        for k in args:
            if k == 'kwargs':
                continue
            if not k in requiredArgs and not k in optionalArgs:
                warnings.warn("Got an unexpected argument \"%s\"" % k)
        for k in args['kwargs']:
            if not k in requiredArgs and not k in optionalArgs:
                warnings.warn("Got an unexpected argument \"%s\"" % k)
    def addDebugHooks(self, kwargs, info):
        assert(not info.get('additional_input_files', False))
        assert(not info.get('additional_environment_options', False))
        envHooks = []
        addFiles = []

        # Handle the taskmanager tarball
        if kwargs.get('taskManagerTarball', None) and \
                kwargs.get('taskManagerCodeLocation', None) and \
                kwargs['taskManagerTarball'] != 'local':
            raise RuntimeError, "Debug.taskManagerTarball must be 'local' if you provide a code location"
        if kwargs.get('taskManagerCodeLocation', None):
            kwargs['taskManagerTarball'] = 'local'

        if kwargs.get('taskManagerTarball', None):
            envHooks.append('CRAB_TASKMANAGER_TARBALL=%s' % kwargs['taskManagerTarball'])
        
        info['one_event_mode'] = kwargs.get('oneEventMode', False)

        if kwargs.get('taskManagerTarball') == 'local':
            if not kwargs.get('taskManagerCodeLocation', None):
                raise RuntimeError, "Tarball was set to local, but not location was set"
            # we need to generate the tarball to ship along with the jobs
            #  CAFTaskWorker/bin/dagman_make_runtime.sh
            self.logger.info('Packing up tarball from local source tree')
            runtimePath = os.path.expanduser( os.path.join( 
                                        kwargs['taskManagerCodeLocation'],
                                        'CRABServer',
                                        'bin',
                                        'dagman_make_runtime.sh') )
            envCopy = copy.copy(os.environ)
            envCopy['CRAB3_VERSION'] = 'SNAPSHOT'
            tarMaker = subprocess.Popen([ runtimePath, kwargs['taskManagerCodeLocation']],
                                         env=envCopy)
            tarMaker.communicate()
            shutil.copy('TaskManagerRun-SNAPSHOT.tar.gz', 'TaskManagerRun.tar.gz')
            addFiles.append('TaskManagerRun.tar.gz')
        info['additional_input_files'] = addFiles
        info['additional_environment_options'] = " ".join(envHooks)
    
    def makeSubmitInfoDict(self, args):
        """
        Parse all the arguments and put it into a dict which will later
        be converted into a ClassAd
        """
        # Poor-man's string escaping.  We do this as classad module isn't guaranteed to be present.
        # TODO: We're going to require it to be present. Sucks to your assmar.
        info = self.pullLocalsIntoClassAd(args)

        # Condor will barf if the requesname is quoted .. for some reason
        assert(info['requestname'].find('"') == -1)

        # Add additional arguments to the info dict
        self.addDebugHooks(args['kwargs'], info)
        info['remote_condor_setup'] = self.getRemoteCondorSetup()
        info['bindir'] = self.getBinDir()
        info['transform_location'] = self.getTransformLocation()
        return info

    def makeRootClassAd(self, info, scratch):
        cmd  = os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh")
        # debug hook
        if info.get('testReturnStdoutStderr', False):
            cmd = os.path.join(self.getBinDir(), 'test_write_to_outputs.sh')
        args = 'master_dag'
        inputFiles = [os.path.join(self.getBinDir(), "dag_bootstrap.sh"),
		               cmd,
                       self.getTransformLocation(),
                       os.path.join(self.getBinDir(), "cmscp.py")]
        scratch_files = ['master_dag', 'DBSDiscovery.submit',
                            'JobSplitting.submit', 'master_dag',
                            'Job.submit', 'ASO.submit']
        inputFiles.extend([os.path.join(scratch, i) for i in scratch_files])
        
        # If the children jobs are gonna want these, we have to ship it too
        # TODO: What happens if these are absolute paths? Does condor's path munging
        # blow up when we move the files to the $CWD on the schedd then the dag
        # looks for them?
        inputFiles.extend(info['additional_input_files'])
        info['inputFiles'] = inputFiles
        outputFiles = ["master_dag.dagman.out", "master_dag.rescue.001", "RunJobs.dag",
            "RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001", "dbs_discovery.err",
            "dbs_discovery.out", "job_splitting.err", "job_splitting.out"]
        info['outputFiles'] = outputFiles
        return self.makeMasterClassAd(info, cmd, args)



    # NOTE NOTE NOTE
    # The following function gets wrapped in a proxy decorator below
    # the wrapped one is called submit (imagine that)
    def submitUnwrapped(self, workflow, jobtype, jobsw, jobarch, inputdata, siteblacklist, sitewhitelist, splitalgo, algoargs, cachefilename, cacheurl, addoutputfiles, userhn, userdn, savelogsflag, publishname, asyncdest, blacklistT1, dbsurl, publishdbsurl, vorole, vogroup, tfileoutfiles, edmoutfiles, runs, lumis, userproxy= None, **kwargs):

        # Esp for unittesting, you can get the same timestamp
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())

        # Parse arguments
        self.validateKWArgParty(locals())
        info = self.makeSubmitInfoDict(locals())

        # Initialize objects, request area
        dagmanSubmitter = DagmanSubmitter(self.config)
        scheddName = dagmanSubmitter.getSchedd()
        schedd, address = dagmanSubmitter.getScheddObj(scheddName)
        requestname = '%s_%s_%s_%s' % (scheddName, timestamp, userhn, workflow)
        if self.requestarea == '/tmp/crab3':
            raise RuntimeError, "This path shouldn't be hit any more. Complain to melo"
        scratch = os.path.join(self.requestarea, "." + requestname)

        # Handle shipping the user sandbox within condor
        if info['cacheurl'] == 'LOCAL':
            sandboxLoc = os.path.join(self.requestarea,
                                      'inputs',
                                      info['cachefilename'])
            info['additional_input_files'].append(sandboxLoc)
        os.makedirs(scratch)
        info['scratch'] = scratch
        info['requestname'] = requestname
        # If true, the dag will just spray the stdout with a bunch of files
        # and return
        info['testReturnStdoutStderr'] = kwargs.get('testReturnStdoutStderr', False)       
        # Generate all the submit files that will be needed on the schedd
        # within the dag
        self.makeDagSubmitFiles(scratch, info)

        # Generate the clasad that we will submit from here (either the client
        # in standalone mode, or on the CRABServer in server mode)
        rootAd = self.makeRootClassAd(info, scratch)
        if address:
            self.logger.info("Submitting directly to HTCondor (via python bindings)")
            # Submit directly to a scheduler -- in this case, we don't need to xfer
            # input files through condor, we can move them ourselves to our scratch
            # dir -- this code can probably end up workng for remote submit too
            absoluteScratchPath = os.path.abspath(scratch)
            for oneFile in str(rootAd['TransferInput']).split(","):
                if os.path.dirname(os.path.abspath(oneFile)) == absoluteScratchPath:
                    continue
                else:
                    shutil.copy(oneFile, absoluteScratchPath)
            # The stdout/err can live where it started
            for extraArg in ('Err','err','Out','out'):
                if not rootAd.get(extraArg, None):
                    continue
                if os.path.dirname(os.path.abspath(rootAd[extraArg])) == absoluteScratchPath:
                    rootAd[extraArg] = os.path.basename(rootAd[extraArg])

            # For now, just do all the dagman stuff in one dir
            del rootAd['TransferInput']
            del rootAd['TransferOutput']
            rootAd['CRAB_SubmitScratchDir'] = absoluteScratchPath
            dagmanSubmitter.submitClassAd(schedd,
                os.path.join(self.getBinDir(), "dag_bootstrap_startup.sh"),
                'master_dag',
                rootAd,
                info['userproxy'])
        else:
            # Submit over Gsissh
            raise NotImplementedError, "This needs to be moved over to classad"
            # testing getting the right directory
            self.logger.info("Submitting remotely to HTCondor (via gsissh)")
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
    # NOTE: Tricky bit that makes submit = the wrapped submit function 
    submit = retrieveUserCert(submitUnwrapped)


    @retrieveUserCert
    def kill(self, workflow, force, userdn, **kwargs):
        """Request to Abort a workflow.

           :arg str workflow: a workflow name"""

        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)

        userproxy = kwargs['userproxy']
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        dag = DagmanSubmitter(self.config)
        scheddName = dag.getSchedd()
        schedd, address = dag.getScheddObj(scheddName)

        const = 'TaskType =?= \"ROOT\" && CRAB_ReqName =?= "%s" && CRAB_UserDN =?= "%s"' % (workflow, userdn)
        if address:
            with ReadFork() as (pid, rpipe, wpipe):
                if pid == 0:
                    htcondor.SecMan().invalidateAllSessions()
                    os.environ['X509_USER_PROXY'] = userproxy
                    try:
                        schedd.edit(const, "HoldReason", "\"%s\"" % self.JOB_KILLED_HOLD_REASON)
                        schedd.act(htcondor.JobAction.Hold, const)
                        schedd.edit(const, "HoldReason", "\"%s\"" % self.JOB_KILLED_HOLD_REASON)
                    except RuntimeError, e:
                        # racy, but hopefully rare
                        if not schedd.query(const, ['JobStatus']):
                            # We tried to edit jobs and none of them existed
                            self.logger.debug("Tried to kill some jobs that didn't exist")
                        else:
                            raise
                    wpipe.write("OK")
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
            with ReadFork() as (pid, rpipe, wpipe):
                if pid == 0:
                    htcondor.SecMan().invalidateAllSessions()
                    os.environ['X509_USER_PROXY'] = userproxy
                    schedd.edit(subDagConst, "HoldKillSig", "\"SIGUSR1\"")
                    schedd.act(htcondor.JobAction.Hold, subDagConst)
                    schedd.edit(finished_jobConst, "DAGManJobId", "-1")
                    wpipe.write("OK")
            results = rpipe.read()
            if results != "OK":
                raise Exception("Failure when killing job: %s" % results)
        else:
            # Use the remoteCondor plugin
            schedd.edit(subDagConst, "HoldKillSig", "SIGUSR1")
            schedd.hold(const) #pylint: disable=E1103

    def getScheddAndAddress(self):
        dag = DagmanSubmitter(self.config)
        scheddName = dag.getSchedd()
        return  dag.getScheddObj(scheddName)

    def getRootTasks(self, workflow, schedd):
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= \"%s\" && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)" % workflow
        rootAttrList = ["JobStatus", "ExitCode", 'CRAB_JobCount', 'CRAB_ReqName', 'TaskType', "HoldReason"]
        #print "Using rootConst: %s" % rootConst
        dag = DagmanSubmitter(self.config)
        scheddName = dag.getSchedd()
        schedd, address = dag.getScheddObj(scheddName)
        if address:
            results = schedd.query(rootConst, rootAttrList)
        else:
            results = schedd.getClassAds(rootConst, rootAttrList)

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            self.logger.info("Tried to read from address %s" % address)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)
        return results

    def getASOJobs(self, workflow, schedd):
        jobConst = "TaskType =?= \"ASO\" && CRAB_ReqName =?= \"%s\"" % workflow
        jobList = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id']
        _, address = self.getScheddAndAddress()
        if address:
            results = schedd.query(jobConst, jobList)
        else:
            results = schedd.getClassAds(jobConst, jobList)
        return results

    def getJobs(self, workflow, schedd):
        jobConst = "TaskType =?= \"Job\" && CRAB_ReqName =?= \"%s\"" % workflow
        jobList = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id']
        _, address = self.getScheddAndAddress()
        if address:
            results = schedd.query(jobConst, jobList)
        else:
            results = schedd.getClassAds(jobConst, jobList)
        return results

    def serializeFinishedJobs(self, workflow, userdn, userproxy=None):
        if not userproxy:
            return
        jobdir = os.path.join(self.requestdir, "."+workflow, "finished_condor_jobs")
        jobdir = os.abspath(jobdir)
        requestdir = os.abspath(self.requestdir)
        assert(jobdir.startswith(requestdir))
        try:
            os.makedirs(jobdir)
        except OSError, oe:
            if oe.errno != errno.EEXIST:
                raise

        dag = DagmanSubmitter(self.config)
        schedd, address = dag.getScheddObj(name)

        finished_jobs = {}

        jobs = self.getJobs(workflow, schedd)
        jobs.extend(self.getASOJobs(workflow, schedd))
        for job in jobs:
            if 'CRAB_Id' not in job: continue
            if (int(job.get("JobStatus", "1")) == 4) and (task_is_finished or (int(job.get("ExitCode", -1)) == 0)):
                finished_jobs["%s.%s" % (job['ClusterID'], job['ProcID'])] = job

        # TODO: consider POSIX data integrity
        for id, job in finished_jobs.items():
            with open(os.path.join(jobdir, id), "w") as fd:
                json.dump(dict(job), fd)

        if address:
            schedd.act(htcondor.JobAction.Remove, finished_jobs.keys())
        else:
            schedd.remove(finished_jobs.keys())

    def getReports(self, workflow, userproxy):
        if not userproxy: return

        jobdir = os.path.join(self.requestdir, "."+workflow, "job_results")
        jobdir = os.abspath(jobdir)
        requestdir = os.abspath(self.requestdir)
        assert(jobdir.startswith(requestdir))
        try:
            os.makedirs(jobdir)
        except OSError, oe:
            if oe.errno != errno.EEXIST:
                raise

        dag = DagmanSubmitter(self.config)
        schedd, address = dag.getScheddObj(name)

        job_ids = {}
        aso_ids = set()
        for job in self.getJobs(workflow, schedd):
            if 'CRAB_Id' not in job: continue
            if int(job.get("JobStatus", "1")) == 4:
                job_ids[int(job['CRAB_Id'])] = "%s.%s" % (job['ClusterID'], job['ProcID'])

        if address:
            schedd.edit(job_ids.values(), "TransferOutputRemaps", 'strcat("jobReport.json.", CRAB_Id, "=%s/jobReport.json.", CRAB_Id, ";job_out.", CRAB_Id, "=/dev/null;job_err.", CRAB_Id, "=/dev/null")' % jobdir)
            schedd.retrieve(job_ids.values())
        else:
            raise NotImplementedError, "Need to implement getreports over gsissh"

    def status(self, workflow, userdn, userproxy=None):
        """Retrieve the status of the workflow

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""
        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name: %s" % workflow)
        
        name = workflow.split("_")[0]
        self.logger.debug("Getting status for workflow %s, looking for schedd %s" %\
                                (workflow, name))
        dag = DagmanSubmitter(self.config)
        schedd, address = dag.getScheddObj(name)

        results = self.getRootTasks(workflow, schedd)
        #print "Got root tasks: %s" % results
        jobsPerStatus = {}
        jobStatus = {}
        jobList = []
        taskStatusCode = int(results[-1]['JobStatus'])
        taskJobCount = int(results[-1].get('CRAB_JobCount', 0))
        codes = {1: 'Idle', 2: 'Running', 4: 'Completed', 5: 'Killed'}
        retval = {"status": codes.get(taskStatusCode, 'Unknown'), "taskFailureMsg": "", "jobSetID": workflow,
            "jobsPerStatus" : jobsPerStatus, "jobList": jobList}
        if taskStatusCode == 5 and \
                results[-1]['HoldReason'] != self.JOB_KILLED_HOLD_REASON:
            retval['status'] = 'InTransition'

        failedJobs = []
        allJobs = self.getJobs(workflow, schedd)
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
        for result in self.getASOJobs(workflow, schedd):
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
        dag = DagmanSubmitter(self.config)
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

        dag = DagmanSubmitter(self.config)
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
            raise RuntimeError, "Couldn't find root dagman job to resubmit. It may have been too long"

        subDagConst = "DAGManJobId =?= %s && DAGParentNodeNames =?= \"JobSplitting\"" % results[0]["ClusterId"]
        if address:
            self.logger.debug('Resubmitting via python bindings')
            with ReadFork() as (pid, rpipe, wpipe):
                if pid == 0:
                    htcondor.SecMan().invalidateAllSessions()
                    os.environ['X509_USER_PROXY'] = userproxy
                    # change the hold reason so status calls will see InTransition
                    # and not Killed
                    schedd.act(htcondor.JobAction.Release, subDagConst)
                    schedd.act(htcondor.JobAction.Release, rootConst)
                    schedd.edit(rootConst, "HoldReason", "\"%s\"" % self.JOB_RESTART_HOLD_REASON)
                    try:
                        schedd.edit(subDagConst, "HoldReason", "\"%s\"" % self.JOB_RESTART_HOLD_REASON)
                    except:
                        # The above blows up if there's no matching jobs.
                        # worst case is some jobs show up as killed temporarily
                        pass
                    schedd.act(htcondor.JobAction.Release, subDagConst)
                    schedd.act(htcondor.JobAction.Release, rootConst)
                    
                    wpipe.write("OK")

            results = rpipe.read()
            if results != "OK":
                raise Exception("Failure when resubmitting job: %s" % results)
        else:
            self.logger.debug('Resubmitting via gsissh')
            schedd.release(subDagConst) #pylint: disable=E1103
            schedd.release(rootConst) #pylint: disable=E1103

    def report(self, workflow, userdn, userproxy=None):

        self.getReports(workflow, userdn, userproxy)

        #load the lumimask
        rows = self.api.query(None, None, ID.sql, taskname = workflow)
        splitArgs = literal_eval(rows.next()[6].read())
        res['lumiMask'] = buildLumiMask(splitArgs['runs'], splitArgs['lumis'])

        #extract the finished jobs from filemetadata
        jobids = [x[1] for x in statusRes['jobList'] if x[0] in ['finished']]
        rows = self.api.query(None, None, GetFromPandaIds.sql, types='EDM', taskname=workflow, jobids=','.join(map(str,jobids)),\
                                        limit=len(jobids)*100)
        res['runsAndLumis'] = {}
        for row in rows:
            res['runsAndLumis'][str(row[GetFromPandaIds.PANDAID])] = { 'parents' : row[GetFromPandaIds.PARENTS].read(),
                    'runlumi' : row[GetFromPandaIds.RUNLUMI].read(),
                    'events'  : row[GetFromPandaIds.INEVENTS],
            }

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

