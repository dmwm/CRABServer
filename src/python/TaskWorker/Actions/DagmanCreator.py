
"""
Create a set of files for a DAG submission.

Generates the condor submit files and the master DAG.
"""

import os
import json
import base64
import shutil
import string
import urllib
import logging
import commands
import tempfile

import TaskWorker.Actions.TaskAction as TaskAction
import TaskWorker.DataObjects.Result
import TaskWorker.WorkerExceptions

import WMCore.WMSpec.WMTask

try:
    from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
except ImportError:
    UserFileCache = None

from ApmonIf import ApmonIf

DAG_FRAGMENT = """
JOB Job%(count)d Job.submit
SCRIPT PRE  Job%(count)d dag_bootstrap.sh PREJOB $RETRY
SCRIPT POST Job%(count)d dag_bootstrap.sh POSTJOB $JOBID $RETURN $RETRY $MAX_RETRIES %(restinstance)s %(resturl)s %(taskname)s %(count)d %(outputData)s %(sw)s %(asyncDest)s %(tempDest)s %(outputDest)s cmsRun_%(count)d.log.tar.gz %(remoteOutputFiles)s
#PRE_SKIP Job%(count)d 3
RETRY Job%(count)d 10 UNLESS-EXIT 2
VARS Job%(count)d count="%(count)d" runAndLumiMask="%(runAndLumiMask)s" inputFiles="%(inputFiles)s" +DESIRED_Sites="\\"%(desiredSites)s\\"" +CRAB_localOutputFiles="\\"%(localOutputFiles)s\\""

"""

CRAB_HEADERS = \
"""
+CRAB_ReqName = %(requestname)s
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
+CRAB_ReqName = %(requestname)s
#CRAB_ReqName = %(requestname_flatten)s
CRAB_DBSURL = %(dbsurl_flatten)s
CRAB_PublishDBSURL = %(publishdbsurl_flatten)s
CRAB_Publish = %(publication)s
CRAB_Id = $(count)
+CRAB_Id = $(count)
+CRAB_Dest = "cms://%(temp_dest)s"
+CRAB_oneEventMode = %(oneEventMode)s
+TaskType = "Job"
+MaxWallTimeMins = 1315
+AccountingGroup = %(userhn)s

+JOBGLIDEIN_CMSSite = "$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\"Unknown\\", GLIDEIN_CMSSite)])"
job_ad_information_attrs = MATCH_EXP_JOBGLIDEIN_CMSSite, JOBGLIDEIN_CMSSite

universe = vanilla
Executable = gWMS-CMSRunAnalysis.sh
Output = job_out.$(CRAB_Id)
Error = job_err.$(CRAB_Id)
Log = job_log
# args changed...

Arguments = "-a $(CRAB_Archive) --sourceURL=$(CRAB_ISB) --jobNumber=$(CRAB_Id) --cmsswVersion=$(CRAB_JobSW) --scramArch=$(CRAB_JobArch) '--inputFile=$(inputFiles)' '--runAndLumis=$(runAndLumiMask)' -o $(CRAB_AdditionalOutputFiles)"

transfer_input_files = CMSRunAnalysis.sh, cmscp.py
transfer_output_files = jobReport.json.$(count)
# TODO: fold this into the config file instead of hardcoding things.
Environment = SCRAM_ARCH=$(CRAB_JobArch);CRAB_TASKMANAGER_TARBALL=http://hcc-briantest.unl.edu/CMSRunAnalysis-3.3.0-pre1.tar.gz;%(additional_environment_options)s
should_transfer_files = YES
#x509userproxy = %(x509up_file)s
use_x509userproxy = true
# TODO: Uncomment this when we get out of testing mode
Requirements = (target.IS_GLIDEIN =!= TRUE) || (target.GLIDEIN_CMSSite =!= UNDEFINED)
#Requirements = ((target.IS_GLIDEIN =!= TRUE) || ((target.GLIDEIN_CMSSite =!= UNDEFINED) && (stringListIMember(target.GLIDEIN_CMSSite, DESIRED_SEs) )))
#leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
periodic_release = (HoldReasonCode == 28) || (HoldReasonCode == 30) || (HoldReasonCode == 13) || (HoldReasonCode == 6)
periodic_remove = (JobStatus =?= 5) && (time() - EnteredCurrentStatus > 7*60)
queue
"""

SPLIT_ARG_MAP = { "LumiBased" : "lumis_per_job",
                  "FileBased" : "files_per_job",}

LOGGER = None

def transform_strings(input):
    """
    Converts the arguments in the input dictionary to the arguments necessary
    for the job submit file string.
    """
    info = {}
    for var in 'workflow', 'jobtype', 'jobsw', 'jobarch', 'inputdata', 'splitalgo', 'algoargs', \
           'cachefilename', 'cacheurl', 'userhn', 'publishname', 'asyncdest', 'dbsurl', 'publishdbsurl', \
           'userdn', 'requestname', 'publication', 'oneEventMode':
        val = input.get(var, None)
        if val == None:
            info[var] = 'undefined'
        else:
            info[var] = json.dumps(val)

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
    info['lumimask'] = '"' + json.dumps(WMCore.WMSpec.WMTask.buildLumiMask(input['runs'], input['lumis'])).replace(r'"', r'\"') + '"'
    splitArgName = SPLIT_ARG_MAP[input['splitalgo']]
    info['algoargs'] = '"' + json.dumps({'halt_job_on_file_boundaries': False, 'splitOnRun': False, splitArgName : input['algoargs']}).replace('"', r'\"') + '"'
    info['attempt'] = 0

    for var in ["cacheurl", "jobsw", "jobarch", "cachefilename", "asyncdest", "dbsurl", "publishdbsurl", "requestname"]:
        info[var+"_flatten"] = input[var]

    # TODO: PanDA wrapper wants some sort of dictionary.
    info["addoutputfiles_flatten"] = '{}'

    #info["output_dest"] = os.path.join("/store/user", input['userhn'], input['workflow'], input['publishname'])
    #info["temp_dest"] = os.path.join("/store/temp/user", input['userhn'], input['workflow'], input['publishname'])
    if input['inputdata']:
        primaryds = input['inputdata'].split('/')[1]
    else:
        # For MC
        primaryds = input['publishname'].rsplit('-', 1)[0]
    info["temp_dest"] = os.path.join("/store/temp/user", input['userhn'], primaryds, input['publishname'].rsplit('-', 1)[0], input['publishname'].rsplit('-', 1)[1])
    info["output_dest"] = os.path.join("/store/user", input['userhn'], primaryds, input['publishname'].rsplit('-', 1)[0], input['publishname'].rsplit('-', 1)[1])
    info['x509up_file'] = os.path.split(input['user_proxy'])[-1]
    info['user_proxy'] = input['user_proxy']
    info['scratch'] = input['scratch']

    return info

# TODO: DagmanCreator started life as a flat module, then the DagmanCreator class
# was later added.  We need to come back and make the below methods class methods

def makeJobSubmit(task):
    if os.path.exists("Job.submit"):
        return
    # From here on out, we convert from tm_* names to the DataWorkflow names
    info = dict(task)
    info['workflow'] = task['tm_taskname']
    info['jobtype'] = 'analysis'
    info['jobsw'] = info['tm_job_sw']
    info['jobarch'] = info['tm_job_arch']
    info['inputdata'] = info['tm_input_dataset']
    info['splitalgo'] = info['tm_split_algo']
    info['algoargs'] = info['tm_split_args']
    info['cachefilename'] = info['tm_user_sandbox']
    info['cacheurl'] = info['tm_cache_url']
    info['userhn'] = info['tm_username']
    info['publishname'] = info['tm_publish_name']
    info['asyncdest'] = info['tm_asyncdest']
    info['dbsurl'] = info['tm_dbs_url']
    info['publishdbsurl'] = info['tm_publish_dbs_url']
    info['publication'] = info['tm_publication']
    info['userdn'] = info['tm_user_dn']
    info['requestname'] = string.replace(task['tm_taskname'],'"', '')
    info['savelogsflag'] = 0
    info['blacklistT1'] = 0
    info['siteblacklist'] = task['tm_site_blacklist']
    info['sitewhitelist'] = task['tm_site_whitelist']
    info['addoutputfiles'] = task['tm_outfiles']
    info['tfileoutfiles'] = task['tm_tfile_outfiles']
    info['edmoutfiles'] = task['tm_edm_outfiles']
    info['oneEventMode'] = 1 if task.get('tm_arguments', {}).get('oneEventMode', 'F') == 'T' else 0
    # TODO: pass through these correctly.
    info['runs'] = []
    info['lumis'] = []
    info = transform_strings(info)
    info.setdefault("additional_environment_options", '')
    with open("Job.submit", "w") as fd:
        fd.write(JOB_SUBMIT % info)

    return info

def make_specs(task, jobgroup, availablesites, outfiles, startjobid):
    specs = []
    i = startjobid
    for job in jobgroup.getJobs():
        inputFiles = json.dumps([inputfile['lfn'] for inputfile in job['input_files']]).replace('"', r'\"\"')
        runAndLumiMask = json.dumps(job['mask']['runAndLumis']).replace('"', r'\"\"')
        desiredSites = ", ".join(availablesites)
        i += 1
        remoteOutputFiles = []
        localOutputFiles = []
        for origFile in outfiles:
            info = origFile.rsplit(".", 1)
            if len(info) == 2:
                fileName = "%s_%d.%s" % (info[0], i, info[1])
            else:
                fileName = "%s_%d" % (origFile, i)
            remoteOutputFiles.append("%s" % fileName)
            localOutputFiles.append("%s?remoteName=%s" % (origFile, fileName))
        remoteOutputFiles = " ".join(remoteOutputFiles)
        localOutputFiles = ", ".join(localOutputFiles)
        if task['tm_input_dataset']:
            primaryds = task['tm_input_dataset'].split('/')[1]
        else:
            # For MC
            primaryds = task['tm_publish_name'].rsplit('-', 1)[0]
        counter = "%04d" % (i / 1000)
        specs.append({'count': i, 'runAndLumiMask': runAndLumiMask, 'inputFiles': inputFiles,
                      'desiredSites': desiredSites, 'remoteOutputFiles': remoteOutputFiles,
                      'localOutputFiles': localOutputFiles, 'asyncDest': task['tm_asyncdest'],
                      'sw': task['tm_job_sw'], 'taskname': task['tm_taskname'],
                      'outputData': task['tm_publish_name'],
                      'tempDest': os.path.join("/store/temp/user", task['tm_username'], primaryds, task['tm_publish_name'].rsplit('-', 1)[0], task['tm_publish_name'].rsplit('-', 1)[1], counter),
                      'outputDest': os.path.join("/store/user", task['tm_username'], primaryds, task['tm_publish_name'].rsplit('-', 1)[0], task['tm_publish_name'].rsplit('-', 1)[1], counter),
                      'restinstance': task['restinstance'], 'resturl': task['resturl']})

        LOGGER.debug(specs[-1])
    return specs, i


def getLocation(default_name, checkout_location):
    loc = default_name
    if not os.path.exists(loc):
        if 'CRABTASKWORKER_ROOT' in os.environ:
            for path in ['xdata','data']:
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

    def buildDashboardInfo(self):

        params = {'tool': 'crab3',
                  'SubmissionType':'direct',
                  'JSToolVersion': '3.3.0',
                  'tool_ui': os.environ.get('HOSTNAME',''),
                  'scheduler': 'GLIDEIN',
                  'GridName': self.task['tm_user_dn'],
                  'ApplicationVersion': 'tm_job_sw',
                  'taskType': 'integration',
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


    def createSubdag(self, splitter_result, **kwargs):

        startjobid = 0
        specs = []

        info = makeJobSubmit(kwargs['task'])

        outfiles = kwargs['task']['tm_outfiles'] + kwargs['task']['tm_tfile_outfiles'] + kwargs['task']['tm_edm_outfiles']

        os.chmod("CMSRunAnalysis.sh", 0755)

        server_data = []

        # This config setting acts as a global black / white list
        global_whitelist = set()
        global_blacklist = set()
        if hasattr(self.config.Sites, 'available'):
            global_whitelist = set(self.config.Sites.available)
        if hasattr(self.config.Sites, 'banned'):
            global_blacklist = set(self.config.Sites.banned)

        for jobgroup in splitter_result:
            jobs = jobgroup.getJobs()

            if not jobs:
                possiblesites = []
            else:
                possiblesites = jobs[0]['input_files'][0]['locations']
            self.logger.debug("Possible sites: %s" % possiblesites)
            self.logger.debug('Blacklist: %s; whitelist %s' % (kwargs['task']['tm_site_blacklist'], kwargs['task']['tm_site_whitelist']))
            if kwargs['task']['tm_site_whitelist']:
                availablesites = set(kwargs['task']['tm_site_whitelist'])
            else:
                availablesites = set(possiblesites) - set(kwargs['task']['tm_site_blacklist'])

            # Apply globals
            availablesites = set(availablesites) - global_blacklist
            if global_whitelist:
                availablesites = set(availablesites) & global_whitelist

            availablesites = [str(i) for i in availablesites]
            self.logger.info("Resulting available sites: %s" % ", ".join(availablesites))

            if not availablesites:
                msg = "No site available for submission of task %s" % (kwargs['task']['tm_taskname'])
                raise TaskWorker.WorkerExceptions.NoAvailableSite(msg)

            jobgroupspecs, startjobid = make_specs(kwargs['task'], jobgroup, availablesites, outfiles, startjobid)
            specs += jobgroupspecs

        dag = "\nNODE_STATUS_FILE node_state 30\n"
        for spec in specs:
            dag += DAG_FRAGMENT % spec

        with open("RunJobs.dag", "w") as fd:
            fd.write(dag)

        task_name = kwargs['task'].get('CRAB_ReqName', kwargs['task'].get('tm_taskname', ''))
        userdn = kwargs['task'].get('CRAB_UserDN', kwargs['task'].get('tm_user_dn', ''))

        info["jobcount"] = len(jobgroup.getJobs())

        # Info for ML:
        ml_info = info.setdefault('apmon', [])
        for idx in range(1, info['jobcount']+1):
            taskid = kwargs['task']['tm_taskname'].replace("_", ":")
            jinfo = {'jobId': ("%d_https://glidein.cern.ch/%d/%s_0" % (idx, idx, taskid)),
                     'sid': "https://glidein.cern.ch/%d/%s" % (idx, taskid),
                     'broker': os.environ.get('HOSTNAME',''),
                     'bossId': str(idx),
                     'TargetSE': ("%d_Selected_SE" % len(specs[idx-1]['desiredSites'])),
                     'localId' : '',
                    }
            ml_info.append(jinfo)

        # When running in standalone mode, we want to record the number of jobs in the task
        if ('CRAB_ReqName' in kwargs['task']) and ('CRAB_UserDN' in kwargs['task']):
            const = 'TaskType =?= \"ROOT\" && CRAB_ReqName =?= "%s" && CRAB_UserDN =?= "%s"' % (task_name, userdn)
            cmd = "condor_qedit -const '%s' CRAB_JobCount %d" % (const, len(jobgroup.getJobs()))
            self.logger.debug("+ %s" % cmd)
            status, output = commands.getstatusoutput(cmd)
            if status:
                self.logger.error(output)
                self.logger.error("Failed to record the number of jobs.")
                return 1

        return info


    def executeInternal(self, *args, **kw):
        global LOGGER
        LOGGER = self.logger

        cwd = None
        if hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'scratchDir'):
            temp_dir = tempfile.mkdtemp(prefix='_' + kw['task']['tm_taskname'], dir=self.config.TaskWorker.scratchDir)

            # FIXME: In PanDA, we provided the executable as a URL.
            # So, the filename becomes http:// -- and doesn't really work.  Hardcoding the analysis wrapper.
            #transform_location = getLocation(kw['task']['tm_transformation'], 'CAFUtilities/src/python/transformation/CMSRunAnalysis/')
            transform_location = getLocation('CMSRunAnalysis.sh', 'CRABServer/scripts/')
            cmscp_location = getLocation('cmscp.py', 'CRABServer/scripts/')
            gwms_location = getLocation('gWMS-CMSRunAnalysis.sh', 'CRABServer/scripts/')
            dag_bootstrap_location = getLocation('dag_bootstrap_startup.sh', 'CRABServer/scripts/')
            bootstrap_location = getLocation("dag_bootstrap.sh", "CRABServer/scripts/")
            adjust_location = getLocation("AdjustSites.py", "CRABServer/scripts/")

            cwd = os.getcwd()
            os.chdir(temp_dir)
            shutil.copy(transform_location, '.')
            shutil.copy(cmscp_location, '.')
            shutil.copy(gwms_location, '.')
            shutil.copy(dag_bootstrap_location, '.')
            shutil.copy(bootstrap_location, '.')
            shutil.copy(adjust_location, '.')

            # Bootstrap the ISB if we are using UFC
            if UserFileCache and (kw['task']['tm_cache_url'] == 'https://cmsweb.cern.ch/crabcache/file'):
                ufc = UserFileCache()
                ufc.download(hashkey=kw['task']['tm_user_sandbox'].split(".")[0], output="sandbox.tar.gz")
                kw['task']['tm_user_sandbox'] = 'sandbox.tar.gz'

            kw['task']['scratch'] = temp_dir

        kw['task']['restinstance'] = self.server['host']
        kw['task']['resturl'] = self.resturl.replace("/workflowdb", "/filemetadata")
        self.task = kw['task']
        params = self.sendDashboardTask()

        try:
            info = self.createSubdag(*args, **kw)
        finally:
            if cwd:
                os.chdir(cwd)

        return TaskWorker.DataObjects.Result.Result(task=kw['task'], result=(temp_dir, info, params))

    def execute(self, *args, **kw):
        try:
            return self.executeInternal(*args, **kw)
        except Exception, e:
            configreq = {'workflow': kw['task']['tm_taskname'],
                             'substatus': "FAILED",
                             'subjobdef': -1,
                             'subuser': kw['task']['tm_user_dn'],
                             'subfailure': base64.b64encode(str(e)),}
            self.logger.error("Pushing information centrally %s" %(str(configreq)))
            data = urllib.urlencode(configreq)
            self.server.put(self.resturl, data=data)
            raise

