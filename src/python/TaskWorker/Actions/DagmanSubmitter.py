"""
Submit a DAG directory created by the DagmanCreator component.
"""

import os
import base64
import random
import urllib
import traceback
import distutils.spawn

import HTCondorUtils
import HTCondorLocator

import TaskWorker.Actions.TaskAction as TaskAction
import TaskWorker.DataObjects.Result as Result

from TaskWorker.Actions.DagmanCreator import CRAB_HEADERS

from ApmonIf import ApmonIf

# force the use of gsissh with pushy
os.environ['PUSHY_NATIVE_SSH']=distutils.spawn.find_executable('gsissh')
assert os.environ['PUSHY_NATIVE_SSH']!=None, "gsissh not found"
os.environ['PUSHY_NATIVE_SCP']=distutils.spawn.find_executable('gsiscp')
import pushy
USE_PUSHY=True

# Bootstrap either the native module or the BossAir variant.
try:
    import classad
    import htcondor
except ImportError, _:
    #pylint: disable=C0103
    classad = None
    htcondor = None

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
transfer_input_files = %(inputFilesString)s
transfer_output_files = %(outputFilesString)s
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
on_exit_remove = ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))
+OtherJobRemoveRequirements = DAGManJobId =?= ClusterId
remove_kill_sig = SIGUSR1
+HoldKillSig = "SIGUSR1"
on_exit_hold = (ExitCode =!= UNDEFINED && ExitCode != 0)
+Environment= strcat("PATH=/usr/bin:/bin:/opt/glidecondor/bin CRAB3_VERSION=3.3.0-pre1 CONDOR_ID=", ClusterId, ".", ProcId, " %(additional_environment_options)s")
+RemoteCondorSetup = "%(remote_condor_setup)s"
+TaskType = "ROOT"
X509UserProxy = %(user_proxy)s
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
            ('CRAB_LumiMask', 'lumimask'),
            ('CRAB_JobCount', 'jobcount'),
            ('CRAB_UserVO', 'tm_user_vo'),
            ('CRAB_UserRole', 'tm_user_role'),
            ('CRAB_UserGroup', 'tm_user_group'),
            ('RequestMemory', 'tm_maxmemory'),
            ('RequestCpus', 'tm_numcores'),
            ('MaxWallTimeMins', 'tm_maxjobruntime'),
            ('JobPrio', 'tm_priority')]

def addCRABInfoToClassAd(ad, info, exprtree=classad.ExprTree):
    """
    Given a submit ClassAd, add in the appropriate CRAB_* attributes
    from the info directory
    """
    for adName, dictName in SUBMIT_INFO:
        if dictName in info and (info[dictName] != None):
            ad[adName] = exprtree(str(info[dictName]))

class DagmanSubmitter(TaskAction.TaskAction):

    """
    Submit a DAG to a HTCondor schedd
    """

    def execute(self, *args, **kw):
        try:
            return self.executeInternal(*args, **kw)
        except Exception, e:
            msg = "Failed to submit task %s; '%s'" % (kw['task']['tm_taskname'], str(e))
            self.logger.error(msg)
            configreq = {'workflow': kw['task']['tm_taskname'],
                         'status': "FAILED",
                         'subresource': 'failure',
                         'failure': base64.b64encode(msg)}
            self.server.post(self.resturl, data = urllib.urlencode(configreq))
            raise

    def executeInternal(self, *args, **kw):

        if not htcondor:
            raise Exception("Unable to import HTCondor module")

        task = kw['task']
        tempDir = args[0][0]
        info = args[0][1]
        #self.logger.debug("Task input information: %s" % str(info))
        dashboard_params = args[0][2]

        cwd = os.getcwd()
        os.chdir(tempDir)

        #FIXME: hardcoding the transform name for now.
        #inputFiles = ['gWMS-CMSRunAnalysis.sh', task['tm_transformation'], 'cmscp.py', 'RunJobs.dag']
        inputFiles = ['gWMS-CMSRunAnalysis.sh', 'CMSRunAnalysis.sh', 'cmscp.py', 'RunJobs.dag', 'Job.submit', 'dag_bootstrap.sh', 'AdjustSites.py', 'site.ad']
        if task.get('tm_user_sandbox') == 'sandbox.tar.gz':
            inputFiles.append('sandbox.tar.gz')
        if os.path.exists("CMSRunAnalysis.tar.gz"):
            inputFiles.append("CMSRunAnalysis.tar.gz")
        if os.path.exists("TaskManagerRun.tar.gz"):
            inputFiles.append("TaskManagerRun.tar.gz")
        info['inputFilesString'] = ", ".join(inputFiles)
        outputFiles = ["RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001"]
        info['outputFilesString'] = ", ".join(outputFiles)
        arg = "RunJobs.dag"

        try:
            info['remote_condor_setup'] = ''
            loc = HTCondorLocator.HTCondorLocator(self.backendurls)
            schedd, address = loc.getScheddObj(task['tm_taskname'])
            if address:
                # the following logic is implemented inside loc, but not exposed
                # To be perfected, though
                schedd_name=task['tm_taskname'].split("_")[2].split(":")[0]
                
                self.submitDirect(schedd, schedd_name, 'dag_bootstrap_startup.sh', arg, info)
            else:
                jdl = MASTER_DAG_SUBMIT_FILE % info
                schedd.submitRaw(task['tm_taskname'], jdl, task['user_proxy'], inputFiles)
        finally:
            os.chdir(cwd)

        configreq = {'workflow': kw['task']['tm_taskname'],
                     'status': "SUBMITTED",
                     'jobset': "-1",
                     'subresource': 'success',}
        self.logger.debug("Pushing information centrally %s" %(str(configreq)))
        data = urllib.urlencode(configreq)
        self.server.post(self.resturl, data = data)

        self.sendDashboardJobs(dashboard_params, info['apmon'])
    
        return Result.Result(task=kw['task'], result=(-1))

    def submitDirect(self, schedd, schedd_name, cmd, arg, info): #pylint: disable=R0201
        """
        Submit directly to the schedd using the HTCondor module
        or the gsissh module
        """

        with HTCondorUtils.AuthenticatedSubprocess(info['user_proxy']) as (parent, rpipe):
                if not parent:
                    if not USE_PUSHY:
                        dagAd = classad.ClassAd()
                        exprtree=classad.ExprTree
                    else:
                        conn = pushy.connect("ssh:%s"%schedd_name)
                        dagAd = conn.modules.classad.ClassAd()
                        exprtree = conn.modules.classad.ExprTree
                    
                    addCRABInfoToClassAd(dagAd, info, exprtree)

		    # Set default task attributes:
		    if 'RequestMemory' not in dagAd:
		        dagAd['RequestMemory'] = 2000
		    if 'RequestCpus' not in dagAd:
		        dagAd['RequestCpus'] = 1
		    if 'MaxWallTimeMins' not in dagAd:
		        dagAd['MaxWallTimeMins'] = 1315
		    if 'JobPrio' not in dagAd:
		        dagAd['JobPrio'] = 10

                    # NOTE: Changes here must be synchronized with the job_submit in DagmanCreator.py in CAFTaskWorker
                    dagAd["CRAB_Attempt"] = 0
                    # We switched from local to scheduler universe.  Why?  It seems there's no way in the
                    # local universe to change the hold signal at runtime.  That's fairly important for our
                    # resubmit implementation.
                    #dagAd["JobUniverse"] = 12
                    dagAd["JobUniverse"] = 7
                    dagAd["HoldKillSig"] = "SIGUSR1"
                    dagAd["Cmd"] = cmd
                    dagAd['Args'] = arg
                    dagAd["OnExitRemove"] = exprtree("( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))")
                    dagAd["OtherJobRemoveRequirements"] = exprtree("DAGManJobId =?= ClusterId")
                    dagAd["RemoveKillSig"] = "SIGUSR1"
                    dagAd["OnExitHold"] = exprtree("(ExitCode =!= UNDEFINED && ExitCode != 0)")
                    dagAd["Environment"] = exprtree('strcat("PATH=/usr/bin:/bin CRAB3_VERSION=3.3.0-pre1 CONDOR_ID=", ClusterId, ".", ProcId," %s")' % " ".join(info['additional_environment_options'].split(";")))
                    dagAd["RemoteCondorSetup"] = info['remote_condor_setup']
                    dagAd["Requirements"] = exprtree('true || false')
                    dagAd["TaskType"] = "ROOT"

                    if not USE_PUSHY:
                        # use direct remote condor calls

                        dagAd["Out"] = str(os.path.join(info['scratch'], "request.out"))
                        dagAd["Err"] = str(os.path.join(info['scratch'], "request.err"))
                        dagAd["TransferInput"] = str(info['inputFilesString'])
                        dagAd["TransferOutput"] = info['outputFilesString']
                        dagAd["LeaveJobInQueue"] = exprtree("(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")
                        dagAd["X509UserProxy"] = info['user_proxy']

                        resultAds = []
                        schedd.submit(dagAd, 1, True, resultAds)
                        schedd.spool(resultAds)
                    else:
                        # create the work dir and copy over the files
                        cwarr=dagAd["CRAB_Workflow"].split('_')
                        rdir='%s/%s'%(conn.modules.os.path.expanduser('~'),"%s_%s"%(cwarr[0],cwarr[1]))
                        conn.modules.os.mkdir(rdir)

                        ifiles=str(info['inputFilesString']).split(',')
                        for f in ifiles + [dagAd["Cmd"]]:
                            f=f.strip()
                            conn.putfile(f,os.path.join(rdir,f))
                        # copy over the proxy... the channel is encrypted, so no security risk
                        # prxy path is also the only one with absdir
                        proxy_shortname=os.path.basename(info['user_proxy'])
                        conn.putfile(info['user_proxy'],os.path.join(rdir,proxy_shortname))

                        # change to that work dir remotely
                        conn.modules.os.chdir(rdir)

                        dagAd["X509UserProxy"]=os.path.join(rdir,proxy_shortname)
                        dagAd["Err"] = "request.err"
                        dagAd["Out"] = "request.out"

                        # create a schedd object on the remote end, and use it
                        rschedd = conn.modules.htcondor.Schedd()
                        rschedd.submit(dagAd, 1, False)    
                            
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when submitting HTCondor task: '%s'" % results)

        schedd.reschedule()


    def sendDashboardJobs(self, params, info):
        apmon = ApmonIf()
        for job in info:
            job.update(params)
            self.logger.debug("Dashboard job info: %s" % str(job))
            apmon.sendToML(job)
        apmon.free()


