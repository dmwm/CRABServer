
"""
Create a set of files for a DAG submission.

Generates the condor submit files and the master DAG.
"""

import os
import json
import shutil
import string
import logging
import commands
import tempfile

import TaskWorker.Actions.TaskAction as TaskAction
import TaskWorker.DataObjects.Result
import TaskWorker.WorkerExceptions

import WMCore.WMSpec.WMTask
from CRABInterface.Dagman.DagmanSnippets import *
# TODO: DagmanCreator started life as a flat module, then the DagmanCreator class
# was later added.  We need to come back and make the below methods class methods

def makeJobSubmit(task):
    """
    Called from create subdag
    """
    if os.path.exists("Job.submit"):
        return
    # From here on out, we convert from tm_* names to the DataWorkflow names
    info = dict(task)
    info['workflow'] = task['tm_taskname'].split("_")[-1]
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
    info['siteblacklist'] = task['tm_site_blacklist']
    info['sitewhitelist'] = task['tm_site_whitelist']
    info['addoutputfiles'] = task['tm_outfiles']
    info['tfileoutfiles'] = task['tm_tfile_outfiles']
    info['edmoutfiles'] = task['tm_edm_outfiles']
    # TODO: pass through these correctly.
    info['runs'] = []
    info['lumis'] = []
    info = escape_strings_to_classads(info)
    print info
    print "There was the info ****"
    logging.info("There was the info ***")
    with open("Job.submit", "w") as fd:
        fd.write("# bring it for melo")
        fd.write(JOB_SUBMIT % info)
        
    return info

def make_specs(task, jobgroup, availablesites, outfiles, startjobid):
    print "Making spec %s %s " % (availablesites, outfiles)
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
            print "Got a remote output file %s from %s" % (fileName, info)
            remoteOutputFiles.append("%s" % fileName)
            localOutputFiles.append("%s?remoteName=%s" % (origFile, fileName))
        remoteOutputFiles = " ".join(remoteOutputFiles)
        localOutputFiles = ", ".join(localOutputFiles)
        specs.append({'count': i, 'runAndLumiMask': runAndLumiMask, 'inputFiles': inputFiles,
                      'desiredSites': desiredSites, 'remoteOutputFiles': remoteOutputFiles,
                      'localOutputFiles': localOutputFiles})
        LOGGER.debug(specs[-1])
    return specs, i

def create_subdag(splitter_result, **kwargs):
    """
    Called from TaskManagerBootstrap to generate a subdag from
    the the split result
    """
    global LOGGER
    if not LOGGER:
        LOGGER = logging.getLogger("DagmanCreator")

    startjobid = 0
    specs = []

    info = makeJobSubmit(kwargs['task'])

    outfiles = kwargs['task']['tm_outfiles'] + kwargs['task']['tm_tfile_outfiles'] + kwargs['task']['tm_edm_outfiles']

    os.chmod("CMSRunAnalysis.sh", 0755)

    #fixedsites = set(self.config.Sites.available)
    for jobgroup in splitter_result:
        jobs = jobgroup.getJobs()
        if not jobs:
            possiblesites = []
        else:
            possiblesites = jobs[0]['input_files'][0]['locations']
        LOGGER.debug("Possible sites: %s" % possiblesites)
        LOGGER.debug('Blacklist: %s; whitelist %s' % (kwargs['task']['tm_site_blacklist'], kwargs['task']['tm_site_whitelist']))
        if kwargs['task']['tm_site_whitelist']:
            availablesites = set(kwargs['task']['tm_site_whitelist'])
        else:
            availablesites = set(possiblesites) - set(kwargs['task']['tm_site_blacklist'])
        #availablesites = set(availablesites) & fixedsites
        availablesites = [str(i) for i in availablesites]
        LOGGER.info("Resulting available sites: %s" % ", ".join(availablesites))

        if not availablesites:
            msg = "No site available for submission of task %s" % (kwargs['task']['tm_taskname'])
            raise TaskWorker.WorkerExceptions.NoAvailableSite(msg)

        jobgroupspecs, startjobid = make_specs(kwargs['task'], jobgroup, availablesites, outfiles, startjobid)
        specs += jobgroupspecs

    dag = ""
    for spec in specs:
        if HTCONDOR_78_WORKAROUND:
            with open("Job.submit", "r") as fd:
                with open("Job.submit.%(count)d" % spec, "w") as out_fd:
                    out_fd.write("+desiredSites=\"%(desiredSites)s\"\n" % spec)
                    out_fd.write("+DESIRED_Sites=\"%(desiredSites)s\"\n" % spec)
                    out_fd.write("+CRAB_localOutputFiles=\"%(localOutputFiles)s\"\n" % spec)
                    #if kwargs.get('tarball_location', None):
                    #    out_fd.write("Environment = CRAB_TASKMANAGER_TARBALL=%s" % kwargs['tarball_location'])
                    #    if kwargs['tarball_location'] == 'local':
                    #        out_fd.write("transfer_input_files = TaskManagerRun.tar.gz")
                    out_fd.write(fd.read())
            dag += DAG_FRAGMENT_WORKAROUND % spec
        else:
            dag += DAG_FRAGMENT % spec

    with open("RunJobs.dag", "w") as fd:
        fd.write(dag)

    task_name = kwargs['task'].get('CRAB_ReqName', kwargs['task'].get('tm_taskname', ''))
    userdn = kwargs['task'].get('CRAB_UserDN', kwargs['task'].get('tm_user_dn', ''))

    # When running in standalone mode, we want to record the number of jobs in the task
    if ('CRAB_ReqName' in kwargs['task']) and ('CRAB_UserDN' in kwargs['task']):
        const = 'TaskType =?= \"ROOT\" && CRAB_ReqName =?= "%s" && CRAB_UserDN =?= "%s"' % (task_name, userdn)
        cmd = "condor_qedit -const '%s' CRAB_JobCount %d" % (const, len(jobgroup.getJobs()))
        LOGGER.debug("+ %s" % cmd)
        status, output = commands.getstatusoutput(cmd)
        if status:
            LOGGER.error(output)
            LOGGER.error("Failed to record the number of jobs.")
            return 1

    return info


def getLocation(default_name, checkout_location):
    raise
    loc = default_name
    if not os.path.exists(loc):
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

    def execute(self, *args, **kw):
        raise "dead code?"
        global LOGGER
        LOGGER = self.logger

        cwd = None
        if hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'scratchDir'):
            temp_dir = tempfile.mkdtemp(prefix='_' + kw['task']['tm_taskname'], dir=self.config.TaskWorker.scratchDir)

            transform_location = getLocation(kw['task']['tm_transformation'], 'CAFUtilities/src/python/transformation/')
            cmscp_location = getLocation('cmscp.py', 'CRABServer/bin/')
            gwms_location = getLocation('gWMS-CMSRunAnalysis.sh', 'CAFTaskWorker/bin/')
            bootstrap_location = getLocation('dag_bootstrap_startup.sh', 'CRABServer/bin/')

            cwd = os.getcwd()
            os.chdir(temp_dir)
            shutil.copy(transform_location, '.')
            shutil.copy(cmscp_location, '.')
            shutil.copy(gwms_location, '.')
            shutil.copy(bootstrap_location, '.')

            if 'X509_USER_PROXY' in os.environ:
                kw['task']['userproxy'] = os.environ['X509_USER_PROXY']
            kw['task']['scratch'] = temp_dir

        try:
            info = create_subdag(*args, **kw)
        finally:
            if cwd:
                os.chdir(cwd)
        return TaskWorker.DataObjects.Result.Result(task=kw['task'], result=(temp_dir, info))

