import os
import json
import base64
import urllib
import traceback

import classad
import htcondor

import HTCondorLocator
import HTCondorUtils

import TaskWorker.Actions.TaskAction as TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException, NoAvailableSite

from httplib import HTTPException


class DagmanResubmitter(TaskAction.TaskAction):
    """
    Given a task name, resubmit failed tasks.

    Internally, we simply release the failed DAG.
    """

    def execute_internal(self, *args, **kw):
        #Marco: I guess these value errors only happens for development instances
        if 'task' not in kw:
            raise ValueError("No task specified.")
        task = kw['task']
        if 'tm_taskname' not in task:
            raise ValueError("No taskname specified.")
        workflow = str(task['tm_taskname'])
        if 'user_proxy' not in task:
            raise ValueError("No proxy provided")
        proxy = task['user_proxy']

        self.logger.info("About to resubmit workflow: %s." % workflow)
        self.logger.info("Task info: %s" % str(task))

        # This config setting acts as a global black / white list
        global_whitelist = []
        global_blacklist = []
        if hasattr(self.config.Sites, 'available'):
            global_whitelist = self.config.Sites.available
        if hasattr(self.config.Sites, 'banned'):
            global_blacklist = self.config.Sites.banned

        #Always block site which is in morgue
        if hasattr(self.config.Sites, 'blacklist_morgue_save_path'):
            save_location = self.config.Sites.blacklist_morgue_save_path
            if os.path.isfile(save_location):
                with open(save_location, 'r') as fd:
                    current_morgue_list = json.load(fd)
                    global_blacklist = current_morgue_list + global_blacklist
        resubmit_site_whitelist = task['resubmit_site_whitelist']
        # If global_whitelist is not empty, means TW is configured to submit only to specific sites
        # But more othen blacklist is used and not whitelist in TaskWorker configuration
        if resubmit_site_whitelist:
            # If any whitelist was specified for task, user wants to run only on that site/sites
            # Check with TaskWorker configuration if it is allowed to submit to that site.
            if global_whitelist:
                resubmit_site_whitelist = [item for item in resubmit_site_whitelist if item in global_whitelist]
            if global_blacklist and resubmit_site_whitelist:
                resubmit_site_whitelist = [item for item in resubmit_site_whitelist if item not in global_blacklist]
            #If list is empty, means that site is banned.
            # TODO: Add warning to CRABServer if one/any site was rejected from users resubmittion and
            # that TaskWorker will resubmit only to good site.
            if not resubmit_site_whitelist:
                msg = "The CRAB3 server backend refuses to resubmit jobs to the Grid scheduler. "\
                      "You put (%s) in the site whitelist, and these sites: %s are blocked in CRAB3 server backend. "\
                      "Please check site whitelist and contact an expert if the error persist."\
                      % (", ".join(task['resubmit_site_whitelist']), global_blacklist)
                raise NoAvailableSite(msg)
            else:
                task['resubmit_site_whitelist'] = resubmit_site_whitelist

        if task['tm_collector']:
            self.backendurls['htcondorPool'] = task['tm_collector']
        loc = HTCondorLocator.HTCondorLocator(self.backendurls)

        schedd = ""
        address = ""
        try:
            schedd, address = loc.getScheddObjNew(task['tm_schedd'])
        except Exception, exp:
            msg = ("%s: The CRAB3 server backend is not able to contact Grid scheduler. Please, retry later. Message from the scheduler: %s") % (workflow, str(exp))
            self.logger.exception(msg)
            raise TaskWorkerException(msg)

        # Release the DAG
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(workflow)

        ## Calculate new parameters for resubmited jobs. These parameters will
        ## be (re)written in the _CONDOR_JOB_AD when we do schedd.edit() below.
        ad = classad.ClassAd()
        params = {'CRAB_ResubmitList'  : 'jobids',
                  'CRAB_SiteBlacklist' : 'site_blacklist',
                  'CRAB_SiteWhitelist' : 'site_whitelist',
                  'MaxWallTimeMins'    : 'maxjobruntime',
                  'RequestMemory'      : 'maxmemory',
                  'RequestCpus'        : 'numcores',
                  'JobPrio'            : 'priority'
                 }
        overwrite = False
        for taskparam in params.values():
            if ('resubmit_'+taskparam in task) and task['resubmit_'+taskparam] != None:
                if type(task['resubmit_'+taskparam]) == list:
                    ad[taskparam] = task['resubmit_'+taskparam]
                if taskparam != 'jobids':
                    overwrite = True

        if ('resubmit_jobids' in task) and task['resubmit_jobids']:
            with HTCondorUtils.AuthenticatedSubprocess(proxy) as (parent, rpipe):
                if not parent:
                    schedd.edit(rootConst, "HoldKillSig", 'SIGKILL')
                    ## Overwrite parameters in the os.environ[_CONDOR_JOB_AD] file. This will affect
                    ## all the jobs, not only the ones we want to resubmit. That's why the pre-job
                    ## is saving the values of the parameters for each job retry in text files (the
                    ## files are in the directory resubmit_info in the schedd).
                    for adparam, taskparam in params.iteritems():
                        if taskparam in ad:
                            schedd.edit(rootConst, adparam, ad[taskparam])
                        elif task['resubmit_'+taskparam] != None:
                            schedd.edit(rootConst, adparam, str(task['resubmit_'+taskparam]))
                    schedd.act(htcondor.JobAction.Hold, rootConst)
                    schedd.edit(rootConst, "HoldKillSig", 'SIGUSR1')
                    schedd.act(htcondor.JobAction.Release, rootConst)
        elif overwrite:
            with HTCondorUtils.AuthenticatedSubprocess(proxy) as (parent, rpipe):
                if not parent:
                    self.logger.debug("Resubmitting under condition overwrite = True")
                    for adparam, taskparam in params.iteritems():
                        if taskparam == 'jobids':
                            continue
                        if taskparam in ad:
                            schedd.edit(rootConst, adparam, ad[taskparam])
                        elif task['resubmit_'+taskparam] != None:
                            schedd.edit(rootConst, adparam, str(task['resubmit_'+taskparam]))
                    schedd.act(htcondor.JobAction.Release, rootConst)
        else:
            with HTCondorUtils.AuthenticatedSubprocess(proxy) as (parent, rpipe):
                if not parent:
                    self.logger.debug("Resubmitting under condition overwrite = False")
                    schedd.edit(rootConst, "HoldKillSig", 'SIGKILL')
                    schedd.edit(rootConst, "CRAB_ResubmitList", classad.ExprTree("true"))
                    schedd.act(htcondor.JobAction.Hold, rootConst)
                    schedd.edit(rootConst, "HoldKillSig", 'SIGUSR1')
                    schedd.act(htcondor.JobAction.Release, rootConst)

        results = rpipe.read()
        if results != "OK":
            raise TaskWorkerException("The CRAB3 server backend could not resubmit your task because the Grid scheduler answered with an error.\n"+\
                                      "This is probably a temporary glitch, please try it again and contact an expert if the error persist.\n"+\
                                      "Error reason: %s" % (results))


    def execute(self, *args, **kwargs):
        self.execute_internal(*args, **kwargs)
        configreq = {'workflow': kwargs['task']['tm_taskname'],
                     'status': "SUBMITTED",
                     'jobset': "-1",
                     'subresource': 'success',}
        self.logger.debug("Setting the task as successfully resubmitted with %s " % str(configreq))
        data = urllib.urlencode(configreq)
        try:
            self.server.post(self.resturi, data = data)
        except HTTPException, hte:
            self.logger.error(hte.headers)
            raise TaskWorkerException("The CRAB3 server backend successfully resubmitted your task to the Grid scheduler, but was unable to update\n"+\
                                      "the task status from QUEUED to SUBMITTED. This should be a harmless (temporary) error.\n")

if __name__ == "__main__":
    import os
    import logging
    from RESTInteractions import HTTPRequests
    from WMCore.Configuration import Configuration

    logging.basicConfig(level = logging.DEBUG)
    config = Configuration()

    config.section_("TaskWorker")
    #will use X509_USER_PROXY var for this test
    config.TaskWorker.cmscert = os.environ["X509_USER_PROXY"]
    config.TaskWorker.cmskey = os.environ["X509_USER_PROXY"]

    server = HTTPRequests('vmatanasi2.cern.ch', config.TaskWorker.cmscert, config.TaskWorker.cmskey)
    resubmitter = DagmanResubmitter(config, server, '/crabserver/dev/workflowdb')
    resubmitter.execute(task={'tm_taskname':'141129_110306_crab3test-5:atanasi_crab_test_resubmit', 'user_proxy' : os.environ["X509_USER_PROXY"],
                              'resubmit_site_whitelist' : ['T2_IT_Bari'], 'resubmit_site_blacklist' : ['T2_IT_Legnaro'], 'resubmit_priority' : 2,
                              'resubmit_numcores' : 1, 'resubmit_maxjobruntime' : 1000, 'resubmit_maxmemory' : 1000
                             })
