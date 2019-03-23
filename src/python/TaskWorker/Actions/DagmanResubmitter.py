import time
import urllib
import datetime

import classad
import htcondor

import HTCondorLocator
import HTCondorUtils

from WMCore.Database.CMSCouch import CouchServer

from ServerUtilities import FEEDBACKMAIL
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.Actions.DagmanSubmitter import checkMemoryWalltime
from TaskWorker.WorkerExceptions import TaskWorkerException

from httplib import HTTPException


class DagmanResubmitter(TaskAction):
    """
    Given a task name, resubmit failed tasks.

    Internally, we simply release the failed DAG.
    """

    def executeInternal(self, *args, **kwargs): #pylint: disable=unused-argument
        #Marco: I guess these value errors only happens for development instances
        if 'task' not in kwargs:
            raise ValueError("No task specified.")
        task = kwargs['task']
        if 'tm_taskname' not in task:
            raise ValueError("No taskname specified.")
        workflow = str(task['tm_taskname'])
        if 'user_proxy' not in task:
            raise ValueError("No proxy provided")
        proxy = task['user_proxy']

        if task.get('resubmit_publication', False):
            resubmitWhat = "publications"
        else:
            resubmitWhat = "jobs"

        self.logger.info("About to resubmit %s for workflow: %s.", resubmitWhat, workflow)
        self.logger.debug("Task info: %s", str(task))

        if task.get('resubmit_publication', False):
            asourl = task.get('tm_asourl', None)
            #Let's not assume the db has been updated (mostly for devs), let's default asodb to asynctransfer!
            #Also the "or" takes care of the case were the new code is executed on old task
            #i.e.: tm_asodb is there but empty.
            asodb = task.get('tm_asodb', 'asynctransfer') or 'asynctransfer'
            if not asourl:
                msg = "ASO URL not set. Can not resubmit publication."
                raise TaskWorkerException(msg)
            self.logger.info("Will resubmit failed publications")
            self.resubmitPublication(asourl, asodb, proxy, workflow)
            return

        if task['tm_collector']:
            self.backendurls['htcondorPool'] = task['tm_collector']
        loc = HTCondorLocator.HTCondorLocator(self.backendurls)

        schedd = ""
        dummyAddress = ""
        try:
            schedd, dummyAddress = loc.getScheddObjNew(task['tm_schedd'])
        except Exception as exp:
            msg  = "The CRAB server backend was not able to contact the Grid scheduler."
            msg += " Please try again later."
            msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
            msg += " Message from the scheduler: %s" % (str(exp))
            self.logger.exception("%s: %s", workflow, msg)
            raise TaskWorkerException(msg)

        # Check memory and walltime
        checkMemoryWalltime(None, task, 'resubmit', self.logger, self.uploadWarning)


        # Find only the originally submitted DAG to hold and release: this
        # will re-trigger the scripts and adjust retries and other
        # resubmission parameters.
        #
        # Processing and tail DAGs will be restarted by these scrips on the
        # schedd after the modifications are made.
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(workflow)

        ## Calculate new parameters for resubmitted jobs. These parameters will
        ## be (re)written in the _CONDOR_JOB_AD when we do schedd.edit() below.
        ad = classad.ClassAd()
        params = {'CRAB_ResubmitList'  : 'jobids',
                  'CRAB_SiteBlacklist' : 'site_blacklist',
                  'CRAB_SiteWhitelist' : 'site_whitelist',
                  'MaxWallTimeMinsRun' : 'maxjobruntime',
                  'RequestMemory'      : 'maxmemory',
                  'RequestCpus'        : 'numcores',
                  'JobPrio'            : 'priority'
                 }
        overwrite = False
        for taskparam in params.values():
            if ('resubmit_'+taskparam in task) and task['resubmit_'+taskparam] != None:
                # In case resubmission parameters contain a list of unicode strings, 
                # convert it to a list of ascii strings because of HTCondor unicode
                # incompatibility.
                # Note that unicode strings that are not in a list are not handled,
                # but so far they don't exist in this part of the code.
                if isinstance(task['resubmit_'+taskparam], list):
                    nonUnicodeList = []
                    for p in task['resubmit_'+taskparam]:
                        if isinstance(p, unicode):
                            nonUnicodeList.append(p.encode('ascii', 'ignore'))
                        else:
                            nonUnicodeList.append(p)
                    ad[taskparam] = nonUnicodeList
                if taskparam != 'jobids':
                    overwrite = True

        if ('resubmit_jobids' in task) and task['resubmit_jobids']:
            with HTCondorUtils.AuthenticatedSubprocess(proxy, logger=self.logger) as (parent, rpipe):
                if not parent:
                    schedd.edit(rootConst, "HoldKillSig", 'SIGKILL')
                    ## Overwrite parameters in the os.environ[_CONDOR_JOB_AD] file. This will affect
                    ## all the jobs, not only the ones we want to resubmit. That's why the pre-job
                    ## is saving the values of the parameters for each job retry in text files (the
                    ## files are in the directory resubmit_info in the schedd).
                    for adparam, taskparam in params.iteritems():
                        if taskparam in ad:
                            schedd.edit(rootConst, adparam, ad.lookup(taskparam))
                        elif task['resubmit_'+taskparam] != None:
                            schedd.edit(rootConst, adparam, str(task['resubmit_'+taskparam]))
                    schedd.act(htcondor.JobAction.Hold, rootConst)
                    schedd.edit(rootConst, "HoldKillSig", 'SIGUSR1')
                    schedd.act(htcondor.JobAction.Release, rootConst)
        elif overwrite:
            self.logger.debug("Resubmitting under condition overwrite = True")
            with HTCondorUtils.AuthenticatedSubprocess(proxy, logger=self.logger) as (parent, rpipe):
                if not parent:
                    for adparam, taskparam in params.iteritems():
                        if taskparam in ad:
                            if taskparam == 'jobids' and len(list(ad[taskparam])) == 0:
                                self.logger.debug("Setting %s = True in the task ad.", adparam)
                                schedd.edit(rootConst, adparam, classad.ExprTree("true"))
                            else:
                                schedd.edit(rootConst, adparam, ad.lookup(taskparam))
                        elif task['resubmit_'+taskparam] != None:
                            schedd.edit(rootConst, adparam, str(task['resubmit_'+taskparam]))
                    schedd.act(htcondor.JobAction.Release, rootConst)
        else:
            ## This should actually not occur anymore in CRAB 3.3.16 or above, because
            ## starting from CRAB 3.3.16 the resubmission parameters are written to the
            ## Task DB with value != None, so the overwrite variable should never be False.
            self.logger.debug("Resubmitting under condition overwrite = False")
            with HTCondorUtils.AuthenticatedSubprocess(proxy, logger=self.logger) as (parent, rpipe):
                if not parent:
                    schedd.edit(rootConst, "HoldKillSig", 'SIGKILL')
                    schedd.edit(rootConst, "CRAB_ResubmitList", classad.ExprTree("true"))
                    schedd.act(htcondor.JobAction.Hold, rootConst)
                    schedd.edit(rootConst, "HoldKillSig", 'SIGUSR1')
                    schedd.act(htcondor.JobAction.Release, rootConst)
        try:
            results = rpipe.read()
        except EOFError:
            results = "Timeout while executing condor commands for resubmission"
        if results != "OK":
            msg  = "The CRAB server backend was not able to resubmit the task,"
            msg += " because the Grid scheduler answered with an error."
            msg += " This is probably a temporary glitch. Please try again later."
            msg += " If the error persists send an e-mail to %s." % (FEEDBACKMAIL)
            msg += " Error reason: %s" % (results)
            raise TaskWorkerException(msg)


    def execute(self, *args, **kwargs):
        """
        The execute method of the DagmanResubmitter class.
        """
        self.executeInternal(*args, **kwargs)
        try:
            configreq = {'subresource': 'state',
                         'workflow': kwargs['task']['tm_taskname'],
                         'status': 'SUBMITTED'}
            self.logger.debug("Setting the task as successfully resubmitted with %s", str(configreq))
            self.server.post(self.resturi, data = urllib.urlencode(configreq))
        except HTTPException as hte:
            self.logger.error(hte.headers)
            msg  = "The CRAB server successfully resubmitted the task to the Grid scheduler,"
            msg += " but was unable to update the task status to %s in the database." % (configreq['status'])
            msg += " This should be a harmless (temporary) error."
            raise TaskWorkerException(msg)


    def resubmitPublication(self, asourl, asodb, proxy, taskname):
        """
        Resubmit failed publications by resetting the publication
        status in the CouchDB documents.
        """
        server = CouchServer(dburl=asourl, ckey=proxy, cert=proxy)
        try:
            database = server.connectDatabase(asodb)
        except Exception as ex:
            msg = "Error while trying to connect to CouchDB: %s" % (str(ex))
            raise TaskWorkerException(msg)
        try:
            failedPublications = database.loadView('DBSPublisher', 'PublicationFailedByWorkflow', {'reduce': False, 'startkey': [taskname], 'endkey': [taskname, {}]})['rows']
        except Exception as ex:
            msg = "Error while trying to load view 'DBSPublisher.PublicationFailedByWorkflow' from CouchDB: %s" % (str(ex))
            raise TaskWorkerException(msg)
        msg = "There are %d failed publications to resubmit: %s" % (len(failedPublications), failedPublications)
        self.logger.info(msg)
        for doc in failedPublications:
            docid = doc['id']
            if doc['key'][0] != taskname: # this should never happen...
                msg = "Skipping document %s as it seems to correspond to another task: %s" % (docid, doc['key'][0])
                self.logger.warning(msg)
                continue
            data = {'last_update': time.time(),
                    'retry': str(datetime.datetime.now()),
                    'publication_state': 'not_published',
                   }
            try:
                database.updateDocument(docid, 'DBSPublisher', 'updateFile', data)
            except Exception as ex:
                msg = "Error updating document %s in CouchDB: %s" % (docid, str(ex))
                self.logger.error(msg)
        return


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

    server_ = HTTPRequests('vmatanasi2.cern.ch', config.TaskWorker.cmscert, config.TaskWorker.cmskey)
    resubmitter = DagmanResubmitter(config, server_, '/crabserver/dev/workflowdb')
    resubmitter.execute(task={'tm_taskname':'141129_110306_crab3test-5:atanasi_crab_test_resubmit', 'user_proxy' : os.environ["X509_USER_PROXY"],
                              'resubmit_site_whitelist' : ['T2_IT_Bari'], 'resubmit_site_blacklist' : ['T2_IT_Legnaro'], 'resubmit_priority' : 2,
                              'resubmit_numcores' : 1, 'resubmit_maxjobruntime' : 1000, 'resubmit_maxmemory' : 1000
                             })
