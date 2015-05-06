import re
import time
import socket
import urllib
import datetime
import traceback

import classad
import htcondor

import TaskWorker.WorkerExceptions
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException

import HTCondorLocator
import HTCondorUtils

import WMCore.Database.CMSCouch as CMSCouch

import ApmonIf

WORKFLOW_RE = re.compile("[a-z0-9_]+")

class DagmanKiller(TaskAction):

    """
    Given a task name, kill the corresponding task in HTCondor.

    We do not actually "kill" the task off, but put the DAG on hold.
    """

    def executeInternal(self, apmon, *args, **kw):
        #Marco: I guess these value errors only happens for development instances
        if 'task' not in kw:
            raise ValueError("No task specified.")
        self.task = kw['task']
        if 'tm_taskname' not in self.task:
            raise ValueError("No taskname specified")
        self.workflow = self.task['tm_taskname']
        if 'user_proxy' not in self.task:
            raise ValueError("No proxy provided")
        self.proxy = self.task['user_proxy']

        try:
            self.killTransfers(apmon)
        except:
            self.logger.exception("Failed to kill transfers; suppressing error until functionality is confirmed") #TODO send a warning?

        self.logger.info("About to kill workflow: %s." % self.workflow)

        self.workflow = str(self.workflow)
        if not WORKFLOW_RE.match(self.workflow):
            raise Exception("Invalid workflow name.")

        # Query HTCondor for information about running jobs and update Dashboard appropriately
        if self.task['tm_collector']:
            self.backendurls['htcondorPool'] = self.task['tm_collector']
        loc = HTCondorLocator.HTCondorLocator(self.backendurls)

        address = ""
        try:
            self.schedd, address = loc.getScheddObjNew(self.task['tm_schedd'])
        except Exception as exp:
            msg = ("%s: The CRAB3 server backend is not able to contact Grid scheduler. Please, retry later. Message from the scheduler: %s") % (self.workflow, str(exp))
            self.logger.exception(msg)
            raise TaskWorkerException(msg)

        ad = classad.ClassAd()
        ad['foo'] = self.task['kill_ids']
        try:
            hostname = socket.getfqdn()
        except:
            hostname = ''

        const = "CRAB_ReqName =?= %s && member(CRAB_Id, %s)" % (HTCondorUtils.quote(self.workflow), ad.lookup("foo").__repr__())
        try:
            for ad in list(self.schedd.xquery(const, ['CRAB_Id', 'CRAB_Retry'])):
                if ('CRAB_Id' not in ad) or ('CRAB_Retry' not in ad):
                    continue
                jobid = str(ad.eval('CRAB_Id'))
                jobretry = str(ad.eval('CRAB_Retry'))
                jinfo = {'jobId': ("%s_https://glidein.cern.ch/%s/%s_%s" % (jobid, jobid, self.workflow, jobretry)),
                         'sid': "https://glidein.cern.ch/%s%s" % (jobid, self.workflow),
                         'broker': hostname,
                         'bossId': jobid,
                         'StatusValue' : 'killed',
                        }
                self.logger.info("Sending kill info to Dashboard: %s" % str(jinfo))
                apmon.sendToML(jinfo)
        except:
            self.logger.exception("Failed to notify Dashboard of job kills") #warning

        # Note that we can not send kills for jobs not in queue at this time; we'll need the
        # DAG FINAL node to be fixed and the node status to include retry number.

        if self.task['kill_all']:
            return self.killAll()
        else:
            return self.killJobs(self.task['kill_ids'])


    def killTransfers(self, apmon):
        self.logger.info("About to kill transfers from workflow %s." % self.workflow)
        ASOURL = self.task.get('tm_asourl', None)
        if not ASOURL:
            self.logger.info("ASO URL not set; will not kill transfers")
            return False

        try:
            hostname = socket.getfqdn()
        except:
            hostname = ''

        server = CMSCouch.CouchServer(dburl=ASOURL, ckey=self.proxy, cert=self.proxy)
        try:
            db = server.connectDatabase('asynctransfer')
        except Exception as ex:
            msg =  "Error while connecting to asynctransfer CouchDB"
            self.logger.exception(msg)
            raise TaskWorkerException(msg)
        self.queryKill = {'reduce':False, 'key':self.workflow, 'include_docs': True}
        try:
            filesKill = db.loadView('AsyncTransfer', 'forKill', self.queryKill)['rows']
        except Exception as ex:
            msg =  "Error while connecting to asynctransfer CouchDB"
            self.logger.exception(msg)
            raise TaskWorkerException(msg)
        if len(filesKill) == 0:
            self.logger.warning('No files to kill found')
        for idt in filesKill:
            now = str(datetime.datetime.now())
            id = idt['value']
            data = {
                'end_time': now,
                'state': 'killed',
                'last_update': time.time(),
                'retry': now,
               }
            updateUri = "/%s/_design/AsyncTransfer/_update/updateJobs/%s?%s" % (db.name, id, urllib.urlencode(data))
            jobid = idt.get('jobid')
            jobretry = idt.get('job_retry_count')
            if not self.task['kill_all']:
                if idt.get("jobid") not in self.task['kill_ids']:
                    continue
            self.logger.info("Killing transfer %s (job ID %s; job retry %s)." % (id, str(jobid), str(jobretry)))
            jobid = str(jobid)
            jobretry = str(jobretry)
            if jobid and jobretry != None:
                jinfo = {'jobId': ("%s_https://glidein.cern.ch/%s/%s_%s" % (jobid, jobid, self.workflow, jobretry)),
                         'sid': "https://glidein.cern.ch/%s%s" % (jobid, self.workflow),
                         'broker': hostname,
                         'bossId': jobid,
                         'StatusValue' : 'killed',
                        }
                self.logger.info("Sending kill info to Dashboard: %s" % str(jinfo))
                apmon.sendToML(jinfo)
            try:
                db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception as ex:
                msg =  "Error updating document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise TaskWorkerException(msg)
        return True


    def killJobs(self, ids):
        ad = classad.ClassAd()
        ad['foo'] = ids
        const = "CRAB_ReqName =?= %s && member(CRAB_Id, %s)" % (HTCondorUtils.quote(self.workflow), ad.lookup("foo").__repr__())
        with HTCondorUtils.AuthenticatedSubprocess(self.proxy) as (parent, rpipe):
            if not parent:
                self.schedd.act(htcondor.JobAction.Remove, const)
        results = rpipe.read()
        if results != "OK":
            raise TaskWorkerException("The CRAB3 server backend could not kill jobs [%s]. because the Grid scheduler answered with an error\n" % ", ".join(ids)+\
                                      "This is probably a temporary glitch, please try it again and contact an expert if the error persist\n"+\
                                      "Error reason %s" % results)


    def killAll(self):

        # Search for and hold the DAG
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(self.workflow)

        with HTCondorUtils.AuthenticatedSubprocess(self.proxy) as (parent, rpipe):
            if not parent:
                self.schedd.act(htcondor.JobAction.Hold, rootConst)
        results = rpipe.read()
        if results != "OK":
            raise TaskWorkerException("The CRAB3 server backend could not kill the task because the Grid scheduler answered with an error\n"\
                                      "This is probably a temporary glitch, please try it again and contact an expert if the error persist\n"+\
                                      "Error reason %s" % results)


    def execute(self, *args, **kw):

        apmon = ApmonIf.ApmonIf()
        try:
            self.executeInternal(apmon, *args, **kw)
            #XXX what's the difference of outting this here or in the else?
            if kw['task']['kill_all']:
                configreq = {'workflow': kw['task']['tm_taskname'], 'status': "KILLED"}
                self.server.post(self.resturi, data = urllib.urlencode(configreq))
            else:
                configreq = {'workflow': kw['task']['tm_taskname'], 'status': "SUBMITTED"}
                self.server.post(self.resturi, data = urllib.urlencode(configreq))
        finally:
            apmon.free()

