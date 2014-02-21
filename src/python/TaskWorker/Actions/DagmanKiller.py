import re
import urllib
import traceback
from base64 import b64encode

import classad
import htcondor

import TaskWorker.Actions.TaskAction as TaskAction

import HTCondorLocator
import HTCondorUtils

import WMCore.Database.CMSCouch as CMSCouch

WORKFLOW_RE = re.compile("[a-z0-9_]+")

class DagmanKiller(TaskAction.TaskAction):

    """
    Given a task name, kill the corresponding task in HTCondor.

    We do not actually "kill" the task off, but put the DAG on hold.
    """

    def executeInternal(self, *args, **kw):

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
            self.killTransfers()
        except:
            self.logger.exception("Failed to kill transfers; suppressing error until functionality is confirmed")

        self.logger.info("About to kill workflow: %s. Getting status first." % self.workflow)

        self.workflow = str(self.workflow)
        if not WORKFLOW_RE.match(self.workflow):
            raise Exception("Invalid workflow name.")

        loc = HTCondorLocator.HTCondorLocator(self.backendurls)
        self.schedd, address = loc.getScheddObj(self.workflow)

        if self.task['kill_all']:
            return self.killAll()
        else:
            return self.killJobs(self.task['kill_ids'])


    def killTransfers(self):
        ASOURL = self.task.get('tm_arguments', {}).get('ASOURL')
        if not ASOURL:
            self.logger.info("ASO URL not set; will not kill transfers")
        server = CMSCouch.CouchServer(dburl=ASOURL, ckey=self.proxy, cert=self.proxy)
        try:
            db = server.connectDatabase('asynctransfer')
        except Exception, ex:
            msg =  "Error while connecting to asynctransfer CouchDB"
            self.logger.exception(msg)
            raise ExecutionError(msg)
        self.queryKill = {'reduce':False, 'key':workflow}
        try:
            self.filesKill = db.loadView('AsyncTransfer', 'forKill', self.queryKill)['rows']
        except Exception, ex:
            msg =  "Error while connecting to asynctransfer CouchDB"
            self.logger.exception(msg)
            raise ExecutionError(msg)
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
            updateUri = "/%s/_design/AsyncTransfer/_update/updateJobs/%s?%s" (db.name, id, urllib.urlencode(data))
            if not self.task['kill_all']:
                try:
                    doc = db.document( doc_id )
                except CMSCouch.CouchNotFoundError:
                    self.logger.exception("Document %d is missing" % doc_id)
                    continue
                if doc.get("jobid") not in self.task['kill_ids']:
                    continue
            try:
                db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception, ex:
                msg =  "Error updating document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise ExecutionError(msg)


    def killJobs(self, ids):
        ad = classad.ClassAd()
        ad['foo'] = ids
        const = "CRAB_ReqName =?= %s && member(CRAB_Id, %s)" % (HTCondorUtils.quote(self.workflow), ad.lookup("foo").__repr__())
        with HTCondorUtils.AuthenticatedSubprocess(self.proxy) as (parent, rpipe):
            if not parent:
               self.schedd.act(htcondor.JobAction.Remove, const)
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing jobs [%s]: %s" % (", ".join(ids), results))


    def killAll(self):

        # Search for and hold the DAG
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(self.workflow)
        rootAttrList = ["ClusterId"]

        with HTCondorUtils.AuthenticatedSubprocess(self.proxy) as (parent, rpipe):
            if not parent:
               self.schedd.act(htcondor.JobAction.Hold, rootConst)
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing task: %s" % results)


    def execute(self, *args, **kw):

        try:
            self.executeInternal(*args, **kw)
        except Exception, exc:
            self.logger.error(str(traceback.format_exc()))
            configreq = {'workflow': kw['task']['tm_taskname'], 'status': 'KILLFAILED', 'subresource': 'failure', 'failure': b64encode(str(exc))}
            self.server.post(self.resturl, data = urllib.urlencode(configreq))
        else:
            if kw['task']['kill_all']:
                configreq = {'workflow': kw['task']['tm_taskname'], 'status': "KILLED"}
                self.server.post(self.resturl, data = urllib.urlencode(configreq))
            else:
                configreq = {'workflow': kw['task']['tm_taskname'], 'status': "SUBMITTED"}
                self.server.post(self.resturl, data = urllib.urlencode(configreq))

