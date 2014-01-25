
import re
import json
import time
import hashlib
import StringIO
import tempfile
import traceback
from ast import literal_eval

import pycurl
import classad
import htcondor

from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.WMSpec.WMTask import buildLumiMask
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.Utils import conn_handler
from Databases.FileMetaDataDB.Oracle.FileMetaData.FileMetaData import GetFromTaskAndType
from WMCore.Services.pycurl_manager import ResponseHeader

import HTCondorUtils
import HTCondorLocator

JOB_KILLED_HOLD_REASON = "Python-initiated action."

def lfn_to_temp(lfn, userdn, username, role, group):
    lfn_parts = lfn[1:].split("/")
    if lfn_parts[1] == "temp":
        return lfn
    del lfn_parts[2]
    hash_input = userdn
    if group:
        hash_input += "," + group
    if role:
        hash_input += "," + role
    user = "%s.%s" % (username, hashlib.sha1(hash_input).hexdigest())
    lfn_parts.insert(2, user)
    lfn_parts.insert(1, "temp")
    return "/" + "/".join(lfn_parts)


def temp_to_lfn(lfn, username):
    lfn_parts = lfn[1:].split("/")
    if lfn_parts[1] != "temp":
        return lfn
    del lfn_parts[1]
    del lfn_parts[2]
    lfn_parts.insert(2, username)
    return "/" + "/".join(lfn_parts)


class HTCondorDataWorkflow(DataWorkflow):
    """ HTCondor implementation of the status command.
    """

    successList = ['finished']
    failedList = ['held', 'failed', 'cooloff']

    @conn_handler(services=['centralconfig'])
    def updateRequest(self, workflow):
        info = workflow.split("_", 3)
        if len(info) < 4:
            return workflow
        hn_name = info[2]
        locator = HTCondorLocator.HTCondorLocator(self.centralcfg.centralconfig["backend-urls"])
        name = locator.getSchedd().split("@")[0].split(".")[0]
        info[2] = "%s:%s" % (name, hn_name)
        return "_".join(info)

    @conn_handler(services=['centralconfig'])
    def status(self, workflow, userdn, userproxy=None, verbose=0):
        """Retrieve the status of the workflow.

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""

        try:
            return self.alt_status(workflow, userdn, userproxy=userproxy, verbose=verbose)
        except:
            s = traceback.format_exc()
            self.logger.exception("Failure of alt status: %s" % s)
        # First, verify the task has been submitted by the backend.
        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow)
        _, jobsetid, status, vogroup, vorole, taskFailure, splitArgs, resJobs, saveLogs, username, userdn = row.next() #just one row is picked up by the previous query
        self.logger.info("Status result for workflow %s: %s. JobsetID: %s" % (workflow, status, jobsetid))
        self.logger.debug("User vogroup=%s and user vorole=%s" % (vogroup, vorole))
        if not taskFailure: taskFailure = ''
        if status != 'SUBMITTED':
            return [ {"status" : status,\
                      "taskFailureMsg" : taskFailure if isinstance(taskFailure,str) else taskFailure.read(),\
                      "jobSetID"        : '',
                      "jobsPerStatus"   : {},
                      "failedJobdefs"   : 0,
                      "totalJobdefs"    : 0,
                      "jobdefErrors"    : [],
                      "jobList"         : [],
                      "saveLogs"        : saveLogs }]

        import cherrypy

        name = workflow.split("_")[0]
        cherrypy.log("Getting status for workflow %s, looking for schedd %s" %\
                                (workflow, name))
        locator = HTCondorLocator.HTCondorLocator(self.centralcfg.centralconfig["backend-urls"])
        cherrypy.log("Will talk to %s." % locator.getCollector())
        schedd, address = locator.getScheddObj(workflow)

        results = self.getRootTasks(workflow, schedd)

        # TODO: handle case where there were no root tasks performed.
        # In such a case, we ought to check the DB.
        jobsPerStatus = {}
        jobStatus = {}
        jobList = []
        taskStatusCode = int(results[-1]['JobStatus'])
        taskJobCount = int(results[-1].get('CRAB_JobCount', 0))
        codes = {1: 'idle', 2: 'running', 3: 'killing', 4: 'finished', 5: 'held'}
        task_codes = {1: 'SUBMITTED', 2: 'SUBMITTED', 4: 'COMPLETED', 5: 'KILLED'}
        retval = {"status": task_codes.get(taskStatusCode, 'unknown'), "taskFailureMsg": "", "jobSetID": workflow,
            "jobsPerStatus" : jobsPerStatus, "jobList": jobList}
        if taskStatusCode == 5 and results[-1]['HoldReasonCode'] == 3:
            retval['status'] = 'FAILED'
        elif taskStatusCode == 5 and results[-1]['HoldReasonCode'] == 16:
            retval['status'] = 'InTransition'
        elif taskStatusCode == 5:
            retval['status'] = 'Unknown'

        # Handle all "real" jobs in HTCondor.
        failedJobs = []
        allJobs = self.getHTCondorJobs(workflow, schedd)
        for result in allJobs:
            jobState = int(result['JobStatus'])
            if result['CRAB_Id'] in failedJobs:
                failedJobs.remove(result['CRAB_Id'])
            if (jobState == 4) and ('ExitCode' in result) and (int(result['ExitCode'])):
                failedJobs.append(result['CRAB_Id'])
                statusName = "failed"
            else:
                statusName = codes.get(jobState, 'unknown')
            jobStatus[int(result['CRAB_Id'])] = statusName

        # Handle all "finished" jobs.
        for result, file_state in self.getFinishedJobs(workflow):
            if result not in jobStatus:
                if file_state == 'FINISHED':
                    jobStatus[result] = 'finished'
                elif file_state == 'COOLOFF':
                    jobStatus[result] = 'cooloff'
                elif file_state == 'FAILED':
                    jobStatus[result] = 'failed'
                    failedJobs.append(result)
                else:
                    jobStatus[result] = 'transferring'
            elif (result in failedJobs) and (file_state == 'FINISHED'):
                failedJobs.remove(result)
                jobStatus[result] = 'finished'
            elif file_state == 'FINISHED':
                jobStatus[result] = 'finished'

        for i in range(1, taskJobCount+1):
            if i not in jobStatus:
                if taskStatusCode == 5:
                    jobStatus[i] = 'killed'
                else:
                    jobStatus[i] = 'unsubmitted'

        for job, status in jobStatus.items():
            jobsPerStatus.setdefault(status, 0)
            jobsPerStatus[status] += 1
            jobList.append((status, job))

        self.logger.debug("Job info: %s" % str(jobStatus))

        retval["failedJobdefs"] = len(failedJobs)
        retval["totalJobdefs"] = len(jobStatus)

        if len(jobStatus) == 0 and taskJobCount == 0 and taskStatusCode == 2:
            retval['status'] = 'Running (jobs not submitted)'

        retval['jobdefErrors'] = []

        return [retval]

    def getRootTasks(self, workflow, schedd):
        rootConst = 'TaskType =?= "ROOT" && CRAB_ReqName =?= %s && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)' % HTCondorUtils.quote(workflow)
        rootAttrList = ["JobStatus", "ExitCode", 'CRAB_JobCount', 'CRAB_ReqName', 'TaskType', "HoldReason", "HoldReasonCode", "CRAB_UserWebDir",
                        "CRAB_SiteWhitelist", "CRAB_SiteBlacklist", "CRAB_SiteResubmitWhitelist", "CRAB_SiteResubmitBlacklist"]

        # Note: may throw if the schedd is down.  We may want to think about wrapping the
        # status function and have it catch / translate HTCondor errors.
        results = schedd.query(rootConst, rootAttrList)

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)
        return results

    def getHTCondorJobs(self, workflow, schedd):
        """
        Retrieve all the jobs for executing cmsRun in this workflow
        """
        jobConst = 'TaskType =?= "Job" && CRAB_ReqName =?= %s' % HTCondorUtils.quote(workflow)
        jobList = ["JobStatus", 'ExitCode', 'ClusterID', 'ProcID', 'CRAB_Id', "HoldReasonCode"]
        return schedd.query(jobConst, jobList)

    def getFinishedJobs(self, workflow):
        """
        Get the finished jobs from the file metadata table.
        """
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype='FAKE', taskname=workflow)

        for row in rows:
            yield row[GetFromTaskAndType.PANDAID], row[GetFromTaskAndType.STATE]

    def logs(self, workflow, howmany, exitcode, jobids, userdn, userproxy=None):
        self.logger.info("About to get log of workflow: %s. Getting status first." % workflow)

        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow)
        _, _, _, tm_user_role, tm_user_group, _, _, _, tm_save_logs, tm_username, tm_user_dn = row.next()

        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring', 'cooloff', 'held']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed']]
        return self.getFiles(workflow, howmany, jobids, ['LOG'], transferingIds, finishedIds, tm_user_dn, tm_username, tm_user_role, tm_user_group, saveLogs=tm_save_logs, userproxy=userproxy)

    def output(self, workflow, howmany, jobids, userdn, userproxy=None):
        self.logger.info("About to get output of workflow: %s. Getting status first." % workflow)

        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow)
        _, _, _, tm_user_role, tm_user_group, _, _, _, tm_save_logs, tm_username, tm_user_dn = row.next()

        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring', 'cooloff', 'held']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed']]
        return self.getFiles(workflow, howmany, jobids, ['EDM', 'TFILE'], transferingIds, finishedIds, tm_user_dn, tm_username, tm_user_role, tm_user_group, userproxy=userproxy)

    @conn_handler(services=['phedex'])
    def getFiles(self, workflow, howmany, jobids, filetype, transferingIds, finishedIds, userdn, username, role, group, saveLogs=None, userproxy=None):
        """
        Retrieves the output PFN aggregating output in final and temporary locations.

        :arg str workflow: the unique workflow name
        :arg int howmany: the limit on the number of PFN to return
        :return: a generator of list of outputs"""

        #check that the jobids passed by the user are finished
        for jobid in jobids:
            if not jobid in transferingIds + finishedIds:
                raise InvalidParameter("The job with id %s is not finished" % jobid)

        #If the user do not give us jobids set them to all possible ids
        if not jobids:
            jobids = transferingIds + finishedIds
        else:
            howmany = -1 #if the user specify the jobids return all possible files with those ids

        #user did not give us ids and no ids available in the task
        if not jobids:
            self.logger.info("No finished jobs found in the task")
            return

        self.logger.debug("Retrieving %s output of jobs: %s" % (','.join(filetype), jobids))
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype=','.join(filetype), taskname=workflow)
        rows = filter(lambda row: row[GetFromTaskAndType.PANDAID] in jobids, rows)
        if howmany!=-1:
            rows=rows[:howmany]
        #jobids=','.join(map(str,jobids)), limit=str(howmany) if howmany!=-1 else str(len(jobids)*100))

        for row in rows:
            if filetype == ['LOG'] and saveLogs == 'F':
                lfn = lfn_to_temp(row[GetFromTaskAndType.LFN], userdn, username, role, group)
                pfn = self.phedex.getPFN(row[GetFromTaskAndType.TMPLOCATION], lfn)[(row[GetFromTaskAndType.TMPLOCATION], lfn)]
            else:
                if row[GetFromTaskAndType.PANDAID] in finishedIds:
                    lfn = temp_to_lfn(row[GetFromTaskAndType.LFN], username)
                    pfn = self.phedex.getPFN(row[GetFromTaskAndType.LOCATION], lfn)[(row[GetFromTaskAndType.LOCATION], lfn)]
                elif row[GetFromTaskAndType.PANDAID] in transferingIds:
                    lfn = lfn_to_temp(row[GetFromTaskAndType.LFN], userdn, username, role, group)
                    pfn = self.phedex.getPFN(row[GetFromTaskAndType.TMPLOCATION], lfn)[(row[GetFromTaskAndType.TMPLOCATION], lfn)]
                else:
                    continue

            yield { 'pfn' : pfn,
                    'size' : row[GetFromTaskAndType.SIZE],
                    'checksum' : {'cksum' : row[GetFromTaskAndType.CKSUM], 'md5' : row[GetFromTaskAndType.ADLER32], 'adler32' : row[GetFromTaskAndType.ADLER32]}
            }

    def report(self, workflow, userdn, userproxy=None):
        res = {}
        self.logger.info("About to compute report of workflow: %s. Getting status first." % workflow)
        statusRes = self.status(workflow, userdn, userproxy)[0]

        #load the lumimask
        rows = self.api.query(None, None, self.Task.ID_sql, taskname = workflow)
        splitArgs = literal_eval(rows.next()[6].read())
        res['lumiMask'] = buildLumiMask(splitArgs['runs'], splitArgs['lumis'])
        self.logger.info("Lumi mask was: %s" % res['lumiMask'])

        #extract the finished jobs from filemetadata
        jobids = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'transferring']]
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype='EDM', taskname=workflow)

        res['runsAndLumis'] = {}
        for row in rows:
            self.logger.debug("Got lumi info for job %d." % row[GetFromTaskAndType.PANDAID])
            if row[GetFromTaskAndType.PANDAID] in jobids:
                res['runsAndLumis'][str(row[GetFromTaskAndType.PANDAID])] = { 'parents' : row[GetFromTaskAndType.PARENTS].read(),
                        'runlumi' : row[GetFromTaskAndType.RUNLUMI].read(),
                        'events'  : row[GetFromTaskAndType.INEVENTS],
                }
        self.logger.info("Got %s edm files for workflow %s" % (len(res['runsAndLumis']), workflow))

        yield res

    @conn_handler(services=['centralconfig'])
    def alt_status(self, workflow, userdn, userproxy=None, verbose=0):
        """Retrieve the status of the workflow.

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""

        # First, verify the task has been submitted by the backend.
        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow)
        _, jobsetid, status, vogroup, vorole, taskFailure, splitArgs, resJobs, saveLogs, username, userdn = row.next() #just one row is picked up by the previous query
        self.logger.info("Status result for workflow %s: %s (detail level %d)" % (workflow, status, verbose))
        self.logger.debug("User vogroup=%s and user vorole=%s" % (vogroup, vorole))
        if status != 'SUBMITTED':
            result = [ {"status" : status,
                      "taskFailureMsg" : taskFailure if isinstance(taskFailure,str) else taskFailure.read(),
                      "jobSetID"        : '',
                      "jobsPerStatus"   : {},
                      "failedJobdefs"   : 0,
                      "totalJobdefs"    : 0,
                      "jobdefErrors"    : [],
                      "jobList"         : [],
                      "saveLogs"        : saveLogs }]
            self.logger.debug("Detailed result: %s\n" % result)
            return result

        name = workflow.split("_")[2].split(":")[0]
        self.logger.info("Getting status for workflow %s, looking for schedd %s" %\
                                (workflow, name))
        locator = HTCondorLocator.HTCondorLocator(self.centralcfg.centralconfig["backend-urls"])
        self.logger.debug("Will talk to %s." % locator.getCollector())
        name = locator.getSchedd()
        self.logger.debug("Schedd name %s." % name)
        schedd, address = locator.getScheddObj(workflow)

        results = self.getRootTasks(workflow, schedd)
        if not results:
            return [ {"status" : "UNKNOWN",
                      "taskFailureMsg" : "Unable to find root task in HTCondor",
                      "jobSetID"        : '',
                      "jobsPerStatus"   : {},
                      "failedJobdefs"   : 0,
                      "totalJobdefs"    : 0,
                      "jobdefErrors"    : [],
                      "jobList"         : [],
                      "saveLogs"        : saveLogs }]

        if 'CRAB_UserWebDir' not in results[-1]:
            return [ {"status" : "UNKNOWN",
                      "taskFailureMsg" : "Task failed to bootstrap on schedd %s." % address,
                      "jobSetID"        : '',
                      "jobsPerStatus"   : {},
                      "failedJobdefs"   : 0,
                      "totalJobdefs"    : 0,
                      "jobdefErrors"    : [],
                      "jobList"         : [],
                      "saveLogs"        : saveLogs }]

        taskStatus, pool = self.taskWebStatus(results[0], verbose=verbose)

        jobsPerStatus = {}
        jobList = []
        taskStatusCode = int(results[-1]['JobStatus'])
        taskJobCount = int(results[-1].get('CRAB_JobCount', 0))
        codes = {1: 'idle', 2: 'running', 3: 'killing', 4: 'finished', 5: 'held'}
        task_codes = {1: 'SUBMITTED', 2: 'SUBMITTED', 4: 'COMPLETED', 5: 'KILLED'}
        retval = {"status": task_codes.get(taskStatusCode, 'unknown'), "taskFailureMsg": "", "jobSetID": workflow,
            "jobsPerStatus" : jobsPerStatus, "jobList": jobList}
        if taskStatusCode == 5 and results[-1]['HoldReasonCode'] == 3:
            retval['status'] = 'FAILED'
        elif taskStatusCode == 5 and results[-1]['HoldReasonCode'] == 16:
            retval['status'] = 'InTransition'
        elif taskStatusCode == 5:
            retval['status'] = 'Unknown'

        for i in range(1, taskJobCount+1):
            i = str(i)
            if i not in taskStatus:
                if taskStatusCode == 5:
                    taskStatus[i] = {'State': 'killed'}
                else:
                    taskStatus[i] = {'State': 'unsubmitted'}

        for job, info in taskStatus.items():
            job = int(job)
            status = info['State']
            jobsPerStatus.setdefault(status, 0)
            jobsPerStatus[status] += 1
            jobList.append((status, job))

        retval["failedJobdefs"] = 0
        retval["totalJobdefs"] = 0

        if len(taskStatus) == 0 and results[0]['JobStatus'] == 2:
            retval['status'] = 'Running (jobs not submitted)'

        retval['jobdefErrors'] = []

        retval['jobs'] = taskStatus
        retval['pool'] = pool

        return [retval]


    cpu_re = re.compile(r"Usr \d+ (\d+):(\d+):(\d+), Sys \d+ (\d+):(\d+):(\d+)")
    def insertCpu(self, event, info):
        if 'TotalRemoteUsage' in event:
            m = self.cpu_re.match(event['TotalRemoteUsage'])
            if m:
                g = [int(i) for i in m.groups()]
                user = g[0]*3600 + g[1]*60 + g[2]
                sys = g[3]*3600 + g[4]*60 + g[5]
                info['TotalUserCpuTimeHistory'][-1] = user
                info['TotalSysCpuTimeHistory'][-1] = sys

    def prepareCurl(self):
        curl = pycurl.Curl()
        curl.setopt(pycurl.NOSIGNAL, 0)
        curl.setopt(pycurl.TIMEOUT, 30)
        curl.setopt(pycurl.CONNECTTIMEOUT, 30)
        curl.setopt(pycurl.FOLLOWLOCATION, 0)
        curl.setopt(pycurl.MAXREDIRS, 0)
        #curl.setopt(pycurl.ENCODING, 'gzip, deflate')
        return curl

    def taskWebStatus(self, task_ad, verbose):
        nodes = {}
        pool_info = {}

        url = task_ad['CRAB_UserWebDir']

        curl = self.prepareCurl()
        fp = tempfile.TemporaryFile()
        curl.setopt(pycurl.WRITEFUNCTION, fp.write)
        hbuf = StringIO.StringIO()
        curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
        self.logger.debug("Retrieving task status from web with verbosity %d." % verbose)
        if verbose == 1:
            jobs_url = url + "/jobs_log.txt"
            curl.setopt(pycurl.URL, jobs_url)
            self.logger.info("Starting download of job log")
            curl.perform()
            self.logger.info("Finished download of job log")
            header = ResponseHeader(hbuf.getvalue())
            if header.status == 200:
                fp.seek(0)
                self.logger.debug("Starting parse of job log")
                self.parseJobLog(fp, nodes)
                self.logger.debug("Finished parse of job log")
                fp.truncate(0)
                hbuf.truncate(0)
            else:
                raise RuntimeError("Failed to parse jobs log")

        elif verbose == 2:
            site_url = url + "/site_ad.txt"
            curl.setopt(pycurl.URL, site_url)
            self.logger.debug("Starting download of site ad")
            curl.perform()
            self.logger.debug("Finished download of site ad")
            header = ResponseHeader(hbuf.getvalue())
            if header.status == 200:
                fp.seek(0)
                self.logger.debug("Starting parse of site ad")
                self.parseSiteAd(fp, task_ad, nodes)
                self.logger.debug("Finished parse of site ad")
                fp.truncate(0)
                hbuf.truncate(0)
            else:
                raise RuntimeError("Failed to parse site ad")
            pool_info_url = self.centralcfg.centralconfig["backend-urls"].get("poolInfo")
            if pool_info_url:
                fp2 = StringIO.StringIO()
                curl.setopt(pycurl.WRITEFUNCTION, fp2.write)
                curl.setopt(pycurl.URL, pool_info_url)
                self.logger.debug("Starting download of pool info from %s" % pool_info_url)
                curl.perform()
                curl.setopt(pycurl.WRITEFUNCTION, fp.write)
                self.logger.debug("Finished download of pool info")
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp2.seek(0)
                    self.logger.debug("Starting parse of pool info")
                    pool_info = json.load(fp2)
                    self.logger.debug("Finished parse of pool info")
                    hbuf.truncate(0)
                else:
                    raise RuntimeError("Failed to retrieve pool info")


        nodes_url = url + "/node_state.txt"
        curl.setopt(pycurl.URL, nodes_url)
        self.logger.debug("Starting download of node state")
        curl.perform()
        self.logger.debug("Finished download of node state")
        header = ResponseHeader(hbuf.getvalue())
        if header.status == 200:
            fp.seek(0)
            self.logger.debug("Starting parse of node state")
            self.parseNodeState(fp, nodes)
            self.logger.debug("Finished parse of node state")
        else:
            raise RuntimeError("Failed to parse node state log")

        return nodes, pool_info

    node_name_re = re.compile("DAG Node: Job(\d+)")
    node_name2_re = re.compile("Job(\d+)")
    def parseJobLog(self, fp, nodes):
        node_map = {}
        count = 0
        for event in htcondor.readEvents(fp):
            count += 1
            eventtime = time.mktime(time.strptime(event['EventTime'], "%Y-%m-%dT%H:%M:%S"))
            if event['MyType'] == 'SubmitEvent':
                m = self.node_name_re.match(event['LogNotes'])
                if m:
                    node = m.groups()[0]
                    proc = event['Cluster'], event['Proc']
                    info = nodes.setdefault(node, {'Retries': 0, 'Restarts': 0, 'SiteHistory': [], 'ResidentSetSize': [], 'SubmitTimes': [], 'StartTimes': [], 'EndTimes': [], 'TotalUserCpuTimeHistory': [], 'TotalSysCpuTimeHistory': [], 'WallDurations': [], 'JobIds': []})
                    info['State'] = 'idle'
                    info['JobIds'].append("%d.%d" % proc)
                    info['RecordedSite'] = False
                    info['SubmitTimes'].append(eventtime)
                    info['TotalUserCpuTimeHistory'].append(0)
                    info['TotalSysCpuTimeHistory'].append(0)
                    info['WallDurations'].append(0)
                    info['ResidentSetSize'].append(0)
                    info['Retries'] = len(info['SubmitTimes'])-1
                    node_map[proc] = node
            elif event['MyType'] == 'ExecuteEvent':
                node = node_map[event['Cluster'], event['Proc']]
                nodes[node]['StartTimes'].append(eventtime)
                nodes[node]['State'] = 'running'
                nodes[node].setdefault('RecordedSite', False)
            elif event['MyType'] == 'JobTerminatedEvent':
                node = node_map[event['Cluster'], event['Proc']]
                nodes[node]['EndTimes'].append(eventtime)
                nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
                self.insertCpu(event, nodes[node])
                if event['TerminatedNormally']:
                    if event['ReturnValue'] == 0:
                            nodes[node]['State'] = 'transferring'
                    else:
                            nodes[node]['State'] = 'cooloff'
                else:
                    nodes[node]['State']  = 'cooloff'
            elif event['MyType'] == 'PostScriptTerminatedEvent':
                m = self.node_name2_re.match(event['DAGNodeName'])
                if m:
                    node = m.groups()[0]
                    if event['TerminatedNormally']:
                        if event['ReturnValue'] == 0:
                            nodes[node]['State'] = 'finished'
                        elif event['ReturnValue'] == 2:
                            nodes[node]['State'] = 'failed'
                        else:
                            nodes[node]['State'] = 'cooloff'
                    else:
                        nodes[node]['State']  = 'cooloff'
            elif event['MyType'] == 'ShadowExceptionEvent':
                node = node_map[event['Cluster'], event['Proc']]
                nodes[node]['EndTimes'].append(eventtime)
                if nodes[node]['WallDurations'] and nodes[node]['EndTimes'] and nodes[node]['StartTimes']:
                    nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
                nodes[node]['State'] = 'idle'
                self.insertCpu(event, nodes[node])
            elif event['MyType'] == 'JobEvictedEvent':
                node = node_map[event['Cluster'], event['Proc']]
                nodes[node]['EndTimes'].append(eventtime)
                if nodes[node]['WallDurations'] and nodes[node]['EndTimes'] and nodes[node]['StartTimes']:
                    nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
                nodes[node]['State'] = 'idle'
                self.insertCpu(event, nodes[node])
            elif event['MyType'] == 'JobAbortedEvent':
                node = node_map[event['Cluster'], event['Proc']]
                nodes[node]['State'] = 'killed'
                self.insertCpu(event, nodes[node])
            elif event['MyType'] == 'JobHeldEvent':
                node = node_map[event['Cluster'], event['Proc']]
                nodes[node]['EndTimes'].append(eventtime)
                if nodes[node]['WallDurations'] and nodes[node]['EndTimes'] and nodes[node]['StartTimes']:
                    nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
                nodes[node]['State'] = 'held'
                self.insertCpu(event, nodes[node])
            elif event['MyType'] == 'JobReleaseEvent':
                node = node_map[event['Cluster'], event['Proc']]
                nodes[node]['State'] = 'idle'
            elif event['MyType'] == 'JobAdInformationEvent':
                node = node_map[event['Cluster'], event['Proc']]
                if (not nodes[node]['RecordedSite']) and ('JOBGLIDEIN_CMSSite' in event) and not event['JOBGLIDEIN_CMSSite'].startswith("$$"):
                    nodes[node]['SiteHistory'].append(event['JOBGLIDEIN_CMSSite'])
                    nodes[node]['RecordedSite'] = True
                self.insertCpu(event, nodes[node])
            elif event['MyType'] == 'JobImageSizeEvent':
                nodes[node]['ResidentSetSize'][-1] = int(event['ResidentSetSize'])
                if nodes[node]['StartTimes']:
                    nodes[node]['WallDurations'][-1] = eventtime - nodes[node]['StartTimes'][-1]
                self.insertCpu(event, nodes[node])
            else:
                self.logger.warning("Unknown event type: %s" % event['MyType'])

        self.logger.debug("There were %d events in the job log." % count)
        now = time.time()
        for node, info in nodes.items():
            last_start = now
            if info['StartTimes']:
                last_start = info['StartTimes'][-1]
            while len(info['WallDurations']) < len(info['SiteHistory']):
                info['WallDurations'].append(now - last_start)
            while len(info['WallDurations']) > len(info['SiteHistory']):
                info['SiteHistory'].append("Unknown")

    job_re = re.compile(r"JOB Job(\d+)\s+([A-Z_]+)\s+\((.*)\)")
    post_failure_re = re.compile(r"POST script failed with status (\d+)")
    def parseNodeState(self, fp, nodes):
        for line in fp.readlines():
            m = self.job_re.match(line)
            if not m:
                continue
            nodeid, status, msg = m.groups()
            if status == "STATUS_READY":
                info = nodes.setdefault(nodeid, {})
                if info.get("State") == "transferring":
                    info["State"] = "cooloff"
                elif info.get('State') != "cooloff":
                    info['State'] = 'unsubmitted'
            elif status == "STATUS_PRERUN":
                info = nodes.setdefault(nodeid, {})
                info['State'] = 'cooloff'
            elif status == 'STATUS_SUBMITTED':
                info = nodes.setdefault(nodeid, {})
                if msg == 'not_idle':
                    info.setdefault('State', 'running')
                else:
                    info.setdefault('State', 'idle')
            elif status == 'STATUS_POSTRUN':
                info = nodes.setdefault(nodeid, {})
                if info.get("State") != "cooloff":
                    info['State'] = 'transferring'
            elif status == 'STATUS_DONE':
                info = nodes.setdefault(nodeid, {})
                info['State'] = 'finished'
            elif status == "STATUS_ERROR":
                info = nodes.setdefault(nodeid, {})
                m = self.post_failure_re.match(msg)
                if m:
                    if m.groups()[0] == '2':
                        info['State'] = 'failed'
                    else:
                        info['State'] = 'cooloff'
                else:
                    info['State'] = 'failed'

    job_name_re = re.compile(r"Job(\d+)")
    def parseSiteAd(self, fp, task_ad, nodes):
        site_ad = classad.parse(fp)

        blacklist = set(task_ad['CRAB_SiteBlacklist'])
        whitelist = set(task_ad['CRAB_SiteWhitelist'])
        if 'CRAB_SiteResubmitWhitelist' in task_ad:
            whitelist.update(task_ad['CRAB_SiteResubmitWhitelist'])
        if 'CRAB_SiteResubmitBlacklist' in task_ad:
            blacklist.update(task_ad['CRAB_SiteResubmitBlacklist'])

        for key, val in site_ad.items():
            m = self.job_name_re.match(key)
            if not m:
                continue
            nodeid = m.groups()[0]
            sites = set(val.eval())
            if whitelist:
                sites &= whitelist
            # Never blacklist something on the whitelist
            sites -= (blacklist-whitelist)

            info = nodes.setdefault(nodeid, {})
            info['AvailableSites'] = list([i.eval() for i in sites])

    def parsePoolAd(self, fp):
        pool_ad = classad.parse(fp)

