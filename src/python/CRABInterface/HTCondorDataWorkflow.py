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
from WMCore.Services.DBS.DBSReader import DBSReader
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.Utils import conn_handler, global_user_throttle
from Databases.FileMetaDataDB.Oracle.FileMetaData.FileMetaData import GetFromTaskAndType
from WMCore.Services.pycurl_manager import ResponseHeader
from WMCore.DataStructs.LumiList import LumiList
import WMCore.Database.CMSCouch as CMSCouch

import HTCondorUtils
import HTCondorLocator

JOB_KILLED_HOLD_REASON = "Python-initiated action."

class MissingNodeStatus(ExecutionError):
    pass

class HTCondorDataWorkflow(DataWorkflow):
    """ HTCondor implementation of the status command.
    """

    successList = ['finished']
    failedList = ['held', 'failed', 'cooloff']

    @conn_handler(services=['centralconfig'])
    def chooseScheduler(self, scheddname=None, backend_urls=None):
        if not scheddname:
            locator = HTCondorLocator.HTCondorLocator(backend_urls)
            scheddname = locator.getSchedd()
        return scheddname


    def getRootTasks(self, workflow, schedd):
        rootConst = 'TaskType =?= "ROOT" && CRAB_ReqName =?= %s && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)' % HTCondorUtils.quote(workflow)
        rootAttrList = ["JobStatus", "ExitCode", 'CRAB_JobCount', 'CRAB_ReqName', 'TaskType', "HoldReason", "HoldReasonCode", "CRAB_UserWebDir",
                        "CRAB_SiteWhitelist", "CRAB_SiteBlacklist", "CRAB_SiteResubmitWhitelist", "CRAB_SiteResubmitBlacklist", "DagmanHoldReason"]

        # Note: may throw if the schedd is down.  We may want to think about wrapping the
        # status function and have it catch / translate HTCondor errors.
        results = list(schedd.xquery(rootConst, rootAttrList))

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)
        return results


    def logs(self, workflow, howmany, exitcode, jobids, userdn, userproxy=None):
        self.logger.info("About to get log of workflow: %s. Getting status first." % workflow)

        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow).next()
        row = self.Task.ID_tuple(*row)

        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring', 'cooloff', 'held']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed', 'transferred']]

        return self.getFiles(workflow, howmany, jobids, ['LOG'], transferingIds, finishedIds, \
                             row.user_dn, row.username, row.user_role, row.user_group, userproxy)


    def output(self, workflow, howmany, jobids, userdn, userproxy=None):
        self.logger.info("About to get output of workflow: %s. Getting status first." % workflow)

        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow).next()
        row = self.Task.ID_tuple(*row)

        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring', 'cooloff', 'held']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed', 'transferred']]

        return self.getFiles(workflow, howmany, jobids, ['EDM', 'TFILE', 'FAKE'], transferingIds, finishedIds, \
                             row.user_dn, row.username, row.user_role, row.user_group, userproxy)


    @conn_handler(services=['phedex'])
    def getFiles(self, workflow, howmany, jobids, filetype, transferingIds, finishedIds, userdn, username, role, group, userproxy = None):
        """
        Retrieves the output PFN aggregating output in final and temporary locations.

        :arg str workflow: the unique workflow name
        :arg int howmany: the limit on the number of PFN to return
        :return: a generator of list of outputs"""

        file_type = 'log' if filetype == ['LOG'] else 'output'

        ## Check that the jobids passed by the user are in a valid state to retrieve files.
        for jobid in jobids:
            if not jobid in transferingIds + finishedIds:
                raise InvalidParameter("The job with id %s is not in a valid state to retrieve %s files" % (file_type, jobid))

        ## If the user does not give us jobids, set them to all possible ids.
        if not jobids:
            jobids = transferingIds + finishedIds
        else:
            howmany = -1 #if the user specify the jobids return all possible files with those ids

        #user did not give us ids and no ids available in the task
        if not jobids:
            self.logger.info("No jobs found in the task with a valid state to retrieve %s files" % file_type)
            return

        self.logger.debug("Retrieving the %s files of the following jobs: %s" % (file_type, jobids))
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype = ','.join(filetype), taskname = workflow)
        rows = filter(lambda row: row[GetFromTaskAndType.PANDAID] in jobids, rows)
        if howmany != -1:
            rows = rows[:howmany]
        #jobids=','.join(map(str,jobids)), limit=str(howmany) if howmany!=-1 else str(len(jobids)*100))
        for row in rows:
            try:
                jobid = row[GetFromTaskAndType.PANDAID]
                if row[GetFromTaskAndType.DIRECTSTAGEOUT]:
                    lfn  = row[GetFromTaskAndType.LFN]
                    site = row[GetFromTaskAndType.LOCATION]
                    self.logger.debug("LFN: %s and site %s" % (lfn, site))
                    pfn  = self.phedex.getPFN(site, lfn)[(site, lfn)]
                else:
                    if jobid in finishedIds:
                        lfn  = row[GetFromTaskAndType.LFN]
                        site = row[GetFromTaskAndType.LOCATION]
                        self.logger.debug("LFN: %s and site %s" % (lfn, site))
                        pfn  = self.phedex.getPFN(site, lfn)[(site, lfn)]
                    elif jobid in transferingIds:
                        lfn  = row[GetFromTaskAndType.TMPLFN]
                        site = row[GetFromTaskAndType.TMPLOCATION]
                        self.logger.debug("LFN: %s and site %s" % (lfn, site))
                        pfn  = self.phedex.getPFN(site, lfn)[(site, lfn)]
                    else:
                        continue
            except Exception, err:
                self.logger.exception(err)
                raise ExecutionError("Exception while contacting PhEDEX.")

            yield {'jobid': jobid,
                   'pfn': pfn,
                   'lfn': lfn,
                   'size': row[GetFromTaskAndType.SIZE],
                   'checksum' : {'cksum' : row[GetFromTaskAndType.CKSUM], 'md5' : row[GetFromTaskAndType.ADLER32], 'adler32' : row[GetFromTaskAndType.ADLER32]}
                  }


    def report(self, workflow, userdn, usedbs):
        """
        Computes the report for workflow. If usedbs is used also query DBS and return information about the input and output datasets
        """

        def _compactLumis(datasetInfo):
            """ Help function that allow to convert from runLumis divided per file (result of listDatasetFileDetails)
                to an aggregated result.
            """
            lumilist = {}
            for file, info in datasetInfo.iteritems():
                for run, lumis in info['Lumis'].iteritems():
                    lumilist.setdefault(str(run), []).extend(lumis)
            return lumilist

        res = {}
        self.logger.info("About to compute report of workflow: %s with usedbs=%s. Getting status first." % (workflow,usedbs))
        statusRes = self.status(workflow, userdn)[0]

        #get the information we need from the taskdb/initilize variables
        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow).next()
        row = self.Task.ID_tuple(*row)
        inputDataset = row.input_dataset
        outputDatasets = literal_eval(row.output_dataset.read() if row.output_dataset else 'None')
        dbsUrl = row.dbs_url

        #load the lumimask
        splitArgs = literal_eval(row.split_args.read())
        res['lumiMask'] = buildLumiMask(splitArgs['runs'], splitArgs['lumis'])
        self.logger.info("Lumi mask was: %s" % res['lumiMask'])

        #extract the finished jobs from filemetadata
        jobids = [x[1] for x in statusRes['jobList'] if x[0] in ['finished']]
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype='EDM,TFILE,POOLIN', taskname=workflow)

        res['runsAndLumis'] = {}
        for row in rows:
            if row[GetFromTaskAndType.PANDAID] in jobids:
                if str(row[GetFromTaskAndType.PANDAID]) not in res['runsAndLumis']:
                    res['runsAndLumis'][str(row[GetFromTaskAndType.PANDAID])] = []
                res['runsAndLumis'][str(row[GetFromTaskAndType.PANDAID])].append( { 'parents' : row[GetFromTaskAndType.PARENTS].read(),
                        'runlumi' : row[GetFromTaskAndType.RUNLUMI].read(),
                        'events'  : row[GetFromTaskAndType.INEVENTS],
                        'type'    : row[GetFromTaskAndType.TYPE],
                        'lfn'     : row[GetFromTaskAndType.LFN],
                })
        self.logger.info("Got %s edm files for workflow %s" % (len(res['runsAndLumis']), workflow))

        if usedbs:
            if not outputDatasets:
                raise ExecutionError("Cannot find any information about the output datasets names. You can try to execute 'crab report' with --dbs=no")
            try:
                #load the input dataset's lumilist
                dbs = DBSReader(dbsUrl)
                inputDetails = dbs.listDatasetFileDetails(inputDataset)
                res['dbsInLumilist'] = _compactLumis(inputDetails)
                self.logger.info("Aggregated input lumilist: %s" % res['dbsInLumilist'])
                #load the output datasets' lumilist
                res['dbsNumEvents'] = 0
                res['dbsNumFiles'] = 0
                res['dbsOutLumilist'] = {}
                dbs = DBSReader("https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader") #We can only publish here with DBS3
                outLumis = []
                for outputDataset in outputDatasets:
                    outputDetails = dbs.listDatasetFileDetails(outputDataset)
                    outLumis.append(_compactLumis(outputDetails))
                    res['dbsNumEvents'] += sum(x['NumberOfEvents'] for x in outputDetails.values())
                    res['dbsNumFiles'] += sum(len(x['Parents']) for x in outputDetails.values())

                outLumis = LumiList(runsAndLumis = outLumis).compactList
                for run,lumis in outLumis.iteritems():
                    res['dbsOutLumilist'][run] = reduce(lambda x1,x2: x1+x2, map(lambda x: range(x[0], x[1]+1), lumis))
                self.logger.info("Aggregated output lumilist: %s" % res['dbsOutLumilist'])
            except Exception, ex:
                msg = "Failed to contact DBS: %s" % str(ex)
                self.logger.exception(msg)
                raise ExecutionError("Exception while contacting DBS. Cannot get the input/output lumi lists. You can try to execute 'crab report' with --dbs=no")

        yield res


    @global_user_throttle.make_throttled()
    @conn_handler(services=['centralconfig', 'servercert'])
    def status(self, workflow, userdn, userproxy=None, verbose=0):
        """Retrieve the status of the workflow.

           :arg str workflow: a valid workflow name
           :return: a workflow status summary document"""

        # First, verify the task has been submitted by the backend.
        self.logger.info("Got status request for workflow %s" % workflow)
        row = self.api.query(None, None, self.Task.ID_sql, taskname = workflow)
        try:
            #just one row is picked up by the previous query
            row = self.Task.ID_tuple(*row.next())
        except StopIteration:
            raise ExecutionError("Impossible to find task %s in the database." % workflow)

        #TODO this has to move to a better place. Commenting now so we remember
#        if db_userdn != userdn:
#            raise ExecutionError("Your DN, %s, is not the same as the original DN used for task submission" % userdn)

        if verbose == None:
            verbose = 0
        self.logger.info("Status result for workflow %s: %s (detail level %d)" % (workflow, row.task_status, verbose))
        taskWarnings = literal_eval(row.task_warnings if isinstance(row.task_warnings, str) else row.task_warnings.read())
        if row.task_status not in ['SUBMITTED', 'KILLFAILED', 'KILLED', 'QUEUED']:
            if isinstance(row.task_failure, str):
                taskFailureMsg = row.task_failure
            elif row.task_failure == None:
                taskFailureMsg = ""
            else:
                taskFailureMsg = row.task_failure.read()
            result = [ {"status" : row.task_status,
                      "taskFailureMsg"  : taskFailureMsg,
                      "taskWarningMsg"  : taskWarnings,
                      "jobSetID"        : '',
                      "jobsPerStatus"   : {},
                      "failedJobdefs"   : 0,
                      "totalJobdefs"    : 0,
                      "jobdefErrors"    : [],
                      "jobList"         : [],
                      "saveLogs"        : row.save_logs }]
            self.logger.debug("Detailed result for workflow %s: %s\n" % (workflow, result))
            return result

        # From 3.4.04 version schedd name is not saved anymore in task_name
        # But support is needed for tasks submitted before this version.
        backend_urls = self.centralcfg.centralconfig["backend-urls"]
        if row.collector:
            backend_urls['htcondorPool'] = row.collector
        name = workflow
        if not row.schedd:
            name = workflow.split("_")[2].split(":")[0]
            self.logger.info("Getting status for workflow %s, looking for schedd %s" % (workflow, name))
            # Debug information to double check later (after ~2,3 months to see if users still checking old tasks)
            # If not, need to clean up this code. Comment here to remember.
            self.logger.debug("User asked information about old task. %s" % workflow)
            try:
                locator = HTCondorLocator.HTCondorLocator(backend_urls)
                self.logger.debug("Will talk to %s." % locator.getCollector())
                name = locator.getSchedd()
                self.logger.debug("Schedd name %s." % name)
                schedd, address = locator.getScheddObj(workflow)
                results = self.getRootTasks(workflow, schedd)
                self.logger.info("Web status for workflow %s done" % workflow)
            except Exception, exp:
                #when the task is submitted for the first time
                if row.task_status in ['QUEUED']:
                    if isinstance(row.task_failure, str):
                        taskFailureMsg = row.task_failure
                    elif row.task_failure == None:
                        taskFailureMsg = ""
                    else:
                        taskFailureMsg = row.task_failure.read()
                    return [ {"status" : row.task_status,
                              "taskFailureMsg"  : taskFailureMsg,
                              "taskWarningMsg"  : taskWarnings,
                              "jobSetID"        : '',
                              "jobsPerStatus"   : {},
                              "failedJobdefs"   : 0,
                              "totalJobdefs"    : 0,
                              "jobdefErrors"    : [],
                              "jobList"         : [],
                              "saveLogs"        : row.save_logs }]
                msg = ("%s: The CRAB3 server frontend is not able to find your task in the Grid scheduler (remember tasks older than 30 days are automatically removed)."
                       "If your task is a recent one, this could mean there is a temporary glicth. Please, retry later. Message from the scheduler: %s") % (workflow, str(exp))
                self.logger.exception(msg)
                return [{"status" : "UNKNOWN",
                         "taskFailureMsg" : str(msg),
                         "taskWarningMsg"  : taskWarnings,
                         "jobSetID"        : '',
                         "jobsPerStatus"   : {},
                         "failedJobdefs"   : 0,
                         "totalJobdefs"    : 0,
                         "jobdefErrors"    : [],
                         "jobList"         : [],
                         "saveLogs"        : row.save_logs }]
        else:
            # This is new implementation and we already know scheduler name
            self.logger.info("Getting status for workflow %s, looking for schedd %s" % (name, row.schedd))
            try:
               locator  = HTCondorLocator.HTCondorLocator(backend_urls)
               self.logger.debug("Will talk to %s." % locator.getCollector())
               self.logger.debug("Schedd name %s." % row.schedd)
               schedd, address = locator.getScheddObjNew(row.schedd)
               results = self.getRootTasks(workflow, schedd)
               self.logger.info("Web status for workflow %s done " % workflow)
            except Exception, exp:
                #when the task is submitted for the first time
                if row.task_status in ['QUEUED']:
                    if isinstance(row.task_failure, str):
                        taskFailureMsg = row.task_failure
                    elif row.task_failure == None:
                        taskFailureMsg = ""
                    else:
                        taskFailureMsg = row.task_failure.read()
                    return [ {"status" : row.task_status,
                              "taskFailureMsg"  : taskFailureMsg,
                              "taskWarningMsg"  : taskWarnings,
                              "jobSetID"        : '',
                              "jobsPerStatus"   : {},
                              "failedJobdefs"   : 0,
                              "totalJobdefs"    : 0,
                              "jobdefErrors"    : [],
                              "jobList"         : [],
                              "saveLogs"        : row.save_logs }]
                msg = ("%s: The CRAB3 server frontend is not able to find your task in the Grid scheduler (remember tasks older than 30 days are automatically removed)."
                       "If your task is a recent one, this could mean there is a temporary glicth. Please, retry later. Message from the scheduler: %s") % (workflow, str(exp))
                self.logger.exception(msg)
                return [{"status" : "UNKNOWN",
                         "taskFailureMsg" : str(msg),
                         "taskWarningMsg"  : taskWarnings,
                         "jobSetID"        : '',
                         "jobsPerStatus"   : {},
                         "failedJobdefs"   : 0,
                         "totalJobdefs"    : 0,
                         "jobdefErrors"    : [],
                         "jobList"         : [],
                         "saveLogs"        : row.save_logs }]
        if not results:
            return [ {"status" : "UNKNOWN",
                      "taskFailureMsg" : ("The CRAB3 server frontend cannot find any information about your jobs in the Grid scheduler."
                                         "Remember, jobs are only kept for a limited amount of time on the scheduler (30 days)"),
                      "taskWarningMsg"  : taskWarnings,
                      "jobSetID"        : '',
                      "jobsPerStatus"   : {},
                      "failedJobdefs"   : 0,
                      "totalJobdefs"    : 0,
                      "jobdefErrors"    : [],
                      "jobList"         : [],
                      "saveLogs"        : row.save_logs }]

        taskStatusCode = int(results[-1]['JobStatus'])
        if 'CRAB_UserWebDir' not in results[-1]:
            if taskStatusCode != 1 and taskStatusCode != 2:
                DagmanHoldReason = results[-1]['DagmanHoldReason'] if 'DagmanHoldReason' in results[-1] else None
                msg = "Your task failed to bootstrap on the Grid scheduler %s. Please contact an expert. Hold Reason: %s" % (address, DagmanHoldReason)
                return [ {"status" : "UNKNOWN",
                      "taskFailureMsg"  : msg,
                      "taskWarningMsg"  : taskWarnings,
                      "jobSetID"        : '',
                      "jobsPerStatus"   : {},
                      "failedJobdefs"   : 0,
                      "totalJobdefs"    : 0,
                      "jobdefErrors"    : [],
                      "jobList"         : [],
                      "saveLogs"        : row.save_logs }]
            else:
                return [ {"status" : "SUBMITTED",
                      "taskFailureMsg"  : "",
                      "taskWarningMsg"  : ["Task has not yet bootstrapped."] + taskWarnings,
                      "jobSetID"        : '',
                      "jobsPerStatus"   : {},
                      "failedJobdefs"   : 0,
                      "totalJobdefs"    : 0,
                      "jobdefErrors"    : [],
                      "jobList"         : [],
                      "saveLogs"        : row.save_logs }]

        try:
            taskStatus, pool = self.taskWebStatus(results[0], verbose=verbose)
        except MissingNodeStatus:
            return [ {"status" : "UNKNOWN",
                "taskFailureMsg"  : "Node status file not currently available.  Retry in a minute if you just submitted the task",
                "taskWarningMsg"  : taskWarnings,
                "jobSetID"        : '',
                "jobsPerStatus"   : {},
                "failedJobdefs"   : 0,
                "totalJobdefs"    : 0,
                "jobdefErrors"    : [],
                "jobList"         : [],
                "saveLogs"        : row.save_logs }]

        jobsPerStatus = {}
        jobList = []
        taskJobCount = int(results[-1].get('CRAB_JobCount', 0))
        codes = {1: 'idle', 2: 'running', 3: 'killing', 4: 'finished', 5: 'held'}
        task_codes = {1: 'SUBMITTED', 2: 'SUBMITTED', 4: 'COMPLETED', 5: 'KILLED'}
        if row.task_status in ['QUEUED']:
            task_status = 'QUEUED'
        else:
            task_status = task_codes.get(taskStatusCode, 'unknown')
        retval = {"status"         : task_status,
                  "taskFailureMsg" : "",
                  "taskWarningMsg" : taskWarnings,
                  "jobSetID"       : workflow,
                  "jobsPerStatus"  : jobsPerStatus,
                  "failedJobdefs"  : 0,
                  "totalJobdefs"   : 0,
                  "jobdefErrors"   : [],
                  "jobList"        : jobList,
                  "saveLogs"       : row.save_logs}
        # HoldReasonCode == 1 indicates that the TW killed the task; perhaps the DB was not properly updated afterward?
        if row.task_status != "KILLED" and taskStatusCode == 5 and results[-1]['HoldReasonCode'] == 1:
            retval['status'] = 'KILLED'
        elif taskStatusCode == 5 and results[-1]['HoldReasonCode'] == 16:
            retval['status'] = 'InTransition'
        elif row.task_status != "KILLED" and taskStatusCode == 5:
            retval['status'] = 'FAILED'

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

        #getting publication information
        publication_info = {}
        outdatasets = literal_eval(row.output_dataset.read() if row.output_dataset else 'None')
        arguments = literal_eval(row.arguments.read())

        #Always returning ASOURL also, it is required for kill, resubmit
        self.logger.info("ASO: %s" % row.asourl)
        retval['ASOURL'] = row.asourl

        if (row.publication == 'T' and 'finished' in retval['jobsPerStatus']):
            publication_info = self.publicationStatus(workflow, row.asourl)
            self.logger.info("Publication status for workflow %s done" % workflow)
        elif (row.publication == 'F'):
            publication_info = {'disabled': []}
        else:
            self.logger.info("No files to publish: Publish flag %s, files transferred: %s" % (row.publication, retval['jobsPerStatus'].get('finished', 0)))

        if len(taskStatus) == 0 and results[0]['JobStatus'] == 2:
            retval['status'] = 'Running (jobs not submitted)'

        retval['jobs'] = taskStatus
        retval['pool'] = pool
        retval['publication'] = publication_info
        retval['outdatasets'] = outdatasets

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
        else:
            if 'RemoteSysCpu' in event:
                info['TotalSysCpuTimeHistory'][-1] = float(event['RemoteSysCpu'])
            if 'RemoteUserCpu' in event:
                info['TotalUserCpuTimeHistory'][-1] = float(event['RemoteUserCpu'])


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
                raise ExecutionError("Cannot get jobs log file. Retry in a minute if you just submitted the task")

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
                raise ExecutionError("Cannot get site ad. Retry in a minute if you just submitted the task")
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
                    raise ExecutionError("Cannot get pool info file. Retry in a minute if you just submitted the task")
        nodes_url = url + "/node_state.txt"
        curl.setopt(pycurl.URL, nodes_url)
        fp.seek(0)
        self.logger.debug("Starting download of node state")
        curl.perform()
        self.logger.debug("Finished download of node state")
        header = ResponseHeader(hbuf.getvalue())
        if header.status == 200:
            fp.seek(0)
            self.logger.debug("Starting parse of node state")
            self.parseNodeState(fp, nodes)
            self.logger.debug("Finished parse of node state")
            fp.truncate(0)
            hbuf.truncate(0)
        else:
            raise MissingNodeStatus("Cannot get node state log. Retry in a minute if you just submitted the task")

        site_url = url + "/error_summary.json"
        fp2 = StringIO.StringIO()
        curl.setopt(pycurl.WRITEFUNCTION, fp2.write)
        curl.setopt(pycurl.URL, site_url)
        self.logger.debug("Starting download of error summary file")
        curl.perform()
        self.logger.debug("Finished download of error summary file")
        header = ResponseHeader(hbuf.getvalue())
        if header.status == 200:
            fp2.seek(0)
            self.logger.debug("Starting parse of summary file")
            self.parseErrorReport(fp2, nodes)
            self.logger.debug("Finished parse of summary file")
        else:
            self.logger.debug("No error summary available")

        fp3 = StringIO.StringIO();
        curl.setopt(pycurl.WRITEFUNCTION, fp3.write)
        aso_url = url + "/aso_status.json"
        curl.setopt(pycurl.URL, aso_url)
        self.logger.debug("Starting download of aso state")
        curl.perform()
        self.logger.debug("Finished download of aso state")
        header = ResponseHeader(hbuf.getvalue())
        if header.status == 200:
            self.logger.debug("Starting parsing of aso state")
            self.parseASOState(fp3, nodes)
            self.logger.debug("Finished parsing of aso state")
        else:
            self.logger.debug("No aso state file available")

        return nodes, pool_info

    def publicationStatus(self, workflow, asourl):
        publication_info = {}
        if not asourl:
            raise ExecutionError("This CRAB server is not configured to publish; no publication status is available.")
        server = CMSCouch.CouchServer(dburl=asourl, ckey=self.serverKey, cert=self.serverCert)
        try:
            db = server.connectDatabase('asynctransfer')
        except Exception, ex:
            msg =  "Error while connecting to asynctransfer CouchDB for workflow %s " % workflow
            self.logger.exception(msg)
            publication_info = {'error' : msg}
            return  publication_info
        query = {'reduce': True, 'key': workflow, 'stale': 'update_after'}
        try:
            publicationlist = None
            publicationlist = db.loadView('AsyncTransfer', 'PublicationStateByWorkflow', query)['rows']
        except Exception, ex:
            msg =  "Error while querying CouchDB for publication status information for workflow %s " % workflow
            self.logger.exception(msg)
            publication_info = {'error' : msg}
            return  publication_info

        if publicationlist and ('value' in publicationlist[0]):
            publication_info.update(publicationlist[0]['value'])

        return publication_info


    node_name_re = re.compile("DAG Node: Job(\d+)")
    node_name2_re = re.compile("Job(\d+)")
    def parseJobLog(self, fp, nodes):
        node_map = {}
        count = 0
        for event in HTCondorUtils.readEvents(fp):
            count += 1
            eventtime = time.mktime(time.strptime(event['EventTime'], "%Y-%m-%dT%H:%M:%S"))
            if event['MyType'] == 'SubmitEvent':
                m = self.node_name_re.match(event['LogNotes'])
                if m:
                    node = m.groups()[0]
                    proc = event['Cluster'], event['Proc']
                    info = nodes.setdefault(node, {'Retries': 0, 'Restarts': 0, 'SiteHistory': [], 'ResidentSetSize': [], 'SubmitTimes': [], 'StartTimes': [],
                                                'EndTimes': [], 'TotalUserCpuTimeHistory': [], 'TotalSysCpuTimeHistory': [], 'WallDurations': [], 'JobIds': []})
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
                nodes[node]['RecordedSite'] = False
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
            elif event['MyType'] == 'ShadowExceptionEvent' or event["MyType"] == "JobReconnectFailedEvent" or event['MyType'] == 'JobEvictedEvent':
                node = node_map[event['Cluster'], event['Proc']]
                if nodes[node]['State'] != 'idle':
                    nodes[node]['EndTimes'].append(eventtime)
                    if nodes[node]['WallDurations'] and nodes[node]['EndTimes'] and nodes[node]['StartTimes']:
                        nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
                    nodes[node]['State'] = 'idle'
                    self.insertCpu(event, nodes[node])
                    nodes[node]['TotalUserCpuTimeHistory'].append(0)
                    nodes[node]['TotalSysCpuTimeHistory'].append(0)
                    nodes[node]['WallDurations'].append(0)
                    nodes[node]['ResidentSetSize'].append(0)
                    nodes[node]['SubmitTimes'].append(-1)
                    nodes[node]['JobIds'].append(nodes[node]['JobIds'][-1])
                    nodes[node]['Restarts'] += 1
            elif event['MyType'] == 'JobAbortedEvent':
                node = node_map[event['Cluster'], event['Proc']]
                if nodes[node]['State'] == "idle" or nodes[node]['State'] == "held":
                    nodes[node]['StartTimes'].append(-1)
                    if not nodes[node]['RecordedSite']:
                        nodes[node]['SiteHistory'].append("Unknown")
                nodes[node]['State'] = 'killed'
                self.insertCpu(event, nodes[node])
            elif event['MyType'] == 'JobHeldEvent':
                node = node_map[event['Cluster'], event['Proc']]
                if nodes[node]['State'] == 'running':
                    nodes[node]['EndTimes'].append(eventtime)
                    if nodes[node]['WallDurations'] and nodes[node]['EndTimes'] and nodes[node]['StartTimes']:
                        nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
                    self.insertCpu(event, nodes[node])
                    nodes[node]['TotalUserCpuTimeHistory'].append(0)
                    nodes[node]['TotalSysCpuTimeHistory'].append(0)
                    nodes[node]['WallDurations'].append(0)
                    nodes[node]['ResidentSetSize'].append(0)
                    nodes[node]['SubmitTimes'].append(-1)
                    nodes[node]['JobIds'].append(nodes[node]['JobIds'][-1])
                    nodes[node]['Restarts'] += 1
                nodes[node]['State'] = 'held'
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
            elif event["MyType"] == "JobDisconnectedEvent" or event["MyType"] == "JobReconnectedEvent":
                # These events don't really affect the node status
                pass
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


    def parseASOState(self, fp, nodes):
        fp.seek(0)
        data = json.load(fp)
        for _, result in data['results'].items():
            if 'state' in result['value']: #this if is for backward compatibility with old postjobs
                state = result['value']['state']
                jobid = str(result['value']['jobid'])
                if nodes[jobid]['State'] == 'transferring' and state == 'done':
                    nodes[jobid]['State'] = 'transferred'
            else:
                self.logger.warning("It seems that the aso_status file has been generated with an old version of the postjob")
                break


    def parseErrorReport(self, fp, nodes):
        def last(joberrors):
            return joberrors[max(joberrors, key=int)]
        fp.seek(0)
        data = json.load(fp)
        #iterate over the jobs and set the error dict for those which are failed
        for jobid, statedict in nodes.iteritems():
            if statedict['State'] == 'failed' and jobid in data:
                statedict['Error'] = last(data[jobid]) #data[jobid] contains all retries. take the last one


    job_re = re.compile(r"JOB Job(\d+)\s+([A-Z_]+)\s+\((.*)\)")
    post_failure_re = re.compile(r"POST [Ss]cript failed with status (\d+)")
    def parseNodeState(self, fp, nodes):
        first_char = fp.read(1)
        fp.seek(0)
        if first_char == "[":
            return self.parseNodeStateV2(fp, nodes)
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


    def parseNodeStateV2(self, fp, nodes):
        """
        HTCondor 8.1.6 updated the node state file to be classad-based.
        This is a more flexible format that allows future extensions but, unfortunately,
        also requires a separate parser.
        """
        for ad in classad.parseAds(fp):
            if ad['Type'] != "NodeStatus":
                continue
            node = ad.get("Node", "")
            if not node.startswith("Job"):
                continue
            nodeid = node[3:]
            status = ad.get('NodeStatus', -1)
            retry = ad.get('RetryCount', -1)
            msg = ad.get("StatusDetails", "")
            if status == 1: # STATUS_READY
                info = nodes.setdefault(nodeid, {})
                if info.get("State") == "transferring":
                    info["State"] = "cooloff"
                elif info.get('State') != "cooloff":
                    info['State'] = 'unsubmitted'
            elif status == 2: # STATUS_PRERUN
                info = nodes.setdefault(nodeid, {})
                if retry == 0:
                    info['State'] = 'unsubmitted'
                else:
                    info['State'] = 'cooloff'
            elif status == 3: # STATUS_SUBMITTED
                info = nodes.setdefault(nodeid, {})
                if msg == 'not_idle':
                    info.setdefault('State', 'running')
                else:
                    info.setdefault('State', 'idle')
            elif status == 4: # STATUS_POSTRUN 
                info = nodes.setdefault(nodeid, {})
                if info.get("State") != "cooloff":
                    info['State'] = 'transferring'
            elif status == 5: # STATUS_DONE
                info = nodes.setdefault(nodeid, {})
                info['State'] = 'finished'
            elif status == 6: # STATUS_ERROR
                info = nodes.setdefault(nodeid, {})
                # Older versions of HTCondor would put jobs into STATUS_ERROR
                # for a short time if the job was to be retried.  Hence, we had
                # some status parsing logic to try and guess whether the job would
                # be tried again in the near future.  This behavior is no longer
                # observed; STATUS_ERROR is terminal.
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

