from __future__ import absolute_import

import re
import json
import time
import copy
import tarfile
import StringIO
import tempfile
import calendar
from ast import literal_eval

import pycurl
import classad

import WMCore.Database.CMSCouch as CMSCouch
from WMCore.WMSpec.WMTask import buildLumiMask
from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.DBS.DBSReader import DBSReader
from CRABInterface.DataWorkflow import DataWorkflow
from WMCore.Services.pycurl_manager import ResponseHeader
from WMCore.REST.Error import ExecutionError, InvalidParameter

# WMCore Utils module
from Utils.Throttled import global_user_throttle

from CRABInterface.Utils import conn_handler
from ServerUtilities import FEEDBACKMAIL, PUBLICATIONDB_STATES, isCouchDBURL, getEpochFromDBTime
from Databases.FileMetaDataDB.Oracle.FileMetaData.FileMetaData import GetFromTaskAndType

import HTCondorUtils
import HTCondorLocator
from functools import reduce


JOB_KILLED_HOLD_REASON = "Python-initiated action."

class MissingNodeStatus(ExecutionError):
    pass

class HTCondorDataWorkflow(DataWorkflow):
    """ HTCondor implementation of the status command.
    """

    def taskads(self, workflow):
        row = next(self.api.query(None, None, self.Task.ID_sql, taskname = workflow))
        row = self.Task.ID_tuple(*row)

        backend_urls = copy.deepcopy(self.centralcfg.centralconfig["backend-urls"])
        if row.schedd and row.collector:
            # Override the collector because it may be different from what's in the external configuration
            # (if the task was submitted by specifying a different collector in the config, for example)
            backend_urls['htcondorPool'] = row.collector

            self.logger.debug("Running condor query for task %s." % workflow)
            try:
                locator = HTCondorLocator.HTCondorLocator(backend_urls)
                schedd, _ = locator.getScheddObjNew(row.schedd)
                results = self.getRootTasks(workflow, schedd)
            except Exception as exp: # Empty results are caught here, because getRootTasks raises InvalidParameter exception.
                self.logger.exception("Exception while querying schedd")
                msg = " Message from the scheduler: %s" % (str(exp))
                self.logger.exception("%s: %s" % (workflow, msg))

            # Convert a classad list object into a dict that can be sent back to the client
            parsedRes = {}
            for ad in results:
                parsedRes[ad] = str(results[ad])

            yield parsedRes
        yield None

    def getRootTasks(self, workflow, schedd):
        rootConst = 'TaskType =?= "ROOT" && CRAB_ReqName =?= %s && (isUndefined(CRAB_Attempt) || CRAB_Attempt == 0)' % HTCondorUtils.quote(workflow)
        rootAttrList = ["JobStatus", "ExitCode", 'CRAB_JobCount', 'CRAB_ReqName', 'TaskType', "HoldReason", "HoldReasonCode", "CRAB_UserWebDir",
                        "CRAB_SiteWhitelist", "CRAB_SiteBlacklist", "DagmanHoldReason"]

        # Note: may throw if the schedd is down.  We may want to think about wrapping the
        # status function and have it catch / translate HTCondor errors.
        results = list(schedd.xquery(rootConst, rootAttrList))

        if not results:
            self.logger.info("An invalid workflow name was requested: %s" % workflow)
            raise InvalidParameter("An invalid workflow name was requested: %s" % workflow)
        return results[-1]


    def logs(self, workflow, howmany, exitcode, jobids, userdn, userproxy=None):
        self.logger.info("About to get log of workflow: %s. Getting status first." % workflow)

        row = next(self.api.query(None, None, self.Task.ID_sql, taskname = workflow))
        row = self.Task.ID_tuple(*row)

        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring', 'cooloff', 'held']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed', 'transferred']]

        return self.getFiles(workflow, howmany, jobids, ['LOG'], transferingIds, finishedIds, \
                             row.user_dn, row.username, row.user_role, row.user_group, userproxy)


    def output(self, workflow, howmany, jobids, userdn, userproxy=None):
        self.logger.info("About to get output of workflow: %s. Getting status first." % workflow)

        row = next(self.api.query(None, None, self.Task.ID_sql, taskname = workflow))
        row = self.Task.ID_tuple(*row)

        statusRes = self.status(workflow, userdn, userproxy)[0]

        transferingIds = [x[1] for x in statusRes['jobList'] if x[0] in ['transferring', 'cooloff', 'held']]
        finishedIds = [x[1] for x in statusRes['jobList'] if x[0] in ['finished', 'failed', 'transferred']]

        return self.getFiles(workflow, howmany, jobids, ['EDM', 'TFILE', 'FAKE'], transferingIds, finishedIds, \
                             row.user_dn, row.username, row.user_role, row.user_group, userproxy)

    def logs2(self, workflow, howmany, jobids):
        self.logger.info("About to get log of workflow: %s." % workflow)
        return self.getFiles2(workflow, howmany, jobids, ['LOG'])

    def output2(self, workflow, howmany, jobids):
        self.logger.info("About to get output of workflow: %s." % workflow)
        return self.getFiles2(workflow, howmany, jobids, ['EDM', 'TFILE', 'FAKE'])

    def getFiles2(self, workflow, howmany, jobids, filetype):
        """
        Retrieves the output PFN aggregating output in final and temporary locations.

        :arg str workflow: the unique workflow name
        :arg int howmany: the limit on the number of PFN to return
        :return: a generator of list of outputs"""

        # Looking up task information from the DB
        row = next(self.api.query(None, None, self.Task.ID_sql, taskname = workflow))
        row = self.Task.ID_tuple(*row)

        file_type = 'log' if filetype == ['LOG'] else 'output'

        self.logger.debug("Retrieving the %s files of the following jobs: %s" % (file_type, jobids))

        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype = ','.join(filetype), taskname = workflow, howmany = howmany)

        # Convert integer jobId lists to string because the JobIds
        # coming from the database after automatic splitting changes are strings
        # (can look like '2-1').
        jobids = [str(intJobId) for intJobId in jobids]
        rows = filter(lambda row: ((row[GetFromTaskAndType.JOBID] in jobids) or (str(row[GetFromTaskAndType.PANDAID])) in jobids), rows)

        for row in rows:
            yield {'jobid': row[GetFromTaskAndType.JOBID] or str(row[GetFromTaskAndType.PANDAID]),
                   'lfn': row[GetFromTaskAndType.LFN],
                   'site': row[GetFromTaskAndType.LOCATION],
                   'tmplfn': row[GetFromTaskAndType.TMPLFN],
                   'tmpsite': row[GetFromTaskAndType.TMPLOCATION],
                   'directstageout': row[GetFromTaskAndType.DIRECTSTAGEOUT],
                   'size': row[GetFromTaskAndType.SIZE],
                   'checksum' : {'cksum' : row[GetFromTaskAndType.CKSUM], 'md5' : row[GetFromTaskAndType.ADLER32], 'adler32' : row[GetFromTaskAndType.ADLER32]}
                  }

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
                raise InvalidParameter("The job with id %s is not in a valid state to retrieve %s files" % (jobid, file_type))

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
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype = ','.join(filetype), taskname = workflow, howmany = howmany)

        # HOTFIX: Convert integer jobId lists to string because the JobIds
        # coming from the database after automatic splitting changes are strings
        # (can look like '2-1').
        jobids = [str(intJobId) for intJobId in jobids]
        transferingIds = [str(intTransferingId) for intTransferingId in transferingIds]
        finishedIds = [str(intFinishedId) for intFinishedId in finishedIds]

        rows = filter(lambda row: ((row[GetFromTaskAndType.JOBID] in jobids) or (str(row[GetFromTaskAndType.PANDAID])) in jobids), rows)
        #jobids=','.join(map(str,jobids)), limit=str(howmany) if howmany!=-1 else str(len(jobids)*100))
        for row in rows:
            try:
                jobid = row[GetFromTaskAndType.JOBID] or str(row[GetFromTaskAndType.PANDAID])
                if row[GetFromTaskAndType.DIRECTSTAGEOUT]:
                    lfn  = row[GetFromTaskAndType.LFN]
                    site = row[GetFromTaskAndType.LOCATION]
                    self.logger.debug("LFN: %s and site %s" % (lfn, site))
                    pfn  = self.phedex.getPFN(site, lfn)[(site, lfn)]
                else:
                    if jobid in transferingIds:
                        lfn  = row[GetFromTaskAndType.LFN]
                        site = row[GetFromTaskAndType.LOCATION]
                        self.logger.debug("LFN: %s and site %s" % (lfn, site))
                        pfn  = self.phedex.getPFN(site, lfn)[(site, lfn)]
                    elif jobid in finishedIds:
                        lfn  = row[GetFromTaskAndType.TMPLFN]
                        site = row[GetFromTaskAndType.TMPLOCATION]
                        self.logger.debug("LFN: %s and site %s" % (lfn, site))
                        pfn  = self.phedex.getPFN(site, lfn)[(site, lfn)]
                    else:
                        continue
            except Exception as err:
                self.logger.exception(err)
                raise ExecutionError("Exception while contacting PhEDEX.")

            yield {'jobid': jobid,
                   'pfn': pfn,
                   'lfn': lfn,
                   'size': row[GetFromTaskAndType.SIZE],
                   'checksum' : {'cksum' : row[GetFromTaskAndType.CKSUM], 'md5' : row[GetFromTaskAndType.ADLER32], 'adler32' : row[GetFromTaskAndType.ADLER32]}
                  }


    def report2(self, workflow, userdn):
        """
        Queries the TaskDB for the webdir, input/output datasets, publication flag.
        Also gets input/output file metadata from the FileMetaDataDB.

        Any other information that the client needs to compute the report is available
        without querying the server.
        """

        res = {}

        ## Get the jobs status first.
        self.logger.info("Fetching report2 information for workflow %s. Getting status first." % (workflow))

        ## Get the information we need from the Task DB.
        row = next(self.api.query(None, None, self.Task.ID_sql, taskname = workflow))
        row = self.Task.ID_tuple(*row)

        outputDatasets = literal_eval(row.output_dataset.read() if row.output_dataset else '[]')
        publication = True if row.publication == 'T' else False

        res['taskDBInfo'] = {"userWebDirURL": row.user_webdir, "inputDataset": row.input_dataset,
                             "outputDatasets": outputDatasets, "publication": publication}

        ## What each job has processed
        ## ---------------------------
        ## Retrieve the filemetadata of output and input files. (The filemetadata are
        ## uploaded by the post-job after stageout has finished for all output and log
        ## files in the job.)
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype='EDM,TFILE,FAKE,POOLIN', taskname=workflow, howmany=-1)

        # Return only the info relevant to the client.
        res['runsAndLumis'] = {}
        for row in rows:
            jobidstr = row[GetFromTaskAndType.JOBID] or str(row[GetFromTaskAndType.PANDAID])
            retRow = {'parents': row[GetFromTaskAndType.PARENTS].read(),
                      'runlumi': row[GetFromTaskAndType.RUNLUMI].read(),
                      'events': row[GetFromTaskAndType.INEVENTS],
                      'type': row[GetFromTaskAndType.TYPE],
                      'lfn': row[GetFromTaskAndType.LFN],
                      }
            if jobidstr not in res['runsAndLumis']:
                res['runsAndLumis'][jobidstr] = []
            res['runsAndLumis'][jobidstr].append(retRow)

        yield res

    def report(self, workflow, userdn):
        """
        Computes the report for the workflow.
        """

        ## This is what we want to return to the client:
        ## 1) The status of each job (useful for the client to return lumi information
        ##    at a per job status basis).
        ## 2) The lumis (and the lumis split across files) in the input dataset at data
        ##    discovery time.
        ## 3) The lumi-mask (lumi-mask AND run-range).
        ## 4) The lumis each job was requested to process.
        ## 5) The lumis each job has processed.
        ## 6) The lumis in the published output datasets.

        def _compactLumis(datasetInfo):
            """ Help function that allow to convert from runLumis divided per file (result of listDatasetFileDetails)
                to an aggregated result.
            """
            lumilist = {}
            for dummyFile, info in datasetInfo.iteritems():
                for run, lumis in info['Lumis'].iteritems():
                    lumilist.setdefault(str(run), []).extend(lumis)
            return lumilist

        res = {}

        ## Get the jobs status first.
        self.logger.info("About to compute report of workflow %s. Getting status first." % (workflow))
        statusRes = self.status(workflow, userdn)[0]
        statusPerJob = {}
        for status, jobid in statusRes['jobList']:
            statusPerJob[str(jobid)] = status
        res['statusPerJob'] = statusPerJob
        numJobs = len(statusPerJob)

        ## Get the information we need from the Task DB.
        row = next(self.api.query(None, None, self.Task.ID_sql, taskname = workflow))
        row = self.Task.ID_tuple(*row)
        userWebDirURL = row.user_webdir
        if not userWebDirURL:
            self.logger.warning("WebDir URL not defined for task %s" % (workflow))
        inputDataset = row.input_dataset
        dbsUrl = row.dbs_url
        outputDatasets = literal_eval(row.output_dataset.read() if row.output_dataset else '[]')
        publication = True if row.publication == 'T' else False
        splitArgs = literal_eval(row.split_args.read())
        res['publication'] = publication

        ## What the input dataset had in DBS when the task was submitted
        ## -------------------------------------------------------------
        ## Get the lumis (and the lumis split across files) in the input dataset. Files
        ## containing this information were created at data discovery time and then
        ## copied to the schedd.
        res['inputDataset'] = {'lumis': {}, 'duplicateLumis': {}}
        if inputDataset and userWebDirURL:
            curl = self.prepareCurl()
            fp = tempfile.TemporaryFile()
            curl.setopt(pycurl.WRITEFUNCTION, fp.write)
            hbuf = StringIO.StringIO()
            curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
            try:
                ## Retrieve the lumis in the input dataset.
                url = userWebDirURL + "/input_dataset_lumis.json"
                curl.setopt(pycurl.URL, url)
                self.myPerform(curl, url)
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp.seek(0)
                    res['inputDataset']['lumis'] = json.load(fp)
                else:
                    self.logger.error("Failed to retrieve input dataset lumis.")
                ## Clean temp file and buffer.
                fp.seek(0); fp.truncate(0); hbuf.truncate(0)
                ## Retrieve the lumis split across files in the input dataset.
                url = userWebDirURL + "/input_dataset_duplicate_lumis.json"
                curl.setopt(pycurl.URL, url)
                self.myPerform(curl, url)
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp.seek(0)
                    res['inputDataset']['duplicateLumis'] = json.load(fp)
                else:
                    self.logger.error("Failed to retrieve input dataset duplicate lumis.")
            finally:
                fp.close()
                hbuf.close()

        ## The filter of lumi-mask, run-range
        ## ----------------------------------
        ## Build a lumi-mask from the splitting arguments in the Task DB. This lumi-mask
        ## is equal to: user lumi-mask AND user run-range.
        res['lumiMask'] = buildLumiMask(splitArgs['runs'], splitArgs['lumis'])

        ## What each job was requested to process
        ## --------------------------------------
        ## Get the lumis to process by each job in the workflow.
        res['lumisToProcess'] = {}
        if userWebDirURL:
            curl = self.prepareCurl()
            fp = tempfile.NamedTemporaryFile()
            curl.setopt(pycurl.WRITEFUNCTION, fp.write)
            hbuf = StringIO.StringIO()
            curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
            try:
                url = userWebDirURL + "/run_and_lumis.tar.gz"
                curl.setopt(pycurl.URL, url)
                self.myPerform(curl, url)
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp.seek(0)
                    tarball = tarfile.open(fp.name)
                    try:
                        for jobid in xrange(1, numJobs+1):
                            filename = "job_lumis_%d.json" % (jobid)
                            try:
                                member = tarball.getmember(filename)
                            except KeyError:
                                self.logger.warning("File %s not found in run_and_lumis.tar.gz for task %s" % (filename, workflow))
                            else:
                                fd = tarball.extractfile(member)
                                try:
                                    res['lumisToProcess'][str(jobid)] = json.load(fd)
                                finally:
                                    fd.close()
                    finally:
                        tarball.close()
            finally:
                fp.close()
                hbuf.close()

        ## What each job has processed
        ## ---------------------------
        ## Retrieve the filemetadata of output and input files. (The filemetadata are
        ## uploaded by the post-job after stageout has finished for all output and log
        ## files in the job.)
        rows = self.api.query(None, None, self.FileMetaData.GetFromTaskAndType_sql, filetype='EDM,TFILE,FAKE,POOLIN', taskname=workflow, howmany=-1)
        ## Extract from the filemetadata the necessary information.
        res['runsAndLumis'] = {}
        for row in rows:
            jobidstr = row[GetFromTaskAndType.JOBID] or str(row[GetFromTaskAndType.PANDAID])
            if statusPerJob.get(jobidstr) in ['finished']:
                if jobidstr not in res['runsAndLumis']:
                    res['runsAndLumis'][jobidstr] = []
                res['runsAndLumis'][jobidstr].append({'parents': row[GetFromTaskAndType.PARENTS].read(),
                                                      'runlumi': row[GetFromTaskAndType.RUNLUMI].read(),
                                                      'events': row[GetFromTaskAndType.INEVENTS],
                                                      'type': row[GetFromTaskAndType.TYPE],
                                                      'lfn': row[GetFromTaskAndType.LFN],
                                                     })
        self.logger.info("Got %s EDM,TFILE,FAKE,POOLIN filemetadata for workflow %s" % (len(res['runsAndLumis']), workflow))

        ## What has been published
        ## -----------------------
        ## Get the lumis and number of events in the published output datasets.
        res['outputDatasets'] = {}
        if publication:
            for outputDataset in outputDatasets:
                res['outputDatasets'][outputDataset] = {'lumis': {}, 'numEvents': 0}
                try:
                    dbs = DBSReader("https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader") #We can only publish here with DBS3
                    outputDatasetDetails = dbs.listDatasetFileDetails(outputDataset)
                except Exception as ex:
                    msg  = "Failed to retrieve information from DBS for output dataset %s." % (outputDataset)
                    msg += " Exception while contacting DBS: %s" % (str(ex))
                    self.logger.exception(msg)
                else:
                    outputDatasetLumis = _compactLumis(outputDatasetDetails)
                    outputDatasetLumis = LumiList(runsAndLumis=outputDatasetLumis).getCompactList()
                    res['outputDatasets'][outputDataset]['lumis'] = outputDatasetLumis
                    for outputFileDetails in outputDatasetDetails.values():
                        res['outputDatasets'][outputDataset]['numEvents'] += outputFileDetails['NumberOfEvents']

        yield res

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

    @classmethod
    def prepareCurl(cls):
        curl = pycurl.Curl()
        curl.setopt(pycurl.NOSIGNAL, 0)
        curl.setopt(pycurl.TIMEOUT, 30)
        curl.setopt(pycurl.CONNECTTIMEOUT, 30)
        curl.setopt(pycurl.FOLLOWLOCATION, 0)
        curl.setopt(pycurl.MAXREDIRS, 0)
        #curl.setopt(pycurl.ENCODING, 'gzip, deflate')
        return curl

    @classmethod
    def cleanTempFileAndBuff(cls, fp, hbuf):
        """
        Go to the beginning of temp file
        Truncate buffer and file and return
        """
        fp.seek(0)
        fp.truncate(0)
        hbuf.truncate(0)
        return fp, hbuf

    @classmethod
    def myPerform(cls, curl, url):
        try:
            curl.perform()
        except pycurl.error as e:
            raise ExecutionError(("Failed to contact Grid scheduler when getting URL %s. "
                                  "This might be a temporary error, please retry later and "
                                  "contact %s if the error persist. Error from curl: %s"
                                  % (url, FEEDBACKMAIL, str(e))))

    def taskWebStatus(self, task_ad, verbose, statusResult):
        nodes = {}
        url = task_ad['CRAB_UserWebDir']
        curl = self.prepareCurl()
        fp = tempfile.TemporaryFile()
        curl.setopt(pycurl.WRITEFUNCTION, fp.write)
        hbuf = StringIO.StringIO()
        curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
        try:
            self.logger.debug("Retrieving task status from web with verbosity %d." % verbose)
            if verbose == 1:
                jobs_url = url + "/jobs_log.txt"
                curl.setopt(pycurl.URL, jobs_url)
                self.logger.info("Starting download of job log")
                self.myPerform(curl, jobs_url)
                self.logger.info("Finished download of job log")
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp.seek(0)
                    self.logger.debug("Starting parse of job log")
                    self.parseJobLog(fp, nodes)
                    self.logger.debug("Finished parse of job log")
                else:
                    raise ExecutionError("Cannot get jobs log file. Retry in a minute if you just submitted the task")
            elif verbose == 2:
                site_url = url + "/site_ad.txt"
                curl.setopt(pycurl.URL, site_url)
                self.logger.debug("Starting download of site ad")
                self.myPerform(curl, site_url)
                self.logger.debug("Finished download of site ad")
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp.seek(0)
                    self.logger.debug("Starting parse of site ad")
                    self.parseSiteAd(fp, task_ad, nodes)
                    self.logger.debug("Finished parse of site ad")
                else:
                    raise ExecutionError("Cannot get site ad. Retry in a minute if you just submitted the task")

            nodes_url = url + "/node_state.txt"
            curl.setopt(pycurl.URL, nodes_url)
            # Before executing any new curl, truncate and clean temp file
            fp, hbuf = self.cleanTempFileAndBuff(fp, hbuf)
            self.logger.debug("Starting download of node state")
            self.myPerform(curl, nodes_url)
            self.logger.debug("Finished download of node state")
            header = ResponseHeader(hbuf.getvalue())
            if header.status == 200:
                fp.seek(0)
                self.logger.debug("Starting parse of node state")
                self.parseNodeState(fp, nodes)
                self.logger.debug("Finished parse of node state")
            else:
                raise MissingNodeStatus("Cannot get node state log. Retry in a minute if you just submitted the task")

            site_url = url + "/error_summary.json"
            # Before executing any new curl, truncate and clean temp file
            fp, hbuf = self.cleanTempFileAndBuff(fp, hbuf)
            curl.setopt(pycurl.URL, site_url)
            self.logger.debug("Starting download of error summary file")
            self.myPerform(curl, site_url)
            self.logger.debug("Finished download of error summary file")
            header = ResponseHeader(hbuf.getvalue())
            if header.status == 200:
                fp.seek(0)
                self.logger.debug("Starting parse of summary file")
                self.parseErrorReport(fp, nodes)
                self.logger.debug("Finished parse of summary file")
            else:
                self.logger.debug("No error summary available")

            # Before executing any new curl, truncate and clean temp file
            fp, hbuf = self.cleanTempFileAndBuff(fp, hbuf)
            aso_url = url + "/aso_status.json"
            curl.setopt(pycurl.URL, aso_url)
            self.logger.debug("Starting download of aso state")
            curl.perform()
            self.logger.debug("Finished download of aso state")
            header = ResponseHeader(hbuf.getvalue())
            if header.status == 200:
                fp.seek(0)
                self.logger.debug("Starting parsing of aso state")
                self.parseASOState(fp, nodes, statusResult)
                self.logger.debug("Finished parsing of aso state")
            else:
                self.logger.debug("No aso state file available")
            return nodes
        finally:
            fp.close()
            hbuf.close()

    @conn_handler(services=['servercert'])
    def publicationStatus(self, workflow, asourl, asodb, user):
        """Here is what basically the function return, a dict called publicationInfo in the subcalls:
                publicationInfo['status']: something like {'publishing': 0, 'publication_failed': 0, 'not_published': 0, 'published': 5}.
                                           Later on goes into dictresult['publication'] before being returned to the client
                publicationInfo['status']['error']: String containing the error message if not able to contact couch or oracle
                                                    Later on goes into dictresult['publication']['error']
                publicationInfo['failure_reasons']: errors of single files (not yet implemented for oracle..)
        """
        if isCouchDBURL(asourl):
            return self.publicationStatusCouch(workflow, asourl, asodb)
        else:
            return self.publicationStatusOracle(workflow, user)

    def publicationStatusCouch(self, workflow, asourl, asodb):
        publicationInfo = {'status': {}, 'failure_reasons': {}}
        if not asourl:
            raise ExecutionError("This CRAB server is not configured to publish; no publication status is available.")
        server = CMSCouch.CouchServer(dburl=asourl, ckey=self.serverKey, cert=self.serverCert)
        try:
            db = server.connectDatabase(asodb)
        except Exception:
            msg = "Error while connecting to asynctransfer CouchDB for workflow %s " % workflow
            msg += "\n asourl=%s asodb=%s" % (asourl, asodb)
            self.logger.exception(msg)
            publicationInfo['status'] = {'error': msg}
            return publicationInfo
        # Get the publication status for the given workflow. The next query to the
        # CouchDB view returns a list of 1 dictionary (row) with:
        # 'key'   : workflow,
        # 'value' : a dictionary with possible publication statuses as keys and the
        #           counts as values.
        query = {'reduce': True, 'key': workflow, 'stale': 'update_after'}
        try:
            publicationList = db.loadView('AsyncTransfer', 'PublicationStateByWorkflow', query)['rows']
        except Exception:
            msg = "Error while querying CouchDB for publication status information for workflow %s " % workflow
            self.logger.exception(msg)
            publicationInfo['status'] = {'error': msg}
            return publicationInfo
        if publicationList:
            publicationStatusDict = publicationList[0]['value']
            publicationInfo['status'] = publicationStatusDict
            # Get the publication failure reasons for the given workflow. The next query to
            # the CouchDB view returns a list of N_different_publication_failures
            # dictionaries (rows) with:
            # 'key'   : [workflow, publication failure],
            # 'value' : count.
            numFailedPublications = publicationStatusDict['publication_failed']
            if numFailedPublications:
                query = {'group': True, 'startkey': [workflow], 'endkey': [workflow, {}], 'stale': 'update_after'}
                try:
                    publicationFailedList = db.loadView('DBSPublisher', 'PublicationFailedByWorkflow', query)['rows']
                except Exception:
                    msg = "Error while querying CouchDB for publication failures information for workflow %s " % workflow
                    self.logger.exception(msg)
                    publicationInfo['failure_reasons']['error'] = msg
                    return publicationInfo
                publicationInfo['failure_reasons']['result'] = []
                for publicationFailed in publicationFailedList:
                    failureReason = publicationFailed['key'][1]
                    numFailedFiles = publicationFailed['value']
                    publicationInfo['failure_reasons']['result'].append((failureReason, numFailedFiles))

        return publicationInfo

    def publicationStatusOracle(self, workflow, user):
        publicationInfo = {}

        #query oracle for the information
        binds = {}
        binds['username'] = user
        binds['taskname'] = workflow
        res = list(self.api.query(None, None, self.transferDB.GetTaskStatusForPublication_sql, **binds))

        #group results by state
        statusDict = {}
        for row in res:
            status = row[2]
            statusStr = PUBLICATIONDB_STATES[status].lower()
            if status != 5:
                statusDict[statusStr] = statusDict.setdefault(statusStr, 0) + 1

        #format and return
        publicationInfo['status'] = statusDict

        #Generic errors (like oracle errors) goes here. N.B.: single files errors should not go here
#        msg = "Publication information for oracle not yet implemented"
#        publicationInfo['status']['error'] = msg

        return publicationInfo


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


    def parseASOState(self, fp, nodes, statusResult):
        """ Parse aso_status and for each job change the job status from 'transferring'
            to 'transferred' in case all files in the job have already been successfully
            transferred.

            fp: file pointer to the ASO status file downloaded from the shedd
            nodes: contains the data-structure representing the node state file created by dagman
            statusResult: the dictionary it is going to be returned by the status to the client.
                          we need this to add a warning in case there are jobs missing in the node_state file
        """
        transfers = {}
        data = json.load(fp)
        for docid, result in data['results'].iteritems():
            #Oracle has an improved structure in aso_status
            if isCouchDBURL(self.asoDBURL):
                result = result['value']
            else:
                result = result[0]
            jobid = str(result['jobid'])
            if jobid not in nodes:
                msg = ("It seems one or more jobs are missing from the node_state file."
                       " It might be corrupted as a result of a disk failure on the schedd (maybe it is full?)"
                       " This might be interesting for analysis operation (%s) %" + FEEDBACKMAIL)
                statusResult['taskWarningMsg'] = [msg] + statusResult['taskWarningMsg']
            if jobid in nodes and nodes[jobid]['State'] == 'transferring':
                transfers.setdefault(jobid, {})[docid] = result['state']
            return
        for jobid in transfers:
            ## The aso_status file is created/updated by the post-jobs when monitoring the
            ## transfers, i.e. after all transfer documents for the given job have been
            ## successfully inserted into the ASO database. Thus, if aso_status contains N
            ## documents for a given job_id it means there are exactly N files to transfer
            ## for that job.
            if set(transfers[jobid].values()) == set(['done']):
                nodes[jobid]['State'] = 'transferred'


    @classmethod
    def parseErrorReport(cls, fp, nodes):
        def last(joberrors):
            return joberrors[max(joberrors, key=int)]
        fp.seek(0)
        data = json.load(fp)
        #iterate over the jobs and set the error dict for those which are failed
        for jobid, statedict in nodes.iteritems():
            if 'State' in statedict and statedict['State'] == 'failed' and jobid in data:
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


    @classmethod
    def parseNodeStateV2(cls, fp, nodes):
        """
        HTCondor 8.1.6 updated the node state file to be classad-based.
        This is a more flexible format that allows future extensions but, unfortunately,
        also requires a separate parser.
        """
        taskStatus = nodes.setdefault("DagStatus", {})
        for ad in classad.parseAds(fp):
            if ad['Type'] == "DagStatus":
                taskStatus['Timestamp'] = ad.get('Timestamp', -1)
                taskStatus['NodesTotal'] = ad.get('NodesTotal', -1)
                taskStatus['DagStatus'] = ad.get('DagStatus', -1)
                continue
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

