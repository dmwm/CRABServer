#!/usr/bin/python

import os
import re
import sys
import time
import json
import errno
import shutil
import signal
import urllib
import logging
import commands
import unittest
import classad
import datetime
import traceback
import uuid
import tempfile
import WMCore.Services.PhEDEx.PhEDEx as PhEDEx
import WMCore.Database.CMSCouch as CMSCouch
from RESTInteractions import HTTPRequests
from httplib import HTTPException
import hashlib
import TaskWorker.Actions.RetryJob as RetryJob
import pprint
import time, datetime

import DashboardAPI

logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

fts_server = 'https://fts3-pilot.cern.ch:8443'

g_Job = None
config = None

def sighandler(*args):
    if g_Job:
        g_Job.cancel()

signal.signal(signal.SIGHUP,  sighandler)
signal.signal(signal.SIGINT,  sighandler)
signal.signal(signal.SIGTERM, sighandler)

REGEX_ID = re.compile("([a-f0-9]{8,8})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{12,12})")

class FTSJob(object):

    def __init__(self, dest_site, source_dir, dest_dir, source_sites, count, filenames, reqname, output, log_size, output_metadata, task_ad):
        self._id = None
        self._cancel = False
        self._sleep = 20

        transfer_list = resolvePFNs(dest_site, source_dir, dest_dir, source_sites, filenames)

        self._transfer_list = transfer_list
        self._count = count
        with open("copyjobfile_%s" % count, "w") as fd:
            for source, dest in transfer_list:
                fd.write("%s %s\n" % (source, dest))

    def cancel(self):
        if self._id:
            cmd = "glite-transfer-cancel -s %s %s" % (fts_server, self._id)
            print "+", cmd
            os.system(cmd)

    def submit(self):
        cmd = "glite-transfer-submit -o -s %s -f copyjobfile_%s" % (fts_server, self._count)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-submit exited with status %s.\n%s" % (status, output))
        output = output.strip()
        print "Resulting transfer ID: %s" % output
        return output

    def status(self, long_status=False):
        if long_status:
            cmd = "glite-transfer-status -l -s %s %s" % (fts_server, self._id)
        else:
            cmd = "glite-transfer-status -s %s %s" % (fts_server, self._id)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-status exited with status %s.\n%s" % (status, output))
        return output.strip()

    def run(self):
        self._id = self.submit()
        if not REGEX_ID.match(self._id):
            raise Exception("Invalid ID returned from FTS transfer submit")
        while True:
            time.sleep(self._sleep)
            status = self.status()
            print status

            if status in ['Submitted', 'Pending', 'Ready', 'Active', 'Canceling', 'Hold']:
                continue

            #if status in ['Done', 'Finished', 'FinishedDirty', 'Failed', 'Canceled']:
            #TODO: Do parsing of "-l"
            if status in ['Done', 'Finished', 'Finishing']:
                return 0

            if status in ['FinishedDirty', 'Failed', 'Canceled']:
                print self.status(True)
                return 1

class ASOServerJob(object):
    def __init__(self, dest_site, source_dir, dest_dir, source_sites, count, filenames, reqname, outputdata, log_size, output_metadata, task_ad):
        self.id = None
        self.couchServer = None
        self.couchDatabase = None
        self.cancel = False
        self.sleep = 2
        self.count = count
        self.dest_site = dest_site
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.source_sites = source_sites
        self.filenames = filenames
        self.reqname = reqname
        self.output_metadata = output_metadata
        self.log_size = log_size
        self.publish = outputdata
        self.task_ad = task_ad
        proxy = os.environ.get('X509_USER_PROXY', None)
        aso_auth_file = os.path.expanduser("~/auth_aso_plugin.config")
        if config:
            aso_auth_file = getattr(config, "authfile", "auth_aso_plugin.config")
        try:
            f = open(aso_auth_file)
            authParams = json.loads(f.read())
            self.aso_db_url = authParams['ASO_DB_URL']
            print "Got aso %s" % self.aso_db_url
            self.couchServer = CMSCouch.CouchServer(dburl=self.aso_db_url, ckey=proxy, cert=proxy)
            self.couchDatabase = self.couchServer.connectDatabase("asynctransfer", create = False)
        except:
            print traceback.format_exc()
            raise

    def cancel(self):
        print "cancelling"
        if self.id:
            for oneID in self.id:
                doc = self.couchDatabase.document(oneID)
                doc['state'] = 'killed'
                res = self.couchDatabase.commitOne(doc)
                if error in res:
                    raise RuntimeError, "Got error killing transfer: %s" % res

    def submit(self):
        allIDs = []
        outputFiles = []
        input_dataset = str(self.task_ad['CRAB_InputData'])
        if 'CRAB_UserRole' in self.task_ad:
            role = str(self.task_ad['CRAB_UserRole'])
        else:
            role = ''
        if 'CRAB_UserGroup' in self.task_ad:
            group = str(self.task_ad['CRAB_UserGroup'])
        else:
            group = ''
        dbs_url = str(self.task_ad['CRAB_DBSUrl'])
        publish_dbs_url = str(self.task_ad['CRAB_PublishDBSUrl'])
        if publish_dbs_url.lower() == 'undefined':
            publish_dbs_url = "https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet"
        # TODO: Add a method to resolve a single PFN or use resolvePFNs
        last_update = int(time.time())
        now = str(datetime.datetime.now())
        found_log = False
        file_index = 0
        for outputModule in self.output_metadata.values():
            for outputFile in outputModule:
                print outputFile
                fileInfo = {}
                fileInfo['checksums'] = outputFile.get(u"checksums", {"cksum": "0", "adler32": "0"})
                fileInfo['outsize'] = outputFile.get(u"size", 0)
                outputFiles.append(fileInfo)
        for oneFile in zip(self.source_sites, self.filenames):
            if found_log:
                lfn = "%s/%s" % (self.source_dir, oneFile[1])
                file_type = "output"
                size = outputFiles[file_index]['outsize']
                checksums = outputFiles[file_index]['checksums']
                file_index += 1
            else:
                lfn = "%s/log/%s" % (self.source_dir, oneFile[1])
                file_type = "log"
                size = self.log_size
                checksums = {'adler32': 'abc'}
                found_log = True
            if len(lfn.split('/')) > 2:
                if lfn.split('/')[2] == 'temp':
                    user = lfn.split('/')[4].rsplit(".", 1)[0]
                else:
                    user = lfn.split('/')[3]
            else:
                user = ''
            # TODO: need to pass the user params: publish flag, role/group, inputdataset,  publish_dbs_url, dbs_url through
            doc_id = getHashLfn( lfn )
            try:
                doc = self.couchDatabase.document( doc_id )
                doc['state'] = 'new'
                doc['source'] = oneFile[0]
                doc['destination'] = self.dest_site
                doc['checksums'] = checksums,
                doc['size'] = size,
                doc['last_update'] = last_update
                doc['start_time'] = now
                doc['end_time'] = ''
                doc['job_end_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                doc['retry_count'] = []
                doc['failure_reason'] = []
                print "Updating doc %s for %s" % (doc_id, lfn)
            except CMSCouch.CouchNotFoundError:
                msg = "Document %s does not exist in Couch" % doc_id
                msg += str(traceback.format_exc())
                print msg
                print "Uploading new doc for %s" % lfn
                # FIXME: need to pass publish flag, checksums, role/group, size, inputdataset,  publish_dbs_url, dbs_url through
                doc = { "_id": doc_id,
                        "inputdataset": input_dataset,
                        "group": group,
                        # TODO: Remove this if it is not required
                        "lfn": lfn.replace('/store/user', '/store/temp/user', 1),
                        "checksums": checksums,
                        "size": size,
                        "user": user,
                        "source": oneFile[0],
                        "destination": self.dest_site,
                        "last_update": last_update,
                        "state": "new",
                        "role": role,
                        "dbSource_url": "gWMS",
                        "publish_dbs_url": publish_dbs_url,
                        "dbs_url": dbs_url,
                        "workflow": self.reqname,
                        "start_time": now,
                        "end_time": '',
                        "job_end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                        "jobid": self.count,
                        "retry_count": [],
                        "failure_reason": [],
                        "publication_state": 'not_published',
                        "publication_retry_count": [],
                        "type" : file_type,
                        "publish" : 1
                    }
            except Exception, ex:
                msg = "Error loading document from couch. Transfer submission failed."
                msg += str(ex)
                msg += str(traceback.format_exc())
                print msg
                return False
            pprint.pprint(doc)
            allIDs.append(getHashLfn(lfn))
            if 'error' in self.couchDatabase.commitOne(doc)[0]:
                print "Couldn't add to couch database"
                return False
        return allIDs

    def status(self, long_status=False):
        statuses = []
        for oneDoc in self.id:
            couchDoc = self.couchDatabase.document(oneDoc)
            statuses.append(couchDoc['state'])
        return statuses

    def run(self):
        self.id = self.submit()
        if self.id == False:
            raise RuntimeError, "Couldn't send to couchdb"
        while True:
            time.sleep(self.sleep)
            status = self.status()
            print "Got statuses: %s" % status
            allDone = True
            for oneStatus, jobID in zip(status, self.id):
                # states to wait on
                if oneStatus in ['new', 'acquired']:
                    allDone = False
                    continue
                # good states
                elif oneStatus in ['done']:
                    continue
                # states to stop immediately
                elif oneStatus in ['failed', 'killed']:
                    print "Job failed with status %s" % oneStatus
                    couchDoc = self.couchDatabase.document(jobID)
                    if "_attachments" in couchDoc:
                        print "FTS outputs are:"
                        for log in couchDoc["_attachments"]:
                            print self.couchDatabase.getAttachment(jobID, log)
                    return 1
                else:
                    raise RuntimeError, "Got a unknown status: %s" % oneStatus
            if allDone:
                print "All jobs succeeded"
                return 0


def determineSizes(transfer_list):
    sizes = []
    for pfn in transfer_list:
        cmd = "lcg-ls --srm-timeout 300 --connect-timeout 300 --sendreceive-timeout 60 -D srmv2 -b -l %s" % pfn
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            sizes.append(-1)
            continue
        info = output.split("\n")[0].split()
        if len(info) < 5:
            print "Invalid lcg-ls output:\n%s" % output
            sizes.append(-1)
            continue
        try:
            sizes.append(info[4])
        except ValueError:
            print "Invalid lcg-ls output:\n%s" % output
            sizes.append(-1)
            continue
    return sizes


def reportResults(job_id, dest_list, sizes):
    filtered_dest = [dest_list[i] for i in range(len(dest_list)) if sizes[i] >= 0]
    filtered_sizes = [i for i in sizes if i >= 0]
    retval = 0

    cmd = 'condor_qedit %s OutputSizes "\\"%s\\""' % (job_id, ",".join(filtered_sizes))
    print "+", cmd
    status, output = commands.getstatusoutput(cmd)
    if status:
        retval = status
        print output

    cmd = 'condor_qedit %s OutputPFNs "\\"%s\\""' % (job_id, ",".join(filtered_dest))
    print "+", cmd
    status, output = commands.getstatusoutput(cmd)
    if status:
        retval = status
        print output

    return retval

def resolvePFNs(dest_site, source_dir, dest_dir, source_sites, filenames):

    p = PhEDEx.PhEDEx()
    found_log = False
    lfns = []
    for filename in filenames:
        if not found_log and filename.startswith("cmsRun") and (filename[-7:] == ".tar.gz"):
            slfn = os.path.join(source_dir, "log", filename)
            dlfn = os.path.join(dest_dir, "log", filename)
        else:
            slfn = os.path.join(source_dir, filename)
            dlfn = os.path.join(dest_dir, filename)
        found_log = True
        lfns.append(slfn)
        lfns.append(dlfn)
    dest_sites_ = [dest_site]
    if dest_site.startswith("T1_"):
        dest_sites_.append(dest_site + "_Buffer")
        dest_sites_.append(dest_site + "_Disk")
    dest_info = p.getPFN(nodes=(source_sites + dest_sites_), lfns=lfns)
    results = []
    found_log = False
    for source_site, filename in zip(source_sites, filenames):
        if not found_log and filename.startswith("cmsRun") and (filename[-7:] == ".tar.gz"):
            slfn = os.path.join(source_dir, "log", filename)
            dlfn = os.path.join(dest_dir, "log", filename)
        else:
            slfn = os.path.join(source_dir, filename)
            dlfn = os.path.join(dest_dir, filename)
        found_log = True
        dest_site_= dest_site
        if (dest_site + "_Disk", dlfn) in dest_info:
            dest_site_ = dest_site + "_Disk"
        elif (dest_site + "_Buffer", dlfn) in dest_info:
            dest_site_ = dest_site + "_Buffer"
        if ((source_site, slfn) not in dest_info) or (not dest_info[source_site, slfn]):
            print "Unable to map LFN %s at site %s" % (slfn, source_site)
        if ((dest_site_, dlfn) not in dest_info) or (not dest_info[dest_site_, dlfn]):
            print "Unable to map LFN %s at site %s" % (dlfn, dest_site_)
        results.append((dest_info[source_site, slfn], dest_info[dest_site_, dlfn]))
    return results

def getHashLfn(lfn):
    """
    stolen from asyncstageout
    """
    return hashlib.sha224(lfn).hexdigest()

REQUIRED_ATTRS = ['CRAB_ReqName', 'CRAB_Id', 'CRAB_OutputData', 'CRAB_JobSW', 'CRAB_AsyncDest']

class PostJob():


    def __init__(self):
        self.ad = None
        self.task_ad = {}
        self.crab_id = -1
        self.report = None
        self.output = None
        self.input = None
        self.outputFiles = []
        self.retry_count = "0"
        self.logfiles = None


    def getTaskAd(self):
        try:
            self.task_ad = classad.parseOld(open(os.environ["_CONDOR_JOB_AD"]))
        except Exception:
            print traceback.format_exc()


    def makeAd(self, reqname, id, outputdata, sw, async_dest):
        self.ad = classad.ClassAd()
        self.ad['CRAB_ReqName'] = reqname
        self.ad['CRAB_Id'] = id
        self.ad['CRAB_OutputData'] = outputdata
        self.ad['CRAB_JobSW'] = sw
        self.ad['CRAB_AsyncDest'] = async_dest
        self.crab_id = int(self.ad['CRAB_Id'])


    def parseJson(self):
        with open("jobReport.json.%d" % self.crab_id) as fd:
            self.full_report = json.load(fd)

        if 'steps' not in self.full_report or 'cmsRun' not in self.full_report['steps']:
            raise ValueError("Invalid jobReport.json: missing cmsRun")
        self.report = self.full_report['steps']['cmsRun']

        if 'input' not in self.report or 'output' not in self.report:
            raise ValueError("Invalid jobReport.json: missing input/output")
        self.output = self.report['output']
        self.input = self.report['input']

        for outputModule in self.output.values():
            for outputFile in outputModule:
                print outputFile
                # Note incorrect spelling of 'output module' in current WMCore
                fileInfo = {}
                if outputFile.get(u"output_module_class") == u'PoolOutputModule' or \
                        outputFile.get(u"ouput_module_class") == u'PoolOutputModule':
                    fileInfo['filetype'] = "EDM"
                elif outputFile.get(u"Source"):
                    fileInfo['filetype'] = "TFILE"
                else:
                    continue
                self.outputFiles.append(fileInfo)

                fileInfo['inparentlfns'] = [str(i) for i in outputFile.get(u"input", [])]

                fileInfo['events'] = outputFile.get(u"events", -1)
                fileInfo['checksums'] = outputFile.get(u"checksums", {"cksum": "0", "adler32": "0"})
                fileInfo['outsize'] = outputFile.get(u"size", 0)

                if u'SEName' in outputFile and self.node_map.get(str(outputFile['SEName'])):
                    fileInfo['outtmplocation'] = self.node_map[outputFile['SEName']]

                if u'runs' not in outputFile:
                    continue
                fileInfo['outfileruns'] = []
                fileInfo['outfilelumis'] = []
                for run, lumis in outputFile[u'runs'].items():
                    for lumi in lumis:
                        fileInfo['outfileruns'].append(str(run))
                        fileInfo['outfilelumis'].append(str(lumi))


    def fixPerms(self):
        """
        HTCondor will default to a umask of 0077 for stdout/err.  When the DAG finishes,
        the schedd will chown the sandbox to the user which runs HTCondor.

        This prevents the user who owns the DAGs from retrieving the job output as they
        are no longer world-readable.
        """
        for base_file in ["job_err", "job_out"]:
            try:
                os.chmod("%s.%d" % (base_file, self.crab_id), 0644)
            except OSError, oe:
                if oe.errno != errno.ENOENT and oe.errno != errno.EPERM:
                    raise

    def upload(self):
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        # CMS convention for outdataset: /primarydataset>/<yourHyperNewsusername>-<publish_data_name>-<PSETHASH>/USER
        outdataset = os.path.join('/' + str(self.task_ad['CRAB_InputData']).split('/')[1], self.task_ad['CRAB_UserHN'] + '-' + self.task_ad['CRAB_PublishName'], 'USER')
        for fileInfo in self.outputFiles:
            configreq = {"taskname":        self.ad['CRAB_ReqName'],
                         "globalTag":       "None",
                         "pandajobid":      self.crab_id,
                         "outsize":         fileInfo['outsize'],
                         "publishdataname": self.ad['CRAB_OutputData'],
                         "appver":          self.ad['CRAB_JobSW'],
                         "outtype":         fileInfo['filetype'],
                         "checksummd5":     "asda", # Not implemented; garbage value taken from ASO
                         "checksumcksum":   fileInfo['checksums']['cksum'],
                         "checksumadler32": fileInfo['checksums']['adler32'],
                         "outlocation":     fileInfo['outlocation'],
                         "outtmplocation":  fileInfo.get('outtmplocation', self.source_site),
                         "acquisitionera":  "null", # Not implemented
                         "outlfn":          fileInfo['outlfn'],
                         "events":          fileInfo['events'],
                         "outdatasetname":  outdataset,
                    }
            configreq = configreq.items()
            if 'outfileruns' in fileInfo:
                for run in fileInfo['outfileruns']:
                    configreq.append(("outfileruns", run))
            if 'outfilelumis' in fileInfo:
                for lumi in fileInfo['outfilelumis']:
                    configreq.append(("outfilelumis", lumi))
            if 'inparentlfns' in fileInfo:
                for lfn in fileInfo['inparentlfns']:
                    configreq.append(("inparentlfns", lfn))
            logger.debug("Uploading output file to %s: %s" % (self.resturl, configreq))
            try:
                self.server.put(self.resturl, data = urllib.urlencode(configreq))
            except HTTPException, hte:
                print hte.headers
                raise

    def uploadLog(self, dest_dir, filename):
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        outlfn = os.path.join(dest_dir, "log", filename)
        source_site = self.source_site
        if 'SEName' in self.full_report:
            source_site = self.node_map.get(self.full_report['SEName'], source_site)
        self.log_size = self.full_report.get(u'log_size', 0)
        configreq = {"taskname":        self.ad['CRAB_ReqName'],
                     "pandajobid":      self.crab_id,
                     "outsize":         self.log_size, # Not implemented
                     "publishdataname": self.ad['CRAB_OutputData'],
                     "appver":          self.ad['CRAB_JobSW'],
                     "outtype":         "LOG",
                     "checksummd5":     "asda", # Not implemented
                     "checksumcksum":   "3701783610", # Not implemented
                     "checksumadler32": "6d1096fe", # Not implemented
                     "outlocation":     self.ad['CRAB_AsyncDest'],
                     "outtmplocation":  source_site,
                     "acquisitionera":  "null", # Not implemented
                     "events":          "0",
                     "outlfn":          outlfn,
                     "outdatasetname":  "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER"
                    }
        logger.debug("Uploading log file record to %s: %s" % (self.resturl, configreq))
        try:
            self.server.put(self.resturl, data = urllib.urlencode(configreq))
        except HTTPException, hte:
            print hte.headers
            if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
                   not hte.headers.get('X-Error-Http', -1) == '400':
                raise

    def setDashboardState(self, state, reason=None):
        if state == "COOLOFF":
            state = "Cooloff"
        elif state == "TRANSFERRING":
            state = "Transferring"
        elif state == "FAILED":
            state = "Aborted"
        elif state == "FINISHED":
            state = "Done"
        logger.info("Setting Dashboard state to %s." % state)
        params = {
            'MonitorID': self.ad['CRAB_ReqName'],
            'MonitorJobID': '%d_https://glidein.cern.ch/%d/%s_%s' % (self.crab_id, self.crab_id, self.ad['CRAB_ReqName'].replace("_", ":"), self.retry_count),
            'StatusValue': state,
        }
        if reason:
            params['StatusValueReason'] = reason
        if self.logfiles:
            params['StatusLogFile'] = ",".join(self.logfiles)
        logger.info("Dashboard parameters: %s" % str(params))
        DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)

    def uploadFakeLog(self, state="TRANSFERRING"):
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        self.setDashboardState(state)

        # Upload a fake log status.  This is to be replaced by a proper job state table.
        configreq = {"taskname":        self.ad['CRAB_ReqName'],
                     "pandajobid":      self.crab_id,
                     "outsize":         "0",
                     "publishdataname": self.ad['CRAB_OutputData'],
                     "appver":          self.ad['CRAB_JobSW'],
                     "outtype":         "FAKE", # Not implemented
                     "checksummd5":     "asda", # Not implemented
                     "checksumcksum":   "3701783610", # Not implemented
                     "checksumadler32": "6d1096fe", # Not implemented
                     "outlocation":     "T2_US_Nowhere",
                     "outtmplocation":  "T2_US_Nowhere",
                     "acquisitionera":  "null", # Not implemented
                     "events":          "0",
                     "outlfn":          "/store/user/fakefile/FakeDataset/FakePublish/5b6a581e4ddd41b130711a045d5fecb9/1/log/fake.log.%s" % str(self.crab_id),
                     "outdatasetname":  "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER",
                     "filestate":       state,
                    }
        print self.resturl, urllib.urlencode(configreq)
        try:
            self.server.put(self.resturl, data = urllib.urlencode(configreq))
        except HTTPException, hte:
            if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
                   not hte.headers.get('X-Error-Http', -1) == '400':
                raise
            self.uploadState(state)


    def uploadState(self, state):
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return

        self.setDashboardState(state)
        # Record this job as a permanent failure
        configreq = {"taskname":  self.ad['CRAB_ReqName'],
                     "filestate": state,
                     "outlfn":    "/store/user/fakefile/FakeDataset/FakePublish/5b6a581e4ddd41b130711a045d5fecb9/1/log/fake.log.%s" % str(self.crab_id),
                    }
        self.server.post(self.resturl, data = urllib.urlencode(configreq))
        return 2

    def getSourceSite(self):
        if self.full_report.get('executed_site', None):
            print "Getting source_site from jobReport"
            return self.full_report['executed_site']

        raise ValueError("Unable to determine source side")

    def getFileSourceSite(self, filename):
        filename = os.path.split(filename)[-1]
        for outfile in self.outputFiles:
            if (u'pfn' not in outfile) or (u'SEName' not in outfile):
                continue
            json_pfn = outfile[u'pfn']
            pfn = os.path.split(filename)[-1]
            left_piece, fileid = pfn.rsplit("_", 1)
            right_piece = fileid.split(".", 1)[-1]
            pfn = left_piece + "." + right_piece
            if pfn == json_pfn:
                return self.node_map.get(outfile[u'SEName'], self.source_site)
        return self.node_map.get(self.full_report.get(u"SEName"), self.source_site)


    def stageout(self, source_dir, dest_dir, *filenames):
        self.dest_site = self.ad['CRAB_AsyncDest']

        source_sites = []
        for filename in filenames:
            source_sites.append(self.getFileSourceSite(filename))

        transfer_list = resolvePFNs(self.dest_site, source_dir, dest_dir, source_sites, filenames)
        for source, dest in transfer_list:
            logger.info("Copying %s to %s" % (source, dest))

        # Skip the first file - it's a tarball of the stdout/err
        for outfile in zip(filenames[1:], self.outputFiles, source_sites[1:]):
            outlfn = os.path.join(dest_dir, outfile[0])
            outfile[1]['outlfn'] = outlfn
            if 'outtmplocation' not in outfile[1]:
                outfile[1]['outtmplocation'] = outfile[2]
            outfile[1]['outlocation'] = self.dest_site

        global g_Job
        aso_auth_file = os.path.expanduser("~/auth_aso_plugin.config")
        if config:
            aso_auth_file = getattr(config, "authfile", "auth_aso_plugin.config")
        if os.path.isfile(aso_auth_file) or os.environ.get("TEST_POSTJOB_ENABLE_ASOSERVER", False):
            targetClass = ASOServerJob
        else:
            targetClass = FTSJob

        g_Job = targetClass(self.dest_site, source_dir, dest_dir, source_sites, self.crab_id, filenames, self.reqname, self.outputData, self.log_size, self.output, self.task_ad)
        fts_job_result = g_Job.run()
        # If no files failed, return success immediately.  Otherwise, see how many files failed.
        if not fts_job_result:
            return fts_job_result

        source_list = [i[0] for i in transfer_list]
        print "Source list", source_list
        dest_list = [i[1] for i in transfer_list]
        print "Dest list", dest_list
        source_sizes = determineSizes(source_list)
        print "Source sizes", source_sizes
        dest_sizes = determineSizes(dest_list)
        print "Dest sizes", dest_sizes
        sizes = zip(source_sizes, dest_sizes)

        failures = len([i for i in sizes if (i[1]<0 or (i[0] != i[1]))])
        if failures:
            raise RuntimeError("There were %d failed stageout attempts" % failures)

        return fts_job_result


    def makeNodeMap(self):
        p = PhEDEx.PhEDEx()
        nodes = p.getNodeMap()['phedex']['node']
        self.node_map = {}
        for node in nodes:
            self.node_map[str(node[u'se'])] = str(node[u'name'])

    def execute(self, *args, **kw):
        retry_count = args[2]
        self.retry_count = retry_count
        id = args[7]
        reqname = args[6]
        logpath = os.path.expanduser("~/%s" % reqname)
        postjob = os.path.join(logpath, "postjob.%s.%s.txt" % (id, retry_count))
        logger.debug("The post-job script will be saved to %s" % postjob)
        try:
            retval = self.execute_internal(*args, **kw)
        except:
            logger.exception("Failure during post-job execution.")
            sys.stdout.flush()
            sys.stderr.flush()
            shutil.copy("postjob.%s" % id, postjob)
            os.chmod(postjob, 0644)
            DashboardAPI.apmonFree()
            raise
        if retval:
            sys.stdout.flush()
            sys.stderr.flush()
            shutil.copy("postjob.%s" % id, postjob)
            os.chmod(postjob, 0644)
        DashboardAPI.apmonFree()
        return retval

    def execute_internal(self, cluster, status, retry_count, max_retries, restinstance, resturl, reqname, id, outputdata, sw, async_dest, source_dir, dest_dir, *filenames):
        self.sw = sw
        self.reqname = reqname
        self.outputData = outputdata
        stdout = "job_out.%s" % id
        stderr = "job_err.%s" % id
        jobreport = "jobReport.json.%s" % id

        new_stdout = "postjob.%s" % id
        fd = os.open(new_stdout, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0644)
        if not os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            os.dup2(fd, 1)
            os.dup2(fd, 2)
            logger.info("Post-job started with output redirected to %s." % new_stdout)
        else:
            logger.info("Post-job started with no output redirection.")

        logpath = os.path.expanduser("~/%s" % reqname)
        try:
            os.makedirs(logpath)
        except OSError, oe:
            if oe.errno != errno.EEXIST:
                logger.exception("Failed to create log web-shared directory %s" % logpath)
                raise

        if os.path.exists(stdout):
            fname = os.path.join(logpath, "job_out."+id+"."+retry_count+".txt")
            logger.debug("Copying job stdout from %s to %s" % (stdout, fname))
            shutil.copy(stdout, fname)
            os.chmod(fname, 0644)
        # NOTE: we now redirect stdout -> stderr; hence, I think we don't need this in the webdir.
        #if os.path.exists(stderr):
        #    fname = os.path.join(logpath, "job_err."+id+"."+retry_count+".txt")
        #    shutil.copy(stderr, fname)
        #    os.chmod(fname, 0644)
        if os.path.exists(jobreport):
            fname = os.path.join(logpath, "job_fjr."+id+"."+retry_count+".json")
            logger.debug("Copying job FJR from %s to %s" % (jobreport, fname))
            shutil.copy(jobreport, fname)
            os.chmod(fname, 0644)

        if 'X509_USER_PROXY' not in os.environ:
            logger.error("Failure due to X509_USER_PROXY not being present in environment.")
            return 10

        self.server = HTTPRequests(restinstance, os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY'])
        self.resturl = resturl

        self.makeAd(reqname, id, outputdata, sw, async_dest)
        self.getTaskAd()
        if 'CRAB_UserWebDir' in self.task_ad:
            self.logfiles = [("job_out", "txt"), ("job_fjr", "json"), ("postjob", "txt")]
            self.logfiles = ["%s/%s.%s.%s.%s" % (self.task_ad['CRAB_UserWebDir'], i[0], str(id), str(retry_count), i[1]) for i in self.logfiles]

        self.makeNodeMap()

        status = int(status)

        self.uploadFakeLog(state="TRANSFERRING")

        logger.info("Retry count %s; max retry %s" % (retry_count, max_retries))
        fail_state = "COOLOFF"
        if retry_count == max_retries:
            fail_state = "FAILED"
        if status and (retry_count == max_retries):
            # This was our last retry and it failed.
            logger.info("The max retry count was hit and the job failed; setting this node to permanent failure.")
            return self.uploadState("FAILED")
        retry = RetryJob.RetryJob()
        retval = None
        if not os.environ.get('TEST_POSTJOB_DISABLE_RETRIES', False):
            retval = retry.execute(status, retry_count, max_retries, self.crab_id, cluster)
        if retval:
            if retval == RetryJob.FATAL_ERROR:
                logger.info("The retry handler indicated this was a fatal error.")
                return self.uploadState("FAILED")
            else:
                if fail_state == "FAILED":
                    logger.info("The retry handler indicated this was a recoverable error, but the max retries was already hit.  DAGMan will NOT retry.")
                else:
                    logger.info("The retry handler indicated this was a recoverable error.  DAGMan will retry")
                self.uploadState(fail_state)
                return retval

        self.parseJson()
        self.source_site = self.getSourceSite()

        self.fixPerms()
        try:
            self.uploadLog(dest_dir, filenames[0])
            self.stageout(source_dir, dest_dir, *filenames)
            try:
                self.upload()
            except HTTPException, hte:
                # Suppressing this exception is a tough decision.  If the file made it back alright,
                # I suppose we can proceed.
                logger.exception("Potentially fatal error when uploading file locations: %s" % str(hte.headers))
                if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
                        not hte.headers.get('X-Error-Http', -1) == '400':
                    raise
        except:
            logger.exception("Stageout failed.")
            self.uploadState(fail_state)
            raise
        self.uploadFakeLog(state="FINISHED")

        return 0

class testServer(unittest.TestCase):
    def generateJobJson(self, sourceSite = 'srm.unl.edu'):
        return {"steps" : {
            "cmsRun" : { "input" : {},
              "output":
                {"outmod1" :
                    [ { "output_module_class" : "PoolOutputModule",
                        "input" : ["/test/input2",
                                   "/test/input2"
                                  ],
                        "events" : 200,
                        "size" : 100,
                        "SEName" : sourceSite,
                        "runs" : { 1: [1,2,3],
                                   2: [2,3,4]},
                      }]}}}}
    def setUp(self):
        self.pj = PostJob()
        #self.job = ASOServerJob()
        #status, retry_count, max_retries, restinstance, resturl, reqname, id,
        #outputdata, sw, async_dest, source_dir, dest_dir, *filenames
        self.fullArgs = ['0', 1, 3, 'restinstance', 'resturl',
                         'reqname', 1234, 'outputdata', 'sw', 'T2_US_Vanderbilt']
        self.jsonName = "jobReport.json.%s" % self.fullArgs[6]
        open(self.jsonName, 'w').write(json.dumps(self.generateJobJson()))

    def makeTempFile(self, size, pfn):
        fh, path = tempfile.mkstemp()
        try:
            inputString = "CRAB3POSTJOBUNITTEST"
            os.write(fh, (inputString * ((size/len(inputString))+1))[:size])
            os.close(fh)
            cmdLine = "env -u LD_LIBRAY_PATH lcg-cp -b -D srmv2 -v file://%s %s" % (path, pfn)
            print cmdLine
            status, res = commands.getstatusoutput(cmdLine)
            if status:
                raise RuntimeError, "Couldn't make file: %s" % res
        finally:
            if os.path.exists(path):
                os.unlink(path)
    def getLevelOneDir(self):
        return datetime.datetime.now().strftime("%Y-%m")

    def getLevelTwoDir(self):
        return datetime.datetime.now().strftime("%d%p")

    def getUniqueFilename(self):
        return "%s-postjob.txt" % uuid.uuid4()

    def testNonexistent(self):
        self.fullArgs.extend(['/store/temp/user/meloam/CRAB3-UNITTEST-NONEXISTENT/b/c/',
                             '/store/user/meloam/CRAB3-UNITTEST-NONEXISTENT/b/c',
                             self.getUniqueFilename()])
        self.assertNotEqual(self.pj.execute(*self.fullArgs), 0)

    sourcePrefix = "srm://dcache07.unl.edu:8443/srm/v2/server?SFN=/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms"
    def testExistent(self):
        sourceDir  = "/store/temp/user/meloam/CRAB3-UnitTest/%s/%s" % \
                        (self.getLevelOneDir(), self.getLevelTwoDir())
        sourceFile = self.getUniqueFilename()
        sourceLFN  = "%s/%s" % (sourceDir, sourceFile)
        destDir = sourceDir.replace("temp/user", "user")
        self.makeTempFile(200, "%s/%s" %(self.sourcePrefix, sourceLFN))
        self.fullArgs.extend([sourceDir, destDir, sourceFile])
        self.assertEqual(self.pj.execute(*self.fullArgs), 0)

    def tearDown(self):
        if os.path.exists(self.jsonName):
            os.unlink(self.jsonName)

if __name__ == '__main__':
    if len(sys.argv) >= 2 and sys.argv[1] == 'UNIT_TEST':
        sys.argv = [sys.argv[0]]
        print "Beginning testing"
        unittest.main()
        print "Testing over"
        sys.exit()
    pj = PostJob()
    sys.exit(pj.execute(*sys.argv[2:]))

