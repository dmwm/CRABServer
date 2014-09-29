#!/usr/bin/python

"""
In the PostJob we read the FrameworkJobReport (FJR) to retrieve information about the output files.
The FJR contains information for output files produced either via PoolOutputModule or TFileService,
which respectively produce files of type EDM and TFile. Below are examples of the output part of a
FJR for each of these two cases.
Example FJR['steps']['cmsRun']['output'] for an EDM file produced via PoolOutputModule:
{
 u'SEName': u'se1.accre.vanderbilt.edu',
 u'pfn': u'dumper.root',
 u'checksums': {u'adler32': u'47d823c0', u'cksum': u'640736586'},
 u'size': 3582626,
 u'direct_stageout': False,
 u'ouput_module_class': u'PoolOutputModule',
 u'runs': {u'1': [666668, 666672, 666675, 666677, ...]},
 u'branch_hash': u'd41d8cd98f00b204e9800998ecf8427e',
 u'input': [u'/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0011/404C27A2-22AE-E111-BC20-003048D373AE.root', ...],
 u'inputpfns': [u'/cms/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0011/404C27A2-22AE-E111-BC20-003048D373AE.root', ...],
 u'lfn': u'',
 u'pset_hash': u'eede9f2e261619d27929da51104cf9d7',
 u'catalog': u'',
 u'module_label': u'o',
 u'guid': u'48C5017F-A405-E411-9BE6-00A0D1E70940',
 u'events': 30650
}
Example FJR['steps']['cmsRun']['output'] for a TFile produced via TFileService:
{
 u'SEName': u'se1.accre.vanderbilt.edu',
 u'pfn': u'/tmp/1882789.vmpsched/glide_Paza70/execute/dir_27876/histo.root',
 u'checksums': {u'adler32': u'e8ed4a12', u'cksum': u'2439186609'},
 u'size': 360,
 u'direct_stageout': False,
 u'fileName': u'/tmp/1882789.vmpsched/glide_Paza70/execute/dir_27876/histo.root',
 u'Source': u'TFileService'
}
For other type of output files, we add in cmscp.py the basic necessary information about the file
to the FJR. The information is:
{
 u'SEName': u'se1.accre.vanderbilt.edu',
 u'pfn': u'out.root',
 u'size': 45124,
 u'direct_stageout': False
}
"""

import os
import re
import sys
import time
import json
import errno
import random
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

import DashboardAPI

logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
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


    def __init__(self, dest_site, source_dir, dest_dir, source_sites, count, filenames, reqname, output, log_size, log_needs_transfer, job_report_output, task_ad, retry_count, retry_timeout, cmsRun_failed):
        self._id = None
        self._cancel = False
        self._sleep = 20

        transfer_logs = int(task_ad['CRAB_SaveLogsFlag'])
        transfer_outputs = int(task_ad['CRAB_TransferOutputs'])
        transfer_list = resolvePFNs(dest_site, source_dir, dest_dir, source_sites, filenames, transfer_logs, transfer_outputs)

        self._transfer_list = transfer_list
        self._count = count
        if len(transfer_list):
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
        if not os.path.exists("copyjobfile_%s" % self._count):
            return 0
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


    def getLastFailure(self):
        return "Unknown"


def getUserFromLFN(lfn):
    if len(lfn.split('/')) > 2:
        if lfn.split('/')[2] == 'temp':
            # /store/temp/user/$USER.$HASH/foo
            user = lfn.split('/')[4].rsplit(".", 1)[0]
        else:
            # /store/user/$USER/foo
            user = lfn.split('/')[3]
    else:
        # Unknown; bail
        user = ''
    return user


class ASOServerJob(object):

    def __init__(self, dest_site, source_dir, dest_dir, source_sites, count, filenames, reqname, outputdata, log_size, log_needs_transfer, job_report_output, task_ad, retry_count, retry_timeout, cmsRun_failed):
        self.id = None
        self.retry_count = retry_count
        self.retry_timeout = retry_timeout
        self.couchServer = None
        self.couchDatabase = None
        self.sleep = 200
        self.count = count
        self.dest_site = dest_site
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        if cmsRun_failed:
            self.source_dir = os.path.join(source_dir, "failed")
            self.dest_dir = os.path.join(dest_dir, "failed")
        self.cmsRun_failed = cmsRun_failed
        self.source_sites = source_sites
        self.filenames = filenames
        self.reqname = reqname
        self.job_report_output = job_report_output
        self.log_size = log_size
        self.log_needs_transfer = log_needs_transfer
        self.outputData = outputdata
        self.task_ad = task_ad
        self.failure = None
        self.aso_start_timestamp = None
        proxy = os.environ.get('X509_USER_PROXY', None)
        if 'CRAB_ASOURL' in self.task_ad and self.task_ad['CRAB_ASOURL']:
            self.aso_db_url = self.task_ad['CRAB_ASOURL']
        else:
            logger.info("Cannot determine ASO url. Exiting")
            raise RuntimeError, "Cannot determine ASO url."
        try:
            logger.info("Will use ASO server at %s" % self.aso_db_url)
            self.couchServer = CMSCouch.CouchServer(dburl=self.aso_db_url, ckey=proxy, cert=proxy)
            self.couchDatabase = self.couchServer.connectDatabase("asynctransfer", create = False)
        except:
            logger.exception("Failed to connect to ASO database")
            raise


    def cancel(self):
        logger.info("Cancelling ASO data transfer.")
        if self.id:
            now = str(datetime.datetime.now())
            for oneID in self.id:
                doc = self.couchDatabase.document(oneID)
                doc['state'] = 'killed'
                doc['end_time'] = now
                res = self.couchDatabase.commitOne(doc)
                if 'error' in res:
                    raise RuntimeError, "Got error killing transfer: %s" % res


    def submit(self):
        allIDs = []
        outputFiles = []

        aso_start_time = None
        try:
            with open("jobReport.json.%d" % self.count) as fd:
                job_report = json.load(fd)
            aso_start_time = job_report.get("aso_start_time")
            self.aso_start_timestamp = job_report.get("aso_start_timestamp")
        except:
            self.aso_start_timestamp = int(time.time())
            logger.exception("Unable to determine ASO start time from worker node")

        input_dataset = str(self.task_ad['CRAB_InputData'])
        if 'CRAB_UserRole' in self.task_ad and str(self.task_ad['CRAB_UserRole']).lower() != 'undefined':
            role = str(self.task_ad['CRAB_UserRole'])
        else:
            role = ''
        if 'CRAB_UserGroup' in self.task_ad and str(self.task_ad['CRAB_UserGroup']).lower() != 'undefined':
            group = str(self.task_ad['CRAB_UserGroup'])
        else:
            group = ''
        dbs_url = str(self.task_ad['CRAB_DBSUrl'])
        task_publish = int(self.task_ad['CRAB_Publish'])
        publish_dbs_url = str(self.task_ad['CRAB_PublishDBSUrl'])
        if publish_dbs_url.lower() == 'undefined':
            publish_dbs_url = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/"
        # TODO: Add a method to resolve a single PFN or use resolvePFNs
        last_update = int(time.time())
        now = str(datetime.datetime.now())
        for output_module in self.job_report_output.values():
            for output_file_info in output_module:
                file_info = {}
                file_info['checksums'] = output_file_info.get(u'checksums', {'cksum': '0', 'adler32': '0'})
                file_info['outsize'] = output_file_info.get(u'size', 0)
                file_info['direct_stageout'] = output_file_info.get(u'direct_stageout', False)
                if output_file_info.get(u'output_module_class', '') == u'PoolOutputModule' or \
                   output_file_info.get(u'ouput_module_class', '')  == u'PoolOutputModule':
                    file_info['filetype'] = 'EDM'
                elif output_file_info.get(u'Source', '') == u'TFileService':
                    file_info['filetype'] = 'TFILE'
                else:
                    file_info['filetype'] = 'FAKE'
                if u'pfn' in output_file_info:
                    file_info['pfn'] = str(output_file_info[u'pfn'])
                outputFiles.append(file_info)
        transfer_outputs = int(self.task_ad['CRAB_TransferOutputs'])
        if not transfer_outputs:
            logger.debug("Transfer output flag is false; skipping outputs stageout.")
        transfer_logs = int(self.task_ad['CRAB_SaveLogsFlag'])
        if not transfer_logs:
            logger.debug("Save logs flag is false; skipping logs stageout.")
        found_log = False
        for source_site, filename in zip(self.source_sites, self.filenames):
            ## We assume that the first file in self.filenames is the logs tarball.
            if found_log:
                if not transfer_outputs:
                    continue
                lfn = "%s/%s" % (self.source_dir, filename)
                file_type = 'output'
                ifile = getFileIndex(filename, outputFiles)
                if ifile is None:
                    continue
                size = outputFiles[ifile]['outsize']
                checksums = outputFiles[ifile]['checksums']
                needs_transfer = not outputFiles[ifile]['direct_stageout']
                file_output_type = outputFiles[ifile]['filetype']
            else:
                found_log = True
                if not transfer_logs:
                    continue
                lfn = "%s/log/%s" % (self.source_dir, filename)
                file_type = 'log'
                size = self.log_size
                checksums = {'adler32': 'abc'}
                needs_transfer = self.log_needs_transfer
            user = getUserFromLFN(lfn)
            doc_id = getHashLfn(lfn)
            common_info = {"state": 'new',
                           "source": source_site,
                           "destination": self.dest_site,
                           "checksums": checksums,
                           "size": size,
                           "last_update": last_update,
                           "start_time": now,
                           "end_time": '',
                           "job_end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                           "retry_count": [],
                           "job_retry_count": self.retry_count,
                           "failure_reason": [],
                          }
            if not needs_transfer:
                logger.debug("File %s is marked as not needing transfer." % filename)
                common_info['state'] = 'done'
                common_info['end_time'] = now
            needs_commit = True
            try:
                doc = self.couchDatabase.document(doc_id)
                ## The document was already uploaded to Couch from the WN. If the transfer is done or ongoing,
                ## there is no need to commit the document again. Otherwise we "reset" the document in Couch
                ## so that ASO retries the transfer.
                if doc.get("state") in ['acquired', 'new', 'retry']:
                    logger.info("LFN %s (id %s) was injected from WN and transfer is ongoing." % (lfn, doc_id))
                    needs_commit = False
                elif doc.get("state") == 'done' and doc.get("start_time") == aso_start_time:
                    logger.info("LFN %s (id %s) was injected from WN and transfer has finished." % (lfn, doc_id))
                    needs_commit = False
                else:
                    logger.info("Will retry LFN %s (id %s)" % (lfn, doc_id))
                    logger.debug("Previous document: %s" % pprint.pformat(doc))
                if needs_commit:
                    doc.update(common_info)
                allIDs.append(doc_id)
            except CMSCouch.CouchNotFoundError:
                ## Set the publication flag.
                if file_type == 'output':
                    publish = task_publish
                    if publish and self.cmsRun_failed:
                        logger.info("Disabling publication of output file %s, because it is marked as failed job." % filename)
                        publish = 0
                    if publish and file_output_type != 'EDM':
                        logger.info("Disabling publication of output file %s, because it is not of EDM type." % filename)
                        publish = 0
                else:
                    publish = 0
                ## If the file doesn't need transfer nor publication, we don't upload the document to Couch.
                if not needs_transfer and not publish:
                    logger.info("File %s is marked as not needing transfer nor publication; skipping upload to ASO database." % filename)
                    needs_commit = False
                if needs_commit:
                    logger.info("LFN %s (id %s) is not yet known to ASO; uploading new document to ASO database." % (lfn, doc_id))
                    # FIXME: need to pass checksums, role/group, size, inputdataset, publish_dbs_url, dbs_url through
                    doc = {"_id": doc_id,
                           "inputdataset": input_dataset,
                           "group": group,
                           # TODO: Remove this if it is not required
                           "lfn": lfn.replace('/store/user', '/store/temp/user', 1),
                           "checksums": checksums,
                           "user": user,
                           "role": role,
                           "dbs_url": dbs_url,
                           "workflow": self.reqname,
                           "jobid": self.count,
                           "publication_state": 'not_published',
                           "publication_retry_count": [],
                           "type": file_type,
                           "publish": publish,
                          }
                    if not needs_transfer:
                        # The "/store/user" variant of the LFN should be used for files that are marked as 'done'.
                        # Otherwise, publication may break.
                        doc['lfn'] = lfn.replace('/store/temp/user', '/store/user', 1)
                    doc.update(common_info)
                    allIDs.append(doc_id)
            except Exception, ex:
                msg = "Error loading document from couch. Transfer submission failed."
                msg += str(ex)
                msg += str(traceback.format_exc())
                logger.info(msg)
                return False
            if needs_commit:
                logger.info("Stageout job description: %s" % pprint.pformat(doc))
                commit_result_msg = self.couchDatabase.commitOne(doc)[0]
                if 'error' in commit_result_msg:
                    logger.info("Couldn't add to ASO database: %s" % commit_result_msg)
                    return False

        return allIDs


    def status(self, long_status=False):

        query_view = False
        if not os.path.exists("aso_status.json"):
            query_view = True
        aso_info = {}
        if not query_view:
            query_view = True
            try:
                with open("aso_status.json") as fd:
                    aso_info = json.load(fd)
            except:
                logger.exception("Failed to load common ASO status.")
                return self.statusFallback()
            last_query = aso_info.get("query_timestamp", 0)
            # We can use the cached data if:
            # - It is from the last 5 minutes, AND
            # - It is from after we submitted the transfer.
            # Without the second condition, we run the risk of using the previous stageout
            # attempts results.
            if (time.time() - last_query < 300) and (last_query > self.aso_start_timestamp):
                query_view = False
            for oneDoc in self.id:
                if oneDoc not in aso_info.get("results", {}):
                    query_view = True
                    break

        if query_view:
            query = {'reduce': False, 'key': self.reqname, 'stale': 'update_after'}
            logger.debug("Querying task view.")
            try:
                states = self.couchDatabase.loadView('AsyncTransfer', 'JobsStatesByWorkflow', query)['rows']
                states_dict = {}
                for state in states:
                    states_dict[state['id']] = state
            except Exception, ex:
                logger.exception("Error while querying the asynctransfer CouchDB")
                return self.statusFallback()
            aso_info = {"query_timestamp": time.time(), "results": states_dict}
            tmp_fname = "aso_status.%d.json" % os.getpid()
            with open(tmp_fname, "w") as fd:
                json.dump(aso_info, fd)
            os.rename(tmp_fname, "aso_status.json")
        if not aso_info:
            return self.statusFallback()
        statuses = []
        for oneDoc in self.id:
            if oneDoc not in aso_info.get("results", {}):
                return self.statusFallback()
            statuses.append(aso_info['results'][oneDoc]['value'])
        return statuses


    def statusFallback(self):
        logger.debug("Querying transfer status using fallback method.")
        statuses = []
        for oneDoc in self.id:
            couchDoc = self.couchDatabase.document(oneDoc)
            statuses.append(couchDoc['state'])
        return statuses


    def getLatestLog(self, jobID):
        try:
            couchDoc = self.couchDatabase.document(jobID)
        except:
            logger.exception("Failed to retrieve updated document for %s." % jobID)
            return {}, ""
        if "_attachments" in couchDoc:
            bestLog = None
            maxRev = 0
            for log, loginfo in couchDoc["_attachments"].items():
                if bestLog == None:
                    bestLog = log
                else:
                    rev = loginfo.get(u"revpos", 0)
                    if rev > maxRev:
                        maxRev = rev
                        bestLog = log
            try:
                return couchDoc, self.couchDatabase.getAttachment(jobID, bestLog)
            except:
                logger.exception("Failed to retrieve log attachment for %s: %s" % (jobID, bestLog))
        return couchDoc, ""


    def run(self):
        self.id = self.submit()
        if self.id == False:
            raise RuntimeError, "Couldn't send to couchdb"
        if not self.id:
            logger.info("No files to transfer via ASO. Done!")
            return 0
        starttime = time.time()
        if self.aso_start_timestamp:
            starttime = self.aso_start_timestamp
        while True:
            status = self.status()
            logger.info("Got statuses: %s; %.1f hours since transfer submit." % (", ".join(status), (time.time()-starttime)/3600.0))
            allDone = True
            for oneStatus, jobID in zip(status, self.id):
                # states to wait on
                if oneStatus in ['new', 'acquired', 'retry']:
                    allDone = False
                    continue
                # good states
                elif oneStatus in ['done']:
                    continue
                # states to stop immediately
                elif oneStatus in ['failed', 'killed']:
                    logger.error("Job (internal ID %s) failed with status %s" % (jobID, oneStatus))
                    couchDoc, attachment = self.getLatestLog(jobID)
                    if not attachment:
                        logger.warning("WARNING: no FTS logfile available.")
                    else:
                        logger.error("== BEGIN FTS interaction log ==")
                        print attachment
                        logger.error("== END FTS interaction log ==")
                    if ('failure_reason' in couchDoc) and couchDoc['failure_reason']:
                        logger.error("Failure reason: %s" % couchDoc['failure_reason'])
                        self.failure = couchDoc['failure_reason']
                    else:
                        logger.warning("WARNING: no failure reason available.")
                        self.failure = "Failure reason unavailable."
                    return 1
                else:
                    raise RuntimeError, "Got a unknown status: %s" % oneStatus
            if allDone:
                logger.info("All transfers were successful")
                couchDoc, attachment = self.getLatestLog(jobID)
                if not attachment:
                    logger.warning("WARNING: no FTS logfile available.")
                else:
                    logger.info("== BEGIN FTS interaction log ==")
                    print attachment
                    logger.info("== END FTS interaction log ==")
                return 0
            if self.retry_timeout != -1 and time.time() - starttime > self.retry_timeout: #timeout = -1 means it's disabled
                self.failure = "Killed ASO transfer after timeout of %d." % self.retry_timeout
                logger.warning("Killing ASO transfer after timeout of %d." % self.retry_timeout)
                self.cancel()
                return 1
            else:
                # Sleep is done here in case if the transfer is done immediately (direct stageout case).
                time.sleep(self.sleep + random.randint(0, 60))


    def getLastFailure(self):
        if self.failure:
            return self.failure
        return "Unknown"


def determineSizes(transfer_list):
    sizes = []
    for pfn in transfer_list:
        cmd = "lcg-ls --srm-timeout 300 --connect-timeout 300 --sendreceive-timeout 60 -D srmv2 -b -l %s" % pfn
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            print "lcg-ls command exited with code %s. Output was: %s" % (status, output)
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


def resolvePFNs(dest_site, source_dir, dest_dir, source_sites, filenames, transfer_logs, transfer_outputs):

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
        lfns.append(slfn)
        lfns.append(dlfn)
        found_log = True

    dest_sites_ = [dest_site]
    if dest_site.startswith("T1_"):
        dest_sites_.append(dest_site + "_Buffer")
        dest_sites_.append(dest_site + "_Disk")
    dest_info = p.getPFN(nodes=(source_sites + dest_sites_), lfns=lfns)

    results = []
    found_log = False
    for source_site, filename in zip(source_sites, filenames):
        if not found_log and filename.startswith("cmsRun") and (filename[-7:] == ".tar.gz"):
            if not transfer_logs:
                found_log = True
                continue
            slfn = os.path.join(source_dir, "log", filename)
            dlfn = os.path.join(dest_dir, "log", filename)
        else:
            if not transfer_outputs:
                continue
            slfn = os.path.join(source_dir, filename)
            dlfn = os.path.join(dest_dir, filename)
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
        found_log = True

    return results


def getHashLfn(lfn):
    """
    stolen from asyncstageout
    """
    return hashlib.sha224(lfn).hexdigest()


def isFailurePermanent(reason, task_ad):
    if "CRAB_RetryOnASOFailures" in task_ad and not task_ad["CRAB_RetryOnASOFailures"]:
        logger.debug("Considering transfer error as a permanent failure because CRAB_RetryOnASOFailures was 0")
        return True
    reason = str(reason).lower()
    if re.match(".*killed aso transfer after timeout.*", reason):
        return True
    if re.match(".*failed to get source file size.*", reason):
        return False
    if re.match(".*permission denied.*", reason):
        return True
    if re.match(".*disk quota exceeded.*", reason):
        return True
    if re.match(".*operation not permitted*", reason):
        return True
    return False


def getFileIndex(file_name, output_files):
    for i, outfile in enumerate(output_files):
        if ('pfn' not in outfile):
            continue
        json_pfn = os.path.split(outfile['pfn'])[-1]
        pfn = os.path.split(file_name)[-1]
        left_piece, fileid = pfn.rsplit("_", 1)
        right_piece = fileid.split(".", 1)[-1]
        pfn = left_piece + "." + right_piece
        if pfn == json_pfn:
            return i
    return None


class PermanentStageoutError(RuntimeError):
    pass


class RecoverableStageoutError(RuntimeError):
    pass


REQUIRED_ATTRS = ['CRAB_ReqName', 'CRAB_Id', 'CRAB_OutputData', 'CRAB_JobSW', 'CRAB_AsyncDest']


class PostJob():


    def __init__(self):
        self.ad = None
        self.task_ad = {}
        self.crab_id = -1
        self.job_report = None
        self.report = None
        self.job_report_output = None
        self.input = None
        self.output_files_info = []
        self.retry_count = "0"
        self.logfiles = None
        self.log_needs_transfer = True
        self.retry_timeout = 2*3600
        self.cmsRunFailed = False


    def getTaskAd(self):
        ad = os.environ.get("_CONDOR_JOB_AD", ".job.ad")
        if not os.path.exists(ad) or not os.stat(ad).st_size:
            print "Missing task ad!"
            return 2
        try:
            adfile = open(ad)
            self.task_ad = classad.parseOld(adfile)
            adfile.close()
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
            self.job_report = json.load(fd)
        if 'steps' not in self.job_report:
            raise ValueError("Invalid jobReport.json: missing 'steps'")
        if 'cmsRun' not in self.job_report['steps']:
            raise ValueError("Invalid jobReport.json: missing 'cmsRun'")
        self.report = self.job_report['steps']['cmsRun']
        if 'input' not in self.report:
            raise ValueError("Invalid jobReport.json: missing 'input'")
        self.input = self.report['input']
        if 'output' not in self.report:
            raise ValueError("Invalid jobReport.json: missing 'output'")
        self.job_report_output = self.report['output']

        self.log_needs_transfer = not self.job_report.get(u'direct_stageout', False)
        if not self.log_needs_transfer:
            logger.debug("Log file is marked as directly transferred from WN.")

        if 'jobExitCode' in self.job_report:
            self.cmsRunFailed = bool(self.job_report['jobExitCode'])

        for output_module in self.job_report_output.values():
            for output_file_info in output_module:
                print "Output file from job:", output_file_info
                file_info = {}
                self.output_files_info.append(file_info)
                ## Note incorrect spelling of 'output module' in current WMCore
                if output_file_info.get(u'output_module_class', '') == u'PoolOutputModule' or \
                   output_file_info.get(u'ouput_module_class', '')  == u'PoolOutputModule':
                    file_info['filetype'] = 'EDM'
                elif output_file_info.get(u'Source', '') == u'TFileService':
                    file_info['filetype'] = 'TFILE'
                else:
                    file_info['filetype'] = 'FAKE'
                file_info['module_label'] = output_file_info.get(u'module_label', 'unknown')
                file_info['inparentlfns'] = [str(i) for i in output_file_info.get(u'input', [])]
                file_info['events'] = output_file_info.get(u'events', 0)
                file_info['checksums'] = output_file_info.get(u'checksums', {'cksum': '0', 'adler32': '0'})
                file_info['outsize'] = output_file_info.get(u'size', 0)
                if u'pset_hash' in output_file_info:
                    file_info['pset_hash'] = output_file_info[u'pset_hash']
                if u'pfn' in output_file_info:
                    file_info['pfn'] = str(output_file_info[u'pfn'])
                file_info['direct_stageout'] = output_file_info.get(u'direct_stageout', False)
                if file_info['direct_stageout']:
                    logger.debug("Output file does not need to be transferred.")
                if u'SEName' in output_file_info and self.node_map.get(str(output_file_info['SEName'])):
                    file_info['outtmplocation'] = self.node_map[output_file_info['SEName']]
                if u'runs' not in output_file_info:
                    continue
                file_info['outfileruns'] = []
                file_info['outfilelumis'] = []
                for run, lumis in output_file_info[u'runs'].items():
                    file_info['outfileruns'].append(str(run))
                    file_info['outfilelumis'].append(','.join(map(str,lumis)))


    def fixPerms(self):
        """
        HTCondor will default to a umask of 0077 for stdout/err.  When the DAG finishes,
        the schedd will chown the sandbox to the user which runs HTCondor.

        This prevents the user who owns the DAGs from retrieving the job output as they
        are no longer world-readable.
        """
        for base_file in ["job_err", "job_out.tmp"]:
            try:
                os.chmod("%s.%d" % (base_file, self.crab_id), 0644)
            except OSError, oe:
                if oe.errno != errno.ENOENT and oe.errno != errno.EPERM:
                    raise


    def upload(self):
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return

        edm_file_count = 0
        for file_info in self.output_files_info:
            if file_info['filetype'] == 'EDM':
                edm_file_count += 1
        multiple_edm = edm_file_count > 1
        for file_info in self.output_files_info:
            publishname = self.task_ad['CRAB_PublishName']
            if 'pset_hash' in file_info:
                publishname = "%s-%s" % (publishname.rsplit("-", 1)[0], file_info['pset_hash'])
            ## CMS convention for outdataset: /primarydataset>/<yourHyperNewsusername>-<publish_data_name>-<PSETHASH>/USER
            if file_info['filetype'] == 'EDM':
                if multiple_edm and file_info.get("module_label"):
                    left, right = publishname.rsplit("-", 1)
                    publishname = "%s_%s-%s" % (left, file_info['module_label'], right)
                outdataset = os.path.join('/' + str(self.task_ad['CRAB_InputData']).split('/')[1], self.task_ad['CRAB_UserHN'] + '-' + publishname, 'USER')
            else:
                outdataset = "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER"
            configreq = {"taskname":        self.ad['CRAB_ReqName'],
                         "globalTag":       "None",
                         "pandajobid":      self.crab_id,
                         "outsize":         file_info['outsize'],
                         "publishdataname": publishname,
                         "appver":          self.ad['CRAB_JobSW'],
                         "outtype":         file_info['filetype'],
                         "checksummd5":     "asda", # Not implemented; garbage value taken from ASO
                         "checksumcksum":   file_info['checksums']['cksum'],
                         "checksumadler32": file_info['checksums']['adler32'],
                         "outlocation":     self.ad['CRAB_AsyncDest'],
                         "outtmplocation":  file_info.get('outtmplocation', self.source_site),
                         "acquisitionera":  "null", # Not implemented
                         "outlfn":          file_info['outlfn'],
                         "events":          file_info.get('events', 0),
                         "outdatasetname":  outdataset,
                         "directstageout":  int(file_info['direct_stageout'])
                        }
            configreq = configreq.items()
            if 'outfileruns' in file_info:
                for run in file_info['outfileruns']:
                    configreq.append(("outfileruns", run))
            if 'outfilelumis' in file_info:
                for lumi in file_info['outfilelumis']:
                    configreq.append(("outfilelumis", lumi))
            if 'inparentlfns' in file_info:
                for lfn in file_info['inparentlfns']:
                    # If the user specified a PFN as input, then the LFN is an empty string
                    # and does not pass validation.
                    if lfn:
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
        if 'SEName' in self.job_report:
            source_site = self.node_map.get(self.job_report['SEName'], source_site)
        self.log_size = self.job_report.get(u'log_size', 0)
        direct_stageout = int(self.job_report.get(u'direct_stageout', 0))
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
                     "outdatasetname":  "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER",
                     "directstageout":  direct_stageout
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
        # Unfortunately, Dashboard only has 1-second resolution; we must separate all
        # updates by at least 1s in order for it to get the correct ordering.
        time.sleep(1)
        logger.info("Dashboard parameters: %s" % str(params))
        DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)


    def uploadState(self, state):
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        self.setDashboardState(state)
        return 2


    def getSourceSite(self):
        if self.job_report.get('executed_site', None):
            print "Getting source site from jobReport"
            return self.job_report['executed_site']
        raise ValueError("Unable to determine source site")


    def getFileSourceSite(self, file_name, ifile = None):
        if ifile is None:
            ifile = getFileIndex(file_name, self.output_files_info)
        if ifile is not None and ('outtmplocation' in self.output_files_info[ifile]):
            return self.output_files_info[ifile]['outtmplocation']
        return self.node_map.get(self.job_report.get(u"SEName"), self.source_site)


    def stageout(self, source_dir, dest_dir, *filenames):

        self.dest_site = self.ad['CRAB_AsyncDest']
        transfer_logs = int(self.task_ad['CRAB_SaveLogsFlag'])
        transfer_outputs = int(self.task_ad['CRAB_TransferOutputs'])
        source_sites = []
        for i, filename in enumerate(filenames):
            ifile = getFileIndex(filename, self.output_files_info)
            source_sites.append(self.getFileSourceSite(filename, ifile))
            if ifile is None:
                continue
            outlfn = os.path.join(dest_dir, filename)
            self.output_files_info[ifile]['outlfn'] = outlfn
            if 'outtmplocation' not in self.output_files_info[ifile]:
                self.output_files_info[ifile]['outtmplocation'] = source_sites[-1]
            self.output_files_info[ifile]['outlocation'] = self.dest_site

        transfer_list = resolvePFNs(self.dest_site, source_dir, dest_dir, source_sites, filenames, transfer_logs, transfer_outputs)
        for source, dest in transfer_list:
            logger.info("Copying %s to %s" % (source, dest))

        global g_Job
        aso_auth_file = os.path.expanduser("~/auth_aso_plugin.config")
        if config:
            aso_auth_file = getattr(config, "authfile", "auth_aso_plugin.config")
        if os.path.isfile(aso_auth_file) or os.environ.get("TEST_POSTJOB_ENABLE_ASOSERVER", False):
            targetClass = ASOServerJob
        elif 'CRAB_ASOURL' in self.task_ad and self.task_ad['CRAB_ASOURL']:
            targetClass = ASOServerJob
        else:
            targetClass = FTSJob

        g_Job = targetClass(self.dest_site, source_dir, dest_dir, source_sites, self.crab_id, filenames, self.reqname, self.outputData, self.log_size, self.log_needs_transfer, self.job_report_output, self.task_ad, self.retry_count, self.retry_timeout, self.cmsRunFailed)
        fts_job_result = g_Job.run()
        # If no files failed, return success immediately.  Otherwise, see how many files failed.
        if not fts_job_result:
            return fts_job_result

        failureReason = g_Job.getLastFailure()
        g_Job = None
        isPermanent = isFailurePermanent(failureReason, self.task_ad)

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
            msg = "There were %d failed stageout attempts; last failure reason: %s" % (failures, failureReason)
            if isPermanent:
                raise PermanentStageoutError(msg)
            else:
                raise RecoverableStageoutError(msg)

        msg = "Stageout failed with code %d; last failure reason: %s" % (fts_job_result, failureReason)
        if isPermanent:
            raise PermanentStageoutError(msg)
        else:
            raise RecoverableStageoutError(msg)


    def makeNodeMap(self):
        p = PhEDEx.PhEDEx()
        nodes = p.getNodeMap()['phedex']['node']
        self.node_map = {}
        for node in nodes:
            self.node_map[str(node[u'se'])] = str(node[u'name'])


    def calculateRetry(self, id, retry_num):
        """
        Calculate the retry number we're on.  See the notes in pre-job.
        """
        fname = "retry_info/job.%s.txt" % id
        if os.path.exists(fname):
            try:
                with open(fname, "r") as fd:
                    retry_info = json.load(fd)
            except:
                return retry_num
        else:
            retry_info = {"pre": 0, "post": 0}
        if 'pre' not in retry_info or 'post' not in retry_info:
            return retry_num
        retry_num = str(retry_info['post'])
        retry_info['post'] += 1
        try:
            with open(fname + ".tmp", "w") as fd:
                json.dump(retry_info, fd)
            os.rename(fname + ".tmp", fname)
        except:
            return retry_num
        return retry_num


    def execute(self, *args, **kw):
        retry_count = args[2]
        self.retry_count = retry_count
        id = args[7]
        reqname = args[6]

        logpath = os.path.expanduser("~/%s" % reqname)
        try:
            os.makedirs(logpath)
        except OSError, oe:
            if oe.errno != errno.EEXIST:
                logger.exception("Failed to create log web-shared directory %s" % logpath)
                raise

        postjob = os.path.join(logpath, "postjob.%s.%s.txt" % (id, retry_count))
        logger.debug("The post-job script will be saved to %s" % postjob)

        fd = os.open(postjob, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0644)
        os.chmod(postjob, 0644)
        if not os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            os.dup2(fd, 1)
            os.dup2(fd, 2)
            logger.info("Post-job started with output redirected to %s." % postjob)
        else:
            logger.info("Post-job started with no output redirection.")

        retval = 1
        try:
            retval = self.execute_internal(*args, **kw)
            logger.info("Post-job finished executing; status code %d." % retval)
        except:
            logger.exception("Failure during post-job execution.")
        finally:
            DashboardAPI.apmonFree()
        return self.check_abort_dag(retval)


    def check_abort_dag(self, rval):
        """
        Each fatal error is written into this file; we
        count the number of failures and abort the DAG if necessary
        """
        fname = "task_statistics.FATAL_ERROR"
        # Return code 3 is reserved to abort the entire DAG.  Don't let the
        # code otherwise use it.
        if rval == 3:
            rval = 1
        if 'CRAB_FailedNodeLimit' not in self.task_ad:
            return rval
        try:
            limit = int(self.task_ad['CRAB_FailedNodeLimit'])
            counter = 0
            with open(fname, "r") as fd:
                for line in fd.readlines():
                    counter += 1
            if counter > limit:
                logger.error("There are %d failed nodes, greater than the limit of %d. Will abort the whole DAG" % (counter, limit))
                rval = 3
        finally:
            return rval


    def execute_internal(self, cluster, status, retry_count, max_retries, restinstance, resturl, reqname, id, outputdata, sw, async_dest, source_dir, dest_dir, *filenames):
        self.sw = sw
        self.reqname = reqname
        self.outputData = outputdata
        stdout = "job_out.%s" % id
        stdout_tmp = "job_out.tmp.%s" % id
        stderr = "job_err.%s" % id
        job_report = "jobReport.json.%s" % id

        logpath = os.path.expanduser("~/%s" % reqname)
        retry_count = self.calculateRetry(id, retry_count)
        if os.path.exists(stdout):
            os.rename(stdout, stdout_tmp)
            fname = os.path.join(logpath, "job_out."+id+"."+retry_count+".txt")
            logger.debug("Copying job stdout from %s to %s" % (stdout, fname))
            shutil.copy(stdout_tmp, fname)
            stdout_f = open(stdout_tmp, 'w')
            stdout_f.truncate(0)
            stdout_f.close()
            os.chmod(fname, 0644)
        # NOTE: we now redirect stdout -> stderr; hence, we don't keep stderr in the webdir.
        if os.path.exists(job_report):
            fname = os.path.join(logpath, "job_fjr."+id+"."+retry_count+".json")
            logger.debug("Copying job FJR from %s to %s" % (job_report, fname))
            shutil.copy(job_report, fname)
            os.chmod(fname, 0644)

        if 'X509_USER_PROXY' not in os.environ:
            logger.error("Failure due to X509_USER_PROXY not being present in environment.")
            return 10

        self.server = HTTPRequests(restinstance, os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY'])
        self.resturl = resturl

        logger.info("Post-job was asked to transfer up to %d files." % len(filenames))

        self.makeAd(reqname, id, outputdata, sw, async_dest)
        if self.getTaskAd() == 2 or not self.task_ad:
            self.uploadState("FAILED")
            return RetryJob.FATAL_ERROR
        if 'CRAB_UserWebDir' in self.task_ad:
            self.logfiles = [("job_out", "txt"), ("job_fjr", "json"), ("postjob", "txt")]
            self.logfiles = ["%s/%s.%s.%s.%s" % (self.task_ad['CRAB_UserWebDir'], i[0], str(id), str(retry_count), i[1]) for i in self.logfiles]

        self.makeNodeMap()

        status = int(status)

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
            retval = retry.execute(reqname, status, retry_count, max_retries, self.crab_id, cluster)
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

        if 'CRAB_ASOTimeout' in self.task_ad and self.task_ad['CRAB_ASOTimeout']:#if it's 0 use default timeout logic
            self.retry_timeout = self.task_ad['CRAB_ASOTimeout']
        else:
            self.retry_timeout = retry.get_aso_timeout()

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
        except PermanentStageoutError, pse:
            logger.error("This is a permanent stageout error; user will need to resubmit.")
            self.uploadState("FAILED")
            return RetryJob.FATAL_ERROR
        except RecoverableStageoutError, rse:
            logger.error("This is a recoverable stageout error; automatic resubmit is possible.")
            self.uploadState(fail_state)
            return RetryJob.RECOVERABLE_ERROR
        except:
            logger.exception("Stageout failed due to unknown issue.")
            self.uploadState(fail_state)
            raise
        self.uploadState(state="FINISHED")

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

