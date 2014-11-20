#!/usr/bin/python

"""
In the PostJob we read the FrameworkJobReport (FJR) to retrieve information
about the output files. The FJR contains information for output files produced
either via PoolOutputModule or TFileService, which respectively produce files
of type EDM and TFile. Below are examples of the output part of a FJR for each
of these two cases.
Example FJR['steps']['cmsRun']['output'] for an EDM file produced via
PoolOutputModule:
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
For other type of output files, we add in cmscp.py the basic necessary info
about the file to the FJR. The information is:
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
import uuid
import glob
import errno
import random
import shutil
import signal
import urllib
import logging
import classad
import commands
import unittest
import datetime
import tempfile
import traceback
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

ASO_JOB = None
config = None
JOB_REPORT_NAME = None

def sighandler(*args):
    if ASO_JOB:
        ASO_JOB.cancel()

signal.signal(signal.SIGHUP,  sighandler)
signal.signal(signal.SIGINT,  sighandler)
signal.signal(signal.SIGTERM, sighandler)

REGEX_ID = re.compile("([a-f0-9]{8,8})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{12,12})")


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

def prepareErrorSummary(reqname):#, job_id, crab_retry):
    """ Load the current error_summary.json and do the equivalent of an 'ls job_fjr.*.json' to get the completed jobs.
        Then, add the error reason of the job that are not in errorReport.json
    """

    logger.info("==== Start preparing error report ====")
    #open and load an already existing error summary
    error_summary = {}
    try:
        with open('error_summary.json') as fsummary:
            error_summary = json.load(fsummary)
    except (IOError, ValueError):
        #there is nothing to do if the errorSummary file does not exist or is invalid. Just recreate it
        logger.info("error_summary.json is empty, wrong, or does not exist")

#    asosummary = {}
#    try:
#        transferSummary = {}
#        with open('aso_status.json') as asosummary:
#            asoTmp = json.load(asosummary)
#        for _, state_id in asoTmp["results"].iteritems():
#            transferSummary[state_id["value"]["jobid"]] = state_id["value"]["state"]
#    except (IOError, ValueError):
#        #there is nothing to do it the aso_status.json file does not exist or is invalid.
#        print "aso_status.json is empty or does not exist"

    #iterate over the job reports in the task directory
    logpath = os.path.expanduser("~/%s" % reqname)
    for report in glob.glob(logpath+'/job_fjr.*.json'):
        split_rep = report.split('.')
        job_id, crab_retry = split_rep[-3], split_rep[-2]
        if job_id in error_summary and crab_retry in error_summary[job_id]:
            #the report has already been processed
            continue
        fname = os.path.join(logpath, "job_fjr."+job_id+"."+crab_retry+".json")
        with open(fname) as frep:
            try:
                rep = None
                exit_code = -1
                rep = json.load(frep)
                if not 'exitCode' in rep:                    logger.info("'exitCode' key not found in the report");              raise
                exit_code = rep['exitCode']
                if not 'steps'    in rep:                    logger.info("'steps' key not found in the report");                 raise
                if not 'cmsRun'   in rep['steps']:           logger.info("'cmsRun' key not found in report['steps']");           raise
                if not 'errors'   in rep['steps']['cmsRun']: logger.info("'errors' key not found in report['steps']['errors']"); raise
                if rep['steps']['cmsRun']['errors']:
                    if len(rep['steps']['cmsRun']['errors']) != 1:
                        #this should never happen because the report has just one step, but just in case print a message
                        logger.info("more than one error found in the job, just considering the first one")
                    error_summary.setdefault(job_id, {})[crab_retry] = (exit_code, rep['exitMsg'], rep['steps']['cmsRun']['errors'][0])
                else:
                    error_summary.setdefault(job_id, {})[crab_retry] = (exit_code, rep['exitMsg'], {})
            except Exception, ex:
                logger.info(str(ex))
                if not rep:
                    msg = 'Invalid framework job report. The framework job report exists, but it cannot be loaded.'# % (job_id, crab_retry)
                else:
                    msg = rep['exitMsg'] if 'exitMsg' in rep else 'The framework job report could be loaded but no error message found there'
                error_summary.setdefault(job_id, {})[crab_retry] = (exit_code, msg, {})

    #write the file. Use a temporary file and rename to avoid concurrent writing of the file
    tmp_fname = "error_summary.%d.json" % os.getpid()
    with open(tmp_fname, "w") as fd:
        json.dump(error_summary, fd)
    os.rename(tmp_fname, "error_summary.json")
    logger.info("==== Finished preparing error report ====")

class ASOServerJob(object):

    def __init__(self, dest_site, source_dir, dest_dir, source_sites, count, filenames, reqname, outputdata, log_size, \
                 log_needs_transfer, job_report_output, task_ad, crab_retry_count, retry_timeout, cmsrun_failed):
        self.doc_ids = None
        self.crab_retry_count = crab_retry_count
        self.retry_timeout = retry_timeout
        self.couch_server = None
        self.couch_database = None
        self.sleep = 200
        self.count = count
        self.dest_site = dest_site
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        if cmsrun_failed:
            self.source_dir = os.path.join(source_dir, "failed")
            self.dest_dir = os.path.join(dest_dir, "failed")
        self.cmsrun_failed = cmsrun_failed
        self.source_sites = source_sites
        self.filenames = filenames
        self.reqname = reqname
        self.job_report_output = job_report_output
        self.log_size = log_size
        self.log_needs_transfer = log_needs_transfer
        self.output_data = outputdata
        self.task_ad = task_ad
        self.failures = {}
        self.aso_start_timestamp = None
        proxy = os.environ.get('X509_USER_PROXY', None)
        if 'CRAB_ASOURL' in self.task_ad and self.task_ad['CRAB_ASOURL']:
            self.aso_db_url = self.task_ad['CRAB_ASOURL']
        else:
            logger.info("Cannot determine ASO url. Exiting")
            raise RuntimeError, "Cannot determine ASO URL."
        try:
            logger.info("Will use ASO server at %s" % self.aso_db_url)
            self.couch_server = CMSCouch.CouchServer(dburl=self.aso_db_url, ckey=proxy, cert=proxy)
            self.couch_database = self.couch_server.connectDatabase("asynctransfer", create = False)
        except:
            logger.exception("Failed to connect to ASO database")
            raise


    def cancel(self, doc_ids_reasons = None):
        now = str(datetime.datetime.now())
        if not doc_ids_reasons:
            for doc_id in self.doc_ids:
                doc_ids_reasons[doc_id] = ''
        for doc_id, reason in doc_ids_reasons.iteritems():
            msg = "Cancelling ASO data transfer %s" % doc_id
            if reason:
                msg += " with following reason: %s" % reason
            logger.info(msg)
            doc = self.couch_database.document(doc_id)
            doc['state'] = 'killed'
            doc['end_time'] = now
            if reason:
                if doc['failure_reason']:
                    if type(doc['failure_reason']) == list:
                        doc['failure_reason'].append(reason)
                    elif type(doc['failure_reason']) == str:
                        doc['failure_reason'] = [doc['failure_reason'], reason]
                else:
                    doc['failure_reason'] = reason
            res = self.couch_database.commitOne(doc)
            if 'error' in res:
                raise RuntimeError, "Got error killing ASO data transfer %s: %s" % (doc_id, res)


    def submit(self):
        """
        Upload documents to ASO database if not done by cmscp from worker node.
        """
        logger.info("==== Start checking for uploads to ASO database ====")
        all_ids = []
        output_files = []

        aso_start_time = None
        now = str(datetime.datetime.now())
        last_update = int(time.time())
        try:
            with open(JOB_REPORT_NAME) as fd:
                job_report = json.load(fd)
            self.aso_start_timestamp = job_report.get("aso_start_timestamp")
            aso_start_time = job_report.get("aso_start_time")
        except:
            self.aso_start_timestamp = last_update
            aso_start_time = now
            msg  = "Unable to determine ASO start time from job report."
            msg += " Will use ASO start time %s (%s)." % (aso_start_time, self.aso_start_timestamp)
            logger.exception(msg)

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
        # TODO: Add a method to resolve a single PFN.
        for output_module in self.job_report_output.values():
            for output_file_info in output_module:
                file_info = {}
                file_info['checksums'] = output_file_info.get(u'checksums', {'cksum': '0', 'adler32': '0'})
                file_info['outsize'] = output_file_info.get(u'size', 0)
                file_info['direct_stageout'] = output_file_info.get(u'direct_stageout', False)
                if (output_file_info.get(u'output_module_class', '') == u'PoolOutputModule' or \
                    output_file_info.get(u'ouput_module_class',  '') == u'PoolOutputModule'):
                    file_info['filetype'] = 'EDM'
                elif output_file_info.get(u'Source', '') == u'TFileService':
                    file_info['filetype'] = 'TFILE'
                else:
                    file_info['filetype'] = 'FAKE'
                if u'pfn' in output_file_info:
                    file_info['pfn'] = str(output_file_info[u'pfn'])
                output_files.append(file_info)
        transfer_outputs = int(self.task_ad['CRAB_TransferOutputs'])
        if not transfer_outputs:
            logger.debug("Transfer outputs flag is false; skipping outputs stageout.")
        transfer_logs = int(self.task_ad['CRAB_SaveLogsFlag'])
        if not transfer_logs:
            logger.debug("Transfer logs flag is false; skipping logs stageout.")
        found_log = False
        for source_site, filename in zip(self.source_sites, self.filenames):
            ## We assume that the first file in self.filenames is the logs tarball.
            if found_log:
                if not transfer_outputs:
                    continue
                lfn = "%s/%s" % (self.source_dir, filename)
                file_type = 'output'
                ifile = getFileIndex(filename, output_files)
                if ifile is None:
                    continue
                size = output_files[ifile]['outsize']
                checksums = output_files[ifile]['checksums']
                needs_transfer = not output_files[ifile]['direct_stageout']
                file_output_type = output_files[ifile]['filetype']
            else:
                found_log = True
                if not transfer_logs:
                    continue
                lfn = "%s/log/%s" % (self.source_dir, filename)
                file_type = 'log'
                size = self.log_size
                checksums = {'adler32': 'abc'}
                needs_transfer = self.log_needs_transfer
            logger.info("Working on file %s" % filename)
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
                           "job_retry_count": self.crab_retry_count,
                           "failure_reason": [],
                          }
            if not needs_transfer:
                logger.info("File %s is marked as having been directly staged out from the worker node." % filename)
                common_info['state'] = 'done'
                common_info['end_time'] = now
            ## Set the publication flag.
            publication_msg = None
            if file_type == 'output':
                publish = task_publish
                if publish and self.cmsrun_failed:
                    publication_msg = "Disabling publication of output file %s, because job is marked as failed." % filename
                    publish = 0
                if publish and file_output_type != 'EDM':
                    publication_msg = "Disabling publication of output file %s, because it is not of EDM type." % filename
                    publish = 0
            else:
                ## This is the log file, so obviously publication should be turned off.
                publish = 0
            ## What does ASO needs to do for this file?
            aso_tasks = []
            if needs_transfer:
                aso_tasks.append("transfer")
            if publish:
                aso_tasks.append("publication")
            if not (needs_transfer or publish):
                ## This file doesn't need transfer nor publication, so we don't need to upload a document
                ## to ASO database.
                if publication_msg:
                    logger.info(publication_msg)
                msg  = "File %s doesn't need transfer nor publication." % filename
                msg += " No need to upload a document to ASO database."
                logger.info(msg)
            else:
                ## This file needs transfer and/or publication. If a document (for the current job retry)
                ## is not yet in ASO database, we need to do the upload.
                needs_commit = True
                try:
                    doc = self.couch_database.document(doc_id)
                    ## The document was already uploaded to ASO database. It could have been uploaded from the
                    ## WN in the current job retry or in a previous job retry, or by the postjob in a previous
                    ## job retry.
                    transfer_status = doc.get('state')
                    if doc.get('start_time') == aso_start_time:
                        ## The document was uploaded from the WN in the current job retry, so we don't upload a new
                        ## document. (If the transfer is done or ongoing, then of course we don't want to re-inject
                        ## the transfer request. OTOH, if the transfer has failed, we don't want the postjob to
                        ## retry it; instead the postjob will exit and the whole job will be retried).
                        msg = "LFN %s (id %s) is already in ASO database (it was uploaded from the worker node in the current job retry) and file transfer status is '%s'." \
                              % (lfn, doc_id, transfer_status)
                        logger.info(msg)
                        needs_commit = False
                    else:
                        ## The document was uploaded in a previous job retry. This means that in the current job
                        ## retry the injection from the WN has failed or cmscp did a direct stageout. We upload a
                        ## new stageout request, unless the transfer is still ongoing (which should actually not
                        ## happen, unless the postjob for the previous job retry didn't run).
                        msg = "LFN %s (id %s) is already in ASO database (file transfer status is '%s'), but does not correspond to the current job retry." \
                              % (lfn, doc_id, transfer_status)
                        if transfer_status in ['acquired', 'new', 'retry']:
                            msg += "\nFile transfer status is not terminal ('done', 'failed' or 'killed'). Will not upload a new document for the current job retry."
                            logger.info(msg)
                            needs_commit = False
                        else:
                            msg += " Will upload a new %s request." % ' and '.join(aso_tasks)
                            logger.info(msg)
                            logger.debug("Previous document: %s" % pprint.pformat(doc))
                except CMSCouch.CouchNotFoundError:
                    ## The document was not yet uploaded to ASO database (if this is the first job retry, then
                    ## either the upload from the WN failed, or cmscp did a direct stageout and here we need to
                    ## inject for publication only). In any case we have to inject a new document.
                    logger.info("LFN %s (id %s) is not in ASO database. Will upload a new %s request." % (lfn, doc_id, ' and '.join(aso_tasks)))
                    if publication_msg:
                        logger.info(publication_msg)
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
                except Exception, ex:
                    msg  = "Got exception loading document from ASO database. Can not upload a new document. Exception follows:"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    logger.info(msg)
                    return False
                ## If after all we need to upload a new document to ASO database, let's do it.
                if needs_commit:
                    doc.update(common_info)
                    logger.info("ASO job description: %s" % pprint.pformat(doc))
                    commit_result_msg = self.couch_database.commitOne(doc)[0]
                    if 'error' in commit_result_msg:
                        msg = "Got error uploading document to ASO database:\n%s" % commit_result_msg
                        logger.info(msg)
                        return False
                ## Record all files for which we want the postjob to monitor their transfer status.
                all_ids.append(doc_id)

        logger.info("==== Finished checking for uploads to ASO database ====")

        return all_ids


    def status(self):

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
            for doc_id in self.doc_ids:
                if doc_id not in aso_info.get("results", {}):
                    query_view = True
                    break

        if query_view:
            query = {'reduce': False, 'key': self.reqname, 'stale': 'update_after'}
            logger.debug("Querying task view.")
            try:
                states = self.couch_database.loadView('AsyncTransfer', 'JobsIdsStatesByWorkflow', query)['rows']
                states_dict = {}
                for state in states:
                    states_dict[state['id']] = state
            except Exception:
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
        for doc_id in self.doc_ids:
            if doc_id not in aso_info.get("results", {}):
                return self.statusFallback()
            statuses.append(aso_info['results'][doc_id]['value']['state'])
        return statuses


    def statusFallback(self):
        logger.debug("Querying transfer status using fallback method.")
        statuses = []
        for doc_id in self.doc_ids:
            couch_doc = self.couch_database.document(doc_id)
            statuses.append(couch_doc['state'])
        return statuses


    def getLatestLog(self, doc_id):
        try:
            couch_doc = self.couch_database.document(doc_id)
        except:
            logger.exception("Failed to retrieve updated document for %s." % doc_id)
            return {}
        return couch_doc


    def run(self):
        self.doc_ids = self.submit()
        if self.doc_ids == False:
            raise RuntimeError, "Couldn't upload document to ASO database"
        if not self.doc_ids:
            logger.info("No files to transfer via ASO. Done!")
            return 0
        failed_killed_transfers = []
        done_transfers = []
        starttime = time.time()
        if self.aso_start_timestamp:
            starttime = self.aso_start_timestamp
        logger.info("==== Start monitoring ASO transfers ====")
        while True:
            status = self.status()
            logger.info("Got statuses: %s; %.1f hours since transfer submit." % (", ".join(status), (time.time()-starttime)/3600.0))
            all_transfers_finished = True
            for transfer_status, doc_id in zip(status, self.doc_ids):
                ## States to wait on.
                if transfer_status in ['new', 'acquired', 'retry']:
                    all_transfers_finished = False
                    continue
                ## Good states.
                elif transfer_status in ['done']:
                    if doc_id not in done_transfers:
                        done_transfers.append(doc_id)
                    continue
                ## Bad states.
                elif transfer_status in ['failed', 'killed']:
                    if doc_id not in failed_killed_transfers:
                        failed_killed_transfers.append(doc_id)
                        msg = "Job (internal ID %s) failed with status '%s'." % (doc_id, transfer_status)
                        couch_doc = self.getLatestLog(doc_id)
                        if ('failure_reason' in couch_doc) and couch_doc['failure_reason']:
                            ## reasons:  The transfer failure reason(s).
                            ## app:      The application that gave the transfer failure reason(s).
                            ##           E.g. 'aso' or '' (meaning the postjob). When printing the transfer
                            ##           failure reasons (e.g. below), print also that the failures come from
                            ##           the given app (if app != '').
                            ## severity: Either 'permanent' or 'recoverable'.
                            ##           It is set in the stageout() function, when isFailurePermanent() is
                            ##           called to determine if a failure is permanent or not.
                            reasons, app, severity = couch_doc['failure_reason'], 'aso', ''
                            msg += " Failure reasons follow:"
                            if app:
                                msg += "\n=== %s log start ===" % str(app).upper()
                            msg += "\n%s" % reasons
                            if app:
                                msg += "\n=== %s log finish ===" % str(app).upper()
                            logger.error(msg)
                        else:
                            reasons, app, severity = 'Failure reason unavailable.', '', ''
                            logger.error(msg)
                            logger.warning("WARNING: no failure reason available.")
                        self.failures[doc_id] = {'reasons': reasons, 'app': app, 'severity': severity}
                else:
                    raise RuntimeError, "Got an unknown status: %s" % transfer_status
            if all_transfers_finished:
                msg = "All transfers finished. There were %s failed/killed transfers" % len(failed_killed_transfers)
                if failed_killed_transfers:
                    msg += " (%s)" % ', '.join(failed_killed_transfers)
                    logger.info(msg)
                    logger.info("==== Finished monitoring ASO transfers ====")
                    return 1
                else:
                    logger.info(msg)
                    logger.info("==== Finished monitoring ASO transfers ====")
                    return 0
            ## If there is a timeout for transfers to complete, check if it was exceeded
            ## and if so kill the ongoing transfers. # timeout = -1 means no timeout.
            if self.retry_timeout != -1 and time.time() - starttime > self.retry_timeout:
                logger.warning("Killing ongoing ASO transfers after timeout of %d (seconds)." % self.retry_timeout)
                reason = "Killed ASO transfer after timeout of %d (seconds)." % self.retry_timeout
                for doc_id in self.doc_ids:
                    if doc_id not in done_transfers + failed_killed_transfers:
                        app, severity = '', ''
                        self.failures[doc_id] = {'reasons': reason, 'app': app, 'severity': severity}
                        self.cancel({doc_id: reason})
                logger.info("==== Finished monitoring ASO transfers ====")
                return 1
            else:
                ## Sleep is done here in case if the transfer is done immediately (direct stageout case).
                time.sleep(self.sleep + random.randint(0, 60))


    def getFailures(self):
        """
        Retrieve failures bookeeping dictionary.
        """
        return self.failures


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
    if re.match(".*mkdir\(\) fail.*", reason):
        return True
    if re.match(".*open/create error.*", reason):
        return True
    if re.match(".*no free space on storage area.*", reason):
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
        self.dag_retry_count = 0
        self.crab_retry_count = 0
        self.log_files = []
        self.log_needs_transfer = True
        self.retry_timeout = 2*3600
        self.cmsrun_failed = False
        self.node_map = {}
        self.source_site = None
        self.output_data = None
        self.server = None
        self.resturl = None
        self.log_size = None
        self.dest_site = None
        self.reqname = None

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


    def makeAd(self, reqname, job_id, outputdata, job_sw, async_dest):
        self.ad = classad.ClassAd()
        self.ad['CRAB_ReqName'] = reqname
        self.ad['CRAB_Id'] = job_id
        self.ad['CRAB_OutputData'] = outputdata
        self.ad['CRAB_JobSW'] = job_sw
        self.ad['CRAB_AsyncDest'] = async_dest
        self.crab_id = int(self.ad['CRAB_Id'])


    def parseJobReport(self):
        """
        Parse the job report json file.
        """
        with open(JOB_REPORT_NAME) as fd:
            self.job_report = json.load(fd)
        logger.debug("==== Start parsing job report file %s ====" % JOB_REPORT_NAME)
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
        if 'jobExitCode' in self.job_report:
            self.cmsrun_failed = bool(self.job_report['jobExitCode'])
        for output_module in self.job_report_output.values():
            for output_file_info in output_module:
                logger.debug("Output file info for %s: %s" % (output_file_info.get(u'pfn', "(unknown 'pfn')").split('/')[-1], output_file_info))
                file_info = {}
                self.output_files_info.append(file_info)
                ## Note incorrect spelling of 'output module' in current WMCore
                if (output_file_info.get(u'output_module_class', '') == u'PoolOutputModule' or \
                    output_file_info.get(u'ouput_module_class',  '') == u'PoolOutputModule'):
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
                if u'SEName' in output_file_info and self.node_map.get(str(output_file_info['SEName'])):
                    file_info['outtmplocation'] = self.node_map[output_file_info['SEName']]
                if u'runs' not in output_file_info:
                    continue
                file_info['outfileruns'] = []
                file_info['outfilelumis'] = []
                for run, lumis in output_file_info[u'runs'].items():
                    file_info['outfileruns'].append(str(run))
                    file_info['outfilelumis'].append(','.join(map(str, lumis)))
        logger.debug("==== Finished parsing job report ====")


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
            except OSError, ose:
                if ose.errno != errno.ENOENT and ose.errno != errno.EPERM:
                    raise


    def upload(self):
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return

        edm_file_count = 0
        for file_info in self.output_files_info:
            if file_info['filetype'] == 'EDM':
                edm_file_count += 1
        multiple_edm = edm_file_count > 1
        output_datasets = set()
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
            output_datasets.add(outdataset)
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
            filename = file_info['pfn'].split('/')[-1]
            logger.debug("Uploading file metadata for %s to %s: %s" % (filename, self.resturl, configreq))
            try:
                self.server.put(self.resturl, data = urllib.urlencode(configreq))
            except HTTPException, hte:
                print hte.headers
                raise

        if not os.path.exists('output_datasets'):
            configreq = [('subresource', 'addoutputdatasets'),
                         ('workflow', self.ad['CRAB_ReqName'])]
            for dset in output_datasets:
                configreq.append(('outputdatasets', dset))
            logger.debug("Uploading output datasets to %s: %s" % (self.resturl.replace('filemetadata', 'task'), configreq))
            try:
                self.server.post(self.resturl.replace('filemetadata', 'task'), data = urllib.urlencode(configreq))
                with open('output_datasets', 'w') as f:
                    f.write(' '.join(output_datasets))
            except HTTPException, hte:
                info = (self.resturl.replace('filemetadata', 'task'), str(hte.headers))
                logger.exception("Error uploading output dataset to %s: %s" % info)


    def uploadLog(self, dest_dir, filename):
        """
        Upload the logs tarball file metadata.
        """
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
                     "outsize":         self.log_size,
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
        logger.debug("Uploading file metadata for %s to %s: %s" % (filename, self.resturl, configreq))
        try:
            self.server.put(self.resturl, data = urllib.urlencode(configreq))
        except HTTPException, hte:
            print hte.headers
            if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
                   not hte.headers.get('X-Error-Http', -1) == '400':
                raise


    def setDashboardState(self, state, reason = None):
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
            'MonitorJobID': '%d_https://glidein.cern.ch/%d/%s_%s' % (self.crab_id, self.crab_id, self.ad['CRAB_ReqName'].replace("_", ":"), self.crab_retry_count),
            'StatusValue': state,
        }
        if reason:
            params['StatusValueReason'] = reason
        if self.log_files:
            params['StatusLogFile'] = ",".join(self.log_files)
        ## Unfortunately, Dashboard only has 1-second resolution; we must separate all
        ## updates by at least 1s in order for it to get the correct ordering.
        time.sleep(1)
        logger.info("Dashboard parameters: %s" % str(params))
        DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)


    def uploadState(self, state):
        """
        Wrapper used when uploading failed state to Dashboard.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        self.setDashboardState(state)
        return 2


    def getSourceSite(self):
        """
        Get the source site from the job report json file.
        """
        if self.job_report.get('executed_site', None):
            logger.debug("Getting source site from job report")
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
        source_sites = []
        for filename in filenames:
            ifile = getFileIndex(filename, self.output_files_info)
            source_sites.append(self.getFileSourceSite(filename, ifile))
            if ifile is None:
                continue
            outlfn = os.path.join(dest_dir, filename)
            self.output_files_info[ifile]['outlfn'] = outlfn
            if 'outtmplocation' not in self.output_files_info[ifile]:
                self.output_files_info[ifile]['outtmplocation'] = source_sites[-1]
            self.output_files_info[ifile]['outlocation'] = self.dest_site

        global ASO_JOB
        ASO_JOB = ASOServerJob(self.dest_site, source_dir, dest_dir, source_sites, self.crab_id, filenames, self.reqname, self.output_data, self.log_size, \
                               self.log_needs_transfer, self.job_report_output, self.task_ad, self.crab_retry_count, self.retry_timeout, self.cmsrun_failed)
        job_result = ASO_JOB.run()
        ## If no files failed, return success immediately.
        if not job_result:
            return job_result

        ## Retrieve the stageout failures (a dictionary where the keys are the IDs of
        ## the ASO documents for which the stageout job failed and the values are the
        ## failure reason(s)). Determine which failures are permament stageout errors
        ## and which ones are recoverable stageout errors.
        failures = ASO_JOB.getFailures()
        ASO_JOB = None
        num_failures = len(failures)
        num_permanent_failures = 0
        for doc_id in failures.keys():
            if type(failures[doc_id]['reasons']) == str:
                failures[doc_id]['reasons'] = [failures[doc_id]['reasons']]
            if type(failures[doc_id]['reasons']) == list:
                last_failure_reason = failures[doc_id]['reasons'][-1]
                if isFailurePermanent(last_failure_reason, self.task_ad):
                    num_permanent_failures += 1
                    failures[doc_id]['severity'] = 'permanent'
                else:
                    failures[doc_id]['severity'] = 'recoverable'
        ## Message for stageout error exception.
        msg = "Stageout failed with code %d.\nThere were %d failed/killed stageout jobs." % (job_result, num_failures)
        if num_permanent_failures:
            msg += " %d of those jobs had a permanent failure." % num_permanent_failures
        msg += "\nFailure reasons (per document) follow:"
        for doc_id in failures.keys():
            msg += "\n- %s:" % doc_id
            if failures[doc_id]['app']:
                msg += "\n  === %s log start ===" % str(failures[doc_id]['app']).upper()
            msg += "\n  %s" % failures[doc_id]['reasons']
            if failures[doc_id]['app']:
                msg += "\n  === %s log finish ===" % str(failures[doc_id]['app']).upper()
            if failures[doc_id]['severity']:
                msg += "\n  The last failure reason is %s." % str(failures[doc_id]['severity']).lower()

        ## Raise stageout exception.
        if num_permanent_failures:
            raise PermanentStageoutError(msg)
        else:
            raise RecoverableStageoutError(msg)


    def makeNodeMap(self):
        phedex = PhEDEx.PhEDEx()
        nodes = phedex.getNodeMap()['phedex']['node']
        for node in nodes:
            self.node_map[str(node[u'se'])] = str(node[u'name'])


    def calculateCRABRetryCount(self, job_id, dag_retry_count):
        """
        Calculate the retry number we're on.  See the notes in pre-job.
        """
        fname = "retry_info/job.%s.txt" % job_id
        if os.path.exists(fname):
            try:
                with open(fname, "r") as fd:
                    retry_info = json.load(fd)
            except:
                return dag_retry_count
        else:
            retry_info = {"pre": 0, "post": 0}
        if 'pre' not in retry_info or 'post' not in retry_info:
            return dag_retry_count
        crab_retry_count = str(retry_info['post'])
        retry_info['post'] += 1
        try:
            with open(fname + ".tmp", "w") as fd:
                json.dump(retry_info, fd)
            os.rename(fname + ".tmp", fname)
        except:
            return crab_retry_count
        return crab_retry_count


    def execute(self, *args, **kw):
        job_id = args[7]
        self.dag_retry_count = args[2]
        self.crab_retry_count = self.calculateCRABRetryCount(job_id, self.dag_retry_count)
        reqname = args[6]

        logpath = os.path.expanduser("~/%s" % reqname)
        try:
            os.makedirs(logpath)
        except OSError, ose:
            if ose.errno != errno.EEXIST:
                logger.exception("Failed to create log web-shared directory %s" % logpath)
                raise

        postjob_log_file_name = os.path.join(logpath, "postjob.%s.%s.txt" % (job_id, self.crab_retry_count))
        logger.debug("The post-job script will be saved to %s" % postjob_log_file_name)

        fd = os.open(postjob_log_file_name, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0644)
        os.chmod(postjob_log_file_name, 0644)
        if not os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            os.dup2(fd, 1)
            os.dup2(fd, 2)
            logger.info("Post-job started with output redirected to %s." % postjob_log_file_name)
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

        #Now preparing the error report. Enclosing it in a try except as we don't want to fail jobs because this fails
        try:
            prepareErrorSummary(reqname)
        except:
            logger.exception("Unknown error while preparing the error report")

        return self.check_abort_dag(retval)


    def check_abort_dag(self, rval):
        """
        For each job that failed with a fatal error, its id is written into the file
        task_statistics.FATAL_ERROR. For each job that didn't fail with a fatal or
        recoverable error, its id is written into the file task_statistics.OK. Notice
        that these files may contain repeated job ids. Besed on these statistics, we
        may decide to abort the whole DAG.
        """
        file_name_jobs_fatal = "task_statistics.FATAL_ERROR"
        file_name_jobs_ok    = "task_statistics.OK"
        ## Return code 3 is reserved to abort the entire DAG. Don't let the code
        ## otherwise use it.
        if rval == 3:
            rval = 1
        if 'CRAB_FailedNodeLimit' not in self.task_ad or self.task_ad['CRAB_FailedNodeLimit'] == -1:
            return rval
        try:
            limit = int(self.task_ad['CRAB_FailedNodeLimit'])
            fatal_failed_jobs = []
            with open(file_name_jobs_fatal, "r") as fd:
                for job_id in fd.readlines():
                    if job_id not in fatal_failed_jobs:
                        fatal_failed_jobs.append(job_id)
            num_fatal_failed_jobs = len(fatal_failed_jobs)
            successful_jobs = []
            with open(file_name_jobs_ok, "r") as fd:
                for job_id in fd.readlines():
                    if job_id not in successful_jobs:
                        successful_jobs.append(job_id)
            num_successful_jobs = len(successful_jobs)
            if (num_successful_jobs + num_fatal_failed_jobs) > limit:
                if num_fatal_failed_jobs > num_successful_jobs:
                    msg  = "There are %d (fatal) failed nodes and %d successful nodes," % (num_fatal_failed_jobs, num_successful_jobs)
                    msg += " adding up to a total of %d (more than the limit of %d)." % (num_successful_jobs + num_fatal_failed_jobs, limit)
                    msg += " The ratio of failed to successful is greater than 1."
                    msg += " Will abort the whole DAG"
                    logger.error(msg)
                    rval = 3
        finally:
            return rval


    def execute_internal(self, cluster, status, dag_retry_count, max_retries, restinstance, \
                         resturl, reqname, job_id, outputdata, job_sw, async_dest, source_dir, dest_dir, *filenames):
        self.reqname = reqname
        self.output_data = outputdata
        stdout = "job_out.%s" % job_id
        stdout_tmp = "job_out.tmp.%s" % job_id
        global JOB_REPORT_NAME
        JOB_REPORT_NAME = "jobReport.json.%s" % job_id

        logpath = os.path.expanduser("~/%s" % reqname)
        if os.path.exists(stdout):
            os.rename(stdout, stdout_tmp)
            fname = os.path.join(logpath, "job_out."+job_id+"."+self.crab_retry_count+".txt")
            logger.debug("Copying job stdout from %s to %s." % (stdout, fname))
            shutil.copy(stdout_tmp, fname)
            stdout_f = open(stdout_tmp, 'w')
            stdout_f.truncate(0)
            stdout_f.close()
            os.chmod(fname, 0644)
        # NOTE: we now redirect stdout -> stderr; hence, we don't keep stderr in the webdir.
        if os.path.exists(JOB_REPORT_NAME):
            fname = os.path.join(logpath, "job_fjr."+job_id+"."+self.crab_retry_count+".json")
            logger.debug("Copying job FJR from %s to %s." % (JOB_REPORT_NAME, fname))
            shutil.copy(JOB_REPORT_NAME, fname)
            os.chmod(fname, 0644)

        if 'X509_USER_PROXY' not in os.environ:
            logger.error("Failure due to X509_USER_PROXY not being present in environment.")
            return 10

        self.server = HTTPRequests(restinstance, os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY'])
        self.resturl = resturl

        logger.info("Post-job was asked to transfer up to %d files." % len(filenames))

        self.makeAd(reqname, job_id, outputdata, job_sw, async_dest)
        if self.getTaskAd() == 2 or not self.task_ad:
            self.uploadState("FAILED")
            return RetryJob.FATAL_ERROR
        if 'CRAB_UserWebDir' in self.task_ad:
            log_files_aux = [("job_out", "txt"), ("job_fjr", "json"), ("postjob", "txt")]
            for log_file_basename, log_file_extension in log_files_aux:
                log_file_name = "%s/%s.%s.%s.%s" % (self.task_ad['CRAB_UserWebDir'], log_file_basename, \
                                                    str(job_id), str(self.crab_retry_count), log_file_extension)
                self.log_files.append(log_file_name)

        self.makeNodeMap()

        status = int(status)

        msg  = "This is job retry number %s." % self.dag_retry_count
        msg += " The maximum allowed number of retries is %s." % max_retries
        logger.info(msg)

        fail_state = "COOLOFF"
        if self.dag_retry_count == max_retries:
            fail_state = "FAILED"
        if status and (self.dag_retry_count == max_retries):
            # This was our last retry and it failed.
            logger.info("The maximum allowed number of retries was hit and the job failed. Setting this node to permanent failure.")
            return self.uploadState("FAILED")
        retry = RetryJob.RetryJob()
        retval = None
        if not os.environ.get('TEST_POSTJOB_DISABLE_RETRIES', False):
            retval = retry.execute(reqname, status, self.crab_retry_count, max_retries, self.crab_id, cluster)
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

        self.parseJobReport()
        self.source_site = self.getSourceSite()

        self.fixPerms()
        try:
            self.uploadLog(dest_dir, filenames[0])
            self.stageout(source_dir, dest_dir, *filenames)
            try:
                if 'CRAB_NoWNStageout' not in self.task_ad:
                    self.upload()
            except HTTPException, hte:
                # Suppressing this exception is a tough decision.  If the file made it back alright,
                # I suppose we can proceed.
                logger.exception("Potentially fatal error when uploading file locations: %s" % str(hte.headers))
                if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
                        not hte.headers.get('X-Error-Http', -1) == '400':
                    raise
        except PermanentStageoutError, pse:
            logger.error(str(pse) + "\nThere was at least one permanent stageout error; user will need to resubmit.")
            self.uploadState("FAILED")
            return RetryJob.FATAL_ERROR
        except RecoverableStageoutError, rse:
            logger.error(str(rse) + "\nThese are all recoverable stageout errors; automatic resubmit is possible.")
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
        self.postjob = PostJob()
        #self.job = ASOServerJob()
        #status, crab_retry_count, max_retries, restinstance, resturl, reqname, id,
        #outputdata, job_sw, async_dest, source_dir, dest_dir, *filenames
        self.full_args = ['0', 0, 2, 'restinstance', 'resturl',
                          'reqname', 1234, 'outputdata', 'sw', 'T2_US_Vanderbilt']
        self.json_name = "jobReport.json.%s" % self.full_args[6]
        open(self.json_name, 'w').write(json.dumps(self.generateJobJson()))


    def makeTempFile(self, size, pfn):
        fh, path = tempfile.mkstemp()
        try:
            inputString = "CRAB3POSTJOBUNITTEST"
            os.write(fh, (inputString * ((size/len(inputString))+1))[:size])
            os.close(fh)
            cmd = "env -u LD_LIBRAY_PATH lcg-cp -b -D srmv2 -v file://%s %s" % (path, pfn)
            print cmd
            status, res = commands.getstatusoutput(cmd)
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
        self.full_args.extend(['/store/temp/user/meloam/CRAB3-UNITTEST-NONEXISTENT/b/c/',
                               '/store/user/meloam/CRAB3-UNITTEST-NONEXISTENT/b/c',
                               self.getUniqueFilename()])
        self.assertNotEqual(self.postjob.execute(*self.full_args), 0)

    source_prefix = "srm://dcache07.unl.edu:8443/srm/v2/server?SFN=/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms"
    def testExistent(self):
        source_dir  = "/store/temp/user/meloam/CRAB3-UnitTest/%s/%s" % \
                        (self.getLevelOneDir(), self.getLevelTwoDir())
        source_file = self.getUniqueFilename()
        source_lfn  = "%s/%s" % (source_dir, source_file)
        dest_dir = source_dir.replace("temp/user", "user")
        self.makeTempFile(200, "%s/%s" %(self.source_prefix, source_lfn))
        self.full_args.extend([source_dir, dest_dir, source_file])
        self.assertEqual(self.postjob.execute(*self.full_args), 0)

    def tearDown(self):
        if os.path.exists(self.json_name):
            os.unlink(self.json_name)

if __name__ == '__main__':
    if len(sys.argv) >= 2 and sys.argv[1] == 'UNIT_TEST':
        sys.argv = [sys.argv[0]]
        print "Beginning testing"
        unittest.main()
        print "Testing over"
        sys.exit()
    POSTJOB = PostJob()
    sys.exit(POSTJOB.execute(*sys.argv[2:]))

