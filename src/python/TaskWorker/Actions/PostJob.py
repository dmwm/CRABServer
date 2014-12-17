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
 u'temp_storage_site': u'T2_US_Nebraska',
 u'storage_site': u'T2_CH_CERN',
 u'pfn': u'dumper.root',
 u'checksums': {u'adler32': u'47d823c0', u'cksum': u'640736586'},
 u'size': 3582626,
 u'local_stageout': True,
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
 u'temp_storage_site': u'T2_US_Nebraska',
 u'storage_site': u'T2_CH_CERN',
 u'pfn': u'/tmp/1882789.vmpsched/glide_Paza70/execute/dir_27876/histo.root',
 u'checksums': {u'adler32': u'e8ed4a12', u'cksum': u'2439186609'},
 u'size': 360,
 u'local_stageout': True,
 u'direct_stageout': False,
 u'fileName': u'/tmp/1882789.vmpsched/glide_Paza70/execute/dir_27876/histo.root',
 u'Source': u'TFileService'
}
For other type of output files, we add in cmscp.py the basic necessary info
about the file to the FJR. The information is:
{
 u'temp_storage_site': u'T2_US_Nebraska',
 u'storage_site': u'T2_CH_CERN',
 u'pfn': u'out.root',
 u'size': 45124,
 u'local_stageout': True,
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
import WMCore.Database.CMSCouch as CMSCouch
from RESTInteractions import HTTPRequests ## Why not to use from WMCore.Services.Requests import Requests
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
G_JOB_REPORT_NAME = None

## Auxiliary variable to make code more readable in if statements for example.
OK = True

def sighandler(*args):
    if ASO_JOB:
        ASO_JOB.cancel()

signal.signal(signal.SIGHUP,  sighandler)
signal.signal(signal.SIGINT,  sighandler)
signal.signal(signal.SIGTERM, sighandler)

##==============================================================================

def prepareErrorSummary(reqname):#, job_id, crab_retry):
    """ Load the current error_summary.json and do the equivalent of an 'ls job_fjr.*.json' to get the completed jobs.
        Then, add the error reason of the job that are not in errorReport.json
    """

    logger.info("====== Starting to prepare error report.")
    #open and load an already existing error summary
    error_summary = {}
    try:
        with open('error_summary.json') as fsummary:
            error_summary = json.load(fsummary)
    except (IOError, ValueError):
        #there is nothing to do if the errorSummary file does not exist or is invalid. Just recreate it
        logger.info("File error_summary.json is empty, wrong or does not exist. Will create a new file.")

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
    logpath = os.path.expanduser("~/%s" % (reqname))
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
                if not 'errors'   in rep['steps']['cmsRun']: logger.info("'errors' key not found in report['steps']['cmsRun']"); raise
                if rep['steps']['cmsRun']['errors']:
                    if len(rep['steps']['cmsRun']['errors']) != 1:
                        #this should never happen because the report has just one step, but just in case print a message
                        logger.info("more than one error found in the job, just considering the first one")
                    error_summary.setdefault(job_id, {})[crab_retry] = (exit_code, rep['exitMsg'], rep['steps']['cmsRun']['errors'][0])
                else:
                    error_summary.setdefault(job_id, {})[crab_retry] = (exit_code, rep['exitMsg'], {})
            except Exception, exmsg:
                logger.info(str(exmsg))
                if not rep:
                    msg = 'Invalid framework job report. The framework job report exists, but it cannot be loaded.'# % (job_id, crab_retry)
                else:
                    msg = rep['exitMsg'] if 'exitMsg' in rep else 'The framework job report could be loaded but no error message found there'
                error_summary.setdefault(job_id, {})[crab_retry] = (exit_code, msg, {})

    #write the file. Use a temporary file and rename to avoid concurrent writing of the file
    tmp_fname = "error_summary.%d.json" % (os.getpid())
    with open(tmp_fname, "w") as fsummary:
        json.dump(error_summary, fsummary)
    os.rename(tmp_fname, "error_summary.json")
    logger.info("====== Finished to prepare error report.")

##==============================================================================

class ASOServerJob(object):
    """
    Class used to inject transfer requests to ASO database.
    """

    def __init__(self, dest_site, source_dir, dest_dir, source_sites, count, \
                 filenames, reqname, log_size, log_needs_transfer, \
                 job_report_output, job_ad, crab_retry_count, retry_timeout, \
                 job_failed):
        """
        ASOServerJob constructor.
        """
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
        self.job_failed = job_failed
        self.source_sites = source_sites
        self.filenames = filenames
        self.reqname = reqname
        self.job_report_output = job_report_output
        self.log_size = log_size
        self.log_needs_transfer = log_needs_transfer
        self.job_ad = job_ad
        self.failures = {}
        self.aso_start_timestamp = None
        proxy = os.environ.get('X509_USER_PROXY', None)
        self.aso_db_url = self.job_ad['CRAB_ASOURL']
        try:
            logger.info("Will use ASO server at %s." % (self.aso_db_url))
            self.couch_server = CMSCouch.CouchServer(dburl = self.aso_db_url, ckey = proxy, cert = proxy)
            self.couch_database = self.couch_server.connectDatabase("asynctransfer", create = False)
        except:
            logger.error("Failed to connect to ASO database.")
            raise

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def run(self):
        """
        This is the main method in ASOServerJob. Should be called after initializing
        an instance.
        """
        self.doc_ids = self.inject_to_aso()
        if self.doc_ids == False:
            exmsg = "Couldn't upload document to ASO database"
            raise RuntimeError, exmsg
        if not self.doc_ids:
            logger.info("No files to transfer via ASO. Done!")
            return 0
        failed_killed_transfers = []
        done_transfers = []
        starttime = time.time()
        if self.aso_start_timestamp:
            starttime = self.aso_start_timestamp
        logger.info("====== Starting to monitor ASO transfers.")
        while True:
            transfers_statuses = self.get_transfers_statuses()
            msg = "Got statuses: %s; %.1f hours since transfer submit." 
            msg = msg % (", ".join(transfers_statuses), (time.time()-starttime)/3600.0)
            logger.info(msg)
            all_transfers_finished = True
            for transfer_status, doc_id in zip(transfers_statuses, self.doc_ids):
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
                        try:
                            doc = self.couch_database.document(doc_id)
                        except:
                            logger.error("Failed to retrieve document for %s." % (doc_id))
                            doc = {}
                        if ('failure_reason' in doc) and doc['failure_reason']:
                            ## reasons:  The transfer failure reason(s).
                            ## app:      The application that gave the transfer failure reason(s).
                            ##           E.g. 'aso' or '' (meaning the postjob). When printing the
                            ##           transfer failure reasons (e.g. below), print also that the
                            ##           failures come from the given app (if app != '').
                            ## severity: Either 'permanent' or 'recoverable'.
                            ##           It is set by PostJob in the perform_transfers() function,
                            ##           when is_failure_permanent() is called to determine if a
                            ##           failure is permanent or not.
                            reasons, app, severity = doc['failure_reason'], 'aso', ''
                            msg += " Failure reasons follow:"
                            if app:
                                msg += "\n-----> %s log start -----" % str(app).upper()
                            msg += "\n%s" % reasons
                            if app:
                                msg += "\n<----- %s log finish ----" % str(app).upper()
                            logger.error(msg)
                        else:
                            reasons, app, severity = 'Failure reason unavailable.', '', ''
                            logger.error(msg)
                            logger.warning("WARNING: no failure reason available.")
                        self.failures[doc_id] = {'reasons': reasons, 'app': app, 'severity': severity}
                else:
                    exmsg = "Got an unknown status: %s" % (transfer_status)
                    raise RuntimeError, exmsg
            if all_transfers_finished:
                msg = "All transfers finished. There were %s failed/killed transfers" % (len(failed_killed_transfers))
                if failed_killed_transfers:
                    msg += " (%s)" % ', '.join(failed_killed_transfers)
                    logger.info(msg)
                    logger.info("====== Finished to monitor ASO transfers.")
                    return 1
                else:
                    logger.info(msg)
                    logger.info("====== Finished to monitor ASO transfers.")
                    return 0
            ## If there is a timeout for transfers to complete, check if it was exceeded
            ## and if so kill the ongoing transfers. # timeout = -1 means no timeout.
            if self.retry_timeout != -1 and time.time() - starttime > self.retry_timeout:
                msg = "Killing ongoing ASO transfers after timeout of %d (seconds)." % (self.retry_timeout)
                logger.warning(msg)
                reason = "Killed ASO transfer after timeout of %d (seconds)." % (self.retry_timeout)
                for doc_id in self.doc_ids:
                    if doc_id not in done_transfers + failed_killed_transfers:
                        app, severity = '', ''
                        self.failures[doc_id] = {'reasons': reason, 'app': app, 'severity': severity}
                        self.cancel({doc_id: reason})
                logger.info("====== Finished to monitor ASO transfers.")
                return 1
            else:
                ## Sleep is done here in case if the transfer is done immediately (direct stageout case).
                time.sleep(self.sleep + random.randint(0, 60))

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def inject_to_aso(self):
        """
        Inject documents to ASO database if not done by cmscp from worker node.
        """
        logger.info("====== Starting to check uploads to ASO database.")
        all_ids = []
        output_files = []

        now = str(datetime.datetime.now())
        last_update = int(time.time())

        ## Get the aso_start_timestamp and aso_start_time from the job report (these
        ## flags are written by cmscp at the moment it does the first injection to ASO
        ## database; if cmscp fails to do the local transfers, it doesn't inject to ASO
        ## database and therefore it doesn't write the flags). If the flags are not in
        ## the job report, define them to the current time.
        aso_start_time = None
        try:
            with open(G_JOB_REPORT_NAME) as fd_job_report:
                job_report = json.load(fd_job_report)
            self.aso_start_timestamp = job_report.get("aso_start_timestamp")
            aso_start_time = job_report.get("aso_start_time")
        except Exception:
            self.aso_start_timestamp = last_update
            aso_start_time = now
            msg  = "Unable to determine ASO start time from job report."
            msg += " Will use ASO start time = %s (%s)."
            msg  = msg % (aso_start_time, self.aso_start_timestamp)
            logger.warning(msg)

        if str(self.job_ad['CRAB_UserRole']).lower() != 'undefined':
            role = str(self.job_ad['CRAB_UserRole'])
        else:
            role = ''
        if str(self.job_ad['CRAB_UserGroup']).lower() != 'undefined':
            group = str(self.job_ad['CRAB_UserGroup'])
        else:
            group = ''

        task_publish = int(self.job_ad['CRAB_Publish'])

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
        transfer_outputs = int(self.job_ad['CRAB_TransferOutputs'])
        if not transfer_outputs:
            logger.debug("Transfer outputs flag is false; skipping outputs stageout.")
        transfer_logs = int(self.job_ad['CRAB_SaveLogsFlag'])
        if not transfer_logs:
            logger.debug("Transfer logs flag is false; skipping logs stageout.")
        found_log = False
        for source_site, filename in zip(self.source_sites, self.filenames):
            ## We assume that the first file in self.filenames is the logs archive.
            if found_log:
                if not transfer_outputs:
                    continue
                source_lfn = os.path.join(self.source_dir, filename)
                dest_lfn = os.path.join(self.dest_dir, filename)
                file_type = 'output'
                ifile = get_file_index(filename, output_files)
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
                source_lfn = os.path.join(self.source_dir, 'log', filename)
                dest_lfn = os.path.join(self.dest_dir, 'log', filename)
                file_type = 'log'
                size = self.log_size
                checksums = {'adler32': 'abc'}
                needs_transfer = self.log_needs_transfer
            logger.info("Working on file %s" % (filename))
            doc_id = hashlib.sha224(source_lfn).hexdigest()
            doc_new_info = {'state'           : 'new',
                            'source'          : source_site,
                            'destination'     : self.dest_site,
                            'checksums'       : checksums,
                            'size'            : size,
                            'last_update'     : last_update,
                            'start_time'      : now,
                            'end_time'        : '',
                            'job_end_time'    : time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                            'retry_count'     : [],
                            'failure_reason'  : [],
                            'job_retry_count' : self.crab_retry_count,
                           }
            if not needs_transfer:
                msg  = "File %s is marked as having been directly staged out"
                msg += " from the worker node to the permanent storage."
                msg  = msg % (filename)
                logger.info(msg)
                doc_new_info['state'] = 'done'
                doc_new_info['end_time'] = now
            ## Set the publication flag.
            publication_msg = None
            if file_type == 'output':
                publish = task_publish
                if publish and self.job_failed:
                    publication_msg  = "Disabling publication of output file %s,"
                    publication_msg += " because job is marked as failed."
                    publication_msg  = publication_msg % (filename)
                    publish = 0
                if publish and file_output_type != 'EDM':
                    publication_msg  = "Disabling publication of output file %s,"
                    publication_msg += " because it is not of EDM type."
                    publication_msg  = publication_msg % (filename)
                    publish = 0
            else:
                ## This is the log file, so obviously publication should be turned off.
                publish = 0
            ## What does ASO needs to do for this file (transfer and/or publication) is
            ## saved in this list for the only purpose of printing a better message later.
            aso_tasks = []
            if needs_transfer:
                aso_tasks.append("transfer")
            if publish:
                aso_tasks.append("publication")
            if not (needs_transfer or publish):
                ## This file doesn't need transfer nor publication, so we don't need to upload
                ## a document to ASO database.
                if publication_msg:
                    logger.info(publication_msg)
                msg  = "File %s doesn't need transfer nor publication."
                msg += " No need to inject a document to ASO."
                msg  = msg % (filename)
                logger.info(msg)
            else:
                ## This file needs transfer and/or publication. If a document (for the current
                ## job retry) is not yet in ASO database, we need to do the upload.
                needs_commit = True
                try:
                    doc = self.couch_database.document(doc_id)
                    ## The document was already uploaded to ASO database. It could have been
                    ## uploaded from the WN in the current job retry or in a previous job retry,
                    ## or by the postjob in a previous job retry.
                    transfer_status = doc.get('state')
                    if doc.get('start_time') == aso_start_time:
                        ## The document was uploaded from the WN in the current job retry, so we don't
                        ## upload a new document. (If the transfer is done or ongoing, then of course we
                        ## don't want to re-inject the transfer request. OTOH, if the transfer has
                        ## failed, we don't want the postjob to retry it; instead the postjob will exit
                        ## and the whole job will be retried).
                        msg  = "LFN %s (id %s) is already in ASO database"
                        msg += " (it was injected from the worker node in the current job retry)"
                        msg += " and file transfer status is '%s'."
                        msg  = msg % (source_lfn, doc_id, transfer_status)
                        logger.info(msg)
                        needs_commit = False
                    else:
                        ## The document was uploaded in a previous job retry. This means that in the
                        ## current job retry the injection from the WN has failed or cmscp did a direct
                        ## stageout. We upload a new stageout request, unless the transfer is still
                        ## ongoing (which should actually not happen, unless the postjob for the
                        ## previous job retry didn't run).
                        msg  = "LFN %s (id %s) is already in ASO database (file transfer status is '%s'),"
                        msg += " but does not correspond to the current job retry."
                        msg  = msg % (source_lfn, doc_id, transfer_status)
                        if transfer_status in ['acquired', 'new', 'retry']:
                            msg += "\nFile transfer status is not terminal ('done', 'failed' or 'killed')."
                            msg += " Will not inject a new document for the current job retry."
                            logger.info(msg)
                            needs_commit = False
                        else:
                            msg += " Will inject a new %s request." % (' and '.join(aso_tasks))
                            logger.info(msg)
                            msg = "Previous document: %s" % (pprint.pformat(doc))
                            logger.debug(msg)
                except CMSCouch.CouchNotFoundError:
                    ## The document was not yet uploaded to ASO database (if this is the first job
                    ## retry, then either the upload from the WN failed, or cmscp did a direct
                    ## stageout and here we need to inject for publication only). In any case we
                    ## have to inject a new document.
                    msg  = "LFN %s (id %s) is not in ASO database."
                    msg += " Will inject a new %s request."
                    msg  = msg % (source_lfn, doc_id, ' and '.join(aso_tasks))
                    logger.info(msg)
                    if publication_msg:
                        logger.info(publication_msg)
                    doc = {'_id'                     : doc_id,
                           'inputdataset'            : str(self.job_ad['CRAB_InputData']),
                           'rest_host'               : str(self.job_ad['CRAB_RestHost']),
                           'rest_uri'                : str(self.job_ad['CRAB_RestURInoAPI']),
                           'lfn'                     : source_lfn,
                           'source_lfn'              : source_lfn,
                           'destination_lfn'         : dest_lfn,
                           'checksums'               : checksums,
                           'user'                    : str(self.job_ad['CRAB_UserHN']),
                           'group'                   : group,
                           'role'                    : role,
                           'dbs_url'                 : str(self.job_ad['CRAB_DBSUrl']),
                           'workflow'                : self.reqname,
                           'jobid'                   : self.count,
                           'publication_state'       : 'not_published',
                           'publication_retry_count' : [],
                           'type'                    : file_type,
                           'publish'                 : publish,
                          }
                    if not needs_transfer:
                        doc['lfn'] = source_lfn.replace('/store/temp', '/store', 1)
                        doc['source_lfn'] = source_lfn.replace('/store/temp', '/store', 1)
                except Exception, exmsg:
                    msg = "Got exception while trying to load the document from ASO database: %s" % (str(exmsg))
                    try:
                        msg += "\n%s" % (traceback.format_exc())
                    except AttributeError:
                        msg += "\nTraceback unavailable."
                    logger.error(msg)
                    return False
                ## If after all we need to upload a new document to ASO database, let's do it.
                if needs_commit:
                    doc.update(doc_new_info)
                    msg = "ASO job description: %s" % (pprint.pformat(doc))
                    logger.info(msg)
                    commit_result_msg = self.couch_database.commitOne(doc)[0]
                    if 'error' in commit_result_msg:
                        msg = "Got error injecting document to ASO database:\n%s" % (commit_result_msg)
                        logger.info(msg)
                        return False
                ## Record all files for which we want the post-job to monitor their transfer.
                if needs_transfer:
                    all_ids.append(doc_id)

        logger.info("====== Finished to check uploads to ASO database.")

        return all_ids

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_transfers_statuses(self):
        """
        Retrieve the status of all transfers from the cached file 'aso_status.json'
        or by querying an ASO database view if the file is more than 5 minutes old
        or if we injected a document after the file was last updated. Otherwise call
        get_transfers_statuses_fallback().
        """
        query_view = False
        if not os.path.exists("aso_status.json"):
            query_view = True
        aso_info = {}
        if not query_view:
            query_view = True
            try:
                with open("aso_status.json") as fd_aso_status:
                    aso_info = json.load(fd_aso_status)
            except:
                logger.exception("Failed to load common ASO status.")
                return self.get_transfers_statuses_fallback()
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
                return self.get_transfers_statuses_fallback()
            aso_info = {"query_timestamp": time.time(), "results": states_dict}
            tmp_fname = "aso_status.%d.json" % (os.getpid())
            with open(tmp_fname, 'w') as fd_aso_status:
                json.dump(aso_info, fd_aso_status)
            os.rename(tmp_fname, "aso_status.json")
        if not aso_info:
            return self.get_transfers_statuses_fallback()
        statuses = []
        for doc_id in self.doc_ids:
            if doc_id not in aso_info.get("results", {}):
                return self.get_transfers_statuses_fallback()
            statuses.append(aso_info['results'][doc_id]['value']['state'])
        return statuses

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_transfers_statuses_fallback(self):
        """
        Retrieve the status of all transfers by loading the correspnding documents
        from ASO database and checking the 'state' field.
        """
        logger.debug("Querying transfer status using fallback method.")
        statuses = []
        for doc_id in self.doc_ids:
            doc = self.couch_database.document(doc_id)
            statuses.append(doc['state'])
        return statuses

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def cancel(self, doc_ids_reasons = None):
        """
        Method used to "cancel/kill" ASO transfers. The only thing that this
        function does is to put the 'state' field of the corresponding documents in
        ASO database to 'killed' (and the 'end_time' field to the current time).
        Killing actual FTS transfers (if possible) is left to ASO.
        """
        now = str(datetime.datetime.now())
        if not doc_ids_reasons:
            for doc_id in self.doc_ids:
                doc_ids_reasons[doc_id] = ''
        for doc_id, reason in doc_ids_reasons.iteritems():
            msg = "Cancelling ASO data transfer %s" % (doc_id)
            if reason:
                msg += " with following reason: %s" % (reason)
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
                exmsg = "Got error killing ASO data transfer %s: %s" % (doc_id, res)
                raise RuntimeError, exmsg

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_failures(self):
        """
        Retrieve failures bookeeping dictionary.
        """
        return self.failures

##==============================================================================

class PostJob():
    """
    Executes on schedd once for each job.
    Checks the job exit status and determines if an error is recoverable or fatal.
    Checks for file transfers (uploading transfer requests to ASO database if
    not done already by the job wrapper from the worker node).
    Uploads logs archive and output files metadata (if not done already by the
    job wrapper from the worker node).
    Finally, it aborts the whole task in case of too many failed jobs.
    """
    def __init__(self):
        """
        PostJob constructor.
        """
        ## These are set from arguments to PostJob (see RunJobs.dag file in the schedd).
        ## We set them in execute(). Is the very first thing we do.
        self.cluster             = None
        self.job_status          = None
        self.dag_retry_count     = None
        self.max_retries         = None
        self.reqname             = None
        self.job_id              = None
        self.source_dir          = None
        self.dest_dir            = None
        self.logs_arch_file_name = None
        self.output_files_names  = None
        ## This is the number of times the post-job was ran for this job.
        self.crab_retry_count    = None
        ## These are read from the job ad file in the schedd (see parse_job_ad()).
        self.job_ad              = {}
        self.dest_site           = None
        self.input_dataset       = None
        self.job_sw              = None
        self.publish_name        = None
        self.rest_host           = None
        self.rest_uri_no_api     = None
        self.retry_timeout       = None
        ## These are read from the job report (see parse_job_report()).
        self.job_report          = {}
        self.job_report_output   = {}
        self.log_needs_transfer  = None
        self.log_needs_file_metadata_upload = None
        self.log_size            = None
        self.executed_site       = None
        self.job_failed          = None
        self.output_files_info   = []
        ## Object we will use for making requests to the REST interface (uploading logs
        ## archive and output files matadata).
        self.server              = None

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute(self, *args, **kw):
        """
        The execute method of PostJob.
        """
        ## Make sure there are enough arguments passed to PostJob.
        if len(args) < 9:
            msg = "PostJob expects at least 9 arguments; %d were given."
            msg = msg % (len(args))
            logger.error(msg)
            return 2
        ## Put the arguments to PostJob into class variables.
        self.cluster             = args[0]
        self.job_status          = args[1]
        self.dag_retry_count     = int(args[2])
        self.max_retries         = int(args[3])
        ## TODO: Why not get the request name from the job ad?
        ## We will need to parse the job ad earlier, that's all.
        self.reqname             = args[4]
        self.job_id              = int(args[5])
        self.source_dir          = args[6]
        self.dest_dir            = args[7]
        self.logs_arch_file_name = args[8]
        ## TODO: We can get the output files from the job ad where we have
        ## CRAB_EDMOutputFiles, CRAB_TFileOutputFiles and CRAB_AdditionalOutputFiles.
        ## (We only need to add the job_id in the file names.)
        self.output_files_names  = []
        for i in xrange(9, len(args)):
            self.output_files_names.append(args[i])
        self.job_status = int(self.job_status)
        self.crab_retry_count = self.calculate_crab_retry_count()
        if self.crab_retry_count is None:
            self.crab_retry_count = self.dag_retry_count

        ## Create the user's task web directory in the schedd. 
        logpath = os.path.expanduser("~/%s" % (self.reqname))
        try:
            os.makedirs(logpath)
        except OSError, ose:
            if ose.errno != errno.EEXIST:
                msg = "Failed to create log web-shared directory %s"
                msg = msg % (logpath)
                logger.exception(msg)
                raise

        ## Create (open) the post-job log file postjob.<jobid>.<crabretrycount>.txt.
        postjob_log_file_name = os.path.join(logpath, 'postjob.%d.%d.txt' % (self.job_id, self.crab_retry_count))
        fd = os.open(postjob_log_file_name, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0644)
        os.chmod(postjob_log_file_name, 0644)
        if os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            msg = "Post-job started with no output redirection."
            logger.info(msg)
        else:
            os.dup2(fd, 1)
            os.dup2(fd, 2)
            msg = "Post-job started with output redirected to %s."
            msg = msg % (postjob_log_file_name)
            logger.info(msg)

        ## Call execute_internal().
        retval = 1
        try:
            retval = self.execute_internal()
            msg = "Post-job finished executing with status code %d."
            msg = msg % (retval)
            logger.info(msg)
        except:
            msg = "Failure during post-job execution."
            logger.exception(msg)
        finally:
            DashboardAPI.apmonFree()

        ## Prepare the error report. Enclosing it in a try except as we don't want to
        ## fail jobs because this fails.
        try:
            prepareErrorSummary(self.reqname)
        except:
            msg = "Unknown error while preparing the error report."
            logger.exception(msg)

        ## Decide if the whole task should be aborted (in case a significant fraction of
        ## the jobs has failed).
        retval = self.check_abort_dag(retval)

        return retval

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute_internal(self):
        """
        The execute method calls this execute_internal method. All post-job actions
        (injections to ASO database, monitoring transfers, file metadata upload)
        are done from here.
        """

        ## Copy the job's stdout file job_out.<jobid> to the schedd web directory,
        ## naming it job_out.<jobid>.<crabretrycount>.txt.
        stdout = "job_out.%d" % (self.job_id)
        stdout_tmp = "job_out.tmp.%d" % (self.job_id)
        logpath = os.path.expanduser("~/%s" % (self.reqname))
        if os.path.exists(stdout):
            os.rename(stdout, stdout_tmp)
            fname = 'job_out.'+str(self.job_id)+'.'+str(self.crab_retry_count)+'.txt'
            fname = os.path.join(logpath, fname)
            msg = "Copying job stdout from %s to %s."
            msg = msg % (stdout, fname)
            logger.debug(msg)
            shutil.copy(stdout_tmp, fname)
            fd_stdout = open(stdout_tmp, 'w')
            fd_stdout.truncate(0)
            fd_stdout.close()
            os.chmod(fname, 0644)

        ## Copy the json job report file jobReport.json.<jobid> to the schedd web
        ## directory, naming it job_fjr.<jobid>.<crabretrycount>.json.
        ## NOTE: We now redirect stdout -> stderr; hence, we don't keep stderr in
        ## the webdir.
        global G_JOB_REPORT_NAME
        G_JOB_REPORT_NAME = "jobReport.json.%d" % (self.job_id)
        if os.path.exists(G_JOB_REPORT_NAME):
            fname = 'job_fjr.'+str(self.job_id)+'.'+str(self.crab_retry_count)+'.json'
            fname = os.path.join(logpath, fname)
            logger.debug("Copying job report from %s to %s." % (G_JOB_REPORT_NAME, fname))
            shutil.copy(G_JOB_REPORT_NAME, fname)
            os.chmod(fname, 0644)

        ## Print a message about what job retry number are we on.
        msg  = "This is job retry number %d." % (self.dag_retry_count)
        msg += " The maximum allowed number of retries is %d." % (self.max_retries)
        logger.info(msg)

        ## Make sure the location of the user's proxy file is set in the environment.
        if 'X509_USER_PROXY' not in os.environ:
            msg = "X509_USER_PROXY is not present in environment."
            logger.error(msg)
            return 10

        ## Parse the job ad.
        job_ad_file_name = os.environ.get("_CONDOR_JOB_AD", ".job.ad")
        logger.info("====== Starting to parse job ad file %s." % (job_ad_file_name))
        parse_job_ad_status = self.parse_job_ad(job_ad_file_name)
        if parse_job_ad_status is not OK:
            self.set_dashboard_state('FAILED')
            logger.info("====== Finished to parse job ad.")
            return RetryJob.FATAL_ERROR
        logger.info("====== Finished to parse job ad.")

        logger.info("====== Starting to analyze job exit status.")
        ## Execute the retry-job. The retry-job decides whether an error is
        ## recoverable or fatal. It uses the cmscp exit code and error message,
        ## and the memory and cpu perfomance information; all from the job
        ## report. If the retry-job returns non 0 (meaning there was an error),
        ## report the state to dashboard and exit the post-job.
        ## This is for the case in which we forgot to update the exit code in
        ## the job report. I mean, it may be that the exit code in the job
        ## is 0, but actually the job failed.
        retry = RetryJob.RetryJob()
        retryjob_retval = None
        if not os.environ.get('TEST_POSTJOB_DISABLE_RETRIES', False):
            print "       -----> RetryJob log start -----"
            retryjob_retval = retry.execute(self.reqname, self.job_status, \
                                            self.crab_retry_count, self.job_id, \
                                            self.cluster)
            print "       <----- RetryJob log finish ----"
        if retryjob_retval:
            if retryjob_retval == RetryJob.FATAL_ERROR:
                msg = "The retry handler indicated this was a fatal error."
                logger.info(msg)
                self.set_dashboard_state('FAILED')
                logger.info("====== Finished to analyze job exit status.")
                return RetryJob.FATAL_ERROR
            elif retryjob_retval == RetryJob.RECOVERABLE_ERROR:
                if self.dag_retry_count >= self.max_retries:
                    msg  = "The retry handler indicated this was a recoverable error,"
                    msg += " but the maximum number of retries was already hit."
                    msg += " DAGMan will NOT retry."
                    logger.info(msg)
                    self.set_dashboard_state('FAILED')
                    logger.info("====== Finished to analyze job exit status.")
                    return RetryJob.FATAL_ERROR
                else:
                    msg  = "The retry handler indicated this was a recoverable error."
                    msg += " DAGMan will retry."
                    logger.info(msg)
                    self.set_dashboard_state('COOLOFF')
                    logger.info("====== Finished to analyze job exit status.")
                    return RetryJob.RECOVERABLE_ERROR
            else:
                msg  = "The retry handler returned an unexpected value (%d)."
                msg += " Will consider this as a fatal error. DAGMan will NOT retry."
                msg  = msg % (retryjob_retval)
                logger.info(msg)
                self.set_dashboard_state('FAILED')
                logger.info("====== Finished to analyze job exit status.")
                return RetryJob.FATAL_ERROR
        ## This is for the case in which we don't run the retry-job.
        elif self.job_status != 0:
            if self.dag_retry_count >= self.max_retries:
                msg  = "The maximum allowed number of retries was hit and the job failed."
                msg += " Setting this node (job) to permanent failure."
                logger.info(msg)
                self.set_dashboard_state('FAILED')
                logger.info("====== Finished to analyze job exit status.")
                return RetryJob.FATAL_ERROR
        else:
            logger.info("====== Finished to analyze job exit status.")
        ## If CRAB_ASOTimeout was not defined in the job ad, get here the ASO timeout
        ## from the retry-job.
        if self.retry_timeout is None:
            self.retry_timeout = retry.get_aso_timeout()

        ## Parse the job report.
        logger.info("====== Starting to parse job report file %s." % (G_JOB_REPORT_NAME))
        parse_job_report_status = self.parse_job_report()
        if parse_job_report_status is not OK:
            self.set_dashboard_state('FAILED')
            logger.info("====== Finished to parse job report.")
            return RetryJob.FATAL_ERROR
        logger.info("====== Finished to parse job report.")

        ## AndresT. We don't need this method IMHO. See note I made in the method.
        self.fix_job_logs_permissions()

        ## If the flag CRAB_NoWNStageout is set, we finish the post-job here.
        if int(self.job_ad.get('CRAB_NoWNStageout', 0)) != 0:
            msg  = "CRAB_NoWNStageout is set to %d in the job ad." % (int(self.job_ad.get('CRAB_NoWNStageout', 0)))
            msg += " Skipping files stageout and files metadata upload."
            msg += " Finishing post-job execution with exit code 0."
            logger.info(msg)
            self.set_dashboard_state('FINISHED')
            return 0

        ## Initialize the object we will use for making requests to the REST interface.
        self.server = HTTPRequests(self.rest_host, os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY'], retry = 2)

        ## Upload the logs archive file metadata if it was not already done from the WN.
        if self.log_needs_file_metadata_upload:
            logger.info("====== Starting upload of logs archive file metadata.")
            try:
                self.upload_log_file_metadata()
            except Exception, exmsg:
                msg = "Fatal error uploading logs archive file metadata: %s" % (str(exmsg)) 
                logger.error(msg)
                logger.info("====== Finished upload of logs archive file metadata.")
                return self.check_retry_count()
            logger.info("====== Finished upload of logs archive file metadata.")
        else:
            msg  = "Skipping logs archive file metadata upload, because it is marked as"
            msg += " having been uploaded already from the worker node."
            logger.info(msg)

        ## Just an info message.
        msg = "Preparing to transfer the logs archive file and %d output files."
        msg = msg % (len(self.output_files_names))
        logger.info(msg)

        ## Do the transfers (inject to ASO database if needed, and monitor the transfers
        ## statuses until they reach a terminal state).
        logger.info("====== Starting to check for ASO transfers.")
        try:
            self.perform_transfers()
        except PermanentStageoutError, pse:
            msg = "Got fatal stageout exception:\n%s" % (str(pse))
            logger.error(msg)
            msg = "There was at least one permanent stageout error; user will need to resubmit."
            logger.error(msg)
            self.set_dashboard_state('FAILED')
            logger.info("====== Finished to check for ASO transfers.")
            return RetryJob.FATAL_ERROR
        except RecoverableStageoutError, rse:
            msg = "Got recoverable stageout exception:\n%s" % (str(rse))
            logger.error(msg)
            msg = "These are all recoverable stageout errors; automatic resubmit is possible."
            logger.error(msg)
            logger.info("====== Finished to check for ASO transfers.")
            return self.check_retry_count()
        except:
            msg = "Stageout failure due to unknown issue."
            logger.exception(msg)
            logger.info("====== Finished to check for ASO transfers.")
            return self.check_retry_count()
        logger.info("====== Finished to check for ASO transfers.")

        ## Upload the output files metadata.
        logger.info("====== Starting upload of input/output files metadata.")
        try:
            self.upload_output_files_metadata()
            self.upload_input_files_metadata()
        except Exception, exmsg:
            msg = "Fatal error uploading input/output files metadata: %s" % (str(exmsg))
            logger.exception(msg)
            logger.info("====== Finished upload of input/output files metadata.")
            return self.check_retry_count()
        logger.info("====== Finished upload of input/output files metadata.")

        self.set_dashboard_state('FINISHED')

        return 0

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def perform_transfers(self):
        """
        Handle the transfer of all files from a source site and LFN to a destination
        site and LFN. We only use ASO so far (via ASOServerJob class, which injects
        documents to ASO database), but could eventually add another way of doing
        the transfers.
        """
        log_and_output_files_names = [self.logs_arch_file_name] + self.output_files_names
        source_sites = []
        for filename in log_and_output_files_names:
            ifile = get_file_index(filename, self.output_files_info)
            source_sites.append(self.get_file_source_site(filename, ifile))

        global ASO_JOB
        ASO_JOB = ASOServerJob(self.dest_site, self.source_dir, self.dest_dir, \
                               source_sites, self.job_id, log_and_output_files_names, \
                               self.reqname, self.log_size, \
                               self.log_needs_transfer, self.job_report_output, \
                               self.job_ad, self.crab_retry_count, \
                               self.retry_timeout, self.job_failed)
        aso_job_result = ASO_JOB.run()
        ## If no transfers failed, return success immediately.
        if not aso_job_result:
            return aso_job_result

        ## Retrieve the stageout failures (a dictionary where the keys are the IDs of
        ## the ASO documents for which the stageout job failed and the values are the
        ## failure reason(s)). Determine which failures are permament stageout errors
        ## and which ones are recoverable stageout errors.
        failures = ASO_JOB.get_failures()
        ASO_JOB = None
        num_failures = len(failures)
        num_permanent_failures = 0
        for doc_id in failures.keys():
            if type(failures[doc_id]['reasons']) == str:
                failures[doc_id]['reasons'] = [failures[doc_id]['reasons']]
            if type(failures[doc_id]['reasons']) == list:
                last_failure_reason = failures[doc_id]['reasons'][-1]
                if self.is_failure_permanent(last_failure_reason):
                    num_permanent_failures += 1
                    failures[doc_id]['severity'] = 'permanent'
                else:
                    failures[doc_id]['severity'] = 'recoverable'
        ## Message for stageout error exception.
        msg  = "Stageout failed with code %d."
        msg += "\nThere were %d failed/killed stageout jobs."
        msg = msg % (aso_job_result, num_failures)
        if num_permanent_failures:
            msg += " %d of those jobs had a permanent failure." % (num_permanent_failures)
        msg += "\nFailure reasons (per document) follow:"
        for doc_id in failures.keys():
            msg += "\n- %s:" % (doc_id)
            if failures[doc_id]['app']:
                msg += "\n  -----> %s log start -----" % (str(failures[doc_id]['app']).upper())
            msg += "\n  %s" % (failures[doc_id]['reasons'])
            if failures[doc_id]['app']:
                msg += "\n  <----- %s log finish ----" % (str(failures[doc_id]['app']).upper())
            if failures[doc_id]['severity']:
                msg += "\n  The last failure reason is %s." % (str(failures[doc_id]['severity']).lower())

        ## Raise stageout exception.
        if num_permanent_failures:
            raise PermanentStageoutError(msg)
        else:
            raise RecoverableStageoutError(msg)

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def upload_log_file_metadata(self):
        """
        Upload the logs archive file metadata.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        temp_storage_site = self.executed_site
        if self.job_report.get(u'temp_storage_site', 'unknown') != 'unknown':
            temp_storage_site = self.job_report.get(u'temp_storage_site')
        configreq = {'taskname'        : self.reqname,
                     'pandajobid'      : self.job_id,
                     'outsize'         : self.job_report.get(u'log_size', 0),
                     'publishdataname' : self.publish_name,
                     'appver'          : self.job_sw,
                     'outtype'         : 'LOG',
                     'checksummd5'     : 'asda',       # Not implemented
                     'checksumcksum'   : '3701783610', # Not implemented
                     'checksumadler32' : '6d1096fe',   # Not implemented
                     'acquisitionera'  : 'null',       # Not implemented
                     'events'          : 0,
                     'outlocation'     : self.dest_site,
                     'outlfn'          : os.path.join(self.dest_dir, 'log', self.logs_arch_file_name),
                     'outtmplocation'  : temp_storage_site,
                     'outtmplfn'       : os.path.join(self.source_dir, 'log', self.logs_arch_file_name),
                     'outdatasetname'  : '/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER',
                     'directstageout'  : int(self.job_report.get('direct_stageout', 0))
                    }
        rest_api = 'filemetadata'
        rest_uri = self.rest_uri_no_api + '/' + rest_api
        rest_url = self.rest_host + rest_uri
        msg = "Uploading file metadata for %s to https://%s: %s"
        msg = msg % (self.logs_arch_file_name, rest_url, configreq)
        logger.debug(msg)
        try:
            self.server.put(rest_uri, data = urllib.urlencode(configreq))
        except HTTPException, hte:
            msg = "Got exception when uploading logs file metadata: %s"
            msg = msg % (str(hte.headers))
            logger.exception(msg)
            if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
               not hte.headers.get('X-Error-Http', -1) == '400':
                raise

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def upload_input_files_metadata(self):
        """
        Upload the output files metadata. We care about the number of events
        and about the lumis for the report.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        temp_storage_site = self.executed_site
        if self.job_report.get(u'temp_storage_site', 'unknown') != 'unknown':
            temp_storage_site = self.job_report.get(u'temp_storage_site')
        if not 'source' in self.job_report.get('steps', {}).get('cmsRun', {}).get('input', {}):
            logger.info("Skipping input filemetadata upload as no inputs were found")
            return
        direct_stageout = int(self.job_report.get(u'direct_stageout', 0))
        for ifile in self.job_report['steps']['cmsRun']['input']['source']:
            #Many of these parameters are not needed and are using fake/defined values
            configreq = {"taskname"        : self.job_ad['CRAB_ReqName'],
                         "globalTag"       : "None",
                         "pandajobid"      : self.job_id,
                         "outsize"         : "0",
                         "publishdataname" : self.publish_name,
                         "appver"          : self.job_ad['CRAB_JobSW'],
                         "outtype"         : "POOLIN",#file['input_source_class'],
                         "checksummd5"     : "0",
                         "checksumcksum"   : "0",
                         "checksumadler32" : "0",
                         "outlocation"     : self.job_ad['CRAB_AsyncDest'],
                         "outtmplocation"  : temp_storage_site,
                         "acquisitionera"  : "null", # Not implemented
                         "outlfn"          : ifile['lfn'] + "_" + str(self.job_id), # jobs can analyze the same input
                         "outtmplfn"       : self.source_dir, #does not have sense for input files
                         "events"          : ifile.get('events', 0),
                         "outdatasetname"  : "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER",
                         "directstageout"  : direct_stageout
                        }
            configreq = configreq.items()
            outfilerun = []
            outfilelumis = []
            for run, lumis in ifile[u'runs'].iteritems():
                outfileruns.append(str(run))
                outfilelumis.append(','.join(map(str, lumis)))
            for run in outfileruns:
                configreq.append(("outfileruns", run))
            for lumi in outfilelumis:
                configreq.append(("outfilelumis", lumi))
            msg = "Uploading file metadata for input file %s" % ifile['lfn']
            logger.debug(msg)
            try:
                self.server.put(self.rest_uri_no_api + '/filemetadata', data = urllib.urlencode(configreq))
            except HTTPException, hte:
                msg = "Error uploading input file metadata: %s" % (str(hte.headers))
                logger.error(msg)
                raise

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def upload_output_files_metadata(self):
        """
        Upload the output files metadata.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        edm_file_count = 0
        for file_info in self.output_files_info:
            if file_info['filetype'] == 'EDM':
                edm_file_count += 1
        multiple_edm = edm_file_count > 1
        output_datasets = set()
        for file_info in self.output_files_info:
            publishname = self.publish_name
            if 'pset_hash' in file_info:
                publishname = "%s-%s" % (publishname.rsplit('-', 1)[0], file_info['pset_hash'])
            ## CMS convention for output dataset name:
            ## /primarydataset>/<username>-<publish_data_name>-<PSETHASH>/USER
            if file_info['filetype'] == 'EDM':
                if multiple_edm and file_info.get('module_label'):
                    left, right = publishname.rsplit('-', 1)
                    publishname = "%s_%s-%s" % (left, file_info['module_label'], right)
                outdataset = os.path.join('/' + self.input_dataset.split('/')[1], \
                                          self.job_ad['CRAB_UserHN'] + '-' + publishname, 'USER')
            else:
                outdataset = '/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER'
            output_datasets.add(outdataset)
            configreq = {'taskname'        : self.reqname,
                         'pandajobid'      : self.job_id,
                         'outsize'         : file_info['outsize'],
                         'publishdataname' : publishname,
                         'appver'          : self.job_sw,
                         'outtype'         : file_info['filetype'],
                         'checksummd5'     : 'asda', # Not implemented; garbage value taken from ASO
                         'checksumcksum'   : file_info['checksums']['cksum'],
                         'checksumadler32' : file_info['checksums']['adler32'],
                         'acquisitionera'  : 'null', # Not implemented
                         'events'          : file_info.get('events', 0),
                         'outlocation'     : file_info['outlocation'],
                         'outlfn'          : file_info['outlfn'],
                         'outtmplocation'  : file_info['outtmplocation'],
                         'outtmplfn'       : file_info['outtmplfn'],
                         'outdatasetname'  : outdataset,
                         'directstageout'  : int(file_info['direct_stageout']),
                         'globalTag'       : 'None'
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
            rest_api = 'filemetadata'
            rest_uri = self.rest_uri_no_api + '/' + rest_api
            rest_url = self.rest_host + rest_uri
            msg = "Uploading file metadata for %s to https://%s: %s" % (filename, rest_url, configreq)
            logger.debug(msg)
            try:
                self.server.put(rest_uri, data = urllib.urlencode(configreq))
            except HTTPException, hte:
                ## BrianB. Suppressing this exception is a tough decision.
                ## If the file made it back alright, I suppose we can proceed.
                msg = "Error uploading output file metadata: %s" % (str(hte.headers))
                logger.error(msg)
                if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
                   not hte.headers.get('X-Error-Http', -1) == '400':
                    raise

            if not os.path.exists('output_datasets'):
                configreq = [('subresource', 'addoutputdatasets'),
                             ('workflow', self.reqname)]
                for dset in output_datasets:
                    configreq.append(('outputdatasets', dset))
                rest_api = 'task'
                rest_uri = self.rest_uri_no_api + '/' + rest_api
                rest_url = self.rest_host + rest_uri
                msg = "Uploading output datasets to https://%s: %s" % (rest_url, configreq)
                logger.debug(msg)
                try:
                    self.server.post(rest_uri, data = urllib.urlencode(configreq))
                    with open('output_datasets', 'w') as f:
                        f.write(' '.join(output_datasets))
                except HTTPException, hte:
                    msg = "Error uploading output dataset: %s" % (str(hte.headers))
                    logger.exception(msg)

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def parse_job_ad(self, job_ad_file_name):
        """
        Parse the job ad, setting some class variables.
        """
        ## Load the job ad.
        if not os.path.exists(job_ad_file_name) or not os.stat(job_ad_file_name).st_size:
            logger.error("Missing job ad!")
            return not OK
        try:
            fd_job_ad = open(job_ad_file_name)
            self.job_ad = classad.parseOld(fd_job_ad)
            fd_job_ad.close()
        except Exception, exmsg:
            msg = "Got exception while trying to parse the job ad: %s"
            msg = msg % (str(exmsg))
            logger.exception(msg)
            return not OK
        ## Check if all the required attributes from the job ad are there.
        if self.check_required_job_ad_attrs() is not OK:
            return not OK
        ## Set some class variables using the job ad.
        self.dest_site       = str(self.job_ad['CRAB_AsyncDest'])
        self.input_dataset   = str(self.job_ad['CRAB_InputData'])
        self.job_sw          = str(self.job_ad['CRAB_JobSW'])
        self.publish_name    = str(self.job_ad['CRAB_PublishName'])
        self.rest_host       = str(self.job_ad['CRAB_RestHost'])
        self.rest_uri_no_api = str(self.job_ad['CRAB_RestURInoAPI'])
        ## If self.job_ad['CRAB_ASOTimeout'] = 0, will use default timeout logic.
        if 'CRAB_ASOTimeout' in self.job_ad and int(self.job_ad['CRAB_ASOTimeout']) > 0:
            self.retry_timeout = int(self.job_ad['CRAB_ASOTimeout'])
        return OK

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_required_job_ad_attrs(self):
        """
        Check if all the required attributes from the job ad are there.
        """
        required_job_ad_attrs = ['CRAB_ASOURL',
                                 'CRAB_AsyncDest',
                                 'CRAB_DBSUrl',
                                 'CRAB_InputData',
                                 'CRAB_JobSW',
                                 'CRAB_Publish',
                                 'CRAB_PublishName',
                                 'CRAB_RestHost',
                                 'CRAB_RestURInoAPI',
                                 'CRAB_RetryOnASOFailures',
                                 'CRAB_SaveLogsFlag',
                                 'CRAB_TransferOutputs',
                                 'CRAB_UserHN',
                                ]
        missing_attrs = []
        for attr in required_job_ad_attrs:
            if attr not in self.job_ad:
                missing_attrs.append(attr)
        if missing_attrs:
            msg = "The following required attributes are missing in the job ad: %s"
            msg = msg % (missing_attrs)
            logger.error(msg)
            return not OK
        undefined_attrs = []
        for attr in required_job_ad_attrs:
            if self.job_ad[attr] is None or str(self.job_ad[attr]).lower() in ['', 'undefined']:
                undefined_attrs.append(attr)
        if undefined_attrs:
            msg = "Could not determine the following required attributes from the job ad: %s"
            msg = msg % (undefined_attrs)
            logger.error(msg)
            return not OK
        return OK

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def parse_job_report(self):
        """
        Parse the job report, setting some class variables (the most important one
        self.output_files_info, which contains information of each output file relevant
        for transfers and file metadata upload).
        """
        ## Load the job report.
        try:
            with open(G_JOB_REPORT_NAME) as fd_job_report:
                self.job_report = json.load(fd_job_report)
        except Exception, exmsg:
            msg = "Got exception while trying to load the job report: %s"
            msg = msg % (str(exmsg))
            logger.exception(msg)
            return not OK
        ## Check that the job_report has the expected structure.
        if 'steps' not in self.job_report:
            logger.error("Invalid job report: missing 'steps'")
            return not OK
        if 'cmsRun' not in self.job_report['steps']:
            logger.error("Invalid job report: missing 'cmsRun'")
            return not OK
        if 'input' not in self.job_report['steps']['cmsRun']:
            logger.error("Invalid job report: missing 'input'")
            return not OK
        if 'output' not in self.job_report['steps']['cmsRun']:
            logger.error("Invalid job report: missing 'output'")
            return not OK
        ## This is the job report part containing information about the output files.
        self.job_report_output = self.job_report['steps']['cmsRun']['output']
        ## Set some class variables using the job report.
        self.log_needs_transfer = not bool(self.job_report.get(u'direct_stageout', False))
        self.log_needs_file_metadata_upload = not bool(self.job_report.get(u'file_metadata_upload', False))
        self.log_size = int(self.job_report.get(u'log_size', 0))
        self.executed_site = self.job_report.get(u'executed_site', None)
        if not self.executed_site:
            msg = "Unable to determine executed site from job report."
            logger.error(msg)
            return not OK
        self.job_failed = bool(self.job_report.get(u'jobExitCode', 0))
        if self.job_failed:
            self.source_dir = os.path.join(self.source_dir, 'failed')
            self.dest_dir = os.path.join(self.dest_dir, 'failed')
        ## Fill self.output_files_info by parsing the job report.
        self.fill_output_files_info()
        return OK

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def fill_output_files_info(self):
        """
        Fill self.output_files_info by parsing the job report.
        """
        for output_module in self.job_report_output.values():
            for output_file_info in output_module:
                msg = "Output file info for %s: %s"
                msg = msg % (output_file_info.get(u'pfn', "(unknown 'pfn')").split('/')[-1], output_file_info)
                logger.debug(msg)
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
                file_info['local_stageout']  = output_file_info.get(u'local_stageout', False)
                file_info['direct_stageout'] = output_file_info.get(u'direct_stageout', False)
                file_info['outlocation'] = self.dest_site
                if output_file_info.get(u'temp_storage_site', 'unknown') != 'unknown':
                    file_info['outtmplocation'] = output_file_info.get(u'temp_storage_site')
                elif self.job_report.get(u'temp_storage_site', 'unknown') != 'unknown':
                    file_info['outtmplocation'] = self.job_report.get(u'temp_storage_site')
                else:
                    file_info['outtmplocation'] = self.executed_site
                if u'runs' not in output_file_info:
                    continue
                file_info['outfileruns'] = []
                file_info['outfilelumis'] = []
                for run, lumis in output_file_info[u'runs'].items():
                    file_info['outfileruns'].append(str(run))
                    file_info['outfilelumis'].append(','.join(map(str, lumis)))
        for filename in self.output_files_names:
            ifile = get_file_index(filename, self.output_files_info)
            if ifile is None:
                continue
            self.output_files_info[ifile]['outlfn'] = os.path.join(self.dest_dir, filename)
            self.output_files_info[ifile]['outtmplfn'] = os.path.join(self.source_dir, filename)

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    ## job_err is empty because we redirect to stderr to stdout, and job_out.tmp is
    ## empty becuase we truncate it to 0 after copying it to the web directory.
    ## So we don't need this method IMHO. AndresT.
    def fix_job_logs_permissions(self):
        """
        HTCondor will default to a umask of 0077 for stdout/err. When the DAG finishes,
        the schedd will chown the sandbox to the user which runs HTCondor.
        This prevents the user who owns the DAGs from retrieving the job output as they
        are no longer world-readable.
        """
        for base_file in ["job_err", "job_out.tmp"]:
            try:
                os.chmod("%s.%d" % (base_file, self.job_id), 0644)
            except OSError, ose:
                if ose.errno != errno.ENOENT and ose.errno != errno.EPERM:
                    raise

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def set_dashboard_state(self, state, reason = None):
        """
        Set the state of the job in Dashboard.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        states_dict = {'COOLOFF'      : 'Cooloff',
                       'TRANSFERRING' : 'Transferring',
                       'FAILED'       : 'Aborted',
                       'FINISHED'     : 'Done'
                      }
        state = states_dict.get(state, state)
        msg = "Setting Dashboard state to %s." % (state)
        params = {'MonitorID'    : self.reqname,
                  'MonitorJobID' : '%d_https://glidein.cern.ch/%d/%s_%d' \
                                   % (self.job_id, self.job_id, \
                                      self.reqname.replace("_", ":"), \
                                      self.crab_retry_count),
                  'StatusValue'  : state,
                 }
        if reason:
            params['StatusValueReason'] = reason
        ## List with the log files that we want to make available in dashboard.
        if 'CRAB_UserWebDir' in self.job_ad:
            log_files = [("job_out", "txt"), ("job_fjr", "json"), ("postjob", "txt")]
            for i, log_file in enumerate(log_files):
                log_file_basename, log_file_extension = log_file
                log_file_name = "%s/%s.%d.%d.%s" % (self.job_ad['CRAB_UserWebDir'], \
                                                    log_file_basename, self.job_id, \
                                                    self.crab_retry_count, \
                                                    log_file_extension)
                log_files[i] = log_file_name
            params['StatusLogFile'] = ",".join(log_files)
        ## Unfortunately, Dashboard only has 1-second resolution; we must separate all
        ## updates by at least 1s in order for it to get the correct ordering.
        time.sleep(1)
        msg += " Dashboard parameters: %s" % (str(params))
        logger.info(msg)
        DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_file_source_site(self, file_name, ifile = None):
        """
        Get the source site for the given file.
        """
        if ifile is None:
            ifile = get_file_index(file_name, self.output_files_info)
        if ifile is not None and self.output_files_info[ifile].get('outtmplocation'):
            return self.output_files_info[ifile]['outtmplocation']
        if self.job_report.get(u'temp_storage_site', 'unknown') != 'unknown':
            return self.job_report.get(u'temp_storage_site')
        return self.executed_site

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def calculate_crab_retry_count(self):
        """
        Calculate the retry number we're on. See the notes in PreJob.
        """
        fname = "retry_info/job.%d.txt" % (self.job_id)
        if os.path.exists(fname):
            try:
                with open(fname, 'r') as fd_retry_info:
                    retry_info = json.load(fd_retry_info)
            except:
                msg  = "Unable to calculate crab retry count."
                msg += " Failed to load file %s." % (fname)
                logger.warning(msg)
                return None
        else:
            retry_info = {'pre': 0, 'post': 0}
        if 'pre' not in retry_info or 'post' not in retry_info:
            msg  = "Unable to calculate crab retry count."
            msg += " File %s doesn't contain the expected information."
            msg = msg % (fname)
            logger.warning(msg)
            return None
        crab_retry_count = retry_info['post']
        retry_info['post'] += 1
        try:
            with open(fname + '.tmp', 'w') as fd_retry_info:
                json.dump(retry_info, fd_retry_info)
            os.rename(fname + '.tmp', fname)
        except Exception:
            msg = "Failed to update file %s with increased post count by +1."
            msg = msg % (fname)
            logger.warning(msg)
        return crab_retry_count

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def is_failure_permanent(self, reason):
        """
        Method that decides whether a failure reason should be considered as a
        permanent failure or as a recoverable failure.
        """
        if int(self.job_ad['CRAB_RetryOnASOFailures']) == 0:
            msg  = "Considering transfer error as a permanent failure"
            msg += " because CRAB_RetryOnASOFailures = 0 in the job ad."
            logger.debug(msg)
            return True
        permanent_failure_reasons = [
                                     ".*killed aso transfer after timeout.*",
                                     ".*permission denied.*",
                                     ".*disk quota exceeded.*",
                                     ".*operation not permitted*",
                                     ".*mkdir\(\) fail.*",
                                     ".*open/create error.*",
                                     #".*failed to get source file size.*",
                                    ]
        reason = str(reason).lower()
        for permanent_failure_reason in permanent_failure_reasons:
            if re.match(permanent_failure_reason, reason):
                return True
        return False

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_retry_count(self):
        """
        When a recoverable error happens, we still have to check if the maximum allowed
        number of job retries was hit. If it was, the post-job should return with the
        fatal error exit code. Otherwise with the recoverable error exit code.
        """
        if self.dag_retry_count >= self.max_retries:
            msg  = "Job could be retried, but the maximum allowed number of retries was hit."
            msg += " Setting this node (job) to permanent failure. DAGMan will NOT retry."
            logger.info(msg)
            self.set_dashboard_state('FAILED')
            return RetryJob.FATAL_ERROR
        else:
            msg = "Job will be retried by DAGMan."
            logger.info(msg)
            self.set_dashboard_state('COOLOFF')
            return RetryJob.RECOVERABLE_ERROR

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_abort_dag(self, rval):
        """
        For each job that failed with a fatal error, its id is written into the file
        task_statistics.FATAL_ERROR. For each job that didn't fail with a fatal or
        recoverable error, its id is written into the file task_statistics.OK.
        Notice that these files may contain repeated job ids.
        Based on these statistics we may decide to abort the whole DAG.
        """
        file_name_jobs_fatal = "task_statistics.FATAL_ERROR"
        file_name_jobs_ok    = "task_statistics.OK"
        ## Return code 3 is reserved to abort the entire DAG. Don't let the code
        ## otherwise use it.
        if rval == 3:
            rval = 1
        if 'CRAB_FailedNodeLimit' not in self.job_ad or self.job_ad['CRAB_FailedNodeLimit'] == -1:
            return rval
        try:
            limit = int(self.job_ad['CRAB_FailedNodeLimit'])
            fatal_failed_jobs = []
            with open(file_name_jobs_fatal, 'r') as fd_jobs_fatal:
                for job_id in fd_jobs_fatal.readlines():
                    if job_id not in fatal_failed_jobs:
                        fatal_failed_jobs.append(job_id)
            num_fatal_failed_jobs = len(fatal_failed_jobs)
            successful_jobs = []
            with open(file_name_jobs_ok, 'r') as fd_jobs_ok:
                for job_id in fd_jobs_ok.readlines():
                    if job_id not in successful_jobs:
                        successful_jobs.append(job_id)
            num_successful_jobs = len(successful_jobs)
            if (num_successful_jobs + num_fatal_failed_jobs) > limit:
                if num_fatal_failed_jobs > num_successful_jobs:
                    msg  = "There are %d (fatal) failed nodes and %d successful nodes,"
                    msg += " adding up to a total of %d (more than the limit of %d)."
                    msg += " The ratio of failed to successful is greater than 1."
                    msg += " Will abort the whole DAG."
                    msg = msg % (num_fatal_failed_jobs, num_successful_jobs, \
                                 num_successful_jobs + num_fatal_failed_jobs, limit)
                    logger.error(msg)
                    rval = 3
        finally:
            return rval

##==============================================================================

class PermanentStageoutError(RuntimeError):
    pass


class RecoverableStageoutError(RuntimeError):
    pass

##==============================================================================

def get_file_index(file_name, output_files):
    """
    Get the index location of the given file name within the list of output files
    dict infos from the job report.
    """
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

##==============================================================================

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
        self.json_name = "jobReport.json.%s" % (self.full_args[6])
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
                exmsg = "Couldn't make file: %s" % (res)
                raise RuntimeError, exmsg
        finally:
            if os.path.exists(path):
                os.unlink(path)


    def getLevelOneDir(self):
        return datetime.datetime.now().strftime("%Y-%m")

    def getLevelTwoDir(self):
        return datetime.datetime.now().strftime("%d%p")

    def getUniqueFilename(self):
        return "%s-postjob.txt" % (uuid.uuid4())

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

##==============================================================================

if __name__ == '__main__':
    if len(sys.argv) >= 2 and sys.argv[1] == 'UNIT_TEST':
        sys.argv = [sys.argv[0]]
        print "Beginning testing"
        unittest.main()
        print "Testing over"
        sys.exit()
    POSTJOB = PostJob()
    sys.exit(POSTJOB.execute(*sys.argv[2:]))

