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

The PostJob and cmscp are the only places in CRAB3 where we should use camel_case instead of snakeCase.
"""

import os
import re
import sys
import time
import json
import uuid
import glob
import errno
import pprint
import random
import shutil
import signal
import urllib
import hashlib
import logging
import classad
import commands
import unittest
import datetime
import tempfile
import traceback
from httplib import HTTPException

import DashboardAPI
import WMCore.Database.CMSCouch as CMSCouch

from ServerUtilities import setDashboardLogs
from RESTInteractions import HTTPRequests ## Why not to use from WMCore.Services.Requests import Requests
from TaskWorker.Actions.RetryJob import RetryJob
from TaskWorker.Actions.RetryJob import JOB_RETURN_CODES


ASO_JOB = None
config = None
G_JOB_REPORT_NAME = None
G_JOB_REPORT_NAME_NEW = None


def sighandler(*args):
    if ASO_JOB:
        ASO_JOB.cancel()

signal.signal(signal.SIGHUP,  sighandler)
signal.signal(signal.SIGINT,  sighandler)
signal.signal(signal.SIGTERM, sighandler)

##==============================================================================

def prepareErrorSummary(logger, job_id, crab_retry):
    """ Load the current error_summary.json and do the equivalent of an 'ls job_fjr.*.json' to get the completed jobs.
        Then, add the error reason of the job that are not in errorReport.json
    """

    ## The job_id and crab_retry variables in PostJob are integers, while here we
    ## mostly use them as strings.
    job_id = str(job_id)
    crab_retry = str(crab_retry)

    logger.info("====== Starting to prepare error report.")

    error_summary = {}
    error_summary_changed = False

    ## Open and load an already existing error summary.
    error_summary_file_name = "error_summary.json"
    try:
        with open(error_summary_file_name) as fsummary:
            error_summary = json.load(fsummary)
    except (IOError, ValueError):
        ## There is nothing to do if the error_summary file doesn't exist or is invalid.
        ## Just recreate it.
        logger.info("File %s is empty, wrong or does not exist. Will create a new file." % (error_summary_file_name))

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

    ## Iterate over all the job reports in the spool directory.
    for fjr_file_name in glob.glob("job_fjr.*.*.json"):
        fjr_file_name_split = fjr_file_name.split('.')
        fjr_job_id, fjr_crab_retry = fjr_file_name_split[-3], fjr_file_name_split[-2]
        ## If this job report has already been written to the error summary, we skip it,
        ## because we assume whatever is in the error summary should not be changed.
        if fjr_job_id in error_summary and fjr_crab_retry in error_summary[fjr_job_id]:
            continue
        logger.info("Processing job report file %s" % (fjr_file_name))
        with open(fjr_file_name) as frep:
            try:
                rep = None
                exit_code = -1
                rep = json.load(frep)
                if not 'exitCode' in rep:                    logger.info("'exitCode' key not found in the report");              raise
                exit_code = rep['exitCode']
                if not 'exitMsg'  in rep:                    logger.info("'exitMsg' key not found in the report");               raise
                exit_msg = rep['exitMsg']
                if not 'steps'    in rep:                    logger.info("'steps' key not found in the report");                 raise
                if not 'cmsRun'   in rep['steps']:           logger.info("'cmsRun' key not found in report['steps']");           raise
                if not 'errors'   in rep['steps']['cmsRun']: logger.info("'errors' key not found in report['steps']['cmsRun']"); raise
                if rep['steps']['cmsRun']['errors']:
                    ## If there are errors in the job report, they come from the job execution. This
                    ## is the error we want to report to the user, so write it to the error summary.
                    if len(rep['steps']['cmsRun']['errors']) != 1:
                        #this should never happen because the report has just one step, but just in case print a message
                        logger.info("More than one error found in report['steps']['cmsRun']['errors']. Just considering the first one.")
                    msg  = "Updating error summary for jobid %s retry %s with following information:" % (fjr_job_id, fjr_crab_retry)
                    msg += "\n'exit code' = %s" % (exit_code)
                    msg += "\n'exit message' = %s" % (exit_msg)
                    msg += "\n'error message' = %s" % (rep['steps']['cmsRun']['errors'][0])
                    logger.info(msg)
                    error_summary.setdefault(fjr_job_id, {})[fjr_crab_retry] = (exit_code, exit_msg, rep['steps']['cmsRun']['errors'][0])
                    error_summary_changed = True
                else:
                    ## If there are no errors in the job report, but there is an exit code and exit
                    ## message from the job (not post-job), we want to report them to the user only
                    ## in case we know this is the terminal exit code and exit message. And this is
                    ## the case if the exit code is not 0. Even a post-job exit code != 0 can be
                    ## added later to the job report, the job exit code takes precedence, so we can
                    ## already write it to the error summary.
                    if exit_code != 0:
                        msg  = "Updating error summary for jobid %s retry %s with following information:" % (fjr_job_id, fjr_crab_retry)
                        msg += "\n'exit code' = %s" % (exit_code)
                        msg += "\n'exit message' = %s" % (exit_msg)
                        logger.info(msg)
                        error_summary.setdefault(fjr_job_id, {})[fjr_crab_retry] = (exit_code, exit_msg, {})
                        error_summary_changed = True
                    else:
                        ## In case the job exit code is 0, write to the error summary only if the job
                        ## report file we are looking at is the one corresponding to the current job id
                        ## and crab retry, since otherwise the job report can still get updated by its
                        ## corresponding post-job.
                        if fjr_job_id == job_id and fjr_crab_retry == crab_retry:
                            ## In case the job exit code is 0, we still have to check if there is an exit
                            ## code from post-job. If there is a post-job exit code != 0, write it to the
                            ## error summary; otherwise write the exit code 0 and exit message from the job
                            ## (the message should be "OK").
                            postjob_exit_code = rep.get('postjob', {}).get('exitCode', -1)
                            postjob_exit_msg  = rep.get('postjob', {}).get('exitMsg', "No post-job error message available.")
                            if postjob_exit_code != 0:
                                ## Use exit code 90000 as a general exit code for failures in the post-processing step.
                                ## The 'crab status' error summary should not show this error code,
                                ## but replace it with the generic message "failed in post-processing".
                                msg  = "Updating error summary for jobid %s retry %s with following information:" % (fjr_job_id, fjr_crab_retry)
                                msg += "\n'exit code' = 90000 ('Post-processing failed')"
                                msg += "\n'exit message' = %s" % (postjob_exit_msg)
                                logger.info(msg)
                                error_summary.setdefault(fjr_job_id, {})[fjr_crab_retry] = (90000, postjob_exit_msg, {})
                            else:
                                msg  = "Updating error summary for jobid %s retry %s with following information:" % (fjr_job_id, fjr_crab_retry)
                                msg += "\n'exit code' = %s" % (exit_code)
                                msg += "\n'exit message' = %s" % (exit_msg)
                                logger.info(msg)
                                error_summary.setdefault(fjr_job_id, {})[fjr_crab_retry] = (exit_code, exit_msg, {})
                            error_summary_changed = True
            except Exception, ex:
                logger.info(str(ex))
                ## Write to the error summary that the job report is not valid or has no error
                ## message, but only if the job report file we are looking at is the one
                ## corresponding to the current job id and crab retry, since the job report can
                ## still get updated by its corresponding post-job.
                if fjr_job_id == job_id and fjr_crab_retry == crab_retry:
                    if not rep:
                        exit_msg = 'Invalid framework job report. The framework job report exists, but it cannot be loaded.'
                    else:
                        exit_msg = rep['exitMsg'] if 'exitMsg' in rep else 'The framework job report could be loaded, but no error message was found there.'
                    msg  = "Updating error summary for jobid %s retry %s with following information:" % (fjr_job_id, fjr_crab_retry)
                    msg += "\n'exit code' = %s" % (exit_code)
                    msg += "\n'exit message' = %s" % (exit_msg)
                    logger.info(msg)
                    error_summary.setdefault(fjr_job_id, {})[fjr_crab_retry] = (exit_code, exit_msg, {})
                    error_summary_changed = True

    ## If we have updated the error summary, write it to the json file.
    ## Use a temporary file and rename to avoid concurrent writing of the file.
    if error_summary_changed:
        tmp_fname = "error_summary.%d.json" % (os.getpid())
        with open(tmp_fname, "w") as fsummary:
            json.dump(error_summary, fsummary)
        os.rename(tmp_fname, error_summary_file_name)
    logger.info("====== Finished to prepare error report.")

##==============================================================================

class ASOServerJob(object):
    """
    Class used to inject transfer requests to ASO database.
    """
    def __init__(self, logger, dest_site, source_dir, dest_dir, source_sites, \
                 count, filenames, reqname, log_size, log_needs_transfer, \
                 job_report_output, job_ad, crab_retry, retry_timeout, \
                 job_failed, transfer_logs, transfer_outputs):
        """
        ASOServerJob constructor.
        """
        self.logger = logger
        self.docs_in_transfer = None
        self.crab_retry = crab_retry
        self.retry_timeout = retry_timeout
        self.couch_server = None
        self.couch_database = None
        self.sleep = 300
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
        self.transfer_logs = transfer_logs
        self.transfer_outputs = transfer_outputs
        self.job_ad = job_ad
        self.failures = {}
        self.aso_start_timestamp = None
        proxy = os.environ.get('X509_USER_PROXY', None)
        self.aso_db_url = self.job_ad['CRAB_ASOURL']
        try:
            self.logger.info("Will use ASO server at %s." % (self.aso_db_url))
            self.couch_server = CMSCouch.CouchServer(dburl = self.aso_db_url, ckey = proxy, cert = proxy)
            self.couch_database = self.couch_server.connectDatabase("asynctransfer", create = False)
        except Exception, ex:
            msg = "Failed to connect to ASO database: %s" % (str(ex))
            self.logger.exception(msg)
            raise RuntimeError, msg

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def run(self):
        """
        This is the main method in ASOServerJob. Should be called after initializing
        an instance.
        """
        self.docs_in_transfer = self.inject_to_aso()
        if self.docs_in_transfer == False:
            exmsg = "Couldn't upload document to ASO database"
            raise RuntimeError, exmsg
        if not self.docs_in_transfer:
            self.logger.info("No files to transfer via ASO. Done!")
            return 0
        failed_killed_transfers = []
        done_transfers = []
        starttime = time.time()
        if self.aso_start_timestamp:
            starttime = self.aso_start_timestamp
        self.logger.info("====== Starting to monitor ASO transfers.")
        ## This call to get_transfers_statuses() is only to make a first call to
        ## the couch view in order to reduce the probability of getting a stale
        ## result when calling the view inside the while loop below.
        self.get_transfers_statuses()
        while True:
            ## Sleep is done before calling get_transfers_statuses(), in order to
            ## give some time to couch to load the view after the above call to
            ## get_transfers_statuses().
            time.sleep(self.sleep + random.randint(0, 60))
            ## Get the transfer status in all documents listed in self.docs_in_transfer.
            transfers_statuses = self.get_transfers_statuses()
            msg = "Got statuses: %s; %.1f hours since transfer submit." 
            msg = msg % (", ".join(transfers_statuses), (time.time()-starttime)/3600.0)
            self.logger.info(msg)
            all_transfers_finished = True
            doc_ids = [doc_info['doc_id'] for doc_info in self.docs_in_transfer]
            for transfer_status, doc_id in zip(transfers_statuses, doc_ids):
                ## States to wait on.
                if transfer_status in ['new', 'acquired', 'retry', 'unknown']:
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
                        msg = "Stageout job (internal ID %s) failed with status '%s'." % (doc_id, transfer_status)
                        doc = self.load_couch_document(doc_id)
                        if doc and ('failure_reason' in doc) and doc['failure_reason']:
                            ## reasons:  The transfer failure reason(s).
                            ## app:      The application that gave the transfer failure reason(s).
                            ##           E.g. 'aso' or '' (meaning the postjob). When printing the
                            ##           transfer failure reasons (e.g. below), print also that the
                            ##           failures come from the given app (if app != '').
                            ## severity: Either 'permanent' or 'recoverable'.
                            ##           It is set by PostJob in the perform_transfers() function,
                            ##           when is_failure_permanent() is called to determine if a
                            ##           failure is permanent or not.
                            reasons, app, severity = doc['failure_reason'], 'aso', None
                            msg += " Failure reasons follow:"
                            if app:
                                msg += "\n-----> %s log start -----" % str(app).upper()
                            msg += "\n%s" % reasons
                            if app:
                                msg += "\n<----- %s log finish ----" % str(app).upper()
                            self.logger.error(msg)
                        else:
                            reasons, app, severity = "Failure reason unavailable.", None, None
                            self.logger.error(msg)
                            self.logger.warning("WARNING: no failure reason available.")
                        self.failures[doc_id] = {'reasons': reasons, 'app': app, 'severity': severity}
                else:
                    exmsg = "Got an unknown transfer status: %s" % (transfer_status)
                    raise RuntimeError, exmsg
            if all_transfers_finished:
                msg = "All transfers finished."
                self.logger.info(msg)
                if failed_killed_transfers:
                    msg  = "There were %d failed/killed transfers:" % (len(failed_killed_transfers))
                    msg += " %s" % (", ".join(failed_killed_transfers))
                    self.logger.info(msg)
                    self.logger.info("====== Finished to monitor ASO transfers.")
                    return 1
                self.logger.info("====== Finished to monitor ASO transfers.")
                return 0
            ## If there is a timeout for transfers to complete, check if it was exceeded
            ## and if so kill the ongoing transfers. # timeout = -1 means no timeout.
            if self.retry_timeout != -1 and time.time() - starttime > self.retry_timeout:
                msg  = "Post-job reached its timeout of %d seconds waiting for ASO transfers to complete." % (self.retry_timeout)
                msg += " Will cancel ongoing ASO transfers."
                self.logger.warning(msg)
                self.logger.info("====== Starting to cancel ongoing ASO transfers.")
                docs_to_cancel = {}
                reason = "Cancelled ASO transfer after timeout of %d seconds." % (self.retry_timeout)
                for doc_info in self.docs_in_transfer:
                    doc_id = doc_info['doc_id']
                    if doc_id not in done_transfers + failed_killed_transfers:
                        docs_to_cancel.update({doc_id: reason})
                cancelled, not_cancelled = self.cancel(docs_to_cancel, max_retries = 2)
                for doc_id in cancelled:
                    failed_killed_transfers.append(doc_id)
                    app, severity = None, None
                    self.failures[doc_id] = {'reasons': reason, 'app': app, 'severity': severity}
                if not_cancelled:
                    msg = "Failed to cancel %d ASO transfers: %s" % (len(not_cancelled), ", ".join(not_cancelled))
                    self.logger.error(msg)
                    self.logger.info("====== Finished to cancel ongoing ASO transfers.")
                    self.logger.info("====== Finished to monitor ASO transfers.")
                    return 2
                self.logger.info("====== Finished to cancel ongoing ASO transfers.")
                self.logger.info("====== Finished to monitor ASO transfers.")
                return 1

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def inject_to_aso(self):
        """
        Inject documents to ASO database if not done by cmscp from worker node.
        """
        self.logger.info("====== Starting to check uploads to ASO database.")
        docs_in_transfer = []
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
            with open(G_JOB_REPORT_NAME) as fd:
                job_report = json.load(fd)
            self.aso_start_timestamp = job_report.get("aso_start_timestamp")
            aso_start_time = job_report.get("aso_start_time")
        except Exception:
            self.aso_start_timestamp = last_update
            aso_start_time = now
            msg  = "Unable to determine ASO start time from job report."
            msg += " Will use ASO start time = %s (%s)."
            msg  = msg % (aso_start_time, self.aso_start_timestamp)
            self.logger.warning(msg)

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
        if self.transfer_outputs:
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
        found_log = False
        for source_site, filename in zip(self.source_sites, self.filenames):
            ## We assume that the first file in self.filenames is the logs archive.
            if found_log:
                if not self.transfer_outputs:
                    continue
                source_lfn = os.path.join(self.source_dir, filename)
                dest_lfn = os.path.join(self.dest_dir, filename)
                file_type = 'output'
                ifile = get_file_index(filename, output_files)
                if ifile is None:
                    continue
                size = output_files[ifile]['outsize']
                checksums = output_files[ifile]['checksums']
                ## needs_transfer is False if and only if the file was staged out
                ## from the worker node directly to the permanent storage.
                needs_transfer = not output_files[ifile]['direct_stageout']
                file_output_type = output_files[ifile]['filetype']
            else:
                found_log = True
                if not self.transfer_logs:
                    continue
                source_lfn = os.path.join(self.source_dir, 'log', filename)
                dest_lfn = os.path.join(self.dest_dir, 'log', filename)
                file_type = 'log'
                size = self.log_size
                checksums = {'adler32': 'abc'}
                ## needs_transfer is False if and only if the file was staged out
                ## from the worker node directly to the permanent storage.
                needs_transfer = self.log_needs_transfer
            self.logger.info("Working on file %s" % (filename))
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
                            ## The 'job_retry_count' is used by ASO when reporting to dashboard,
                            ## so it is OK to set it equal to the crab (post-job) retry count.
                            'job_retry_count' : self.crab_retry,
                           }
            if not needs_transfer:
                msg  = "File %s is marked as having been directly staged out"
                msg += " from the worker node to the permanent storage."
                msg  = msg % (filename)
                self.logger.info(msg)
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
                    self.logger.info(publication_msg)
                msg  = "File %s doesn't need transfer nor publication."
                msg += " No need to inject a document to ASO."
                msg  = msg % (filename)
                self.logger.info(msg)
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
                        self.logger.info(msg)
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
                            self.logger.info(msg)
                            needs_commit = False
                        else:
                            msg += " Will inject a new %s request." % (' and '.join(aso_tasks))
                            self.logger.info(msg)
                            msg = "Previous document: %s" % (pprint.pformat(doc))
                            self.logger.debug(msg)
                except CMSCouch.CouchNotFoundError:
                    ## The document was not yet uploaded to ASO database (if this is the first job
                    ## retry, then either the upload from the WN failed, or cmscp did a direct
                    ## stageout and here we need to inject for publication only). In any case we
                    ## have to inject a new document.
                    msg  = "LFN %s (id %s) is not in ASO database."
                    msg += " Will inject a new %s request."
                    msg  = msg % (source_lfn, doc_id, ' and '.join(aso_tasks))
                    self.logger.info(msg)
                    if publication_msg:
                        self.logger.info(publication_msg)
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
                           'dbs_url'                 : str(self.job_ad['CRAB_DBSURL']),
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
                except Exception, ex:
                    msg = "Error loading document from ASO database: %s" % (str(ex))
                    try:
                        msg += "\n%s" % (traceback.format_exc())
                    except AttributeError:
                        msg += "\nTraceback unavailable."
                    self.logger.error(msg)
                    return False
                ## If after all we need to upload a new document to ASO database, let's do it.
                if needs_commit:
                    doc.update(doc_new_info)
                    msg = "ASO job description: %s" % (pprint.pformat(doc))
                    self.logger.info(msg)
                    commit_result_msg = self.couch_database.commitOne(doc)[0]
                    if 'error' in commit_result_msg:
                        msg = "Error injecting document to ASO database:\n%s" % (commit_result_msg)
                        self.logger.info(msg)
                        return False
                ## Record all files for which we want the post-job to monitor their transfer.
                if needs_transfer:
                    doc_info = {'doc_id'     : doc_id,
                                'start_time' : doc.get('start_time')
                               }
                    docs_in_transfer.append(doc_info)

        self.logger.info("====== Finished to check uploads to ASO database.")

        return docs_in_transfer

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
                with open("aso_status.json") as fd:
                    aso_info = json.load(fd)
            except:
                self.logger.exception("Failed to load common ASO status.")
                return self.get_transfers_statuses_fallback()
            last_query = aso_info.get("query_timestamp", 0)
            # We can use the cached data if:
            # - It is from the last 5 minutes, AND
            # - It is from after we submitted the transfer.
            # Without the second condition, we run the risk of using the previous stageout
            # attempts results.
            if (time.time() - last_query < 300) and (last_query > self.aso_start_timestamp):
                query_view = False
            for doc_info in self.docs_in_transfer:
                doc_id = doc_info['doc_id']
                if doc_id not in aso_info.get("results", {}):
                    query_view = True
                    break
        if query_view:
            query = {'reduce': False, 'key': self.reqname, 'stale': 'update_after'}
            self.logger.debug("Querying ASO view.")
            try:
                view_results = self.couch_database.loadView('AsyncTransfer', 'JobsIdsStatesByWorkflow', query)['rows']
                view_results_dict = {}
                for view_result in view_results:
                    view_results_dict[view_result['id']] = view_result
            except Exception:
                self.logger.exception("Error while querying the asynctransfer CouchDB.")
                return self.get_transfers_statuses_fallback()
            aso_info = {"query_timestamp": time.time(), "results": view_results_dict}
            tmp_fname = "aso_status.%d.json" % (os.getpid())
            with open(tmp_fname, 'w') as fd:
                json.dump(aso_info, fd)
            os.rename(tmp_fname, "aso_status.json")
        else:
            self.logger.debug("Using cached results.")
        if not aso_info:
            return self.get_transfers_statuses_fallback()
        statuses = []
        for doc_info in self.docs_in_transfer:
            doc_id = doc_info['doc_id']
            if doc_id not in aso_info.get("results", {}):
                return self.get_transfers_statuses_fallback()
            ## Use the start_time parameter to check whether the transfer state in aso_info
            ## corresponds to the document we have to monitor. The reason why we have to do
            ## this check is because the information in aso_info might have been obtained by
            ## querying an ASO CouchDB view, which can return stale results. We don't mind
            ## having stale results as long as they correspond to the documents we have to
            ## monitor (the worst that can happen is that we will wait more than necessary
            ## to see the transfers in a terminal state). But it could be that some results
            ## returned by the view correspond to documents injected in a previous job retry
            ## (or restart), and this is not what we want. If the start_time in aso_info is
            ## not the same as in the document (we saved the start_time from the documents
            ## in self.docs_in_transfer), then the view result corresponds to a previous job
            ## retry. In that case, return transfer state = 'unknown'.
            transfer_status = aso_info['results'][doc_id]['value']['state']
            if 'start_time' in aso_info['results'][doc_id]['value']:
                start_time = aso_info['results'][doc_id]['value']['start_time']
                if start_time == doc_info['start_time']:
                    statuses.append(transfer_status)
                else:
                    msg  = "Got stale transfer state '%s' for document %s" % (transfer_status, doc_id)
                    msg += " (got start_time = %s, while in the document is start_time = %s)." % (start_time, doc_info['start_time'])
                    msg += " Transfer state may correspond to a document from a previous job retry."
                    msg += " Returning transfer state = 'unknown'."
                    self.logger.info(msg)
                    statuses.append('unknown')
            else:
                statuses.append(transfer_status)
        return statuses

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def load_couch_document(self, doc_id):
        """
        Wrapper to load a document from CouchDB, catching exceptions.
        """
        doc = None
        try:
            doc = self.couch_database.document(doc_id)
        except CMSCouch.CouchError, cee:
            msg = "Error retrieving document from ASO database for ID %s: %s" % (doc_id, str(cee))
            self.logger.error(msg)
        except HTTPException, hte:
            msg = "Error retrieving document from ASO database for ID %s: %s" % (doc_id, str(hte.headers))
            self.logger.error(msg)
        except Exception:
            msg = "Error retrieving document from ASO database for ID %s" % (doc_id)
            self.logger.exception(msg)
        return doc

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_transfers_statuses_fallback(self):
        """
        Retrieve the status of all transfers by loading the correspnding documents
        from ASO database and checking the 'state' field.
        """
        msg = "Querying transfers statuses using fallback method (i.e. loading each document)."
        self.logger.debug(msg)
        statuses = []
        for doc_info in self.docs_in_transfer:
            doc_id = doc_info['doc_id']
            doc = self.load_couch_document(doc_id)
            status = doc['state'] if doc else 'unknown'
            statuses.append(status)
        return statuses

    ##= = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def cancel(self, doc_ids_reasons = None, max_retries = 0):
        """
        Method used to "cancel/kill" ASO transfers. The only thing that this
        function does is to put the 'state' field of the corresponding documents in
        the ASO database to 'killed' (and the 'end_time' field to the current time).
        Killing actual FTS transfers (if possible) is left to ASO.
        """
        if doc_ids_reasons is None:
            for doc_info in self.docs_in_transfer:
                doc_id = doc_info['doc_id']
                doc_ids_reasons[doc_id] = None
        if not doc_ids_reasons:
            msg = "There are no ASO transfers to cancel."
            self.logger.info(msg)
            return [], []
        now = str(datetime.datetime.now())
        cancelled, not_cancelled = [], []
        max_retries = max(int(max_retries), 0)
        if max_retries:
            msg = "In case of cancellation failure, will retry up to %d times." % (max_retries)
            self.logger.info(msg)
        for retry in range(max_retries + 1):
            if retry > 0:
                time.sleep(3*60)
                msg = "This is cancellation retry number %d." % (retry)
                self.logger.info(msg)
            for doc_id, reason in doc_ids_reasons.iteritems():
                if doc_id in cancelled:
                    continue
                msg = "Cancelling ASO transfer %s" % (doc_id)
                if reason:
                    msg += " with following reason: %s" % (reason)
                self.logger.info(msg)
                doc = self.load_couch_document(doc_id)
                if not doc:
                    msg = "Could not cancel ASO transfer %s; failed to load document." % (doc_id)
                    self.logger.warning(msg)
                    if retry == max_retries:
                        not_cancelled.append((doc_id, msg))
                    continue
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
                    msg = "Error cancelling ASO transfer %s: %s" % (doc_id, res)
                    self.logger.warning(msg)
                    if retry == max_retries:
                        not_cancelled.append((doc_id, msg))
                else:
                    cancelled.append(doc_id)
        return cancelled, not_cancelled

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
        ## These attributes are set from arguments passed to the post-job (see
        ## RunJobs.dag file).
        self.dag_jobid           = None
        self.job_return_code     = None
        self.dag_retry           = None
        self.max_retries         = None
        self.reqname             = None
        self.job_id              = None
        self.source_dir          = None
        self.dest_dir            = None
        self.logs_arch_file_name = None
        self.output_files_names  = None
        ## The crab_retry is the number of times the post-job was ran (not necessarilly
        ## completing) for this job id.
        self.crab_retry          = None
        ## These attributes are read from the job ad (see parse_job_ad()).
        self.job_ad              = {}
        self.dest_site           = None
        self.input_dataset       = None
        self.job_sw              = None
        self.publish_name        = None
        self.rest_host           = None
        self.rest_uri_no_api     = None
        self.retry_timeout       = None
        self.transfer_logs       = None
        self.transfer_outputs    = None
        ## These attributes are read from the job report (see parse_job_report()).
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
        ## Path to the task web directory.
        self.logpath             = None
        ## Set a logger for the post-job.
        self.logger = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", \
                                      datefmt = "%a, %d %b %Y %H:%M:%S %Z(%z)")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute(self, *args, **kw):
        """
        The execute method of PostJob.
        """
        ## Put the arguments to PostJob into class variables.
        self.dag_jobid           = args[0] ## = ClusterId.ProcId
        self.job_return_code     = args[1]
        self.dag_retry           = int(args[2])
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
        ## (We only need to add the job_id in the file names).
        self.output_files_names  = []
        for i in xrange(9, len(args)):
            self.output_files_names.append(args[i])
        self.job_return_code = int(self.job_return_code)
        self.crab_retry = self.calculate_crab_retry()
        if self.crab_retry is None:
            self.crab_retry = self.dag_retry

        ## Create the task web directory in the schedd.
        self.logpath = os.path.expanduser("~/%s" % (self.reqname))
        try:
            os.makedirs(self.logpath)
        except OSError, ose:
            if ose.errno != errno.EEXIST:
                print "Failed to create log web-shared directory %s" % (self.logpath)
                raise

        ## Create (open) the post-job log file postjob.<job_id>.<crab_retry>.txt.
        postjob_log_file_name = "postjob.%d.%d.txt" % (self.job_id, self.crab_retry)
        fd_postjob_log = os.open(postjob_log_file_name, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0644)
        os.chmod(postjob_log_file_name, 0644)
        ## Redirect stdout and stderr to the post-job log file.
        if os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            print "Post-job started with no output redirection."
        else:
            os.dup2(fd_postjob_log, 1)
            os.dup2(fd_postjob_log, 2)
            msg = "Post-job started with output redirected to %s." % (postjob_log_file_name)
            self.logger.info(msg)
        ## Create a symbolic link in the task web directory to the post-job log file.
        ## The pre-job creates already the symlink, but the post-job has to do it by
        ## its own in case the pre-job was not executed (e.g. when DAGMan restarts a
        ## post-job).
        msg = "Creating symbolic link in task web directory to post-job log file: %s -> %s" \
            % (os.path.join(self.logpath, postjob_log_file_name), postjob_log_file_name)
        self.logger.debug(msg)
        try:
            os.symlink(os.path.abspath(os.path.join(".", postjob_log_file_name)), \
                       os.path.join(self.logpath, postjob_log_file_name))
        except:
            pass

        ## Now that we have the job id and retry, we can set the job report file
        ## names.
        global G_JOB_REPORT_NAME
        G_JOB_REPORT_NAME = "jobReport.json.%d" % (self.job_id)
        global G_JOB_REPORT_NAME_NEW
        G_JOB_REPORT_NAME_NEW = "job_fjr.%d.%d.json" % (self.job_id, self.crab_retry)

        ## Call execute_internal().
        retval = 1
        retmsg = "Failure during post-job execution."
        try:
            retval, retmsg = self.execute_internal()
            msg = "Post-job finished executing with status code %d." % (retval)
            self.logger.info(msg)
        except:
            self.logger.exception(retmsg)
        finally:
            DashboardAPI.apmonFree()

        ## Add the post-job exit code and error message to the job report.
        job_report = {}
        try:
            with open(G_JOB_REPORT_NAME_NEW) as fd:
                job_report = json.load(fd)
        except (IOError, ValueError):
            pass
        job_report['postjob'] = {'exitCode': retval, 'exitMsg': retmsg}
        with open(G_JOB_REPORT_NAME_NEW, 'w') as fd:
            json.dump(job_report, fd)

        ## Prepare the error report. Enclosing it in a try except as we don't want to
        ## fail jobs because this fails.
        try:
            prepareErrorSummary(self.logger, self.job_id, self.crab_retry)
        except:
            msg = "Unknown error while preparing the error report."
            self.logger.exception(msg)

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

        ## Copy the job's stdout file job_out.<job_id> to the schedd web directory,
        ## naming it job_out.<job_id>.<crab_retry>.txt.
        ## NOTE: We now redirect stdout -> stderr; hence, we don't keep stderr in
        ## the webdir.
        stdout = "job_out.%d" % (self.job_id)
        stdout_tmp = "job_out.tmp.%d" % (self.job_id)
        if os.path.exists(stdout):
            os.rename(stdout, stdout_tmp)
            fname = "job_out.%d.%d.txt" % (self.job_id, self.crab_retry)
            fname = os.path.join(self.logpath, fname)
            msg = "Copying job stdout from %s to %s." % (stdout, fname)
            self.logger.debug(msg)
            shutil.copy(stdout_tmp, fname)
            fd_stdout = open(stdout_tmp, 'w')
            fd_stdout.truncate(0)
            fd_stdout.close()
            os.chmod(fname, 0644)

        ## Copy the json job report file jobReport.json.<job_id> to
        ## job_fjr.<job_id>.<crab_retry>.json and create a symbolic link in the task web
        ## directory to the new job report file.
        if os.path.exists(G_JOB_REPORT_NAME):
            msg = "Copying job report from %s to %s." % (G_JOB_REPORT_NAME, G_JOB_REPORT_NAME_NEW)
            self.logger.debug(msg)
            shutil.copy(G_JOB_REPORT_NAME, G_JOB_REPORT_NAME_NEW)
            os.chmod(G_JOB_REPORT_NAME_NEW, 0644)
            msg = "Creating symbolic link in task web directory to job report file: %s -> %s" \
                % (os.path.join(self.logpath, G_JOB_REPORT_NAME_NEW), G_JOB_REPORT_NAME_NEW)
            self.logger.debug(msg)
            try:
                os.symlink(os.path.abspath(os.path.join(".", G_JOB_REPORT_NAME_NEW)), \
                           os.path.join(self.logpath, G_JOB_REPORT_NAME_NEW))
            except:
                pass

        ## Print a message about what job retry number are we on.
        msg  = "This is job retry number %d." % (self.dag_retry)
        msg += " The maximum allowed number of retries is %d." % (self.max_retries)
        self.logger.info(msg)

        ## Make sure the location of the user's proxy file is set in the environment.
        if 'X509_USER_PROXY' not in os.environ:
            retmsg = "X509_USER_PROXY is not present in environment."
            self.logger.error(retmsg)
            return 10, retmsg

        ## Parse the job ad.
        job_ad_file_name = os.environ.get("_CONDOR_JOB_AD", ".job.ad")
        self.logger.info("====== Starting to parse job ad file %s." % (job_ad_file_name))
        if self.parse_job_ad(job_ad_file_name):
            self.set_dashboard_state('FAILED')
            self.logger.info("====== Finished to parse job ad.")
            retmsg = "Failure parsing the job ad."
            return JOB_RETURN_CODES.FATAL_ERROR, retmsg
        self.logger.info("====== Finished to parse job ad.")

        self.logger.info("====== Starting to analyze job exit status.")
        ## Execute the retry-job. The retry-job decides whether an error is recoverable
        ## or fatal. It uses the cmscp exit code and error message, and the memory and
        ## cpu perfomance information; all from the job report. We also pass the job
        ## return code in case the exit code in the job report was not updated by cmscp.
        ## If the retry-job returns non 0 (meaning there was an error), report the state
        ## to dashboard and exit the post-job.
        retry = RetryJob()
        retryjob_retval = None
        if not os.environ.get('TEST_POSTJOB_DISABLE_RETRIES', False):
            print "       -----> RetryJob log start -----"
            retryjob_retval = retry.execute(self.reqname, self.job_return_code, \
                                            self.crab_retry, self.job_id, \
                                            self.dag_jobid)
            print "       <----- RetryJob log finish ----"
        if retryjob_retval:
            if retryjob_retval == JOB_RETURN_CODES.FATAL_ERROR:
                msg = "The retry handler indicated this was a fatal error."
                self.logger.info(msg)
                self.set_dashboard_state('FAILED')
                self.logger.info("====== Finished to analyze job exit status.")
                return JOB_RETURN_CODES.FATAL_ERROR, ""
            elif retryjob_retval == JOB_RETURN_CODES.RECOVERABLE_ERROR:
                if self.dag_retry >= self.max_retries:
                    msg  = "The retry handler indicated this was a recoverable error,"
                    msg += " but the maximum number of retries was already hit."
                    msg += " DAGMan will not retry."
                    self.logger.info(msg)
                    self.set_dashboard_state('FAILED')
                    self.logger.info("====== Finished to analyze job exit status.")
                    return JOB_RETURN_CODES.FATAL_ERROR, ""
                else:
                    msg  = "The retry handler indicated this was a recoverable error."
                    msg += " DAGMan will retry."
                    self.logger.info(msg)
                    self.set_dashboard_state('COOLOFF')
                    self.logger.info("====== Finished to analyze job exit status.")
                    return JOB_RETURN_CODES.RECOVERABLE_ERROR, ""
            else:
                msg  = "The retry handler returned an unexpected value (%d)." % (retryjob_retval)
                msg += " Will consider this as a fatal error. DAGMan will not retry."
                self.logger.info(msg)
                self.set_dashboard_state('FAILED')
                self.logger.info("====== Finished to analyze job exit status.")
                return JOB_RETURN_CODES.FATAL_ERROR, ""
        ## This is for the case in which we don't run the retry-job.
        elif self.job_return_code != JOB_RETURN_CODES.OK:
            if self.dag_retry >= self.max_retries:
                msg  = "The maximum allowed number of retries was hit and the job failed."
                msg += " Setting this node (job) to permanent failure."
                self.logger.info(msg)
                self.set_dashboard_state('FAILED')
                self.logger.info("====== Finished to analyze job exit status.")
                return JOB_RETURN_CODES.FATAL_ERROR, ""
        else:
            self.logger.info("====== Finished to analyze job exit status.")
        ## If CRAB_ASOTimeout was not defined in the job ad, get here the ASO timeout
        ## from the retry-job.
        if self.retry_timeout is None:
            self.retry_timeout = retry.get_aso_timeout()

        ## Parse the job report.
        self.logger.info("====== Starting to parse job report file %s." % (G_JOB_REPORT_NAME))
        if self.parse_job_report():
            self.set_dashboard_state('FAILED')
            self.logger.info("====== Finished to parse job report.")
            retmsg = "Failure parsing the job report."
            return JOB_RETURN_CODES.FATAL_ERROR, retmsg
        self.logger.info("====== Finished to parse job report.")

        ## AndresT. We don't need this method IMHO. See note I made in the method.
        self.fix_job_logs_permissions()

        ## If the flag CRAB_NoWNStageout is set, we finish the post-job here.
        ## (I didn't remove this yet, because even if the transfer of the logs and
        ## outputs is off, in a user analysis task it still makes sense to do the
        ## upload of the input files metadata, so that the user can do 'crab report'
        ## and see how many events have been read and/or get the lumiSummary files.)
        if int(self.job_ad.get('CRAB_NoWNStageout', 0)) != 0:
            msg  = "CRAB_NoWNStageout is set to %d in the job ad." % (int(self.job_ad.get('CRAB_NoWNStageout', 0)))
            msg += " Skipping files stageout and files metadata upload."
            msg += " Finishing post-job execution with exit code 0."
            self.logger.info(msg)
            self.set_dashboard_state('FINISHED')
            return 0, ""

        if not self.transfer_logs:
            msg  = "The user has not specified to transfer the log files."
            msg += " No log files stageout (nor log files metadata upload) will be performed."
            self.logger.info(msg)
        if len(self.output_files_names) == 0:
            if not self.transfer_outputs:
                msg  = "Post-job got an empty list of output files (i.e. no output file) to work on."
                msg += " In any case, the user has specified to not transfer output files."
                self.logger.info(msg)
            else:
                msg  = "The transfer of output files flag in on,"
                msg += " but the post-job got an empty list of output files (i.e. no output file) to work on."
                msg += " Turning off the transfer of output files flag."
                self.logger.info(msg)
                self.transfer_outputs = 0
        else:
            if not self.transfer_outputs:
                msg  = "The user has specified to not transfer the output files."
                msg += " No output files stageout (nor output files metadata upload) will be performed."
                self.logger.info(msg)

        ## Initialize the object we will use for making requests to the REST interface.
        self.server = HTTPRequests(self.rest_host, \
                                   os.environ['X509_USER_PROXY'], \
                                   os.environ['X509_USER_PROXY'], \
                                   retry = 2, logger = self.logger)

        ## Upload the logs archive file metadata if it was not already done from the WN.
        if self.transfer_logs:
            if self.log_needs_file_metadata_upload:
                self.logger.info("====== Starting upload of logs archive file metadata.")
                try:
                    self.upload_log_file_metadata()
                except Exception, ex:
                    retmsg = "Fatal error uploading logs archive file metadata: %s" % (str(ex))
                    self.logger.error(retmsg)
                    self.logger.info("====== Finished upload of logs archive file metadata.")
                    return self.check_retry_count(), retmsg
                self.logger.info("====== Finished upload of logs archive file metadata.")
            else:
                msg  = "Skipping logs archive file metadata upload, because it is marked as"
                msg += " having been uploaded already from the worker node."
                self.logger.info(msg)

        ## Just an info message.
        if self.transfer_logs or self.transfer_outputs:
            msg = "Preparing to transfer "
            msgadd = []
            if self.transfer_logs:
                msgadd.append("the logs archive file")
            if self.transfer_outputs:
                msgadd.append("%d output files" % (len(self.output_files_names)))
            msg = msg + ' and '.join(msgadd) + '.'
            self.logger.info(msg)

        ## Do the transfers (inject to ASO database if needed, and monitor the transfers
        ## statuses until they reach a terminal state).
        if self.transfer_logs or self.transfer_outputs:
            self.logger.info("====== Starting to check for ASO transfers.")
            try:
                self.perform_transfers()
            except PermanentStageoutError, pse:
                retmsg = "Got fatal stageout exception:\n%s" % (str(pse))
                self.logger.error(retmsg)
                msg = "There was at least one permanent stageout error; user will need to resubmit."
                self.logger.error(msg)
                self.set_dashboard_state('FAILED')
                self.logger.info("====== Finished to check for ASO transfers.")
                return JOB_RETURN_CODES.FATAL_ERROR, retmsg
            except RecoverableStageoutError, rse:
                retmsg = "Got recoverable stageout exception:\n%s" % (str(rse))
                self.logger.error(retmsg)
                msg = "These are all recoverable stageout errors; automatic resubmit is possible."
                self.logger.error(msg)
                self.logger.info("====== Finished to check for ASO transfers.")
                return self.check_retry_count(), retmsg
            except Exception, ex:
                retmsg = "Stageout failure: %s" % (str(ex))
                self.logger.exception(retmsg)
                self.logger.info("====== Finished to check for ASO transfers.")
                return self.check_retry_count(), retmsg
            self.logger.info("====== Finished to check for ASO transfers.")

        ## Upload the output files metadata.
        if self.transfer_outputs:
            self.logger.info("====== Starting upload of output files metadata.")
            try:
                self.upload_output_files_metadata()
            except Exception, ex:
                retmsg = "Fatal error uploading output files metadata: %s" % (str(ex))
                self.logger.exception(retmsg)
                self.logger.info("====== Finished upload of output files metadata.")
                return self.check_retry_count(), retmsg
            self.logger.info("====== Finished upload of output files metadata.")

        ## Upload the input files metadata.
        self.logger.info("====== Starting upload of input files metadata.")
        try:
            self.upload_input_files_metadata()
        except Exception, ex:
            retmsg = "Fatal error uploading input files metadata: %s" % (str(ex))
            self.logger.exception(retmsg)
            self.logger.info("====== Finished upload of input files metadata.")
            return self.check_retry_count(), retmsg
        self.logger.info("====== Finished upload of input files metadata.")

        self.set_dashboard_state('FINISHED')

        return 0, ""

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
        ASO_JOB = ASOServerJob(self.logger, \
                               self.dest_site, self.source_dir, self.dest_dir, \
                               source_sites, self.job_id, log_and_output_files_names, \
                               self.reqname, self.log_size, \
                               self.log_needs_transfer, self.job_report_output, \
                               self.job_ad, self.crab_retry, \
                               self.retry_timeout, self.job_failed, \
                               self.transfer_logs, self.transfer_outputs)
        aso_job_retval = ASO_JOB.run()

        ## If no transfers failed, return success immediately.
        if aso_job_retval == 0:
            ASO_JOB = None
            return 0

        ## Return code 2 means post-job timed out waiting for transfer to complete and
        ## not all the transfers could be cancelled.
        if aso_job_retval == 2:
            ASO_JOB = None
            msg  = "Stageout failed with code %d." % (aso_job_retval)
            msg += "\nPost-job timed out waiting for ASO transfers to complete."
            msg += "\nAttempts were made to cancel the ongoing transfers,"
            msg += " but cancellation failed for some transfers."
            msg += "\nConsidering cancellation failures as a permament stageout error."
            raise PermanentStageoutError(msg)

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
        msg  = "Stageout failed with code %d." % (aso_job_retval)
        msg += "\nThere were %d failed/killed stageout jobs." % (num_failures)
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
        temp_storage_site = self.job_report.get('temp_storage_site', 'unknown')
        if temp_storage_site == 'unknown':
            msg  = "Temporary storage site for logs archive file not defined in job report."
            msg += " This is expected if there was no attempt to stage out the file into a temporary storage."
            msg += " Will use the executed site as the temporary storage site in the file metadata."
            self.logger.warning(msg)
            temp_storage_site = self.executed_site
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
        self.logger.debug(msg)
        try:
            self.server.put(rest_uri, data = urllib.urlencode(configreq))
        except HTTPException, hte:
            msg = "Error uploading logs file metadata: %s" % (str(hte.headers))
            self.logger.error(msg)
            if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
               not hte.headers.get('X-Error-Http', -1) == '400':
                raise

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def file_exists(self, lfn, type):
        """ Because of a bug in Oracle we need to do this if we get an exception while executing the filemetadata insert. See:
            https://cern.service-now.com/nav_to.do?uri=incident.do?sys_id=92982f051d497500c7138e7019d56af9%26sysparm_view=RPT72d916c9f0d3d94079046a0c9f0e01d6
            With the February deployment we should change the merge to an insert/update
        """

        try:
            configreq = {"taskname" : self.job_ad['CRAB_ReqName'],
                         "filetype" : type
            }
            res, _, _ = self.server.get(self.rest_uri_no_api + '/filemetadata', data = configreq)
            lfns = [x['lfn'] for x in res[u'result']]
        except HTTPException, hte:
            msg = "Error getting list of file metadata: %s" % (str(hte.headers))
            self.logger.error(msg)
            return False
        return (lfn in lfns)

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
            self.logger.info("Skipping input filemetadata upload as no inputs were found")
            return
        direct_stageout = int(self.job_report.get(u'direct_stageout', 0))
        for ifile in self.job_report['steps']['cmsRun']['input']['source']:
            if ifile['input_source_class'] != 'PoolSource' or not ifile['lfn']:
                #TODO: should we also check that "input_type" = "primaryFiles"?
                #The problematic 'input_type' = 'mixingFiles' is catched now because it does not have an lfn.
                #See https://cms-logbook.cern.ch/elog/Analysis+Operations/488
                continue
            ## Many of these parameters are not needed and are using fake/defined values
            lfn = ifile['lfn'] + "_" + str(self.job_id) ## jobs can analyze the same input
            configreq = {"taskname"        : self.job_ad['CRAB_ReqName'],
                         "globalTag"       : "None",
                         "pandajobid"      : self.job_id,
                         "outsize"         : "0",
                         "publishdataname" : self.publish_name,
                         "appver"          : self.job_ad['CRAB_JobSW'],
                         "outtype"         : "POOLIN", ## file['input_source_class'],
                         "checksummd5"     : "0",
                         "checksumcksum"   : "0",
                         "checksumadler32" : "0",
                         "outlocation"     : self.job_ad['CRAB_AsyncDest'],
                         "outtmplocation"  : temp_storage_site,
                         "acquisitionera"  : "null", ## Not implemented
                         "outlfn"          : lfn,
                         "outtmplfn"       : self.source_dir, ## does not have sense for input files
                         "events"          : ifile.get('events', 0),
                         "outdatasetname"  : "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER",
                         "directstageout"  : direct_stageout
                        }
            configreq = configreq.items()
            outfileruns = []
            outfilelumis = []
            for run, lumis in ifile[u'runs'].iteritems():
                outfileruns.append(str(run))
                outfilelumis.append(','.join(map(str, lumis)))
            for run in outfileruns:
                configreq.append(("outfileruns", run))
            for lumi in outfilelumis:
                configreq.append(("outfilelumis", lumi))
            msg = "Uploading file metadata for input file %s" % ifile['lfn']
            self.logger.debug(msg)
            try:
                self.server.put(self.rest_uri_no_api + '/filemetadata', data = urllib.urlencode(configreq))
            except HTTPException, hte:
                msg = "Error uploading input file metadata: %s" % (str(hte.headers))
                self.logger.error(msg)
                if not self.file_exists(lfn, 'POOLIN'):
                    raise
                else:
                    msg = "Ignoring the error since the file %s is already in the database" % lfn
                    self.logger.debug(msg)


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
                ## Extracting the primary dataset name from the input dataset is ok
                ## for an analysis job type. But to make sense of this same logic in
                ## a MC generation job type, the client defines the input dataset as
                ## '/' + primary dataset name. TODO: Some day maybe we could improve
                ## this (define a primary dataset parameter in the job ad and in the
                ## rest interface).
                primary_dataset_name = self.input_dataset.split('/')[1]
                outdataset = os.path.join('/' + primary_dataset_name, self.job_ad['CRAB_UserHN'] + '-' + publishname, 'USER')
                output_datasets.add(outdataset)
            else:
                outdataset = '/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER'
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
            self.logger.debug(msg)
            try:
                self.server.put(rest_uri, data = urllib.urlencode(configreq))
            except HTTPException, hte:
                ## BrianB. Suppressing this exception is a tough decision.
                ## If the file made it back alright, I suppose we can proceed.
                msg = "Error uploading output file metadata: %s" % (str(hte.headers))
                self.logger.error(msg)
                if not self.file_exists(file_info['outlfn'], file_info['filetype']):
                    raise
                else:
                    msg = "Ignoring the error since the file %s is already in the database" % lfn
                    self.logger.debug(msg)

        if not os.path.exists('output_datasets') and output_datasets:
            configreq = [('subresource', 'addoutputdatasets'),
                         ('workflow', self.reqname)]
            for dset in output_datasets:
                configreq.append(('outputdatasets', dset))
            rest_api = 'task'
            rest_uri = self.rest_uri_no_api + '/' + rest_api
            rest_url = self.rest_host + rest_uri
            msg = "Uploading output datasets to https://%s: %s" % (rest_url, configreq)
            self.logger.debug(msg)
            try:
                self.server.post(rest_uri, data = urllib.urlencode(configreq))
                with open('output_datasets', 'w') as f:
                    f.write(' '.join(output_datasets))
            except HTTPException, hte:
                msg = "Error uploading output dataset: %s" % (str(hte.headers))
                self.logger.error(msg)
            except IOError, ioe:
                msq = "Error writing the output_datasets file"
                self.logger.error(msg)

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def parse_job_ad(self, job_ad_file_name):
        """
        Parse the job ad, setting some class variables.
        """
        ## Load the job ad.
        if not os.path.exists(job_ad_file_name) or not os.stat(job_ad_file_name).st_size:
            self.logger.error("Missing job ad!")
            return 1
        try:
            with open(job_ad_file_name) as fd:
                self.job_ad = classad.parseOld(fd)
        except Exception, ex:
            msg = "Error parsing job ad: %s" % (str(ex))
            self.logger.exception(msg)
            return 1
        ## Check if all the required attributes from the job ad are there.
        if self.check_required_job_ad_attrs():
            return 1
        ## Set some class variables using the job ad.
        self.dest_site        = str(self.job_ad['CRAB_AsyncDest'])
        self.input_dataset    = str(self.job_ad['CRAB_InputData'])
        self.job_sw           = str(self.job_ad['CRAB_JobSW'])
        self.publish_name     = str(self.job_ad['CRAB_PublishName'])
        self.rest_host        = str(self.job_ad['CRAB_RestHost'])
        self.rest_uri_no_api  = str(self.job_ad['CRAB_RestURInoAPI'])
        if 'CRAB_SaveLogsFlag' not in self.job_ad:
            msg  = "Job's HTCondor ClassAd is missing attribute CRAB_SaveLogsFlag."
            msg += " Will assume CRAB_SaveLogsFlag = False."
            self.logger.warning(msg)
            self.transfer_logs = 0
        else:
            self.transfer_logs = int(self.job_ad['CRAB_SaveLogsFlag'])
        if 'CRAB_TransferOutputs' not in self.job_ad:
            msg  = "Job's HTCondor ClassAd is missing attribute CRAB_TransferOutputs."
            msg += " Will assume CRAB_TransferOutputs = True."
            self.logger.warning(msg)
            self.transfer_outputs = 1
        else:
            self.transfer_outputs = int(self.job_ad['CRAB_TransferOutputs'])
        ## If self.job_ad['CRAB_ASOTimeout'] = 0, will use default timeout logic.
        if 'CRAB_ASOTimeout' in self.job_ad and int(self.job_ad['CRAB_ASOTimeout']) > 0:
            self.retry_timeout = int(self.job_ad['CRAB_ASOTimeout'])
        return 0

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_required_job_ad_attrs(self):
        """
        Check if all the required attributes from the job ad are there.
        """
        required_job_ad_attrs = ['CRAB_ASOURL',
                                 'CRAB_AsyncDest',
                                 'CRAB_DBSURL',
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
            self.logger.error(msg)
            return 1
        undefined_attrs = []
        for attr in required_job_ad_attrs:
            if self.job_ad[attr] is None or str(self.job_ad[attr]).lower() in ['', 'undefined']:
                undefined_attrs.append(attr)
        if undefined_attrs:
            msg = "Could not determine the following required attributes from the job ad: %s"
            msg = msg % (undefined_attrs)
            self.logger.error(msg)
            return 1
        return 0

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def parse_job_report(self):
        """
        Parse the job report, setting some class variables (the most important one
        self.output_files_info, which contains information of each output file relevant
        for transfers and file metadata upload).
        """
        ## Load the job report.
        try:
            with open(G_JOB_REPORT_NAME) as fd:
                self.job_report = json.load(fd)
        except Exception, ex:
            msg = "Error loading job report: %s" % (str(ex))
            self.logger.exception(msg)
            return 1
        ## Check that the job_report has the expected structure.
        if 'steps' not in self.job_report:
            self.logger.error("Invalid job report: missing 'steps'")
            return 1
        if 'cmsRun' not in self.job_report['steps']:
            self.logger.error("Invalid job report: missing 'cmsRun'")
            return 1
        if 'input' not in self.job_report['steps']['cmsRun']:
            self.logger.error("Invalid job report: missing 'input'")
            return 1
        if 'output' not in self.job_report['steps']['cmsRun']:
            self.logger.error("Invalid job report: missing 'output'")
            return 1
        ## This is the job report part containing information about the output files.
        self.job_report_output = self.job_report['steps']['cmsRun']['output']
        ## Set some class variables using the job report.
        self.log_needs_transfer = not bool(self.job_report.get(u'direct_stageout', False))
        self.log_needs_file_metadata_upload = not bool(self.job_report.get(u'file_metadata_upload', False))
        self.log_size = int(self.job_report.get(u'log_size', 0))
        self.executed_site = self.job_report.get(u'executed_site', None)
        if not self.executed_site:
            msg = "Unable to determine executed site from job report."
            self.logger.error(msg)
            return 1
        self.job_failed = bool(self.job_report.get(u'jobExitCode', 0))
        if self.job_failed:
            self.source_dir = os.path.join(self.source_dir, 'failed')
            self.dest_dir = os.path.join(self.dest_dir, 'failed')
        ## Fill self.output_files_info by parsing the job report.
        self.fill_output_files_info()
        return 0

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def fill_output_files_info(self):
        """
        Fill self.output_files_info by parsing the job report.
        """
        def get_output_file_info(filename):
            for output_module in self.job_report_output.values():
                for output_file_info in output_module:
                    ifile = get_file_index(filename, [output_file_info])
                    if ifile is not None:
                        return output_file_info
            return None
        ## Loop over the output files that have to be collected.
        for filename in self.output_files_names:
            ## Search for the output file info in the job report.
            output_file_info = get_output_file_info(filename)
            ## Get the original file name, without the job id.
            left_piece, jobid_fileext = filename.rsplit("_", 1)
            orig_file_name = left_piece
            if "." in jobid_fileext:
                fileext = jobid_fileext.rsplit(".", 1)[-1]
                orig_file_name = left_piece + "." + fileext
            ## If there is an output file info, parse it.
            if output_file_info:
                msg = "Output file info for %s: %s" % (orig_file_name, output_file_info)
                self.logger.debug(msg)
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
                file_info['outlfn'] = os.path.join(self.dest_dir, filename)
                file_info['outtmplfn'] = os.path.join(self.source_dir, filename)
                if u'runs' not in output_file_info:
                    continue
                file_info['outfileruns'] = []
                file_info['outfilelumis'] = []
                for run, lumis in output_file_info[u'runs'].items():
                    file_info['outfileruns'].append(str(run))
                    file_info['outfilelumis'].append(','.join(map(str, lumis)))
            else:
                msg = "Output file info for %s not found in job report." % (orig_file_name)
                self.logger.error(msg)

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    ## job_err is empty because we redirect stderr to stdout, and job_out.tmp is
    ## empty because we truncate it to 0 after copying it to the web directory.
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
                  'MonitorJobID' : "%d_https://glidein.cern.ch/%d/%s_%d" \
                                   % (self.job_id, self.job_id, \
                                      self.reqname.replace("_", ":"), \
                                      self.crab_retry),
                  'StatusValue'  : state,
                 }
        if reason:
            params['StatusValueReason'] = reason
        ## List with the log files that we want to make available in dashboard.
        ## Disabling, The one in CMSRunAnalysis should be enough
        if 'CRAB_UserWebDir' in self.job_ad:
            setDashboardLogs(params, self.job_ad['CRAB_UserWebDir'], self.job_id, self.crab_retry)
        else:
           print "Not setting dashboard logfiles as I cannot find CRAB_UserWebDir in myad."
        ## Unfortunately, Dashboard only has 1-second resolution; we must separate all
        ## updates by at least 1s in order for it to get the correct ordering.
        time.sleep(1)
        msg += " Dashboard parameters: %s" % (str(params))
        self.logger.info(msg)
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

    def calculate_crab_retry(self):
        """
        Calculate the retry number we're on. See the notes in PreJob.
        """
        fname = "retry_info/job.%d.txt" % (self.job_id)
        if os.path.exists(fname):
            try:
                with open(fname, 'r') as fd:
                    retry_info = json.load(fd)
            except:
                msg  = "Unable to calculate post-job retry count."
                msg += " Failed to load file %s." % (fname)
                self.logger.warning(msg)
                return None
        else:
            retry_info = {'pre': 0, 'post': 0}
        if 'pre' not in retry_info or 'post' not in retry_info:
            msg  = "Unable to calculate post-job retry count."
            msg += " File %s doesn't contain the expected information."
            msg = msg % (fname)
            self.logger.warning(msg)
            return None
        crab_retry = retry_info['post']
        retry_info['post'] += 1
        try:
            with open(fname + '.tmp', 'w') as fd:
                json.dump(retry_info, fd)
            os.rename(fname + '.tmp', fname)
        except Exception:
            msg = "Failed to update file %s with increased post-job count by +1."
            msg = msg % (fname)
            self.logger.warning(msg)
        return crab_retry

    ## = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def is_failure_permanent(self, reason):
        """
        Method that decides whether a failure reason should be considered as a
        permanent failure or as a recoverable failure.
        """
        if int(self.job_ad['CRAB_RetryOnASOFailures']) == 0:
            msg  = "Considering transfer error as a permanent failure"
            msg += " because CRAB_RetryOnASOFailures = 0 in the job ad."
            self.logger.debug(msg)
            return True
        permanent_failure_reasons = [
                                     ".*cancelled aso transfer after timeout.*",
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
        if self.dag_retry >= self.max_retries:
            msg  = "Job could be retried, but the maximum allowed number of retries was hit."
            msg += " Setting this node (job) to permanent failure. DAGMan will NOT retry."
            self.logger.info(msg)
            self.set_dashboard_state('FAILED')
            return JOB_RETURN_CODES.FATAL_ERROR
        else:
            msg = "Job will be retried by DAGMan."
            self.logger.info(msg)
            self.set_dashboard_state('COOLOFF')
            return JOB_RETURN_CODES.RECOVERABLE_ERROR

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
            with open(file_name_jobs_fatal, 'r') as fd:
                for job_id in fd.readlines():
                    if job_id not in fatal_failed_jobs:
                        fatal_failed_jobs.append(job_id)
            num_fatal_failed_jobs = len(fatal_failed_jobs)
            successful_jobs = []
            with open(file_name_jobs_ok, 'r') as fd:
                for job_id in fd.readlines():
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
                    self.logger.error(msg)
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
        #status, crab_retry, max_retries, restinstance, resturl, reqname, id,
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

