#!/usr/bin/python
# This is a long term issue and to maintain ~3k lines of code in one file is hard.
# The code it is hard to read: workflow, taskname, reqname. All are the same....
# pylint: disable=too-many-lines, too-many-arguments
# pylint: disable=too-many-nested-blocks, too-many-branches, too-many-locals, no-self-use
# pylint: disable=too-many-statements, too-many-instance-attributes, too-many-public-methods

# this code intenionally uses some GLOBAL statements
# pylint: disable=global-statement
# too many open statements here to worry about specifying and encoding
# pylint: disable=unspecified-encoding
# here we intentionally (!?) use snake-case instead of CamelCase
# pylint: disable=invalid-name
# this file is too long, too complex etc. etc. so no hope that we put
# a docstring on all functions
# pylint: disable=missing-function-docstring, missing-class-docstring
# and no hope that we cleanup things, until we rewrite it
# pylint: disable=W0511   # do not barf on "TODO" comments
# pylint: disable=consider-using-f-string
# pylint: disable=logging-fstring-interpolation
# yeah, we have a lot of try-except where we really want to "catch everything"
# pylint: disable=broad-except
# there are a few very intricate dictionaries where current code works. better not modify it to be pretty
# pylint: disable=consider-using-dict-items

"""
In the PostJob we read the FrameworkJobReport (FJR) to retrieve information
about the output files. The FJR contains information for output files produced
either via PoolOutputModule or TFileService, which respectively produce files
of type EDM and TFile. Below are examples of the output part of a FJR for each
of these two cases.
Example FJR['steps']['cmsRun']['output'] for an EDM file produced via
PoolOutputModule:
{
 'temp_storage_site': 'T2_US_Nebraska',
 'storage_site': 'T2_CH_CERN',
 'pfn': 'dumper.root',
 'checksums': {'adler32': '47d823c0', 'cksum': '640736586'},
 'size': 3582626,
 'local_stageout': True,
 'direct_stageout': False,
 'ouput_module_class': 'PoolOutputModule',
 'runs': {'1': [666668, 666672, 666675, 666677, ...]},
 'branch_hash': 'd41d8cd98f00b204e9800998ecf8427e',
 'input': ['/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0011/404C27A2-22AE-E111-BC20-003048D373AE.root', ...],
 'inputpfns': ['/cms/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0011/404C27A2-22AE-E111-BC20-003048D373AE.root', ...],
 'lfn': '',
 'pset_hash': 'eede9f2e261619d27929da51104cf9d7',
 'catalog': '',
 'module_label': 'o',
 'guid': '48C5017F-A405-E411-9BE6-00A0D1E70940',
 'events': 30650
}
Example FJR['steps']['cmsRun']['output'] for a TFile produced via TFileService:
{
 'temp_storage_site': 'T2_US_Nebraska',
 'storage_site': 'T2_CH_CERN',
 'pfn': '/tmp/1882789.vmpsched/glide_Paza70/execute/dir_27876/histo.root',
 'checksums': {'adler32': 'e8ed4a12', 'cksum': '2439186609'},
 'size': 360,
 'local_stageout': True,
 'direct_stageout': False,
 'fileName': '/tmp/1882789.vmpsched/glide_Paza70/execute/dir_27876/histo.root',
 'Source': 'TFileService'
}
For other type of output files, we add in cmscp.py the basic necessary info
about the file to the FJR. The information is:
{
 'temp_storage_site': 'T2_US_Nebraska',
 'storage_site': 'T2_CH_CERN',
 'pfn': 'out.root',
 'size': 45124,
 'local_stageout': True,
 'direct_stageout': False
}

The PostJob and cmscp are the only places in CRAB3 where we should use camel_case instead of snakeCase.
"""

import os
import sys
import time
import json
import uuid
import pprint
import signal
import tarfile
import logging
import logging.handlers
import subprocess
import unittest
import datetime
import tempfile
import pickle
import traceback
import random
import shutil
import hashlib
from shutil import move
from http.client import HTTPException
import requests
from requests.auth import HTTPBasicAuth

from WMCore import Lexicon
from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.WMArchive.DataMap import createArchiverDoc

from TaskWorker import __version__
from TaskWorker.Actions.RetryJob import RetryJob
from TaskWorker.Actions.RetryJob import JOB_RETURN_CODES
from ServerUtilities import isFailurePermanent, mostCommon, TRANSFERDB_STATES, PUBLICATIONDB_STATES, encodeRequest, oracleOutputMapping
from ServerUtilities import getLock, getHashLfn
from RESTInteractions import CRABRest

if 'useHtcV2' in os.environ:
    import htcondor2 as htcondor
    import classad2 as classad
else:
    import htcondor
    import classad

ASO_JOB = None
G_JOB_REPORT_NAME = None
G_JOB_REPORT_NAME_NEW = None
G_WMARCHIVE_REPORT_NAME = None
G_WMARCHIVE_REPORT_NAME_NEW = None
G_ERROR_SUMMARY_FILE_NAME = "error_summary.json"
G_FJR_PARSE_RESULTS_FILE_NAME = "task_process/fjr_parse_results.txt"
G_FAKE_OUTDATASET = '/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER'


def sighandler():
    if ASO_JOB:
        ASO_JOB.cancel()

signal.signal(signal.SIGHUP, sighandler)
signal.signal(signal.SIGINT, sighandler)
signal.signal(signal.SIGTERM, sighandler)

# ==============================================================================


class NotFound(Exception):
    """Not Found is raised only if there is no document found in RDBMS.
       This makes PostJob to submit new transfer request to database."""
    pass  # pylint: disable=unnecessary-pass


DEFER_NUM = -1


def first_pj_execution():
    return DEFER_NUM == 0

# ==============================================================================

def compute_outputdataset_name(primaryDS=None, username=None, publish_name=None, pset_hash=None, module_label=None):
    """
    a standalone function to compute the name of DBS a dataset where to publish data
    Name of output dataset is a property of each output file, so it is not
    unique inside the PostJob and needs to be computed "as needed" both in ASO class
    (to communicate it to Rucio) and in PostJob class (to upload metadata) so
    we compute it here in a uniform way from informations available inside both places
    see
    https://twiki.cern.ch/twiki/bin/view/CMSPublic/Crab3DataHandling#Output_dataset_names_in_DBS
    Convention for output dataset name:
        /<primarydataset>/<username>-<tag>[-<module>]-<PSETHASH>/USER
    Input args are all strings:
    primaryDS: the primaryDataSet, usually the same as the input
    username: could be username or group name
    publish_name : value of the tm_publish_name field in TaskDB, derived from configuration parameter Data.outputDatasetTag
    module : if multiple EMD outputs are presents, the module label of the PoolOutputModule,
             otherwise missing
    pset_hash : the 32-char PSet hash . This is not the output of edmConfigHash but taken from edmProvDump.
    returns: DBS datasetname as a string
    """
    if pset_hash:
        # replace placeholder (32*'0') with correct pset hash for this file
        publish_name = "%s-%s" % (publish_name.rsplit('-', 1)[0], pset_hash)
    # #hash = pset_hash if pset_hash else 32*'0'
    if module_label:
        # insert output module name before the pset hash
        left, right = publish_name.rsplit('-', 1)
        publish_name = "%s-%s-%s" % (left, module_label, right)
    outdataset = '/' + primaryDS + '/' + username + '-' + publish_name + '/USER'
    return outdataset

# ==============================================================================

def prepareErrorSummary(logger, fsummary, job_id, crab_retry):
    """Parse the job_fjr file corresponding to the current PostJob. If an error
       message is found, it is inserted into the error_summary.json file
    """

    # The job_id and crab_retry variables in PostJob are integers, while here we
    # mostly use them as strings.
    job_id = str(job_id)
    crab_retry = str(crab_retry)

    error_summary = []
    error_summary_changed = False
    fjr_file_name = "job_fjr." + job_id + "." + crab_retry + ".json"

    with open(fjr_file_name) as frep:
        try:
            rep = None
            exit_code = -1
            rep = json.load(frep)
            if not 'exitCode' in rep:
                raise Exception("'exitCode' key not found in the report")
            exit_code = rep['exitCode']
            if not 'exitMsg'  in rep:
                raise Exception("'exitMsg' key not found in the report")
            exit_msg = rep['exitMsg']
            if not 'steps'    in rep:
                raise Exception("'steps' key not found in the report")
            if not 'cmsRun'   in rep['steps']:
                raise Exception("'cmsRun' key not found in report['steps']")
            if not 'errors'   in rep['steps']['cmsRun']:
                raise Exception("'errors' key not found in report['steps']['cmsRun']")
            if rep['steps']['cmsRun']['errors']:
                # If there are errors in the job report, they come from the job execution. This
                # is the error we want to report to the user, so write it to the error summary.
                if len(rep['steps']['cmsRun']['errors']) != 1:
                    #this should never happen because the report has just one step, but just in case print a message
                    logger.info("More than one error found in report['steps']['cmsRun']['errors']. Just considering the first one.")
                msg = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
                msg += "\n'exit code' = %s" % (exit_code)
                msg += "\n'exit message' = %s" % (exit_msg)
                msg += "\n'error message' = %s" % (rep['steps']['cmsRun']['errors'][0])
                logger.info(msg)
                error_summary = [exit_code, exit_msg, rep['steps']['cmsRun']['errors'][0]]
                error_summary_changed = True
            else:
                # If there are no errors in the job report, but there is an exit code and exit
                # message from the job (not post-job), we want to report them to the user only
                # in case we know this is the terminal exit code and exit message. And this is
                # the case if the exit code is not 0. Even a post-job exit code != 0 can be
                # added later to the job report, the job exit code takes precedence, so we can
                # already write it to the error summary.
                if exit_code != 0:
                    msg = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
                    msg += "\n'exit code' = %s" % (exit_code)
                    msg += "\n'exit message' = %s" % (exit_msg)
                    logger.info(msg)
                    error_summary = [exit_code, exit_msg, {}]
                    error_summary_changed = True
                else:
                    # In case the job exit code is 0, we still have to check if there is an exit
                    # code from post-job. If there is a post-job exit code != 0, write it to the
                    # error summary; otherwise write the exit code 0 and exit message from the job
                    # (the message should be "OK").
                    postjob_exit_code = rep.get('postjob', {}).get('exitCode', -1)
                    postjob_exit_msg = rep.get('postjob', {}).get('exitMsg', "No post-job error message available.")
                    if postjob_exit_code != 0:
                        # Use exit code 90000 as a general exit code for failures in the post-processing step.
                        # The 'crab status' error summary should not show this error code,
                        # but replace it with the generic message "failed in post-processing".
                        msg = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
                        msg += "\n'exit code' = 90000 ('Post-processing failed')"
                        msg += "\n'exit message' = %s" % (postjob_exit_msg)
                        logger.info(msg)
                        error_summary = [90000, postjob_exit_msg, {}]
                    else:
                        msg = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
                        msg += "\n'exit code' = %s" % (exit_code)
                        msg += "\n'exit message' = %s" % (exit_msg)
                        logger.info(msg)
                        error_summary = [exit_code, exit_msg, {}]
                    error_summary_changed = True
        except Exception as ex:
            logger.info(str(ex))
            # Write to the error summary that the job report is not valid or has no error
            # message
            if not rep:
                exit_msg = 'Invalid framework job report. The framework job report exists, but it cannot be loaded.'
            else:
                exit_msg = rep['exitMsg'] if 'exitMsg' in rep else 'The framework job report could be loaded, but no error message was found there.'
            msg = "Updating error summary for jobid %s retry %s with following information:" % (job_id, crab_retry)
            msg += "\n'exit code' = %s" % (exit_code)
            msg += "\n'exit message' = %s" % (exit_msg)
            logger.info(msg)
            error_summary = [exit_code, exit_msg, {}]
            error_summary_changed = True

    # Write the fjr report summary of this postjob to a file which task_process reads incrementally
    if error_summary_changed:
        with getLock(G_FJR_PARSE_RESULTS_FILE_NAME):
            with open(G_FJR_PARSE_RESULTS_FILE_NAME, "a+") as fjr_parse_results:
                # make sure the "json file" is written as multiple lines
                fjr_parse_results.write(json.dumps({job_id : {crab_retry : error_summary}}) + "\n")

    # Read, update and re-write the error_summary.json file
    try:
        error_summary_old_content = {}
        if os.stat(G_ERROR_SUMMARY_FILE_NAME).st_size != 0:
            fsummary.seek(0)
            error_summary_old_content = json.load(fsummary)
    except (IOError, ValueError):
        # There is nothing to do if the error_summary file doesn't exist or is invalid.
        # Just recreate it.
        logger.info("File %s is empty, wrong or does not exist. Will create a new file." % (G_ERROR_SUMMARY_FILE_NAME))
    error_summary_new_content = error_summary_old_content
    error_summary_new_content[job_id] = {crab_retry : error_summary}

    # If we have updated the error summary, write it to the json file.
    # Use a temporary file and rename to avoid concurrent writing of the file.
    if error_summary_changed:
        logger.debug("Writing error summary file")
        tempFilename = (G_ERROR_SUMMARY_FILE_NAME + ".%s") % os.getpid()
        with open(tempFilename, "w") as tempFile:
            json.dump(error_summary_new_content, tempFile)
        move(tempFilename, G_ERROR_SUMMARY_FILE_NAME)
        logger.debug("Written error summary file")


def fixUpTempStorageSite(logger=None, siteName=None):
    """
    overrides the name of the site used for local stageout at WN in /store/temp/user
    for cases where new need to direct Rucio to use a different site name for
    LFN2PFN resolution in order to work around the fact that Rucio does not
    implement full CMS TFC functionalities
    see https://github.com/dmwm/CRABServer/issues/6285
    :param logger: a logger object where to log messages
    :param siteName: the name to ovrerride
    :return: the new Site
    """
    # pretend that CRAB output staged out at FNAL are at T3_US_FNALLPC
    newName = siteName
    if siteName == 'T1_US_FNAL_Disk':
        newName = 'T3_US_FNALLPC'
        if logger:
            logger.warning('temp storage site changed from T1_US_FNAL_Disk to T3_US_FNALLPC ')
    return newName

# ==============================================================================

class ASOServerJob():
    """
    Class used to inject transfer requests to ASO database.
    """
    def __init__(self, logger, aso_start_time, aso_start_timestamp, dest_site, source_dir,
                 dest_dir, source_sites, job_id, filenames, reqname, log_size,
                 log_needs_transfer, job_report_output, job_ad, crab_retry, retry_timeout,
                 job_failed, transfer_logs, transfer_outputs, rest_host, db_instance, pubname):
        """
        ASOServerJob constructor.
        """
        self.logger = logger
        self.publishname = pubname
        self.docs_in_transfer = None
        self.crab_retry = crab_retry
        self.retry_timeout = retry_timeout
        self.job_id = job_id
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
        self.aso_start_time = aso_start_time
        self.aso_start_timestamp = aso_start_timestamp
        proxy = os.environ.get('X509_USER_PROXY', None)
        self.proxy = proxy
        self.rest_host = rest_host
        self.db_instance = db_instance
        self.rest_url = rest_host + '/crabserver/' + db_instance + '/'  # used in logging
        self.found_doc_in_db = False
        try:
            self.crabserver = CRABRest(self.rest_host, proxy, proxy, retry=2, userAgent='CRABSchedd')
            self.crabserver.setDbInstance(self.db_instance)
        except Exception as ex:
            msg = "Failed to connect to ASO database via CRABRest: %s" % (str(ex))
            self.logger.exception(msg)
            raise RuntimeError(msg) from ex

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def save_docs_in_transfer(self):
        """ The function is used to save into a file the documents we are transfering so
            we do not have to query the DB to get this list every time the postjob is restarted.
        """
        try:
            filename = 'transfer_info/docs_in_transfer.%s.%d.json' % (self.job_id, self.crab_retry)
            with open(filename, 'w') as fd:
                json.dump(self.docs_in_transfer, fd)
        except:
            #Only printing a generic message, the full stacktrace is printed in execute()
            self.logger.error("Failed to save the docs in transfer. Aborting the postjob")
            raise

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def load_docs_in_transfer(self):
        """ Function that loads the object saved as a json by save_docs_in_transfer
        """
        try:
            filename = 'transfer_info/docs_in_transfer.%s.%d.json' % (self.job_id, self.crab_retry)
            with open(filename) as fd:
                self.docs_in_transfer = json.load(fd)
        except Exception as ex:
            #Only printing a generic message, the full stacktrace is printed in execute()
            self.logger.error("Failed to load the docs in transfer. Aborting the postjob")
            raise Exception from ex

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_transfers(self):
        self.load_docs_in_transfer()
        failed_killed_transfers = []
        done_transfers = []
        starttime = time.time()
        if self.aso_start_timestamp:
            starttime = self.aso_start_timestamp
        if first_pj_execution():
            self.logger.info("====== Starting to monitor ASO transfers.")
        try:
            with getLock('get_transfers_statuses'):
                # Get the transfer status in all documents listed in self.docs_in_transfer.
                transfers_statuses = self.get_transfers_statuses()
        except TransferCacheLoadError as e:
            self.logger.info("Error getting the status of the transfers. Deferring PJ. Got: %s" % e)
            return 4
        msg = "Got statuses: %s; %.1f hours since transfer submit."
        msg = msg % (", ".join(transfers_statuses), (time.time()-starttime)/3600.0)
        self.logger.info(msg)
        all_transfers_finished = True
        doc_ids = [doc_info['doc_id'] for doc_info in self.docs_in_transfer]
        for transfer_status, doc_id in zip(transfers_statuses, doc_ids):
            # States to wait on.
            if transfer_status in ['new', 'acquired', 'retry', 'unknown', 'submitted']:
                all_transfers_finished = False
                continue
            # Good states.
            if transfer_status in ['done']:
                if doc_id not in done_transfers:
                    done_transfers.append(doc_id)
                continue
            # Bad states.
            if transfer_status in ['failed', 'killed', 'kill']:
                if doc_id not in failed_killed_transfers:
                    failed_killed_transfers.append(doc_id)
                    msg = "Stageout job (internal ID %s) failed with status '%s'." % (doc_id, transfer_status)
                    doc = self.load_transfer_document(doc_id)
                    if doc and ('transfer_failure_reason' in doc) and doc['transfer_failure_reason']:
                        # reasons:  The transfer failure reason(s).
                        # app:      The application that gave the transfer failure reason(s).
                        #           E.g. 'aso' or '' (meaning the postjob). When printing the
                        #           transfer failure reasons (e.g. below), print also that the
                        #           failures come from the given app (if app != '').
                        # severity: Either 'permanent' or 'recoverable'.
                        #           It is set by PostJob in the perform_transfers() function,
                        #           when is_failure_permanent() is called to determine if a
                        #           failure is permanent or not.
                        reasons, app, severity = doc['transfer_failure_reason'], 'aso', None
                        if reasons:
                            # Do not add this if FTS jobID is not available
                            if doc['fts_id']:
                                reasons += " [...CUT...] Full log at https://fts3-cms.cern.ch:8449/fts3/ftsmon/#/job/%s" % doc['fts_id']
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
                raise RuntimeError(exmsg)
        if all_transfers_finished:
            msg = "All transfers finished."
            self.logger.info(msg)
            if failed_killed_transfers:
                msg = "There were %d failed/killed transfers:" % (len(failed_killed_transfers))
                msg += " %s" % (", ".join(failed_killed_transfers))
                self.logger.info(msg)
                self.logger.info("====== Finished to monitor ASO transfers.")
                return 1
            self.logger.info("====== Finished to monitor ASO transfers.")
            return 0
        # If there is a timeout for transfers to complete, check if it was exceeded
        # and if so kill the ongoing transfers. # timeout = -1 means no timeout.
        if self.retry_timeout != -1 and time.time() - starttime > self.retry_timeout:
            msg = "Post-job reached its timeout of %d seconds waiting for ASO transfers to complete." % (self.retry_timeout)
            msg += " Will cancel ongoing ASO transfers."
            self.logger.warning(msg)
            self.logger.info("====== Starting to cancel ongoing ASO transfers.")
            docs_to_cancel = {}
            reason = "Cancelled ASO transfer after timeout of %d seconds." % (self.retry_timeout)
            for doc_info in self.docs_in_transfer:
                doc_id = doc_info['doc_id']
                if doc_id not in done_transfers + failed_killed_transfers:
                    docs_to_cancel.update({doc_id: reason})
            cancelled, not_cancelled = self.cancel(docs_to_cancel, max_retries=2)
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
        # defer the execution of the postjob
        return 4

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def run(self):
        """
        This is the main method in ASOServerJob. Should be called after initializing
        an instance.
        """
        with getLock('get_transfers_statuses'):
            self.docs_in_transfer = self.inject_to_aso()
        if self.docs_in_transfer is False:
            exmsg = "Couldn't upload document to ASO database"
            raise RuntimeError(exmsg)
        if not self.docs_in_transfer:
            self.logger.info("No files to transfer via ASO. Done!")
            return 0
        self.save_docs_in_transfer()
        return 4

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def recordASOStartTime(self):
        # Add the post-job exit code and error message to the job report.
        job_report = {}
        try:
            with open(G_JOB_REPORT_NAME) as fd:
                job_report = json.load(fd)
        except (IOError, ValueError):
            pass
        job_report['aso_start_timestamp'] = int(time.time())
        job_report['aso_start_time'] = str(datetime.datetime.now())
        with open(G_JOB_REPORT_NAME, 'w') as fd:
            json.dump(job_report, fd)

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def inject_to_aso(self):
        """
        Inject documents to ASO database if not done by cmscp from worker node.
        returs: docs_in_trasnfer a list of documents injected to ASO, possibly an emtpy list []
                 if e.g. the file was transferred directly from job to destination
                It returns False in case of error. So the caller must be careful to consider
                 and empty list as success and only the False boolean as failure.
                Clearly this could have been done better... e.g. raising an exception
        """
        self.found_doc_in_db = False  # This is only for oracle implementation and we want to check before adding new doc.
        self.logger.info("====== Starting to check uploads to ASO database.")
        docs_in_transfer = []
        output_files = []
        now = str(datetime.datetime.now())
        last_update = int(time.time())

        if self.aso_start_timestamp is None or self.aso_start_time is None:
            self.aso_start_timestamp = last_update
            self.aso_start_time = now
            msg = "Unable to determine ASO start time from job report."
            msg += " Will use ASO start time = %s (%s)."
            msg = msg % (self.aso_start_time, self.aso_start_timestamp)
            self.logger.warning(msg)

        role = str(self.job_ad['CRAB_UserRole'])
        if str(self.job_ad['CRAB_UserRole']).lower() == 'undefined':
            role = ''
        group = str(self.job_ad['CRAB_UserGroup'])
        if str(self.job_ad['CRAB_UserGroup']).lower() == 'undefined':
            group = ''

        with open('taskinformation.pkl', 'rb') as fd:
            task = pickle.load(fd)

        # Now need to loop on all output files in this job, build the outputdatset
        # name for each (if applicable) and fill the file_info dictionary for each
        # For reasons detailed below, need to repeat the loop 3 times
        # Loop #1. Find information for all output files in this job
        for output_module in self.job_report_output.values():
            for output_file_info in output_module:
                file_info = {}
                file_info['pfn'] = str(output_file_info['pfn'])
                file_info['checksums'] = output_file_info.get('checksums', {'cksum': '0', 'adler32': '0'})
                file_info['outsize'] = output_file_info.get('size', 0)
                file_info['direct_stageout'] = output_file_info.get('direct_stageout', False)
                file_info['pset_hash'] = output_file_info.get('pset_hash', 32*'0')
                file_info['module_label'] = output_file_info.get('module_label')
                # assign filetype based on the classification made by CRAB Client
                if file_info['pfn'] in task['tm_edm_outfiles']:
                    file_info['filetype'] = 'EDM'
                elif file_info['pfn'] in task['tm_tfile_outfiles']:
                    file_info['filetype'] = 'TFILE'
                elif file_info['pfn'] in task['tm_outfiles']:
                    file_info['filetype'] = 'FAKE'
                else:
                    file_info['filetype'] = 'UNKNOWN'
                output_files.append(file_info)

        # Loop #2. find out if there are multiple output modules, since rule
        # for constructing outputdataset name is different in that case, see
        # https://twiki.cern.ch/twiki/bin/view/CMSPublic/Crab3DataHandling#Output_dataset_names_in_DBS
        edm_file_count = 0
        for file_info in output_files:
            if file_info['filetype'] == 'EDM':
                edm_file_count += 1
        multiple_edm = edm_file_count > 1
        # Loop #3. compute the output dataset name for each file and add it to file_info
        for file_info in output_files:
            # use common function with PostJob to ensure uniform dataset name
            if file_info['filetype'] in ['EDM', 'DQM'] and task['tm_publication'] == 'T':
                # group name overrides username when present:
                username = self.job_ad['CRAB_UserHN']
                if self.dest_dir.startswith('/store/group/') and self.dest_dir.split('/')[3]:
                    username = self.dest_dir.split('/')[3]
                module_label = file_info.get('module_label') if multiple_edm else None
                outdataset = compute_outputdataset_name(primaryDS=self.job_ad['CRAB_PrimaryDataset'],
                                                        username=username,
                                                        publish_name=self.publishname,
                                                        pset_hash=file_info['pset_hash'],
                                                        module_label=module_label)
            else:
                outdataset = G_FAKE_OUTDATASET
            file_info['outputdataset'] = outdataset

        found_log = False
        # need to process output fils grouped by where they are
        # files may be at different sites if local stageout failed for some
        for source_site, filename in zip(self.source_sites, self.filenames):
            # PREPARE TRANSFER INFO
            # We assume that the first file in self.filenames is the logs archive.
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
                # needs_transfer is False if and only if the file was staged out
                # from the worker node directly to the permanent storage.
                needs_transfer = not output_files[ifile]['direct_stageout']
                file_output_type = output_files[ifile]['filetype']
                outputdataset = output_files[ifile]['outputdataset']
            else:
                found_log = True
                if not self.transfer_logs:
                    continue
                source_lfn = os.path.join(self.source_dir, 'log', filename)
                dest_lfn = os.path.join(self.dest_dir, 'log', filename)
                file_type = 'log'
                size = self.log_size
                # a few fake values for log files for fields which will go in CRAB internal DB's
                checksums = {'adler32': 'abc'}  # this is not what FTS will use !
                outputdataset = G_FAKE_OUTDATASET
                # needs_transfer is False if and only if the file was staged out
                # from the worker node directly to the permanent storage.
                needs_transfer = self.log_needs_transfer
            self.logger.info("Working on file %s" % (filename))
            doc_id = getHashLfn(source_lfn)
            doc_new_info = {'state': 'new',
                            'source': fixUpTempStorageSite(logger=self.logger, siteName=source_site),
                            'destination': self.dest_site,
                            'checksums': checksums,
                            'size': size,
                            'last_update': last_update,
                            'start_time': now,
                            'end_time': '',
                            'job_end_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                            'retry_count': [],
                            'failure_reason': [],
                            # The 'job_retry_count' is used by ASO when reporting to dashboard,
                            # so it is OK to set it equal to the crab (post-job) retry count.
                            'job_retry_count': self.crab_retry,
                            'outputdataset': outputdataset,
                           }
            direct = False
            if not needs_transfer:
                msg = "File %s is marked as having been directly staged out"
                msg += " from the worker node to the permanent storage."
                msg = msg % (filename)
                self.logger.info(msg)
                doc_new_info['state'] = 'done'
                doc_new_info['end_time'] = now
                direct = True
            # SET THE PUBLLICATION FLAG
            task_publish = int(self.job_ad['CRAB_Publish'])
            publication_msg = None
            if file_type == 'output':
                publish = task_publish
                if publish and self.job_failed:
                    publication_msg = "Disabling publication of output file %s,"
                    publication_msg += " because job is marked as failed."
                    publication_msg = publication_msg % (filename)
                    publish = 0
                if publish and file_output_type != 'EDM':
                    publication_msg = "Disabling publication of output file %s,"
                    publication_msg += " because it is not of EDM type."
                    publication_msg = publication_msg % (filename)
                    publish = 0
            else:
                # This is the log file, so obviously publication should be turned off.
                publish = 0
            # FINALLY INJECT TO ASO (MODIFY EXISTING DOC OR CREATE NEW ONE)
            # What does ASO needs to do for this file (transfer and/or publication) is
            # saved in this list for the only purpose of printing a better message later.
            aso_tasks = []
            if needs_transfer:
                aso_tasks.append("transfer")
            if publish:
                aso_tasks.append("publication")
            delayed_publicationflag_update = False
            if not (needs_transfer or publish or direct):
                # This file doesn't need transfer nor publication, so we don't need to upload
                # a document to ASO database.
                if publication_msg:
                    self.logger.info(publication_msg)
                msg = "File %s doesn't need transfer nor publication."
                msg += " No need to inject a document to ASO."
                msg = msg % (filename)
                self.logger.info(msg)
            else:
                # If the file needs both transfer and publication we turn the publication flag off and
                # we will update the database once the filemetadata are uploaded
                # The other cases are:
                #   1) Publication already off: we obviously do not need to do anything
                #   2) Transfer not required (e.g.: direct stageout) but publication necessary:
                #      In this case we just upload the document now with publication requested
                if needs_transfer and publish:
                    publish = 0
                    delayed_publicationflag_update = True
                    msg = "Temporarily disabling publication flag."
                    msg += "It will be updated once the transfer is done (and filemetadata uploaded)."
                    self.logger.info(msg)
                # This file needs transfer and/or publication. If a document (for the current
                # job retry) is not yet in ASO database, we need to do the upload.
                needs_commit = True
                try:
                    doc = self.getDocByID(doc_id)
                    # The document was already uploaded to ASO database. It could have been
                    # uploaded from the WN in the current job retry or in a previous job retry,
                    # or by the postjob in a previous job retry.
                    transfer_status = doc.get('state')
                    if not transfer_status:
                        # This means it is RDBMS database as we changed fields to match what they are :)
                        transfer_status = doc.get('transfer_state')
                    if doc.get('start_time') == self.aso_start_time or \
                       doc.get('start_time') == self.aso_start_timestamp:
                        # The document was uploaded from the WN in the current job retry, so we don't
                        # upload a new document. (If the transfer is done or ongoing, then of course we
                        # don't want to re-inject the transfer request. OTOH, if the transfer has
                        # failed, we don't want the postjob to retry it; instead the postjob will exit
                        # and the whole job will be retried).
                        msg = "LFN %s (id %s) is already in ASO database"
                        msg += " (it was injected from the worker node in the current job retry)"
                        msg += " and file transfer status is '%s'."
                        msg = msg % (source_lfn, doc_id, transfer_status)
                        self.logger.info(msg)
                        needs_commit = False
                    else:
                        # The document was uploaded in a previous job retry. This means that in the
                        # current job retry the injection from the WN has failed or cmscp did a direct
                        # stageout. We upload a new stageout request, unless the transfer is still
                        # ongoing (which should actually not happen, unless the postjob for the
                        # previous job retry didn't run).
                        msg = "LFN %s (id %s) is already in ASO database (file transfer status is '%s'),"
                        msg += " but does not correspond to the current job retry."
                        msg = msg % (source_lfn, doc_id, transfer_status)
                        if transfer_status in ['acquired', 'new', 'retry']:
                            msg += "\nFile transfer status is %s, which is not terminal ('done', 'failed' or 'killed')." % transfer_status
                            msg += " Will not inject a new document for the current job retry."
                            self.logger.info(msg)
                            needs_commit = False
                        else:
                            msg += " Will inject a new %s request." % (' and '.join(aso_tasks))
                            self.logger.info(msg)
                            msg = "Previous document: %s" % (pprint.pformat(doc))
                            self.logger.debug(msg)

                except NotFound:
                    # The document was not yet uploaded to ASO database (if this is the first job
                    # retry, then either the upload from the WN failed, or cmscp did a direct
                    # stageout and here we need to inject for publication only). In any case we
                    # have to inject a new document.
                    msg = "LFN %s (id %s) is not in ASO database."
                    msg += " Will inject a new %s request."
                    msg = msg % (source_lfn, doc_id, ' and '.join(aso_tasks))
                    self.logger.info(msg)
                    if publication_msg:
                        self.logger.info(publication_msg)
                    input_dataset = str(self.job_ad['DESIRED_CMSDataset'])
                    if str(self.job_ad['DESIRED_CMSDataset']).lower() == 'undefined':
                        input_dataset = ''
                    primary_dataset = str(self.job_ad['CRAB_PrimaryDataset'])
                    if input_dataset:
                        input_dataset_or_primary_dataset = input_dataset
                    elif primary_dataset:
                        input_dataset_or_primary_dataset = '/'+primary_dataset # Adding the '/' until we fix ASO
                    else:
                        input_dataset_or_primary_dataset = '/'+'NotDefined' # Adding the '/' until we fix ASO
                    doc = {'_id': doc_id,
                           'inputdataset': input_dataset_or_primary_dataset,
                           'lfn': source_lfn,
                           'source_lfn': source_lfn,
                           'destination_lfn': dest_lfn,
                           'checksums': checksums,
                           'user': str(self.job_ad['CRAB_UserHN']),
                           'group': group,
                           'role': role,
                           'dbs_url': str(self.job_ad['CRAB_DBSURL']),
                           'workflow': self.reqname,
                           'jobid': self.job_id,
                           'publication_state': 'not_published',
                           'publication_retry_count': [],
                           'type': file_type,
                          }
                    # TODO: We do the following, only because that's what ASO does when a file has
                    # been successfully transferred. But this modified LFN makes no sence when it
                    # starts with /store/temp/user/, because the modified LFN is then
                    # /store/user/<username>.<hash>/bla/blabla, i.e. it contains the <hash>, which
                    # is never part of a destination LFN, but only of temp source LFNs.
                    # Once ASO uses the source_lfn and the destination_lfn instead of only the lfn,
                    # this should not be needed anymore.
                    if not needs_transfer:
                        doc['lfn'] = source_lfn.replace('/store/temp', '/store', 1)
                except Exception as ex:
                    msg = "Error loading document from ASO database: %s" % (str(ex))
                    try:
                        msg += "\n%s" % (traceback.format_exc())
                    except AttributeError:
                        msg += "\nTraceback unavailable."
                    self.logger.error(msg)
                    return False
                # If after all we need to upload a new document to ASO database, let's do it.
                if needs_commit:
                    doc.update(doc_new_info)
                    doc['publish'] = publish
                    msg = "ASO job description: %s" % (pprint.pformat(doc))
                    self.logger.info(msg)
                    commit_result_msg = self.updateOrInsertDoc(doc, needs_transfer)
                    if 'error' in commit_result_msg:
                        msg = "Error injecting document to ASO database:\n%s" % (commit_result_msg)
                        self.logger.info(msg)
                        return False
                # Record all files for which we want the post-job to monitor their transfer.
                if needs_transfer:
                    doc_info = {'doc_id'     : doc_id,
                                'start_time' : doc.get('start_time'),
                                'delayed_publicationflag_update' : delayed_publicationflag_update
                               }
                    docs_in_transfer.append(doc_info)
                    # Make sure that the fjr has the record of the ASO start transfer time stamp
                    self.recordASOStartTime()


        self.logger.info("====== Finished to check uploads to ASO database.")

        return docs_in_transfer

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def getDocByID(self, doc_id):
        docInfo = self.crabserver.get(api='fileusertransfers', data=encodeRequest({'subresource': 'getById', "id": doc_id}))
        if docInfo and len(docInfo[0]['result']) == 1:
            # Means that we have already a document in database!
            docInfo = oracleOutputMapping(docInfo)
            # Just to be 100% sure not to break after the mapping been added
            if not docInfo:
                self.found_doc_in_db = False
                raise NotFound('Document not found in database')
            # transfer_state and publication_state is a number in database.
            docInfo[0]['transfer_state'] = TRANSFERDB_STATES[docInfo[0]['transfer_state']]
            docInfo[0]['publication_state'] = PUBLICATIONDB_STATES[docInfo[0]['publication_state']]
            # Also change id to doc_id
            docInfo[0]['job_id'] = docInfo[0]['id']
            self.found_doc_in_db = True  # This is needed for further if there is a need to update doc info in DB
            return docInfo[0]
        self.found_doc_in_db = False
        raise NotFound('Document not found in database!')

    def updateOrInsertDoc(self, doc, toTransfer):
        """ need a docstring here """
        returnMsg = {}
        if not self.found_doc_in_db:
            # This means that it was not founded in DB and we will have to insert new doc
            newDoc = {'id': doc['_id'],
                      'username': doc['user'],
                      'taskname': doc['workflow'],
                      'start_time': self.aso_start_timestamp,
                      'destination': doc['destination'],
                      'destination_lfn': doc['destination_lfn'],
                      'source': doc['source'],
                      'source_lfn': doc['source_lfn'],
                      'filesize': doc['size'],
                      'publish': doc['publish'],
                      'transfer_state': doc['state'].upper(),
                      'publication_state': 'NEW' if doc['publish'] else 'NOT_REQUIRED',
                      'job_id': doc['jobid'],
                      'job_retry_count': doc['job_retry_count'],
                      'type': doc['type'],
                      }
            try:
                self.crabserver.put(api='fileusertransfers', data=encodeRequest(newDoc))
            except HTTPException as hte:
                msg = "Error uploading document to database."
                msg += " Transfer submission failed."
                msg += "\n%s" % (str(hte.headers))
                returnMsg['error'] = msg
            updateDoc={'subresource':'updateTransfers', 'list_of_ids':[doc['_id']]}
            updateDoc['list_of_transfer_state'] = [newDoc['transfer_state']]
            # make sure that asoworker field in transfersdb is always filled, since
            # otherwise whichever Publisher process looks first for things to do, grabs them
            # https://github.com/dmwm/CRABServer/blob/8012e1297759bab620d89c8cb253f1832b4eb466/src/python/Databases/FileTransfersDB/Oracle/FileTransfers/FileTransfers.py#L27-L33
            # but since PUT API ignores an asoworker argument when inserting
            # https://github.com/dmwm/CRABServer/blob/43f6377447922d46353072e86d960e3c78967a17/src/python/CRABInterface/RESTFileUserTransfers.py#L122-L125
            # we need to update the record after insetion with a POST, which requires list of ids and states
            # https://github.com/dmwm/CRABServer/blob/43f6377447922d46353072e86d960e3c78967a17/src/python/CRABInterface/RESTFileTransfers.py#L131-L133
            updateDoc['asoworker'] = 'schedd'
            try:
                self.crabserver.post(api='filetransfers', data=encodeRequest(updateDoc))
            except HTTPException as hte:
                msg = "Error uploading document to database."
                msg += " Transfer submission failed."
                msg += "\n%s" % (str(hte.headers))
                returnMsg['error'] = msg
        else:
            # This means it is in database and we need only update specific fields.
            newDoc = {'id': doc['id'],
                      'username': doc['username'],
                      'taskname': doc['taskname'],
                      'start_time': self.aso_start_timestamp,
                      'source': doc['source'],
                      'source_lfn': doc['source_lfn'],
                      'filesize': doc['filesize'],
                      'transfer_state': doc.get('state', 'NEW').upper(),
                      'publish': doc['publish'],
                      'publication_state': 'NEW' if doc['publish'] else 'NOT_REQUIRED',
                      'job_id': doc['jobid'],
                      'job_retry_count': doc['job_retry_count'],
                      'transfer_retry_count': 0,
                      'subresource': 'updateDoc'}
            try:
                self.crabserver.post(api='fileusertransfers', data=encodeRequest(newDoc))
            except HTTPException as hte:
                msg = "Error updating document in database."
                msg += " Transfer submission failed."
                msg += "\n%s" % (str(hte.headers))
                returnMsg['error'] = msg
            # Previous post resets asoworker to NULL. This is not good, so we set it again
            # using a different API to update the transfersDB record
            updateDoc = {}
            updateDoc['list_of_ids'] = [newDoc['id']]
            updateDoc['list_of_transfer_state'] = [newDoc['transfer_state']]
            updateDoc['subresource'] = 'updateTransfers'
            updateDoc['asoworker'] = 'schedd'
            try:
                self.crabserver.post(api='filetransfers', data=encodeRequest(updateDoc))
            except HTTPException as hte:
                msg = "Error uploading document to database."
                msg += " Transfer submission failed."
                msg += "\n%s" % (str(hte.headers))
                returnMsg['error'] = msg
        if toTransfer:
            if not 'publishname' in newDoc:
                newDoc['publishname'] = self.publishname
            if not 'checksums' in newDoc:
                newDoc['checksums'] = doc['checksums']
            if not 'destination_lfn' in newDoc:
                newDoc['destination_lfn'] = doc['destination_lfn']
            if not 'destination' in newDoc:
                newDoc['destination'] = doc['destination']
            if not 'outputdataset' in newDoc:
                newDoc['outputdataset'] = doc['outputdataset']
            # Rucio ASO requires the "type" field to handle the log transfers.
            if not 'type' in newDoc:
                newDoc['type'] = doc['type']
            with open('task_process/transfers.txt', 'a+') as transfers_file:
                transfer_dump = json.dumps(newDoc)
                transfers_file.write(transfer_dump+"\n")
            if not os.path.exists('task_process/RestInfoForFileTransfers.json'):
            #if not os.path.exists('task_process/rest_filetransfers.txt'):
                restInfo = {'host':self.rest_host,
                            'dbInstance': self.db_instance,
                            'proxyfile': os.path.basename(self.proxy)}
                with open('task_process/RestInfoForFileTransfers.json', 'w') as fp:
                    json.dump(restInfo, fp)
        else:
            if not 'publishname' in newDoc:
                newDoc['publishname'] = self.publishname
            if not 'checksums' in newDoc:
                newDoc['checksums'] = doc['checksums']
            if not 'destination_lfn' in newDoc:
                newDoc['destination_lfn'] = doc['destination_lfn']
            if not 'destination' in newDoc:
                newDoc['destination'] = doc['destination']
            with open('task_process/transfers_direct.txt', 'a+') as transfers_file:
                transfer_dump = json.dumps(newDoc)
                transfers_file.write(transfer_dump+"\n")
            if not os.path.exists('task_process/RestInfoForFileTransfers.json'):
            #if not os.path.exists('task_process/rest_filetransfers.txt'):
                restInfo = {'host':self.rest_host,
                            'dbInstance': self.db_instance,
                            'proxyfile': os.path.basename(self.proxy)}
                with open('task_process/RestInfoForFileTransfers.json','w') as fp:
                    json.dump(restInfo, fp)
        return returnMsg

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def build_failed_cache(self):
        aso_info = {
            "query_timestamp": time.time(),
            "query_succeded": False,
            "query_jobid": self.job_id,
            "results": {},
        }
        tmp_fname = "aso_status.%d.json" % (os.getpid())
        with open(tmp_fname, 'w') as fd:
            json.dump(aso_info, fd)
        os.rename(tmp_fname, "aso_status.json")

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_transfers_statuses(self):
        """
        Retrieve the status of all transfers from the cached file 'aso_status.json'
        or by querying an ASO database view if the file is more than 5 minutes old
        or if we injected a document after the file was last updated.
        """
        statuses = []
        query_view = False
        if not os.path.exists("aso_status.json"):
            query_view = True
        aso_info = {}
        if not query_view:
            query_view = True
            try:
                with open("aso_status.json") as fd:
                    aso_info = json.load(fd)
            except Exception as ex:
                msg = "Failed to load transfer cache."
                self.logger.exception(msg)
                raise TransferCacheLoadError(msg) from ex
            last_query = aso_info.get("query_timestamp", 0)
            last_jobid = aso_info.get("query_jobid", "unknown")
            last_succeded = aso_info.get("query_succeded", True)
            # We can use the cached data if:
            # - It is from the last 15 minutes, AND
            # - It is from after we submitted the transfer.
            # Without the second condition, we run the risk of using the previous stageout
            # attempts results.
            if (time.time() - last_query < 900) and (last_query > self.aso_start_timestamp):
                msg = f"Using the cache since it is up to date (last_query={last_query}) "\
                f"and it is after we submitted the transfer (aso_start_timestamp={self.aso_start_timestamp})"
                self.logger.info(msg)
                query_view = False
                if not last_succeded:
                    #no point in continuing if the last query failed. Just defer the PJ and retry later
                    msg = ("Not using info about transfer statuses from the transfer cache. "
                           "Deferring the postjob."
                           "PJ num %s failed to load the information from the DB and cache has not expired yet." % last_jobid)
                    raise TransferCacheLoadError(msg)
            for doc_info in self.docs_in_transfer:
                doc_id = doc_info['doc_id']
                if doc_id not in aso_info.get("results", {}):
                    self.logger.debug("Changing query_view back to true")
                    query_view = True
                    break
        if query_view:
            self.logger.debug("Querying ASO RDBMS database.")
            try:
                view_results = self.crabserver.get(api='fileusertransfers',
                                                   data=encodeRequest({'subresource': 'getTransferStatus',
                                                                       'username': str(self.job_ad['CRAB_UserHN']),
                                                                       'taskname': self.reqname}))
                view_results_dict = oracleOutputMapping(view_results, 'id')
                # There is so much noise values in aso_status.json file. So lets provide a new file structure.
                # We will not run ever for one task which can use also RDBMS
                # New Structure for view_results_dict is
                # {"DocumentHASHID": [{"id": "DocumentHASHID", "start_time": timestamp, "transfer_state": NUMBER!, "last_update": timestamp}]}
                for document in view_results_dict:
                    view_results_dict[document][0]['state'] = TRANSFERDB_STATES[view_results_dict[document][0]['transfer_state']].lower()
            except Exception as ex:
                msg = "Error while querying the RDBMS (Oracle) database."
                self.logger.exception(msg)
                self.build_failed_cache()
                raise TransferCacheLoadError(msg) from ex
            aso_info = {
                "query_timestamp": time.time(),
                "query_succeded": True,
                "query_jobid": self.job_id,
                "results": view_results_dict,
            }
            tmp_fname = "aso_status.%d.json" % (os.getpid())
            with open(tmp_fname, 'w') as fd:
                json.dump(aso_info, fd)
            os.rename(tmp_fname, "aso_status.json")
        else:
            self.logger.debug("Using cached ASO results.")
        if not aso_info:
            raise TransferCacheLoadError("Unexpected error. aso_info is not set. Deferring postjob")
        for doc_info in self.docs_in_transfer:
            doc_id = doc_info['doc_id']
            if doc_id not in aso_info.get("results", {}):
                msg = "Document with id %s not found in the transfer cache. Deferring PJ." % doc_id
                raise TransferCacheLoadError(msg)                # Not checking timestamps for oracle since we are not injecting from WN
            # and it cannot happen that condor restarts screw up things.
            transfer_status = aso_info['results'][doc_id][0]['state']
            statuses.append(transfer_status)
        return statuses

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def load_transfer_document(self, doc_id):
        """
        Wrapper to load a document from RDBMS, catching exceptions.
        """
        doc = None
        try:
            doc = self.getDocByID(doc_id)
        except HTTPException as hte:
            msg = "Error retrieving document from ASO database for ID %s: %s" % (doc_id, str(hte.headers))
            self.logger.error(msg)
        except NotFound:
            msg = "Document is not found in ASO RDBMS database for ID %s" % (doc_id)
            self.logger.warning(msg)
        except Exception:
            msg = "Error retrieving document from ASO database for ID %s" % (doc_id)
            self.logger.exception(msg)
        return doc

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_transfers_statuses_fallback(self):
        """
        Retrieve the status of all transfers by loading the corresponding documents
        from ASO database and checking the 'state' field.
        """
        msg = "Querying transfers statuses using fallback method (i.e. loading each document)."
        self.logger.debug(msg)
        statuses = []
        for doc_info in self.docs_in_transfer:
            doc_id = doc_info['doc_id']
            doc = self.load_transfer_document(doc_id)
            status = doc['transfer_state'] if doc else 'unknown'
            statuses.append(status)
        return statuses

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def cancel(self, doc_ids_reasons=None, max_retries=0):
        """
        Method used to "cancel/kill" ASO transfers. The only thing that this
        function does is to put the 'state' field of the corresponding documents in
        the ASO database to 'killed' (and the 'end_time' field to the current time).
        Killing actual FTS transfers (if possible) is left to ASO.
        """
        if doc_ids_reasons is None:
            doc_ids_reasons = {}
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
        transfersToKill = []  # This is for RDBMS implementation only
        if max_retries:
            msg = "In case of cancellation failure, will retry up to %d times." % (max_retries)
            self.logger.info(msg)
        for retry in range(max_retries + 1):
            if retry > 0:
                time.sleep(3*60)
                msg = "This is cancellation retry number %d." % (retry)
                self.logger.info(msg)
            for doc_id, reason in doc_ids_reasons.items():
                if doc_id in cancelled:
                    continue
                msg = "Cancelling ASO transfer %s" % (doc_id)
                if reason:
                    msg += " with following reason: %s" % (reason)
                self.logger.info(msg)
                doc = self.load_transfer_document(doc_id)
                if not doc:
                    msg = "Could not cancel ASO transfer %s; failed to load document." % (doc_id)
                    self.logger.warning(msg)
                    if retry == max_retries:
                        not_cancelled.append((doc_id, msg))
                    continue
                doc['state'] = 'killed'
                doc['end_time'] = now
                username = doc['username']
                transfersToKill.append(doc_id)

            # Now this means that we have a list of ids which needs to be killed
            # First try to kill ALL in one API call
            newDoc = {'listOfIds': transfersToKill,
                      'publish' : 0,
                      'username': username,
                      'subresource': 'killTransfersById'}
            try:
                killedFiles = self.crabserver.post(api='fileusertransfers', data=encodeRequest(newDoc, ['listOfIds']))
                not_cancelled = killedFiles[0]['result'][0]['failedKill']
                cancelled = killedFiles[0]['result'][0]['killed']
                break  # no need to retry
            except HTTPException as hte:
                msg = "Error setting KILL status in database."
                msg += " Transfer KILL failed."
                msg += "\n%s" % (str(hte.headers))
                self.logger.warning(msg)
                not_cancelled = transfersToKill
                cancelled = []
            except Exception as ex:
                msg = "Unknown error setting KILL status in database."
                msg += " Transfer KILL failed."
                msg += "\n%s" % (str(ex))
                self.logger.error(msg)
                not_cancelled = transfersToKill
                cancelled = []


            # Ok Now lets do a double check on doc_ids which failed to update one by one.
            # It is just to make proof concept to cover KILL to RDBMS
            self.logger.info("Failed to kill %s and succeeded to kill %s", not_cancelled, cancelled)
            transfersToKill = list(set(transfersToKill) - set(cancelled))
            # Now if there are any transfers left which failed to be killed, try to get it's status
            # and also kill if status is not in KILL or KILLED
            for docIdKill in transfersToKill:
                newDoc = {'listOfIds': [docIdKill],
                          'publish': 0,
                          'username': self.job_ad['CRAB_UserHN'],
                          'subresource': 'killTransfersById'}
                try:
                    doc_out = self.getDocByID(docIdKill)
                    if doc_out['transfer_state'] not in ['kill', 'killed']:
                        killedFiles = self.crabserver.post(api='fileusertransfers', data=encodeRequest(newDoc, ['listOfIds']))
                        failedFiles = killedFiles[0]['result'][0]['failedKill']
                        notfailedFiles = killedFiles[0]['result'][0]['killed']
                        if failedFiles:
                            not_cancelled.append(docIdKill)
                        elif notfailedFiles:
                            cancelled.append(docIdKill)
                    else:
                        cancelled.append(docIdKill)
                except HTTPException as hte:
                    msg = "Error setting KILL status in database."
                    msg += " Transfer KILL failed."
                    msg += "\n%s" % (str(hte.headers))
                    self.logger.warning(msg)
                    not_cancelled.append(docIdKill)
                    cancelled.append(docIdKill)
                except NotFound:
                    # This is strange that document is not found in database.
                    # Just add it as canceled and move on.
                    cancelled.append(docIdKill)
        return cancelled, not_cancelled

    # = = = = = ASOServerJob = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_failures(self):
        """
        Retrieve failures bookeeping dictionary.
        """
        return self.failures

# ==============================================================================

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
        # These attributes are set from arguments passed to the post-job (see
        # RunJobs.dag file). The first four arguments correspond to special script
        # argument macros set by DAGMan. Documentation about these four arguments in
        # http://research.cs.wisc.edu/htcondor/manual/v8.2/2_10DAGMan_Applications.html
        self.dag_jobid           = None # $JOBID
        self.job_return_code     = None # $RETURN
        self.dag_retry           = None # $RETRY
        self.max_retries         = None # $MAX_RETRIES
        self.reqname             = None
        self.job_id              = None
        self.source_dir          = None
        self.dest_dir            = None
        self.logs_arch_file_name = None
        self.stage               = None
        self.output_files_names  = None
        self.output_dataset      = None
        # The crab_retry is the number of times the post-job was ran (not necessarilly
        # completing) for this job id.
        self.crab_retry          = None
        # These attributes are read from the job ad (see parse_job_ad()).
        self.job_ad              = {}
        self.dest_site           = None
        self.job_sw              = None
        self.publish_name        = None
        self.rest_host           = None
        self.db_instance         = None
        self.rest_url            = None
        self.retry_timeout       = None
        self.transfer_logs       = None
        self.transfer_outputs    = None
        # These attributes are read from the job report (see parse_job_report()).
        self.job_report          = {}
        self.job_report_output   = {}
        self.log_needs_transfer  = None
        self.log_needs_file_metadata_upload = None
        self.log_size            = None
        self.executed_site       = None
        self.job_failed          = None
        self.output_files_info   = []
        # Object we will use for making requests to the CRAB REST interface (uploading
        # logs archive and output files matadata).
        self.crabserver          = None
        # Path to the task web directory.
        self.logpath             = None
        # Time-stamps of when transfer documents were inserted into ASO database.
        self.aso_start_time      = None
        self.aso_start_timestamp = None
        # The return value of the retry-job.
        self.retryjob_retval     = None
        # Stageout exit codes for ASO transfers
        self.stageout_exit_codes = []
        # The DAG cluster ID of the job to which this post-job is associated. If the
        # job hasn't run (because the pre-job return value was != 0), the DAG cluster
        # ID is -1 (and the $RETURN argument macro is -1004). This is just the first
        # number in self.dag_jobid.
        self.dag_clusterid       = None
        self.schedd = htcondor.Schedd()

        # Set a logger for the post-job. Use a memory handler by default. Once we know
        # the name of the log file where all the logging should go, we will flush the
        # memory handler there, remove the memory handler and add a file handler. Or we
        # add a stream handler to stdout if no redirection to a log file was requested.
        self.logger = logging.getLogger('PostJob')
        self.logger.setLevel(logging.DEBUG)
        self.memory_handler = logging.handlers.MemoryHandler(capacity=1024*10, flushLevel=logging.CRITICAL + 10)
        self.logging_formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", \
                                                   datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
        self.memory_handler.setFormatter(self.logging_formatter)
        self.memory_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(self.memory_handler)
        self.logger.propagate = False
        self.postjob_log_file_name = None

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_defer_num(self):

        DEFER_INFO_FILE = 'defer_info/defer_num.%s.%d.txt' % (self.job_id, self.dag_retry)
        defer_num = 0

        #read retry number
        if os.path.exists(DEFER_INFO_FILE):
            try:
                with open(DEFER_INFO_FILE) as fd:
                    line = fd.readline().strip()
                    #Protect from empty files, see https://github.com/dmwm/CRABServer/issues/5199
                    if line:
                        defer_num = int(line)
                    else:
                        #if the line is empty we are sure it's the first try. See comment 10 lines below
                        defer_num = 0
            except IOError as e:
                self.logger.error("I/O error(%d): %s", e.errno, e.strerror)
                raise
            except ValueError:
                self.logger.error("Could not convert data to an integer.")
                raise
            except:
                self.logger.exception("Unexpected error: %s", sys.exc_info()[0])
                raise
        else:
            #create the file if it does not exist
            with open(DEFER_INFO_FILE, 'w') as dummyFD:
                pass


        #update retry number
        try:
            #open in rb+ mode instead of w so if the schedd crashes between the open and the write
            #we do not end up with an empty (corrupted) file. (Well, this can only happens the first try)
            with open(DEFER_INFO_FILE, 'r+') as fd:
                #put some spaces to overwrite possibly longer numbers (should never happen, but..)
                fd.write(str(defer_num + 1) + ' '*10)
        except IOError as e:
            self.logger.error("I/O error(%d): %s", e.errno, e.strerror)
            raise
        except:
            self.logger.exception("Unexpected error: %s", sys.exc_info()[0])
            raise

        return defer_num

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def create_taskwebdir(self):
        # The task web directory in the schedd has been created by AdjustSites.py
        self.logpath = os.path.realpath('WEB_DIR')

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def handle_logfile(self):
        # Create a file handler (or a stream handler to stdout), flush the memory
        # handler content to the file (or stdout) and remove the memory handler.
        if os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            handler = logging.StreamHandler(sys.stdout)
        else:
            print("Writing post-job output to %s." % (self.postjob_log_file_name))
            mode = 'w' if first_pj_execution() else 'a'
            handler = logging.FileHandler(filename=self.postjob_log_file_name, mode=mode)
        handler.setFormatter(self.logging_formatter)
        handler.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)
        self.memory_handler.setTarget(handler)
        self.memory_handler.close()
        self.logger.removeHandler(self.memory_handler)
        # Unhandled exceptions are catched in TaskManagerBootstrap and printed. So we
        # need to redirect stdout/err to the post-job log file if we want to see the
        # unhandled exceptions.
        if not os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            fd_postjob_log = os.open(self.postjob_log_file_name, os.O_RDWR | os.O_CREAT | os.O_APPEND, 0o644)
            os.chmod(self.postjob_log_file_name, 0o644)
            os.dup2(fd_postjob_log, 1)
            os.dup2(fd_postjob_log, 2)

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def handle_webdir(self):
        # If the post-job log file exists, create a symbolic link in the task web
        # directory. The pre-job creates already the symlink, but the post-job has to
        # do it by its own in case the pre-job was not executed (e.g. when DAGMan
        # restarts a post-job).
        if os.path.isfile(self.postjob_log_file_name):
            msg = "Creating symbolic link in task web directory to post-job log file: %s -> %s" \
                  % (os.path.join(self.logpath, self.postjob_log_file_name), self.postjob_log_file_name)
            self.logger.debug(msg)
            try:
                os.symlink(os.path.abspath(os.path.join(".", self.postjob_log_file_name)), os.path.join(self.logpath, self.postjob_log_file_name))
            except OSError as ose:
                if ose.errno != 17: #ignore the error if the symlink was already there
                    msg = "Cannot create symbolic link to the postjob log."
                    msg += "\nDetails follow:"
                    self.logger.exception(msg)
                    self.logger.info("Continuing since this is not a critical error.")
            except Exception:
                msg = "Cannot create symbolic link to the postjob log."
                msg += "\nDetails follow:"
                self.logger.exception(msg)
                self.logger.info("Continuing since this is not a critical error.")
        else:
            msg = "Post-job log file %s doesn't exist." % (self.postjob_log_file_name)
            msg += " Will not (re)create the symbolic link in the task web directory."
            self.logger.warning(msg)
        # Copy the job's stdout file job_out.<job_id> to the schedd web directory,
        # naming it job_out.<job_id>.<crab_retry>.txt.
        # NOTE: We now redirect stdout -> stderr; hence, we don't keep stderr in the
        # webdir.
        stdout = "job_out.%s" % (self.job_id)
        stdout_tmp = "job_out.tmp.%s" % (self.job_id)
        if os.path.exists(stdout):
            os.rename(stdout, stdout_tmp)
            fname = "job_out.%s.%d.txt" % (self.job_id, self.crab_retry)
            fname = os.path.join(self.logpath, fname)
            msg = "Copying job stdout from %s to %s." % (stdout, fname)
            self.logger.debug(msg)
            shutil.copy(stdout_tmp, fname)
            with open(stdout_tmp, 'w') as fd_stdout:
                fd_stdout.truncate(0)
            os.chmod(fname, 0o644)
        # Copy the json job report file jobReport.json.<job_id> to
        # job_fjr.<job_id>.<crab_retry>.json and create a symbolic link in the task web
        # directory to the new job report file.
        if os.path.exists(G_JOB_REPORT_NAME):
            msg = "Copying job report from %s to %s." % (G_JOB_REPORT_NAME, G_JOB_REPORT_NAME_NEW)
            self.logger.debug(msg)
            shutil.copy(G_JOB_REPORT_NAME, G_JOB_REPORT_NAME_NEW)
            os.chmod(G_JOB_REPORT_NAME_NEW, 0o644)
            msg = "Creating symbolic link in task web directory to job report file: %s -> %s" \
                  % (os.path.join(self.logpath, G_JOB_REPORT_NAME_NEW), G_JOB_REPORT_NAME_NEW)
            self.logger.debug(msg)
            try:
                os.symlink(os.path.abspath(os.path.join(".", G_JOB_REPORT_NAME_NEW)), os.path.join(self.logpath, G_JOB_REPORT_NAME_NEW))
            except OSError as ose:
                if ose.errno != 17: #ignore the error if the symlink was already there
                    msg = "Cannot create symbolic link to the postjob log."
                    msg += "\nDetails follow:"
                    self.logger.exception(msg)
                    self.logger.info("Continuing since this is not a critical error.")
            except Exception:
                msg = "Cannot create symbolic link to the jobreport json."
                msg += "\nDetails follow:"
                self.logger.exception(msg)
                self.logger.info("Continuing since this is not a critical error.")

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def reportReadBranches(self, branchList=None, username=None, inputDataset=None, taskName=None):
        """ send to MONIT / ElasticSearch the list of Read Branches """

        # get secrets
        secrets = None
        try:
            with open('/data/certs/monit.d/MONIT-CRAB.json', 'r', encoding='utf-8') as fp:
                secrets = json.load(fp)
        except FileNotFoundError:
            self.logger.error("Could not find MONIT secret file, will not report read branches")
            return
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.error(f"Error parsing MONIT secret file, will not report read branches.\n{ex}")
            return
        if not secrets:
            self.logger.error("Error parsing MONIT secret file, will not report read branches")
            return
        user = secrets['username']
        password = secrets['password']
        monitUrl = secrets['url']
        msg = f"Reporting Branches to MONIT USER: {user}"
        self.logger.info(msg)

        # prepare a data dictionary
        docToSend = {"producer": user,
                     "type": "branches",
                     "branches": branchList,  # a list of strings
                     "nBranches": len(branchList),
                     "inputDataset": inputDataset.split(':')[-1],  # strip rucio scope if any
                     "inputDataTier": inputDataset.split('/')[-1],
                     "username": username,
                     "taskname": taskName,
                     }
        # POST data as JSON
        try:
            r = requests.post(url=f"{monitUrl}",
                          auth=HTTPBasicAuth(user, password),
                          data=json.dumps(docToSend),
                          headers={"Content-Type": "application/json; charset=UTF-8"},
                          verify=False
                          )
            msg = f"List of Branches sent to MONIT with status {r.status_code}. Output {r.text}"
            self.logger.info(msg)
        except Exception as ex:
            self.logger.error(f"Failed to send branch list to MONIT. Exception:\n{ex}")

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute(self, *args, **kw):  # pylint: disable=unused-argument
        """
        The execute method of PostJob.
        """
        # Put the arguments to PostJob into class variables.
        self.dag_jobid           = args[0] # = ClusterId.ProcId
        self.job_return_code     = int(args[1])
        self.dag_retry           = int(args[2])
        self.max_retries         = int(args[3])
        # TODO: Why not get the request name from the job ad?
        # We will need to parse the job ad earlier, that's all.
        self.reqname             = args[4]
        self.job_id              = args[5]
        self.source_dir          = args[6]
        self.dest_dir            = args[7]
        self.logs_arch_file_name = args[8]
        self.stage = args[9]
        # TODO: We can get the output files from the job ad where we have
        # CRAB_EDMOutputFiles, CRAB_TFileOutputFiles and CRAB_AdditionalOutputFiles.
        # (We only need to add the job_id in the file names).
        self.output_files_names  = []
        for i in range(10, len(args)):
            self.output_files_names.append(args[i])

        # Get the DAG cluster ID. Needed to know whether the job has run or not.
        try:
            self.dag_clusterid = int(self.dag_jobid.split('.')[0])
        except ValueError:
            pass

        # Get/update the post-job deferral number.
        global DEFER_NUM
        DEFER_NUM = self.get_defer_num()

        if first_pj_execution():
            self.logger.info("======== Starting execution on %s. Task Worker Version %s", os.uname()[1], __version__)
        else:
            self.logger.info("\n======== Starting execution again after deferral")

        # Create the task web directory in the schedd. Ignore if it exists already.
        self.create_taskwebdir()

        # Get/update the crab retry.
        calculate_crab_retry_retval, self.crab_retry = self.calculate_crab_retry()

        # Define the name of the post-job log file.
        if self.crab_retry is None:
            self.postjob_log_file_name = "postjob.%s.error.txt" % (self.job_id)
        else:
            self.postjob_log_file_name = "postjob.%s.%d.txt" % (self.job_id, self.crab_retry)

        #it needs an existing webdir and the postjob_log_file_name
        self.handle_logfile()

        # Fail the post-job if an error occurred when getting/updating the crab retry.
        # MM: I think the only way self.crab_retry could be None is through a bug or
        # wrong schedd setup (e.g.: permissions).
        if calculate_crab_retry_retval:
            msg = "Error in getting or updating the crab retry count."
            msg += " Making this a fatal error."
            self.logger.error(msg)
            if self.crab_retry is not None:
                self.set_dashboard_state('FAILED')
                self.set_state_ClassAds('FAILED')
            retval = JOB_RETURN_CODES.FATAL_ERROR
            self.log_finish_msg(retval)
            return retval

        # Now that we have the job id and retry, we can set the job report file
        # names.
        global G_JOB_REPORT_NAME
        G_JOB_REPORT_NAME = "jobReport.json.%s" % (self.job_id)
        global G_JOB_REPORT_NAME_NEW
        G_JOB_REPORT_NAME_NEW = "job_fjr.%s.%s.json" % (self.job_id, self.crab_retry)
        global G_WMARCHIVE_REPORT_NAME
        G_WMARCHIVE_REPORT_NAME = "WMArchiveReport.json.%s" % (self.job_id)
        global G_WMARCHIVE_REPORT_NAME_NEW
        G_WMARCHIVE_REPORT_NAME_NEW = "WMArchiveReport.%s.%s.json" % (self.job_id, self.crab_retry)

        if first_pj_execution():
            self.handle_webdir()
            # Print a message about what job retry number are we on. Here what matters is
            # the DAGMan retry count (i.e. dag_retry), which counts how many times the full
            # cycle [pre-job + job + post-job] has been run. OTOH the crab_retry includes
            # also post-job restarts from condor. If the post-job exits with return code 1,
            # DAGMan will resubmit the job unless the maximum allowed number of retries has
            # been reached already (DAGMan compares the DAGMan retry count against the
            # maximum allowed number of retries).
            msg = "This is job retry number %d." % (self.dag_retry)
            msg += " The maximum allowed number of retries is %d." % (self.max_retries)
            self.logger.info(msg)
            # Print a message about the DAG cluster ID of the job, which can be -1 when the
            # job has not been executed. The later can happen when a DAG node is restarted
            # after the job has finished, but before the post-job started. In such a case
            # the pre-job will detect that the 'pre' count in the retry_info dictionary is
            # greater than the 'post' count and, if the job log file exists already, it
            # will exit with return code 1 so that the job execution is skipped and the
            # post-job is executed. When the job is skipped because of a failure in the PRE
            # script, the job_return_code (i.e. the $RETURN argument macro) is -1004.
            if self.dag_clusterid == -1:
                msg = "Condor ID is -1 (and $RETURN argument macro is %d)," % (self.job_return_code)
                if self.job_return_code == -1004:
                    msg += " meaning that the job was (intentionally?) skipped."
                else:
                    msg += " meaning that the job has not run." #AndresT: Consider making this a fatal error.
                msg += " Will continue with the post-job execution."
                self.logger.warning(msg)
            else:
                msg = "This post-job corresponds to job with Condor ID %s." % (self.dag_clusterid)
                self.logger.debug(msg)

        # alter G_FAKE_OUTDATASET
        taskhash = hashlib.md5(self.reqname.encode()).hexdigest()
        global G_FAKE_OUTDATASET
        G_FAKE_OUTDATASET = f'/FakeDataset/fakefile-FakePublish-{taskhash}/USER'

        # Call execute_internal().
        retval = JOB_RETURN_CODES.RECOVERABLE_ERROR
        retmsg = "Failure during post-job execution."
        try:
            retval, retmsg = self.execute_internal()
            if retval == 4:
                msg = "Deferring the execution of the post-job."
                self.logger.info(msg)
                self.log_finish_msg(retval)
                return retval
        except Exception:
            self.logger.exception(retmsg)

        # Add the post-job exit code and error message to the job report.
        job_report = {}
        try:
            with open(G_JOB_REPORT_NAME_NEW) as fd:
                job_report = json.load(fd)
        except (IOError, ValueError):
            pass
        job_report['postjob'] = {'exitCode': retval, 'exitMsg': retmsg}
        with open(G_JOB_REPORT_NAME_NEW, 'w') as fd:
            json.dump(job_report, fd)

        # Prepare the error report. Enclosing it in a try except as we don't want to
        # fail jobs because this fails.
        self.logger.info("====== Starting to prepare error report.")
        self.logger.debug("Acquiring lock on error summary file.")
        try:
            with getLock(G_ERROR_SUMMARY_FILE_NAME):
                self.logger.debug("Acquired lock on error summary file.")
                with open(G_ERROR_SUMMARY_FILE_NAME, "a+") as fsummary:
                    prepareErrorSummary(self.logger, fsummary, self.job_id, self.crab_retry)
        except Exception:
            msg = "Unknown error while preparing the error report."
            self.logger.exception(msg)
        self.logger.info("====== Finished to prepare error report.")

        # Prepare the WMArchive report and put it in the direcotry to be processes
        self.logger.info("====== Starting to prepare WMArchive report.")
        try:
            self.processWMArchive(retval)
        except Exception:
            msg = "Unknown error while preparing the WMArchive report."
            self.logger.exception(msg)
        self.logger.info("====== Finished to prepare WMArchive report.")


        # Decide if the whole task should be aborted (in case a significant fraction of
        # the jobs has failed).
        retval = self.check_abort_dag(retval)

        # If return value is not 0 (success) write env variables to a file if it is not
        # present and print job ads in PostJob log file.
        # All this information is useful for debugging purpose.
        if retval != JOB_RETURN_CODES.OK:
            #Print system environment and job classads
            self.print_env_and_ads()
        self.log_finish_msg(retval)

        return retval

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def log_finish_msg(self, retval):
        """
        Logs a message with the post-job return code. The meaning of the
        exit codes has to be inferred by how/where appear in the DAGMAN spec
        created in DagmanCreator.py's variable DAG_FRAGMENT and DAG manual
        https://htcondor.readthedocs.io/en/v8_8_4/users-manual/dagman-applications.html
        """
        msg = "*"*80 + "\n"
        msg += "======== Finished post-job execution with status code %s " % retval
        if retval == 0:
            msg += " : SUCCESS"
        if retval == 1:
            msg += " : RECOVERABLE JOB FAIL. This jobs will be retried"
        if retval == 2:
            msg += " : NON-RECOVERABLE JOB FAIL. No retries"
        if retval == 3:
            msg += " : FATAL GLOBAL FAIL. Full DAG will stop"
        if retval == 4:
            msg += " : DEFERRING. PostJob will run again after 30 min"
        self.logger.info(msg)

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_exit_code(self, used_job_ad):
        """ Execute the retry-job. The retry-job decides whether an error is recoverable
            or fatal. It uses the cmscp exit code and error message, and the memory and
            cpu perfomance information; all from the job report. We also pass the job
            return code in case the exit code in the job report was not updated by cmscp.
            If the retry-job returns non 0 (meaning there was an error), report the state
            to dashboard and exit the post-job.
        """

        res = 0, ""

        self.logger.info("====== Starting to analyze job exit status.")
        retry = RetryJob()
        if not os.environ.get('TEST_POSTJOB_DISABLE_RETRIES', False):
            self.logger.info("       -----> RetryJob log start -----")
            username = self.job_ad['CRAB_UserHN']
            self.retryjob_retval = retry.execute(self.logger, self.reqname, username,
                                                 self.job_return_code, self.dag_retry, self.crab_retry,
                                                 self.job_id, self.dag_jobid,
                                                 self.job_ad, used_job_ad)
            self.logger.info("       <----- RetryJob log finish ----")
        if self.retryjob_retval:
            if self.retryjob_retval == JOB_RETURN_CODES.FATAL_ERROR:
                msg = "The retry handler indicated this was a fatal error."
                self.logger.info(msg)
                self.set_dashboard_state('FAILED')
                self.set_state_ClassAds('FAILED')
                self.logger.info("====== Finished to analyze job exit status.")
                res = JOB_RETURN_CODES.FATAL_ERROR, ""
            elif self.retryjob_retval == JOB_RETURN_CODES.RECOVERABLE_ERROR:
                if self.dag_retry >= self.max_retries:
                    msg = "The retry handler indicated this was a recoverable error,"
                    msg += " but the maximum number of retries was already hit."
                    msg += " DAGMan will not retry."
                    self.logger.info(msg)
                    self.set_dashboard_state('FAILED')
                    self.set_state_ClassAds('FAILED')
                    self.logger.info("====== Finished to analyze job exit status.")
                    res = JOB_RETURN_CODES.FATAL_ERROR, ""
                else:
                    msg = "The retry handler indicated this was a recoverable error."
                    msg += " DAGMan will retry."
                    self.logger.info(msg)
                    self.set_dashboard_state('COOLOFF')
                    self.set_state_ClassAds('COOLOFF')
                    self.logger.info("====== Finished to analyze job exit status.")
                    res = JOB_RETURN_CODES.RECOVERABLE_ERROR, ""
            else:
                msg = "The retry handler returned an unexpected value (%d)." % (self.retryjob_retval)
                msg += " Will consider this as a fatal error. DAGMan will not retry."
                self.logger.info(msg)
                self.set_dashboard_state('FAILED')
                self.set_state_ClassAds('FAILED')
                self.logger.info("====== Finished to analyze job exit status.")
                res = JOB_RETURN_CODES.FATAL_ERROR, "" #MarcoM: this should never happen
        # This is for the case in which we don't run the retry-job.
        elif self.job_return_code != JOB_RETURN_CODES.OK:
            if self.dag_retry >= self.max_retries:
                msg = "The maximum allowed number of retries was hit and the job failed."
                msg += " Setting this node (job) to permanent failure."
                self.logger.info(msg)
                self.set_dashboard_state('FAILED')
                self.set_state_ClassAds('FAILED')
                self.logger.info("====== Finished to analyze job exit status.")
                res = JOB_RETURN_CODES.FATAL_ERROR, ""
        else:
            self.logger.info("====== Finished to analyze job exit status.")
        return res

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def saveAutomaticSplittingData(self):
        """
        Sets the run, lumi information in the task information for the
        completion jobs.  Returns True if completion jobs are needed,
        otherwise False.
        """
        if self.stage not in ('probe', 'processing'):
            return

        throughputFileName = "automatic_splitting/throughputs/{0}".format(self.job_id)
        if not os.path.exists(os.path.dirname(throughputFileName)):
            os.makedirs(os.path.dirname(throughputFileName))
        with open(throughputFileName, 'w') as fd:
            report = self.job_report['steps']['cmsRun']
            throughput = float(report['performance']['cpu'].get('EventThroughput', 0))

            def valid(fi):
                return fi['input_source_class'] == 'PoolSource' and fi.get('input_type', '') == "primaryFiles"

            outsize = sum(fi['outsize'] for fi in self.output_files_info)  # in bytes
            events = sum(fi.get('events', 0) for fi in report['input']['source'] if valid(fi))

            eventsize = (outsize // events + 1) if events > 0 else 0  # do not round to zero small eventsize

            json.dump([throughput, eventsize], fd)

        if self.stage == 'probe':
            return
        self.logger.info("====== Starting to parse the lumi file")
        try:
            tmpdir = tempfile.mkdtemp()
            fn = "job_lumis_{0}.json".format(self.job_id)
            with tarfile.open("run_and_lumis.tar.gz") as f:
                f.extract(fn, path=tmpdir)
            with open(os.path.join(tmpdir, fn)) as fd:
                injson = json.load(fd)
                inlumis = LumiList(compactList=injson)
        finally:
            shutil.rmtree(tmpdir)

        outlumis = LumiList()
        for input_ in self.job_report['steps']['cmsRun']['input']['source']:
            outlumis += LumiList(runsAndLumis=input_['runs'])

        missing = inlumis - outlumis
        self.logger.info("Lumis expected to be processed: %d", len(inlumis.getLumis()))
        self.logger.info("Lumis actually processed:       %d", len(outlumis.getLumis()))
        self.logger.info("Difference in lumis:            %d", len(missing.getLumis()))
        if not missing.getLumis():
            # we don't want to create the subjobs if the job processed everything
            return
        missingLumisFileName = "automatic_splitting/missing_lumis/{0}".format(self.job_id)
        if not os.path.exists(os.path.dirname(missingLumisFileName)):
            os.makedirs(os.path.dirname(missingLumisFileName))
        with open(missingLumisFileName, 'w') as fd:
            fd.write(str(missing))

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def execute_internal(self):
        """
        The execute method calls this execute_internal method. All post-job actions
        (injections to ASO database, monitoring transfers, file metadata upload)
        are done from here.
        """

        # Make sure the location of the user's proxy file is set in the environment.
        if 'X509_USER_PROXY' not in os.environ:
            retmsg = "X509_USER_PROXY is not present in environment."
            self.logger.error(retmsg)
            return 10, retmsg

        # If this is a deferred post-job execution, reduce the log level to WARNING.
        if not first_pj_execution():
            self.logger.setLevel(logging.WARNING)
        # Parse the job ad and use it if possible.
        # If not, use the main ROOT job ad and for job ad information use condor_q.
        # Please see: https://github.com/dmwm/CRABServer/issues/4618
        used_job_ad = False
        if self.dag_clusterid == -1:
            # the grid job did not run, see the comments in execute() method
            job_ad_file_name = os.path.join(".", "finished_jobs", "job.%s.%d" % (self.job_id, self.dag_retry))
        else:
            condor_history_dir = os.environ.get("_CONDOR_PER_JOB_HISTORY_DIR", "")
            job_ad_file_name = os.path.join(condor_history_dir, str("history." + str(self.dag_jobid)))
            jobad_in_condor_history = True  # will set to false if can't find the file above
            # Since Summer 2019 ( https://github.com/dmwm/CRABServer/issues/5854 ) we keep
            # jobs in HTCondor queue after job terminate so that PostJob can edit classAds
            # and get the info propagated to MONIT, therefore we create the job ad file 1st time PostJob runs
            if not os.path.exists(job_ad_file_name):
                self.logger.info('PER_JOB_HISTORY file %s not found , look in ./finished_jobs', job_ad_file_name)
                jobad_in_condor_history = False
                job_ad_file_name = os.path.join(".", "finished_jobs", "job.%s.%d" % (self.job_id, self.dag_retry))
            if not os.path.exists(job_ad_file_name):
                self.logger.info('History file not found in ./finished_jobs. Create it by querying schedd')
                counter = 0
                while counter < 5:
                    cmd = 'condor_q -l %s | grep = > %s' % \
                          (self.dag_jobid, job_ad_file_name)  # no empty lines ! #8285
                    self.logger.info('Executing %s', cmd)
                    subproc = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True) # pylint: disable=consider-using-with
                    (stdout_data, stderr_data) = subproc.communicate()
                    rc = subproc.returncode
                    if rc == 0:
                        if os.path.getsize(job_ad_file_name) == 0:
                            stderr_data = 'Empty ad file created. Maybe job %s was not found?' % self.dag_jobid
                        else:
                            time.sleep(2) # take a breath before opening the file which we just wrote
                            break
                    sleep_time = 10*pow(2, counter) # exponential backoff: 10, 20, 40, 80 ...
                    self.logger.error('condor_q failed with rc= %d, stdout:%s and stderr:\n%s', rc, stdout_data, stderr_data)
                    self.logger.info('will try again in %d seconds', sleep_time)
                    time.sleep(sleep_time)
                    counter += 1
                if rc != 0:
                    self.logger.error("%d tries, still can't talk to schedd. Reschedule the PostJob", counter)
                    return 4
                self.logger.info('History file %s created', job_ad_file_name)

        # there's no good reason anymore for the loop below. Guess was there to wait out a possible
        # race where PostJob is started very quickly but job has not left the queue yet and thus the
        # job_ad file is not present. I am keeping the code to minimize changes but reduce the counter to 1
        counter = 0
        num_iter = 1
        self.logger.info("====== Starting to parse job ad file %s.", job_ad_file_name)
        while counter < num_iter:
            self.logger.info("       -----> Attempt # %s of %s -----", str(counter), str(num_iter))
            parse_job_ad_exit = self.parse_job_ad(job_ad_file_name)    # note: returns 0 if OK
            if not parse_job_ad_exit:    # means success in previous line
                used_job_ad = True
                self.logger.info("       -----> Succeeded to parse job ad file -----")
                if self.dag_clusterid != -1 and jobad_in_condor_history:
                    # copy the file to ./finished_jobs so further PJ iterations will find it
                    try:
                        shutil.copy2(job_ad_file_name, './finished_jobs/')
                        job_ad_source = "history.%s" % (self.dag_jobid)
                        job_ad_symlink = os.path.join(".", "finished_jobs", "job.%s.%d" % (self.job_id, self.dag_retry))
                        os.symlink(job_ad_source, job_ad_symlink)
                        self.logger.info("       -----> Succeeded to copy job ad file.")
                    except Exception:
                        self.logger.info("       -----> Failed to copy job ad file. Continuing")
                self.logger.info("====== Finished to parse job ad.")
                break
            counter += 1
            self.logger.info("       -----> Failed to parse job ad file -----")
            time.sleep(5)
        if not used_job_ad:
            # Parse the main ROOT job ad. This job ad is created when the main ROOT job is
            # submitted to DAGMan; and is never modified. Of course that means that this
            # job ad doesn't have information of parameters that change at a per job or per
            # job retry basis. For these parameters one has to use the job ads. Note that
            # the main ROOT job ad is owned by user 'condor' and therefore it can not be
            # updated (by user 'cmsxxxx'), as we would like to do in e.g. AdjustSites.py to
            # add some global parameters in it. Fortunately, here in the post-job we need
            # only (actually almost only) global parameters that are available in the main
            # ROOT job ad, so we can fallback to read this main ROOT job ad here.
            job_ad_file_name = os.environ.get("_CONDOR_JOB_AD", ".job.ad")
            self.logger.info("====== Starting to parse ROOT job ad file %s.", job_ad_file_name)
            if self.parse_job_ad(job_ad_file_name):
                self.set_dashboard_state('FAILED', exitCode=89999)
                self.set_state_ClassAds('FAILED', exitCode=89999)
                self.logger.info("====== Finished to parse ROOT job ad.")
                retmsg = "Failure parsing the ROOT job ad."
                return JOB_RETURN_CODES.FATAL_ERROR, retmsg
            self.logger.info("====== Finished to parse ROOT job ad.")
        # If this is a deferred post-job execution, put back the log level to DEBUG.
        if not first_pj_execution():
            self.logger.setLevel(logging.DEBUG)
            if used_job_ad:
                msg = "(Parsed job ad.)"
            else:
                msg = "(Parsed ROOT job ad.)"
            self.logger.debug(msg)

        if first_pj_execution():
            pj_error, msg = self.check_exit_code(used_job_ad)
            if pj_error:
                return pj_error, msg

        # If this is a deferred post-job execution, reduce the log level to ERROR.
        if not first_pj_execution():
            self.logger.setLevel(logging.ERROR)
        # Parse the job report.
        self.logger.info("====== Starting to parse job report file %s.", G_JOB_REPORT_NAME)
        if self.parse_job_report():
            self.set_dashboard_state('FAILED', exitCode=89999)
            self.set_state_ClassAds('FAILED', exitCode=89999)
            self.logger.info("====== Finished to parse job report.")
            retmsg = "Failure parsing the job report."
            return JOB_RETURN_CODES.FATAL_ERROR, retmsg
        self.logger.info("====== Finished to parse job report.")

        self.logger.info("====== Starting saving data for automatic splitting.")
        self.saveAutomaticSplittingData()
        self.logger.info("====== Finished saving data for automatic splitting.")

        # If this is a deferred post-job execution, put back the log level to DEBUG.
        if not first_pj_execution():
            self.logger.setLevel(logging.DEBUG)

        # If the flag CRAB_NoWNStageout is set, we finish the post-job here.
        # (I didn't remove this yet, because even if the transfer of the logs and
        # outputs is off, in a user analysis task it still makes sense to do the
        # upload of the input files metadata, so that the user can do 'crab report'
        # and see how many events have been read and/or get the lumiSummary files.)
        if int(self.job_ad.get('CRAB_NoWNStageout', 0)) != 0:
            msg = "CRAB_NoWNStageout is set to %d in the job ad." % (int(self.job_ad.get('CRAB_NoWNStageout', 0)))
            msg += " Skipping files stageout and files metadata upload."
            msg += " Finishing post-job execution with exit code 0."
            self.logger.info(msg)
            self.set_dashboard_state('FINISHED')
            self.set_state_ClassAds('FINISHED')
            return 0, ""

        # Give a message about the transfer flags.
        if first_pj_execution():
            if not self.transfer_logs:
                msg = "The user has not specified to transfer the log files."
                msg += " No log files stageout (nor log files metadata upload) will be performed."
                self.logger.info(msg)
            if not self.output_files_names:
                if not self.transfer_outputs:
                    msg = "Post-job got an empty list of output files (i.e. no output file) to work on."
                    msg += " In any case, the user has specified to not transfer output files."
                    self.logger.info(msg)
                else:
                    msg = "The transfer of output files flag in on,"
                    msg += " but the post-job got an empty list of output files (i.e. no output file) to work on."
                    msg += " Turning off the transfer of output files flag."
                    self.logger.info(msg)
            else:
                if not self.transfer_outputs:
                    msg = "The user has specified to not transfer the output files."
                    msg += " No output files stageout (nor output files metadata upload) will be performed."
                    self.logger.info(msg)
        # Turn off the transfer of output files if the are no output files to transfer.
        if not self.output_files_names:
            self.transfer_outputs = 0

        # Initialize the object we will use for making requests to the REST interface.
        self.crabserver = CRABRest(self.rest_host, \
                                   os.environ['X509_USER_PROXY'], \
                                   os.environ['X509_USER_PROXY'], \
                                   retry=2, logger=self.logger, userAgent='CRABSchedd')
        self.crabserver.setDbInstance(self.db_instance)


        if not os.path.exists('output_datasets') and self.output_dataset:
            self.logger.info("Uploading output_dataset %s to task DB", self.output_dataset)
            configreq = [('subresource', 'addoutputdatasets'),
                         ('workflow', self.reqname),
                         ('outputdatasets', self.output_dataset)]
            rest_api = 'task'
            msg = "Uploading output dataset to https://%s: %s" % (self.rest_url+rest_api, configreq)
            self.logger.debug(msg)
            try:
                self.crabserver.post(api=rest_api, data=encodeRequest(configreq))
                with open('output_datasets', 'w') as f:
                    f.write(self.output_dataset)
            except HTTPException as hte:
                msg = "Error uploading output dataset: %s" % (str(hte.headers))
                self.logger.error(msg)
            except IOError:
                msg = "Error writing the output_datasets file"
                self.logger.error(msg)
        else:
            self.logger.debug("Output dataset should be already uploaded:\n%s", self.output_dataset)


        # Upload the logs archive file metadata if it was not already done from the WN.
        if self.transfer_logs and first_pj_execution():
            if self.log_needs_file_metadata_upload:
                self.logger.info("====== Starting upload of logs archive file metadata.")
                try:
                    self.upload_log_file_metadata()
                except Exception as ex:
                    retmsg = "Fatal error uploading logs archive file metadata: %s" % (str(ex))
                    self.logger.error(retmsg)
                    self.logger.info("====== Finished upload of logs archive file metadata.")
                    return self.check_retry_count(80001), retmsg
                self.logger.info("====== Finished upload of logs archive file metadata.")
            else:
                msg = "Skipping logs archive file metadata upload, because it is marked as"
                msg += " having been uploaded already from the worker node."
                self.logger.info(msg)

        # Give a message about how many files will be transferred.
        if first_pj_execution():
            if self.transfer_logs or self.transfer_outputs:
                msg = "Preparing to transfer "
                msgadd = []
                if self.transfer_logs:
                    msgadd.append("the logs archive file")
                if self.transfer_outputs:
                    msgadd.append("%d output files" % (len(self.output_files_names)))
                msg = msg + ' and '.join(msgadd) + '.'
                self.logger.info(msg)

        # Do the transfers (inject to ASO database if needed, and monitor the transfers
        # statuses until they reach a terminal state).
        if self.transfer_logs or self.transfer_outputs:
            if first_pj_execution():
                self.logger.info("====== Starting to check for ASO transfers.")
            try:
                ret_code = self.perform_transfers()
                if ret_code == 4:
                    #if we need to defer the postjob execution stop here!
                    return ret_code, ""
            except PermanentStageoutError as pse:
                retmsg = "Got fatal stageout exception:\n%s" % (str(pse))
                self.logger.error(retmsg)
                msg = "There was at least one permanent stageout error; user will need to resubmit."
                self.logger.error(msg)
                self.set_dashboard_state('FAILED', exitCode=mostCommon(self.stageout_exit_codes, 60324))
                self.set_state_ClassAds('FAILED', exitCode=mostCommon(self.stageout_exit_codes, 60324))
                self.logger.info("====== Finished to check for ASO transfers.")
                return JOB_RETURN_CODES.FATAL_ERROR, retmsg
            except RecoverableStageoutError as rse:
                retmsg = "Got recoverable stageout exception:\n%s" % (str(rse))
                self.logger.error(retmsg)
                msg = "These are all recoverable stageout errors; automatic resubmit is possible."
                self.logger.error(msg)
                self.logger.info("====== Finished to check for ASO transfers.")
                return self.check_retry_count(mostCommon(self.stageout_exit_codes, 60324)), retmsg
            except Exception as ex:
                retmsg = "Stageout failure: %s" % (str(ex))
                self.logger.exception(retmsg)
                self.logger.info("====== Finished to check for ASO transfers.")
                return self.check_retry_count(mostCommon(self.stageout_exit_codes, 60324)), retmsg
            self.logger.info("====== Finished to check for ASO transfers.")

        # Upload the input files metadata.
        self.logger.info("====== Starting upload of input files metadata.")
        try:
            self.upload_input_files_metadata()
        except Exception as ex:
            retmsg = "Fatal error uploading input files metadata: %s" % (str(ex))
            self.logger.exception(retmsg)
            self.logger.info("====== Finished upload of input files metadata.")
            return self.check_retry_count(80001), retmsg
        self.logger.info("====== Finished upload of input files metadata.")

        # Upload the output files metadata.
        if self.transfer_outputs:
            self.logger.info("====== Starting upload of output files metadata.")
            try:
                self.upload_output_files_metadata()
            except Exception as ex:
                retmsg = "Fatal error uploading output files metadata: %s" % (str(ex))
                self.logger.exception(retmsg)
                self.logger.info("====== Finished upload of output files metadata.")
                return self.check_retry_count(80001), retmsg
            self.logger.info("====== Finished upload of output files metadata.")

            if ASO_JOB:
                self.logger.info("====== About to update publication flags of transfered files.")
                for doc in getattr(ASO_JOB, 'docs_in_transfer', []):
                    doc_id = doc.get('doc_id')
                    self.logger.debug("Found doc %s", doc_id)
                    if doc.get('delayed_publicationflag_update'):
                        newDoc = {
                            'subresource' : 'updatePublication',
                            'publish_flag' : 1,
                            'list_of_ids' : doc_id,
                            'list_of_publication_state' : 'NEW',
                            'asoworker' : '%'
                        }
                        try:
                            self.crabserver.post(api="filetransfers", data=encodeRequest(newDoc))
                        except Exception as ex:
                            retmsg = "Fatal error uploading  publication flags of transfered files: %s" % (str(ex))
                            self.logger.exception(retmsg)
                            self.logger.info("====== Finished to update publication flags of transfered files.")
                            return self.check_retry_count(80001), retmsg
                self.logger.info("====== Finished to update publication flags of transfered files.")

        self.set_dashboard_state('FINISHED')
        self.set_state_ClassAds('FINISHED')

        return 0, ""

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def print_env_and_ads(self):
        """
        Save environment variables which are used in postjob_env.txt if file is not
        present yet. Environment variables for all PostJobs should be the same and
        no need to save for each.
        Print final job ads in PostJob. This is useful for debugging purposes.
        """
        env_file = "postjob_env.txt"
        if not os.path.exists(env_file) or not os.stat(env_file).st_size:
            self.logger.info("Will write env variables to %s file.", env_file)
            try:
                with open(env_file, "w") as fd:
                    fd.write("------ Environment variables:\n")
                    for key in sorted(os.environ.keys()):
                        fd.write("%30s    %s\n" % (key, os.environ[key]))
            except Exception:
                self.logger.info("Not able to write ENV variables to a file. Continuing")
        else:
            self.logger.info("Will not write %s file. Continue.", env_file)
        # if job_ad is available, write it output to PostJob log file
        if self.job_ad:
            self.logger.debug("------ Job classad values for debug purposes:")
            msg = "-"*100
            for key in sorted(self.job_ad.keys(), key=lambda v: (v.upper(), v[0].islower())):
                msg += "\n%35s    %s" % (key, self.job_ad[key])
            self.logger.debug(msg)
            self.logger.debug("-"*100)
        else:
            self.logger.debug("------ Job ad is not available. Continue")
            self.logger.debug("-"*100)

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

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

        # SBSB
        # at this point we have two similar strucutres in memory
        # (Pdb) self.output_files_info[0]['output_dataset']
        # '/GenericTTbar/belforte-Stefano-Test-220309-94ba0e06145abd65ccb1d21786dc7e1d/USER'
        # FIRS STRUCTURE: job_report_output
        # (Pdb) self.job_report_output
        # {'output': [{'output_module_class': 'PoolOutputModule', 'runs': {'1': {'419': 300}},
        # 'temp_storage_site': 'T1_RU_JINR_Disk', 'events': 300, 'branch_hash': 'd41d8cd98f00b204e9800998ecf8427e',
        # 'pset_hash': '94ba0e06145abd65ccb1d21786dc7e1d', 'lfn': '', 'pfn': 'kk.root', 'catalog': '',
        # 'module_label': 'output', 'local_stageout': True,
        # 'input': ['/store/mc/HC/GenericTTbar/AODSIM/CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/00000/8ADD04E5-1776-E711-A1BA-FA163E6741E0.root'],
        # 'checksums': {'adler32': 'bbe03bd1', 'cksum': '3164535815'}, 'storage_site': 'T2_CH_CERN',
        # 'guid': 'ADA2B48E-AEF4-0B4B-B0DD-BCE27657909A',
        # 'inputpfns': ['dcap://dcap01.jinr-t1.ru:22125/pnfs/jinr-t1.ru/data/cms/store/mc/HC/GenericTTbar/AODSIM/CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/00000/8ADD04E5-1776-E711-A1BA-FA163E6741E0.root'],
        # 'size': 561301}], 'analysis': []}
        # SECOND STRUCTURE: output_files_info
        # (Pdb) self.output_files_info
        # [{'filetype': 'EDM', 'module_label': 'output',
        # 'inparentlfns': ['/store/mc/HC/GenericTTbar/AODSIM/CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/00000/8ADD04E5-1776-E711-A1BA-FA163E6741E0.root'],
        # 'events': 300, 'checksums': {'adler32': 'bbe03bd1', 'cksum': '3164535815'}, 'outsize': 561301,
        # 'pset_hash': '94ba0e06145abd65ccb1d21786dc7e1d', 'pfn': 'kk.root',
        # 'output_dataset': '/GenericTTbar/belforte-Stefano-Test-220309-94ba0e06145abd65ccb1d21786dc7e1d/USER',
        # 'local_stageout': True, 'direct_stageout': False, 'outlocation': 'T2_CH_CERN', 'outtmplocation': 'T1_RU_JINR_Disk',
        # 'outlfn': '/store/user/belforte/GenericTTbar/Stefano-Test-220309/220310_151125/0000/kk_1.root',
        # 'outtmplfn': '/store/temp/user/belforte.be1f4dc5be8664cbd145bf008f5399adf42b086f/GenericTTbar/Stefano-Test-220309/220310_151125/0000/kk_1.root',
        # 'outfileruns': ['1'], 'outfilelumis': ['419:300']}]
        # SBSB

        global ASO_JOB
        ASO_JOB = ASOServerJob(self.logger, \
                               self.aso_start_time, self.aso_start_timestamp, \
                               self.dest_site, self.source_dir, self.dest_dir, \
                               source_sites, self.job_id, log_and_output_files_names, \
                               self.reqname, self.log_size, \
                               self.log_needs_transfer, self.job_report_output, \
                               self.job_ad, self.crab_retry, \
                               self.retry_timeout, self.job_failed, \
                               self.transfer_logs, self.transfer_outputs,
                               self.rest_host, self.db_instance, self.publish_name)
        if first_pj_execution():
            aso_job_retval = ASO_JOB.run()
        else:
            aso_job_retval = ASO_JOB.check_transfers()

        # Defer the post-job execution if necessary.
        if aso_job_retval == 4:
            if os.environ.get('TEST_POSTJOB_NO_DEFERRAL', False):
                while aso_job_retval == 4:
                    self.logger.debug("Sleeping for 1 minute...")
                    time.sleep(60)
                    aso_job_retval = ASO_JOB.check_transfers()
            else:
                ASO_JOB = None
                return 4

        # If no transfers failed, return success immediately.
        if aso_job_retval == 0:
            return 0

        # Return code 2 means post-job timed out waiting for transfer to complete and
        # not all the transfers could be cancelled.
        if aso_job_retval == 2:
            ASO_JOB = None
            msg = "Stageout failed with code %d." % (aso_job_retval)
            msg += "\nPost-job timed out waiting for ASO transfers to complete."
            msg += "\nAttempts were made to cancel the ongoing transfers,"
            msg += " but cancellation failed for some transfers."
            msg += "\nConsidering cancellation failures as a permanent stageout error."
            self.stageout_exit_codes.append(60317)
            raise PermanentStageoutError(msg)

        # Retrieve the stageout failures (a dictionary where the keys are the IDs of
        # the ASO documents for which the stageout job failed and the values are the
        # failure reason(s)). Determine which failures are permament stageout errors
        # and which ones are recoverable stageout errors.
        failures = ASO_JOB.get_failures()
        ASO_JOB = None
        num_failures = len(failures)
        num_permanent_failures = 0
        for doc_id in failures.keys():
            if isinstance(failures[doc_id]['reasons'], str):
                failures[doc_id]['reasons'] = [failures[doc_id]['reasons']]
            if isinstance(failures[doc_id]['reasons'], list):
                last_failure_reason = failures[doc_id]['reasons'][-1]
                permanent, _, stageout_exit_code = isFailurePermanent(last_failure_reason, True)
                if permanent:
                    num_permanent_failures += 1
                    failures[doc_id]['severity'] = 'permanent'
                else:
                    failures[doc_id]['severity'] = 'recoverable'
                if stageout_exit_code:
                    self.stageout_exit_codes.append(stageout_exit_code)
        # Message for stageout error exception.
        msg = "Stageout failed with code %d." % (aso_job_retval)
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

        #If CRAB is configured not to retry on ASO failures, any failure is permanent
        if int(self.job_ad['CRAB_RetryOnASOFailures']) == 0:
            msg_retry = "Considering transfer error as a permanent failure"
            msg_retry += " because CRAB_RetryOnASOFailures = 0 in the job ad."
            self.logger.debug(msg_retry)
            num_permanent_failures += 1
        # Raise stageout exception.
        if num_permanent_failures:
            raise PermanentStageoutError(msg)
        raise RecoverableStageoutError(msg)

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def upload_log_file_metadata(self):
        """
        Upload the logs archive file metadata.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        temp_storage_site = self.job_report.get('temp_storage_site', 'unknown')
        if temp_storage_site == 'unknown':
            msg = "Temporary storage site for logs archive file not defined in job report."
            msg += " This is expected if there was no attempt to stage out the file into a temporary storage."
            msg += " Will use the executed site as the temporary storage site in the file metadata."
            self.logger.warning(msg)
            temp_storage_site = self.executed_site
        configreq = {'taskname'        : self.reqname,
                     'jobid'      : self.job_id,
                     'outsize'         : self.job_report.get('log_size', 0),
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
                     'outtmplocation'  : fixUpTempStorageSite(logger=self.logger, siteName=temp_storage_site),
                     'outtmplfn'       : os.path.join(self.source_dir, 'log', self.logs_arch_file_name),
                     'outdatasetname'  : G_FAKE_OUTDATASET,
                     'directstageout'  : int(self.job_report.get('direct_stageout', 0))
                    }
        rest_api = 'filemetadata'
        msg = "Uploading file metadata for %s to https://%s: %s"
        msg = msg % (self.logs_arch_file_name, self.rest_url+rest_api, configreq)
        self.logger.debug(msg)
        try:
            self.crabserver.put(api=rest_api, data=encodeRequest(configreq))
        except HTTPException as hte:
            msg = "Error uploading logs file metadata: %s" % (str(hte.headers))
            self.logger.error(msg)
            if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
               not hte.headers.get('X-Error-Http', -1) == '400':
                raise

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def upload_input_files_metadata(self):
        """
        Upload the (primary) input files metadata. We care about the number of events
        and about the lumis for the report.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        temp_storage_site = self.executed_site
        if self.job_report.get('temp_storage_site', 'unknown') != 'unknown':
            temp_storage_site = self.job_report.get('temp_storage_site')
        if not 'source' in self.job_report.get('steps', {}).get('cmsRun', {}).get('input', {}):
            self.logger.info("Skipping input filemetadata upload as no inputs were found")
            return
        direct_stageout = int(self.job_report.get('direct_stageout', 0))
        for ifile in self.job_report['steps']['cmsRun']['input']['source']:
            if ifile['input_source_class'] != 'PoolSource' or ifile.get('input_type', '') != "primaryFiles":
                continue
            # Many of these parameters are not needed and are using fake/defined values
            if not ifile['lfn']:  # there are valid use case with no input LFN but we need to count files for crab report
                lfn = '/store/user/dummy/DummyLFN'
            else:
                lfn = ifile['lfn']
                # beware the case where cmsRun was only given a PFN to run on and still we get it in the LFN field
                try:
                    Lexicon.lfn(lfn)  # will raise if testLfn is not a valid lfn
                except AssertionError:
                    lfn = '/store/user/dummy/DummyLFN'

            lfn = lfn + "_" + str(self.job_id) # jobs can analyze the same input
            configreq = {"taskname"        : self.job_ad['CRAB_ReqName'],
                         "globalTag"       : "None",
                         "jobid"           : self.job_id,
                         "outsize"         : "0",
                         "publishdataname" : self.publish_name,
                         "appver"          : self.job_ad['CRAB_JobSW'],
                         "outtype"         : "POOLIN", # file['input_source_class'],
                         "checksummd5"     : "0",
                         "checksumcksum"   : "0",
                         "checksumadler32" : "0",
                         "outlocation"     : self.job_ad['CRAB_AsyncDest'],
                         "outtmplocation"  : temp_storage_site,
                         "acquisitionera"  : "null", # Not implemented
                         "outlfn"          : lfn,
                         "outtmplfn"       : self.source_dir, # does not have sense for input files
                         "events"          : ifile.get('events', 0),
                         "outdatasetname"  : G_FAKE_OUTDATASET,
                         "directstageout"  : direct_stageout
                        }
            #TODO: there could be a better py3 way to get lists of outfileruns/lumis
            outfileruns = []
            outfilelumis = []
            for run, lumis in ifile['runs'].items():
                outfileruns.append(str(run))
                outfilelumis.append(','.join(map(str, lumis)))

            # make a real list of (k,v) pairs as rest_api requires (not sure pylint is right to complain !)
            configreq = [item for item in configreq.items()]  # pylint: disable=unnecessary-comprehension
            #configreq['outfileruns'] = [run for run in outfileruns]
            #configreq['outfilelumis'] = [lumis for lumis in outfilelumis]
            for run in outfileruns:
                configreq.append(("outfileruns", run))
            for lumis in outfilelumis:
                configreq.append(("outfilelumis", lumis))

            rest_api = 'filemetadata'
            msg = "Uploading input metadata for %s to https://%s: %s" % (lfn, self.rest_url+rest_api, configreq)
            self.logger.debug(msg)
            try:
                self.crabserver.put(api=rest_api, data=encodeRequest(configreq))
            except HTTPException as hte:
                msg = "Error uploading input file metadata: %s" % (str(hte.headers))
                self.logger.error(msg)
                raise

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def upload_output_files_metadata(self):
        """
        Upload the output files metadata.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        output_datasets = set()
        for file_info in self.output_files_info:
            outdataset = file_info['output_dataset']
            if not 'FakeDataset' in outdataset:
                output_datasets.add(outdataset)
            # the only reason for publishname in metadata is so that Publisher can get
            # the pset hash to store in DBS. But that hash is part of outdataset name as well
            # somehow a log of clenaup could be done here
            publishname = self.publish_name
            if 'pset_hash' in file_info:
                publishname = "%s-%s" % (publishname.rsplit('-', 1)[0], file_info['pset_hash'])
            configreq = {'taskname'        : self.reqname,
                         'jobid'      : self.job_id,
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
                         'outtmplocation'  : fixUpTempStorageSite(logger=self.logger, siteName=file_info['outtmplocation']),
                         'outtmplfn'       : file_info['outtmplfn'],
                         'outdatasetname'  : outdataset,
                         'directstageout'  : int(file_info['direct_stageout']),
                         'globalTag'       : 'None'
                        }
            # make a real list of (k,v) pairs as rest_api requires (not sure pylint is rigth to complain !)
            configreq = [item for item in configreq.items()]  # pylint: disable=unnecessary-comprehension
            #configreq = configreq.items()
            if 'outfileruns' in file_info:
                for run in file_info['outfileruns']:
                    configreq.append(("outfileruns", run))
            if 'outfilelumis' in file_info:
                for lumis in file_info['outfilelumis']:
                    configreq.append(("outfilelumis", lumis))
            if 'inparentlfns' in file_info:
                for lfn in file_info['inparentlfns']:
                    # If the user specified a PFN as input, then the LFN is an empty string
                    # and does not pass validation.
                    if lfn:
                        configreq.append(("inparentlfns", lfn))
            filename = file_info['pfn'].split('/')[-1]
            rest_api = 'filemetadata'
            msg = "Uploading output metadata for %s to https://%s: %s" % (filename, self.rest_url+rest_api, configreq)
            self.logger.debug(msg)
            try:
                self.crabserver.put(api=rest_api, data=encodeRequest(configreq))
            except HTTPException as hte:
                # BrianB. Suppressing this exception is a tough decision.
                # If the file made it back alright, I suppose we can proceed.
                msg = "Error uploading output file metadata: %s" % (str(hte.headers))
                self.logger.error(msg)
                raise

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def parse_job_ad(self, job_ad_file_name):
        """
        Parse the job ad, setting some class variables.
        """
        # Load the job ad.
        if not os.path.exists(job_ad_file_name) or not os.stat(job_ad_file_name).st_size:
            self.logger.error("Missing job ad!")
            return 1
        try:
            with open(job_ad_file_name, 'r', encoding='utf-8') as fh:
                self.job_ad = classad.parseOne(fh)
        except Exception as ex:
            msg = "Error parsing job ad: %s" % (str(ex))
            self.logger.exception(msg)
            return 1
        # Check if all the required attributes from the job ad are there.
        if self.check_required_job_ad_attrs():
            return 1
        # Set some class variables using the job ad.
        self.dest_site        = str(self.job_ad['CRAB_AsyncDest'])
        self.job_sw           = str(self.job_ad['CRAB_JobSW'])
        self.publish_name     = str(self.job_ad['CRAB_PublishName'])
        self.rest_host        = str(self.job_ad['CRAB_RestHost'])
        self.db_instance      = str(self.job_ad['CRAB_DbInstance'])
        self.rest_url = self.rest_host + '/crabserver/' + self.db_instance + '/'  # used in logging
        if 'CRAB_SaveLogsFlag' not in self.job_ad:
            msg = "Job's HTCondor ClassAd is missing attribute CRAB_SaveLogsFlag."
            msg += " Will assume CRAB_SaveLogsFlag = False."
            self.logger.warning(msg)
            self.transfer_logs = 0
        else:
            self.transfer_logs = int(self.job_ad['CRAB_SaveLogsFlag'])
        if 'CRAB_TransferOutputs' not in self.job_ad:
            msg = "Job's HTCondor ClassAd is missing attribute CRAB_TransferOutputs."
            msg += " Will assume CRAB_TransferOutputs = True."
            self.logger.warning(msg)
            self.transfer_outputs = 1
        else:
            self.transfer_outputs = int(self.job_ad['CRAB_TransferOutputs'])
        if self.stage == 'probe':
            self.transfer_logs = 0
            self.transfer_outputs = 0
        # If self.job_ad['CRAB_ASOTimeout'] = 0, will use default timeout logic.
        if 'CRAB_ASOTimeout' in self.job_ad and int(self.job_ad['CRAB_ASOTimeout']) > 0:
            self.retry_timeout = int(self.job_ad['CRAB_ASOTimeout'])
        else:
            # If CRAB_ASOTimeout was not defined in the job ad, use a default of 6 hours.
            self.retry_timeout = 6 * 3600
        return 0

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_required_job_ad_attrs(self):
        """
        Check if all the required attributes from the job ad are there.
        """
        required_job_ad_attrs = {'CRAB_UserRole': {'allowUndefined': True},
                                 'CRAB_UserGroup': {'allowUndefined': True},
                                 'CRAB_AsyncDest': {'allowUndefined': False},
                                 'CRAB_DBSURL': {'allowUndefined': False},
                                 'DESIRED_CMSDataset': {'allowUndefined': True},
                                 'CRAB_JobSW': {'allowUndefined': False},
                                 'CRAB_Publish': {'allowUndefined': False},
                                 'CRAB_PublishName': {'allowUndefined': False},
                                 'CRAB_PrimaryDataset': {'allowUndefined': False},
                                 'CRAB_RestHost': {'allowUndefined': False},
                                 'CRAB_DbInstance': {'allowUndefined': False},
                                 'CRAB_RetryOnASOFailures': {'allowUndefined': False},
                                 'CRAB_SaveLogsFlag': {'allowUndefined': False},
                                 'CRAB_TransferOutputs': {'allowUndefined': False},
                                 'CRAB_UserHN': {'allowUndefined': False},
                                }
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
            if not required_job_ad_attrs[attr]['allowUndefined']:
                if self.job_ad[attr] is None or str(self.job_ad[attr]).lower() in ['', 'undefined']:
                    undefined_attrs.append(attr)
        if undefined_attrs:
            msg = "Could not determine the following required attributes from the job ad: %s"
            msg = msg % (undefined_attrs)
            self.logger.error(msg)
            return 1
        return 0

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def parse_job_report(self):
        """
        Parse the job report, setting some class variables (the most important one
        self.output_files_info, which contains information of each output file relevant
        for transfers and file metadata upload).
        """
        # Load the job report.
        try:
            with open(G_JOB_REPORT_NAME) as fd:
                self.job_report = json.load(fd)
        except Exception as ex:
            msg = "Error loading job report: %s" % (str(ex))
            self.logger.exception(msg)
            return 1
        # Check that the job_report has the expected structure.
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
        # This is the job report part containing information about the output files.
        self.job_report_output = self.job_report['steps']['cmsRun']['output']
        # Set some class variables using the job report.
        self.log_needs_transfer = not bool(self.job_report.get('direct_stageout', False))
        self.log_needs_file_metadata_upload = not bool(self.job_report.get('file_metadata_upload', False))
        self.log_size = int(self.job_report.get('log_size', 0))
        self.executed_site = self.job_report.get('executed_site', None)
        if not self.executed_site:
            msg = "Unable to determine executed site from job report."
            self.logger.error(msg)
            return 1
        self.job_failed = bool(self.job_report.get('jobExitCode', 0))
        if self.job_failed:
            self.source_dir = os.path.join(self.source_dir, 'failed')
            self.dest_dir = os.path.join(self.dest_dir, 'failed')
        self.fill_output_files_info() # Fill self.output_files_info by parsing the job report.
        ##
        # at this point self.output_files_info is a list of dictionaries [file_info, file_info...]
        # file_info.keys() = 'filetype', 'module_label', 'inparentlfns', 'events',
        # 'checksums', 'outsize', 'pset_hash', 'pfn', 'output_dataset', 'local_stageout',
        # 'direct_stageout', 'outlocation', 'outtmplocation', 'outlfn', 'outtmplfn',
        # 'outfileruns', 'outfilelumis'
        ##
        self.aso_start_time = self.job_report.get("aso_start_time", None)
        self.aso_start_timestamp = self.job_report.get("aso_start_timestamp", None)

        # As last step, if ths is the first job in the task and it was successful,
        # and it is first retry, and there is an input dataset,
        # report to MONIT the list of read branches
        # Make sure that this never stops normal flow, even if it fails
        try:
            if (int(self.job_id) == 1 and self.crab_retry == 0 and self.job_report['exitCode'] == 0)\
                    and self.job_ad['DESIRED_CMSDataset'] != 'Unknown':
                self.reportReadBranches(branchList=self.job_report['steps']['cmsRun']['ReadBranches'],
                                        username=self.job_ad['CRAB_UserHN'],
                                        inputDataset=self.job_ad['DESIRED_CMSDataset'],
                                        taskName=self.job_ad['CRAB_ReqName'])
        except Exception as ex:
            self.logger.error("Something went wrong in reportReadBranches:\n%s", ex)

        return 0

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

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

        with open('taskinformation.pkl', 'rb') as fd:
            task = pickle.load(fd)

        # Loop over the output files that have to be collected.
        for filename in self.output_files_names:
            # Search for the output file info in the job report.
            output_file_info = get_output_file_info(filename)
            # Get the original file name, without the job id.
            left_piece, jobid_fileext = filename.rsplit("_", 1)
            orig_file_name = left_piece
            if "." in jobid_fileext:
                fileext = jobid_fileext.rsplit(".", 1)[-1]
                orig_file_name = left_piece + "." + fileext
            # If there is an output file info, parse it.
            if output_file_info:
                msg = "Output file info for %s: %s" % (orig_file_name, output_file_info)
                self.logger.debug(msg)
                file_info = {}
                self.output_files_info.append(file_info)
                # assign filetype based on the classification made by CRAB Client
                file_info['pfn'] = orig_file_name
                if file_info['pfn'] in task['tm_edm_outfiles']:
                    file_info['filetype'] = 'EDM'
                elif file_info['pfn'] in task['tm_tfile_outfiles']:
                    file_info['filetype'] = 'TFILE'
                elif file_info['pfn'] in task['tm_outfiles']:
                    file_info['filetype'] = 'FAKE'
                else:
                    file_info['filetype'] = 'UNKNOWN'
                file_info['module_label'] = output_file_info.get('module_label', 'unknown')
                file_info['inparentlfns'] = [str(i) for i in output_file_info.get('input', [])]
                file_info['events'] = output_file_info.get('events', 0)
                file_info['checksums'] = output_file_info.get('checksums', {'cksum': '0', 'adler32': '0'})
                file_info['outsize'] = output_file_info.get('size', 0)
                if 'pset_hash' in output_file_info:
                    file_info['pset_hash'] = output_file_info['pset_hash']
                else:
                    file_info['pset_hash'] = 32*'0'
                if 'pfn' in output_file_info:
                    file_info['pfn'] = str(output_file_info['pfn'])
                file_info['local_stageout'] = output_file_info.get('local_stageout', False)
                file_info['direct_stageout'] = output_file_info.get('direct_stageout', False)
                file_info['outlocation'] = self.dest_site
                if output_file_info.get('temp_storage_site', 'unknown') != 'unknown':
                    file_info['outtmplocation'] = output_file_info.get('temp_storage_site')
                elif self.job_report.get('temp_storage_site', 'unknown') != 'unknown':
                    file_info['outtmplocation'] = self.job_report.get('temp_storage_site')
                else:
                    file_info['outtmplocation'] = self.executed_site
                file_info['outlfn'] = os.path.join(self.dest_dir, filename)
                file_info['outtmplfn'] = os.path.join(self.source_dir, filename)
                if 'runs' not in output_file_info:
                    continue
                file_info['outfileruns'] = []
                file_info['outfilelumis'] = []
                for run, lumis in output_file_info['runs'].items():
                    file_info['outfileruns'].append(str(run))
                    # Creating a string like '100:20,101:21,105:20...'
                    # where the lumi is followed by a colon and number of events in that lumi.
                    # Note that the events per lumi information is provided by WMCore version >=1.1.2 when parsing FWJR.
                    lumisAndEvents = ','.join(['{0}:{1}'.format(str(lumi), str(numEvents))
                                               for lumi, numEvents in lumis.items()])
                    file_info['outfilelumis'].append(lumisAndEvents)
            else:
                msg = "Output file info for %s not found in job report." % (orig_file_name)
                self.logger.error(msg)
        # now that we have collected info from all files, we can define
        # output dataset name for each.
        # First find if there are multiple datasets in this job
        # since in that case the output dataset name is different. See:
        # https://twiki.cern.ch/twiki/bin/view/CMSPublic/Crab3DataHandling#Output_dataset_names_in_DBS
        edm_file_count = 0
        for file_info in self.output_files_info:
            if file_info['filetype'] == 'EDM':
                edm_file_count += 1
        multiple_edm = edm_file_count > 1
        # now compute the output dataset name for each file and add it to file_info
        outdataset = None
        for file_info in self.output_files_info:
            # use common function with ASOServerJob to ensure uniform dataset name
            if file_info['filetype'] in ['EDM', 'DQM'] and task['tm_publication'] == 'T':
                # group name overrides username when present:
                username = self.job_ad['CRAB_UserHN']
                if self.dest_dir.startswith('/store/group/') and self.dest_dir.split('/')[3]:
                    username = self.dest_dir.split('/')[3]
                module_label = file_info.get('module_label') if multiple_edm else None
                outdataset = compute_outputdataset_name(primaryDS=self.job_ad['CRAB_PrimaryDataset'],
                                                        username=username,
                                                        publish_name=self.publish_name,
                                                        pset_hash=file_info['pset_hash'],
                                                        module_label=module_label)
            else:
                outdataset = G_FAKE_OUTDATASET
            file_info['output_dataset'] = outdataset

        self.output_dataset = outdataset # we do not support multiple output datasets anymore


    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def set_dashboard_state(self, state, reason=None, exitCode=None):
        """
        Set the state of the job in Dashboard.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            return
        self.logger.info("====== Starting to prepare/send report to dashboard.")
        states_dict = {'COOLOFF'      : 'Cooloff',
                       'TRANSFERRING' : 'Transferring',
                       'FAILED'       : 'Aborted',
                       'FINISHED'     : 'Done'
                      }
        state = states_dict.get(state, state)
        params = {'MonitorID': self.reqname,
                  'MonitorJobID': "%s_https://glidein.cern.ch/%s/%s_%d" \
                                   % (self.job_id, self.job_id, \
                                      self.reqname.replace("_", ":"), \
                                      self.crab_retry),
                  'StatusValue': state,
                 }
        if reason:
            params['StatusValueReason'] = reason
        self.monitoringExitCode(params, exitCode)

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def set_state_ClassAds(self, state, exitCode=None):
        """
        Update PostJobStatus and job exit-code among the job ClassAds for the monitoring script to update the Grafana dashboard.
        """
        if os.environ.get('TEST_POSTJOB_NO_STATUS_UPDATE', False):
            try:  # this may fail when running PostJob interactively for debugging
                self.schedd.edit([self.dag_jobid], "LeaveJobInQueue", classad.ExprTree("false"))
            except Exception:
                pass  # caller is not equipped for dealing with exceptions from this method
            return
        self.logger.info("====== Starting to update job ClassAd.")
        msg = "status: %s." % (state)
        params = {'CRAB_PostJobStatus': '"{0}"'.format(state)}
        self.monitoringExitCode(params, exitCode)
        msg += " ClassAd attributes to set are: %s" % (str(params))
        self.logger.info(msg)

        limit = 5  # we retry these actions in case schedd is busy and refuses to edit the job
        counter = 0
        while counter < limit:
            try:
                counter += 1
                msg = "attempt %d out of %d" % (counter, limit)
                self.logger.info("       -----> Started %s -----", msg)
                for param in params:
                    self.schedd.edit([self.dag_jobid], param, str(params[param]))
                self.schedd.edit([self.dag_jobid], 'CRAB_PostJobLastUpdate', str(time.time()))
                # Once ClassAd state attributes have been updated, let HTCondor remove the job from the queue
                self.schedd.edit([self.dag_jobid], "LeaveJobInQueue", classad.ExprTree("false"))
                self.logger.info("       -----> Finished %s -----", msg)
                self.logger.info("====== Finished to update job ClassAd.")
                break
            except Exception:
                self.logger.exception("Exception in setting job ClassAd attributes:")

            if counter != limit:
                self.logger.warning("       -----> Failed to set job ClassAd attributes -----")
                maxSleep = 2*counter -1
                self.logger.warning("Sleeping for %d minute at most...", maxSleep)
                time.sleep(60 * random.randint(2*(counter//3), maxSleep+1))
            else:
                self.logger.error("Failed to set job ClassAd attributes for %d times, will not retry. Dashboard may report stale job status/exit-code.", limit)

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def monitoringExitCode(self, params, exitCode):
        """
        Take exit code from job_fjr.<job_id>.<retry_id>.json only if RetryJob thinks it is FATAL_ERROR.
        """
        if self.retryjob_retval and self.retryjob_retval == JOB_RETURN_CODES.FATAL_ERROR and not exitCode:
            try:
                with open(G_JOB_REPORT_NAME_NEW, 'r') as fd:
                    try:
                        report = json.load(fd)
                        if 'exitCode' in report:
                            params['JobExitCode'] = report['exitCode']
                        if 'jobExitCode' in report:
                            params['ExeExitCode'] = report['jobExitCode']
                    except ValueError as e:
                        self.logger.debug("Not able to load the fwjr because of a ValueError %s. \
                                           Not setting exit code for dashboard. Continuing normally", e)
                        # Means that file exists, but failed to load json
                        # Don`t fail and go ahead with reporting to dashboard
            except IOError as e:
                # File does not exist. Don`t fail and go ahead with reporting to dashboard
                self.logger.debug("Not able to load the fwjr because of a IOError %s. \
                                   Not setting exitcode for dashboard. Continuing normally.", e)
        # If Exit Code is defined we report only it. It is the final exit Code of the job
        elif exitCode:
            self.logger.debug("Dashboard exit code is defined by Postjob execution.")
            params['JobExitCode'] = exitCode
        else:
            self.logger.debug("Dashboard exit code already set on the worker node. Continuing normally.")

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def get_file_source_site(self, file_name, ifile=None):
        """
        Get the source site for the given file.
        """
        if ifile is None:
            ifile = get_file_index(file_name, self.output_files_info)
        if ifile is not None and self.output_files_info[ifile].get('outtmplocation'):
            return self.output_files_info[ifile]['outtmplocation']
        if self.job_report.get('temp_storage_site', 'unknown') != 'unknown':
            return self.job_report.get('temp_storage_site')
        return self.executed_site

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def calculate_crab_retry(self):
        """
        Calculate the retry number we're on. See the notes in PreJob.
        """
        fname = "retry_info/job.%s.txt" % (self.job_id)
        if os.path.exists(fname):
            try:
                with open(fname, 'r') as fd:
                    retry_info = json.load(fd)
            except Exception:
                msg = "Unable to calculate post-job retry count."
                msg += " Failed to load file %s." % (fname)
                msg += "\nDetails follow:"
                self.logger.exception(msg)
                return 1, None
        else:
            retry_info = {'pre': 0, 'post': 0}
        if 'pre' not in retry_info or 'post' not in retry_info:
            msg = "Unable to calculate post-job retry count."
            msg += " File %s doesn't contain the expected information." % (fname)
            self.logger.warning(msg)
            return 1, None
        if first_pj_execution():
            crab_retry = retry_info['post']
            retry_info['post'] += 1
            try:
                with open(fname + '.tmp', 'w') as fd:
                    json.dump(retry_info, fd)
                os.rename(fname + '.tmp', fname)
            except Exception:
                msg = "Failed to update file %s with increased post-job count by +1." % (fname)
                msg += "\nDetails follow:"
                self.logger.exception(msg)
                return 1, crab_retry
        else:
            crab_retry = retry_info['post'] - 1
        return 0, crab_retry

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_retry_count(self, exitCode=None):
        """
        When a recoverable error happens, we still have to check if the maximum allowed
        number of job retries was hit. If it was, the post-job should return with the
        fatal error exit code. Otherwise with the recoverable error exit code.
        """
        if self.dag_retry >= self.max_retries:
            msg = "Job could be retried, but the maximum allowed number of retries was hit."
            msg += " Setting this node (job) to permanent failure. DAGMan will NOT retry."
            self.logger.info(msg)
            self.set_dashboard_state('FAILED', exitCode=exitCode)
            self.set_state_ClassAds('FAILED', exitCode=exitCode)
            return JOB_RETURN_CODES.FATAL_ERROR
        msg = "Job will be retried by DAGMan."
        self.logger.info(msg)
        self.set_dashboard_state('COOLOFF', exitCode=exitCode)
        self.set_state_ClassAds('COOLOFF', exitCode=exitCode)

        return JOB_RETURN_CODES.RECOVERABLE_ERROR

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def check_abort_dag(self, rval):
        """
        For each job that failed with a fatal error, its id is written into the file
        task_statistics.FATAL_ERROR. For each job that didn't fail with a fatal or
        recoverable error, its id is written into the file task_statistics.OK.
        Notice that these files may contain repeated job ids.
        Based on these statistics we may decide to abort the whole DAG.
        """
        file_name_jobs_fatal = "task_statistics.FATAL_ERROR"
        file_name_jobs_ok = "task_statistics.OK"
        # Return code 3 is reserved to abort the entire DAG. Don't let the code
        # otherwise use it.
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
                    msg = "There are %d (fatal) failed nodes and %d successful nodes,"
                    msg += " adding up to a total of %d (more than the limit of %d)."
                    msg += " The ratio of failed to successful is greater than 1."
                    msg += " Will abort the whole DAG."
                    msg = msg % (num_fatal_failed_jobs, num_successful_jobs, \
                                 num_successful_jobs + num_fatal_failed_jobs, limit)
                    self.logger.error(msg)
                    rval = 3
        finally:
            return rval  # pylint: disable=lost-exception

    # = = = = = PostJob = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    def processWMArchive(self, retval):
        try:
            with open("/etc/wmarchive.json") as wma:
                WMARCHIVE_BASE_LOCATION = json.load(wma).get("BASE_DIR", "/data/wmarchive")
        except Exception:
            self.logger.exception("Can not load the WM archive json. Exception follows:")
            return
        WMARCHIVE_BASE_LOCATION = os.path.join(WMARCHIVE_BASE_LOCATION, 'new')

        now = int(time.time())
        archiveDoc = {}
        with open(G_WMARCHIVE_REPORT_NAME) as fd:
            job = {}
            job['id'] = '%s-%s' % (self.job_id, self.crab_retry)
            job['doc'] = {}
            job['doc']["fwjr"] = json.load(fd)
            job['doc']["jobtype"] = 'CRAB3'
            job['doc']["jobstate"] = 'success' if retval == 0 else 'failed'
            job['doc']["timestamp"] = now
            archiveDoc = createArchiverDoc(job)
            archiveDoc['task'] = self.reqname
            archiveDoc["meta_data"]['crab_id'] = self.job_id
            archiveDoc["meta_data"]['crab_exit_code'] = self.job_report.get('exitCode', -1)
            archiveDoc["steps"].append(
                {
                    "errors": [
                        {
                            "details": str(getattr(ASO_JOB, 'failures', "")),
                            "exitCode": 0,
                            "type": ""
                        }
                    ],
                    "input": [
                        {
                            "inputLFNs": [],
                            "inputPFNs": [],
                            "runs": [
                                {
                                    "eventsPerLumi": [],
                                    "lumis": []
                                }
                            ]
                        }
                    ],
                    "name": "aso",
                    "output": [
                        {
                            "inputLFNs": [],
                            "inputPFNs": [],
                            "outputLFNs": [],
                            "outputPFNs": [],
                            "runs": [
                                {
                                    "eventsPerLumi": [],
                                    "lumis": []
                                }
                            ]
                        }
                    ],
                    "performance": {
                        "multicore": {},
                        "storage": {},
                        "cpu": {},
                        "memory": {}
                    },
                    "site": self.dest_site,
                    "start": self.aso_start_timestamp or 0,
                    "status": 0,
                    "stop": now
                }
            )



        with open(G_WMARCHIVE_REPORT_NAME_NEW, 'w') as fd:
            json.dump(archiveDoc, fd)
        if not os.path.isdir(WMARCHIVE_BASE_LOCATION):
            os.makedirs(os.path.join(WMARCHIVE_BASE_LOCATION))
        #not using shutil.move because I want to move the file in the same disk
        self.logger.info("%s , %s", G_WMARCHIVE_REPORT_NAME_NEW, os.path.join(WMARCHIVE_BASE_LOCATION, "%s_%s" % (self.reqname, G_WMARCHIVE_REPORT_NAME_NEW)))
        os.rename(G_WMARCHIVE_REPORT_NAME_NEW, os.path.join(WMARCHIVE_BASE_LOCATION, "%s_%s" % (self.reqname, G_WMARCHIVE_REPORT_NAME_NEW)))

# ==============================================================================

class PermanentStageoutError(RuntimeError):
    pass


class RecoverableStageoutError(RuntimeError):
    pass


class TransferCacheLoadError(RuntimeError):
    pass

# ==============================================================================

def get_file_index(file_name, output_files):
    """
    Get the index location of the given file name within the list of output files
    dict infos from the job report.
    """
    for i, outfile in enumerate(output_files):
        if 'pfn' not in outfile:
            continue
        json_pfn = os.path.split(outfile['pfn'])[-1]
        pfn = os.path.split(file_name)[-1]
        left_piece, fileid = pfn.rsplit("_", 1)
        right_piece = fileid.split(".", 1)[-1]
        pfn = left_piece + "." + right_piece
        if pfn == json_pfn:
            return i
    return None

# ==============================================================================

class testServer(unittest.TestCase):
    def generateJobJson(self, sourceSite='srm.unl.edu'):
        return {"steps" : {
            "cmsRun" : { "input" : {},
              "output":
                {"outmod1" :
                     [ {"output_module_class": "PoolOutputModule",
                        "input": ["/test/input2", "/test/input2"],
                        "events": 200,
                        "size": 100,
                        "SEName": sourceSite,
                        "runs": { 1: [1, 2, 3],
                                   2: [2, 3, 4]},
                }]}}}}


    def setUp(self):
        self.postjob = PostJob()
        #self.job = ASOServerJob()
        #status, crab_retry, max_retries, restinstance, resturl, reqname, id,
        #outputdata, job_sw, async_dest, source_dir, dest_dir, *filenames
        self.full_args = ['0', 0, 2, 'restinstance', 'resturl',
                          'reqname', 1234, 'outputdata', 'sw', 'T2_US_Vanderbilt']
        self.json_name = "jobReport.json.%s" % (self.full_args[6])
        with open(self.json_name, 'w') as fh:
            #fh.write(json.dumps(self.generateJobJson()))
            json.dump(self.generateJobJson, fh)

    def getLevelOneDir(self):
        return datetime.datetime.now().strftime("%Y-%m")

    def getLevelTwoDir(self):
        return datetime.datetime.now().strftime("%d%p")

    def getUniqueFilename(self):
        return "%s-postjob.txt" % (uuid.uuid4())


    def tearDown(self):
        if os.path.exists(self.json_name):
            os.unlink(self.json_name)

# ==============================================================================

if __name__ == '__main__':
    if len(sys.argv) >= 2 and sys.argv[1] == 'UNIT_TEST':
        sys.argv = [sys.argv[0]]
        print("Beginning testing")
        unittest.main()
        print("Testing over")
        sys.exit()
    POSTJOB = PostJob()
    sys.exit(POSTJOB.execute(*sys.argv[2:]))
