#!/usr/bin/python
# pylint: disable=line-too-long
"""

"""
from __future__ import division
from __future__ import print_function
import json
import logging
import threading
import os
import subprocess
from datetime import timedelta
from httplib import HTTPException

import fts3.rest.client.easy as fts3

from rucio.client import Client as rucioclient
from rucio.rse.rsemanager import find_matching_scheme

from RESTInteractions import CRABRest
from ServerUtilities import encodeRequest
#from TransferInterface import CRABDataInjector

FTS_ENDPOINT = "https://fts3-cms.cern.ch:8446/"
FTS_MONITORING = "https://fts3-cms.cern.ch:8449/"

if not os.path.exists('task_process/transfers'):
    os.makedirs('task_process/transfers')

logging.basicConfig(
    filename='task_process/transfers/transfer_inject.log',
    level=logging.INFO,
    format='%(asctime)s[%(relativeCreated)6d]%(threadName)s: %(message)s'
)

if os.path.exists('task_process/RestInfoForFileTransfers.json'):
    with open('task_process/RestInfoForFileTransfers.json') as fp:
        restInfo = json.load(fp)
        proxy = os.getcwd() + "/" + str(restInfo['proxyfile'])  # make sure no to unicode to FTS clients
        #rest_filetransfers = restInfo['host'] + '/crabserver/' + restInfo['dbInstance']
        os.environ["X509_USER_PROXY"] = proxy

#if os.path.exists('task_process/rest_filetransfers.txt'):
#    with open("task_process/rest_filetransfers.txt", "r") as _rest:
#        rest_filetransfers = _rest.readline().split('\n')[0]
#        proxy = os.getcwd() + "/" + _rest.readline()
#        print("Proxy: %s" % proxy)

if os.path.exists('USE_NEW_PUBLISHER'):
    asoworker = 'schedd'
else:
    asoworker = 'asoless'

if os.path.exists('USE_FTS_REUSE'):
    ftsReuse = True
else:
    ftsReuse = False

def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    :param l: list to splitt in chunks
    :param n: chunk size
    :return: yield the next list chunk
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def mark_transferred(ids, crabserver):
    """
    Mark the list of files as tranferred
    :param ids: list of Oracle file ids to update
    :param crabserver: a CRABRest object for doing POST to CRAB server REST
    :return: True success, False failure
    """
    try:
        logging.debug("Marking done %s", ids)

        data = dict()
        data['asoworker'] = asoworker
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["DONE" for _ in ids]

        crabserver.post('/filetransfers', data=encodeRequest(data))
        logging.info("Marked good %s", ids)
    except Exception:
        logging.exception("Error updating documents")
        return False
    return True


def mark_failed(ids, failures_reasons, crabserver):
    """
    Mark the list of files as failed
    :param ids: list of Oracle file ids to update
    :param failures_reasons: list of strings with transfer failure messages
    :param crabserver: an CRABRest object for doing POST to CRAB server REST
    :return: True success, False failure
    """
    try:
        data = dict()
        data['asoworker'] = asoworker
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["FAILED" for _ in ids]
        data['list_of_failure_reason'] = failures_reasons
        data['list_of_retry_value'] = [0 for _ in ids]

        crabserver.post('/filetransfers', data=encodeRequest(data))
        logging.info("Marked failed %s", ids)
    except Exception:
        logging.exception("Error updating documents")
        return False
    return True


def remove_files_in_bkg(pfns, logFile, timeout=None):
    """
    fork a process to remove the indicated PFN's without
    wainting for it to complete and w/o any error checking
    gfal-rm output is added to logFile
    A timeout is applied on the gfal-rm command anyhow as a sanity measure
        against runaway processes, we accept that some file may not be deleted.
    :param pfns: list of SURL's
    :param logFile: name of the logFile
    :param timeout: timeout as a string valid as arg. for linux timeout command, default is 4 hours
    :return: none
    """

    if not timeout:
        timeout = '%dm' % (len(pfns) * 3)    # default is 3minutes per file to be removed
    command = 'env -i X509_USER_PROXY=%s timeout %s gfal-rm -v -t 180 %s >> %s 2>&1 &'  % \
              (proxy, timeout, pfns, logFile)
    logging.debug("Running remove command %s", command)
    subprocess.call(command, shell=True)

    return


class check_states_thread(threading.Thread):
    """
    get transfers state per jobid
    """
    def __init__(self, threadLock, log, ftsContext, jobid, jobsEnded, jobs_ongoing, done_id, failed_id, failed_reasons):
        """

        :param threadLock:
        :param log:
        :param ftsContext:
        :param jobid:
        :prarm jobsEnded:
        :param jobs_ongoing:
        :param done_id:
        :param failed_id:
        :param failed_reasons:
        """
        threading.Thread.__init__(self)
        self.ftsContext = ftsContext
        self.jobid = jobid
        self.jobsEnded = jobsEnded
        self.jobs_ongoing = jobs_ongoing
        self.log = log
        self.threadLock = threadLock
        self.done_id = done_id
        self.failed_id = failed_id
        self.failed_reasons = failed_reasons

    def run(self):
        """
        - check if the fts job is in final state (FINISHED, FINISHEDDIRTY, CANCELED, FAILED)
        - get file transfers states and get corresponding oracle ID from FTS file metadata
        - update states on oracle
        """

        self.threadLock.acquire()
        self.log.info("Getting state of job %s" % self.jobid)

        self.jobs_ongoing.append(self.jobid)

        try:
            status = fts3.get_job_status(self.ftsContext, self.jobid, list_files=False)
        except HTTPException as hte:
            self.log.exception("failed to retrieve status for %s " % self.jobid)
            self.log.exception("httpExeption headers %s " % hte.headers)
            if hte.status == 404:
                self.log.exception("%s not found in FTS3 DB" % self.jobid)
                self.jobs_ongoing.remove(self.jobid)
            return
        except Exception:
            self.log.exception("failed to retrieve status for %s " % self.jobid)
            self.threadLock.release()
            return

        self.log.info("State of job %s: %s" % (self.jobid, status["job_state"]))

        if status["job_state"] in ['FINISHED', 'FINISHEDDIRTY', "FAILED", "CANCELED"]:
            self.jobsEnded.append(self.jobid)
        if status["job_state"] in ['ACTIVE', 'FINISHED', 'FINISHEDDIRTY', "FAILED", "CANCELED"]:
            file_statuses = fts3.get_job_status(self.ftsContext, self.jobid, list_files=True)['files']
            self.done_id[self.jobid] = []
            self.failed_id[self.jobid] = []
            self.failed_reasons[self.jobid] = []
            files_to_remove = []

            for file_status in file_statuses:
                _id = file_status['file_metadata']['oracleId']
                tx_state = file_status['file_state']

                # xfers have only 3 terminal states: FINISHED, FAILED, and CANCELED see
                # https://fts3-docs.web.cern.ch/fts3-docs/docs/state_machine.html
                if tx_state == 'FINISHED':
                    self.done_id[self.jobid].append(_id)
                    files_to_remove.append(file_status['source_surl'])
                elif tx_state == 'FAILED' or tx_state == 'CANCELED':
                    self.failed_id[self.jobid].append(_id)
                    if file_status['reason']:
                        self.log.info('Failure reason: ' + file_status['reason'])
                        self.failed_reasons[self.jobid].append(file_status['reason'])
                    else:
                        self.log.exception('Failure reason not found')
                        self.failed_reasons[self.jobid].append('unable to get failure reason')
                    files_to_remove.append(file_status['source_surl'])
                else:
                    # file transfer is not terminal:
                    if status["job_state"] == 'ACTIVE':
                        # if job is still ACTIVE file status will be updated in future run. See:
                        # https://fts3-docs.web.cern.ch/fts3-docs/docs/state_machine.html
                        pass
                    else:
                        # job status is terminal but file xfer status is not.
                        # something went wrong inside FTS and a stuck transfers is waiting to be
                        # removed by the reapStalledTransfers https://its.cern.ch/jira/browse/FTS-1714
                        # mark as failed
                        self.failed_id[self.jobid].append(_id)
                        self.log.info('Failure reason: stuck inside FTS')
                        self.failed_reasons[self.jobid].append(file_status['reason'])
            try:
                list_of_surls = ''   # gfal commands take list of SURL as a list of blank-separated strings
                for f in files_to_remove:
                    list_of_surls += str(f) + ' '  # convert JSON u'srm://....' to plain srm://...
                removeLogFile = './task_process/transfers/remove_files.log'
                remove_files_in_bkg(list_of_surls, removeLogFile)
            except Exception:
                self.log.exception('Failed to remove temp files')

        self.threadLock.release()


class submit_thread(threading.Thread):
    """Thread for actual FTS job submission

    """

    def __init__(self, threadLock, log, ftsContext, files, source, jobids, toUpdate):
        """
        :param threadLock: lock for the thread
        :param log: log object
        :param ftsContext: FTS context
        :param files: [
               [source_pfn,
                dest_pfn,
                file oracle id,
                source site,
                username,
                taskname,
                file size],
               ...]
        :param source: source site name
        :param jobids: collect the list of job ids when submitted
        :param toUpdate: list of oracle ids to update
        """
        threading.Thread.__init__(self)
        self.log = log
        self.threadLock = threadLock
        self.files = files
        self.source = source
        self.jobids = jobids
        self.ftsContext = ftsContext
        self.toUpdate = toUpdate


    def run(self):
        """

        """

        self.threadLock.acquire()
        self.log.info("Processing transfers from: %s" % self.source)

        # create destination and source pfns for job
        transfers = []
        for lfn in self.files:
            transfers.append(fts3.new_transfer(lfn[0],
                                               lfn[1],
                                               filesize=lfn[6],
                                               metadata={'oracleId': lfn[2]}
                                               )
                             )
        self.log.info("Submitting %s transfers to FTS server" % len(self.files))
        #SB#
        for lfn in self.files:
            self.log.info("%s %s %s %s", lfn[0],lfn[1],lfn[6],{'oracleId': lfn[2]})
        #SB#

        # Submit fts job
        job = fts3.new_job(transfers,
                           overwrite=True,
                           verify_checksum=True,
                           metadata={"issuer": "ASO",
                                     "userDN": self.files[0][4],
                                     "taskname": self.files[0][5]},
                           copy_pin_lifetime=-1,
                           bring_online=None,
                           source_spacetoken=None,
                           spacetoken=None,
                           # max time for job in the FTS queue in hours. From FTS experts in
                           # https://cern.service-now.com/service-portal?id=ticket&table=incident&n=INC2776329
                           # The max_time_in_queue applies per job, not per retry.
                           # The max_time_in_queue is a timeout for how much the job can stay in
                           # a SUBMITTED, ACTIVE or STAGING state.
                           # When a job's max_time_in_queue is reached, the job and all of its
                           # transfers not yet in a terminal state are marked as CANCELED
                           # StefanoB: I see that we hit this at times with 6h, causing job resubmissions,
                           # so will try to make it longer to give FTS maximum chances within our
                           # 24h overall limit (which takes care also of non-FTS related issues !)
                           # ASO transfers never require STAGING so jobs can spend max_time_in_queue only
                           # as SUBMITTED (aka queued) or ACTIVE (at least one transfer has been activated)
                           max_time_in_queue=10,
                           # from same cern.service-now.com ticket as above:
                           # The number of retries applies to each transfer within that job.
                           # A transfer is granted the first execution + number_of_retries.
                           # E.g.: retry=3 --> first execution + 3 retries
                           # so retry=3 means each transfer has 4 chances at most during the 6h
                           # max_time_in_queue
                           retry=3,
                           reuse=ftsReuse,
                           # seconds after which the transfer is retried
                           # this is a transfer that fails, gets put to SUBMITTED right away,
                           # but the scheduler will avoid it until NOW() > last_retry_finish_time + retry_delay
                           # reduced under FTS suggestion w.r.t. the 3hrs of asov1
                           # StefanoB: indeed 10 minutes makes much more sense for storage server glitches
                           retry_delay=600
                           # timeout on the single transfer process
                           # TODO: not clear if we may need it
                           # timeout = 1300
                           )

        jobid = fts3.submit(self.ftsContext, job)

        self.jobids.append(jobid)

        # TODO: manage exception here, what we should do?
        fileDoc = dict()
        fileDoc['asoworker'] = asoworker
        fileDoc['subresource'] = 'updateTransfers'
        fileDoc['list_of_ids'] = [x[2] for x in self.files]
        fileDoc['list_of_transfer_state'] = ["SUBMITTED" for _ in self.files]
        fileDoc['list_of_fts_instance'] = [FTS_ENDPOINT for _ in self.files]
        fileDoc['list_of_fts_id'] = [jobid for _ in self.files]

        self.log.info("Marking submitted %s files" % (len(fileDoc['list_of_ids'])))

        self.toUpdate.append(fileDoc)
        self.threadLock.release()


def submit(rucioClient, ftsContext, toTrans, crabserver):
    """
    submit tranfer jobs

    - group files to be transferred by source site
    - prepare jobs chunks of max 200 transfers
    - submit fts job

    :param ftsContext: fts client ftsContext
    :param toTrans: [[source pfn,
                      destination pfn,
                      oracle file id,
                      source site,
                      destination,
                      username,
                      taskname,
                      filesize, checksum],....]
    :param crabserver: an CRABRest object for doing POST to CRAB server REST
    :return: list of jobids submitted
    """
    threadLock = threading.Lock()
    threads = []
    jobids = []
    to_update = []

    # some things are the same for all files, pick them from the first one
    username = toTrans[0][5]
    scope = "user."+username
    taskname = toTrans[0][6]
    dst_rse = toTrans[0][4]
    a_dst_lfn = toTrans[0][1]
    a_src_lfn = toTrans[0][0]

    # this is not ols ASO anymore. All files here have same destination
    # and LFN's and PFN's can only differ from each other in the directory counter and actual file name
    # no reason to lookup lfn2pfn by individual file
    # remember that src and dst LFN's are in general different (/store/user vs. /store/temp/user)
    # so we assume: (P/L)FN = (P/L)FN_PREFIX/FILEID
    # where: FILEID = xxxx/filename.filetype and xxxx is a 0000, 0001, 0002 etc.

    dst_lfn_prefix = '/'.join(a_dst_lfn.split("/")[:-2])
    dst_did = scope + ':' + dst_lfn_prefix + '/0000/file.root'

    src_lfn_prefix = '/'.join(a_src_lfn.split("/")[:-2])
    src_did = scope + ':' + src_lfn_prefix + '/0000/file.root'

    sourceSites = list(set([x[3] for x in toTrans]))

    for source in sourceSites:

        # find all files for this source
        toTransFromThisSource = [x for x in toTrans if x[3] == source]
        # find matching transfer protocol and proper PFN prefixes
        src_rse = source
        try:
            dst_scheme, src_scheme, _, _ = find_matching_scheme(
                {"protocols": rucioClient.get_protocols(dst_rse)},
                {"protocols": rucioClient.get_protocols(src_rse)},
                "third_party_copy",
                "third_party_copy",
            )
            dst_pfn_template = rucioClient.lfns2pfns(dst_rse, [dst_did],
                                                     operation="third_party_copy", scheme=dst_scheme)
            src_pfn_template = rucioClient.lfns2pfns(src_rse, [src_did],
                                                     operation="third_party_copy", scheme=src_scheme)
            dst_pfn_prefix = '/'.join(dst_pfn_template[dst_did].split("/")[:-2])
            src_pfn_prefix = '/'.join(src_pfn_template[src_did].split("/")[:-2])
        except Exception as ex:
            logging.error("Failed to map lfns to pfns: %s", ex)
            ids = [x[2] for x in toTransFromThisSource]
            mark_failed(ids, ["Failed to map lfn to pfn: " + str(ex) for _ in ids], crabserver)
            # try next source
            continue

        # OK, have good protocols and PFNs, build info for FTS
        tx_from_source = []
        for f in toTransFromThisSource:
            id = f[2]
            size = f[7]
            checksums = f[8]
            src_lfn = f[0]
            dst_lfn = f[1]
            fileid = '/'.join(src_lfn.split('/')[-2:])
            src_pfn = src_pfn_prefix + '/' + fileid
            dst_pfn = dst_pfn_prefix + '/' + fileid

            xfer = [src_pfn, dst_pfn, id, source, username, taskname, size, checksums['adler32'].rjust(8, '0')]
            tx_from_source.append(xfer)

        xfersPerFTSJob = 50 if ftsReuse else 200
        for files in chunks(tx_from_source, xfersPerFTSJob):
            thread = submit_thread(threadLock, logging, ftsContext, files, source, jobids, to_update)
            thread.start()
            threads.append(thread)

    for t in threads:
        t.join()

    for fileDoc in to_update:
        _ = crabserver.post('/filetransfers', data=encodeRequest(fileDoc))
        logging.info("Marked submitted %s files", fileDoc['list_of_ids'])

    return jobids


def perform_transfers(inputFile, lastLine, _lastFile, ftsContext, rucioClient, crabserver):
    """
    get transfers and update last read line number

    :param inputFile: path to the file with list of files to be transferred
    :param lastLine: number of the last line processed
    :param _last: path to the file keeping track of the last read line
    :param ftsContext: FTS context
    :param rucioClient: a Rucio Client object
    :return:
    """

    transfers = []
    logging.info("starting from line: %s", lastLine)

    with open(inputFile) as _list:
        for _data in _list.readlines()[lastLine:]:
            try:
                lastLine += 1
                doc = json.loads(_data)
            except Exception:
                continue
            transfers.append([doc["source_lfn"],
                              doc["destination_lfn"],
                              doc["id"],
                              doc["source"],
                              doc["destination"],
                              doc["username"],
                              doc["taskname"],
                              doc["filesize"],
                              doc["checksums"]])

        jobids = []
        if transfers:
            jobids = submit(rucioClient, ftsContext, transfers, crabserver)

            for jobid in jobids:
                logging.info("Monitor link: " + FTS_MONITORING + "fts3/ftsmon/#/job/%s", jobid)  # pylint: disable=logging-not-lazy

            # TODO: send to dashboard

        _lastFile.write(str(lastLine))

    return transfers, jobids


def state_manager(ftsContext, crabserver):
    """

    """
    threadLock = threading.Lock()
    threads = []
    jobs_done = []
    jobs_ongoing = []
    jobsEnded = []
    failed_id = {}
    failed_reasons = {}
    done_id = {}

    # TODO: puo esser utile togliere questo file? mmm forse no

    if os.path.exists('task_process/transfers/fts_jobids.txt'):
        with open("task_process/transfers/fts_jobids.txt", "r") as _jobids:
            lines = _jobids.readlines()
            for line in list(set(lines)):
                if line:
                    jobid = line.split('\n')[0]
                if jobid:
                    thread = check_states_thread(threadLock, logging, ftsContext, jobid, jobsEnded, jobs_ongoing, done_id, failed_id, failed_reasons)
                    thread.start()
                    threads.append(thread)
            _jobids.close()

        for t in threads:
            t.join()

        # the threads above have filled:
        # job_ongoing: list of FTS jobs processed
        # done_id: a dictionary {jobId:[list_of_tnasfers_id_done in that job]}  the list may be empty
        # failed_id, failed_reasons: dictionaries with same format as above containing list of failed xfers and reasons
        # but at this point a given FTS job may be compelted, or still ACTIVE and only part of its transfers are
        # reported as done/failed.
        try:
            # process all jobs for which some xfers have been reported as dobe
            for jobID in done_id:
                logging.info('Marking job %s files done and %s files failed for job %s', len(done_id[jobID]), len(failed_id[jobID]), jobID)

                if done_id[jobID]:
                    markDone = mark_transferred(done_id[jobID], crabserver)
                else:
                    markDone = True
                if failed_id[jobID]:
                    markFailed = mark_failed(failed_id[jobID], failed_reasons[jobID], crabserver)
                else:
                    markFailed = True
                if jobID in jobsEnded:
                    # only remove a Terminated FTS job from the list of xfer marking was successful, otherwise will try again
                    if markDone and markFailed:
                        jobs_done.append(jobID)
                        jobs_ongoing.remove(jobID)
                    else:
                        jobs_ongoing.append(jobID)   # SB is this necessary ? AFAIU check_states_thread has filled it
                else:
                    jobs_ongoing.append(jobID)  # should not be necessary.. but for consistency with above
        except Exception:
            logging.exception('Failed to update states')
    else:
        logging.warning('No FTS job ID to monitor yet')

    with open("task_process/transfers/fts_jobids_new.txt", "w+") as _jobids:
        for line in list(set(jobs_ongoing)):  # SB if we remove the un-necessary append above, non eed for a set here
            logging.info("Writing: %s", line)
            _jobids.write(line+"\n")

    os.rename("task_process/transfers/fts_jobids_new.txt", "task_process/transfers/fts_jobids.txt")

    return jobs_ongoing


def submission_manager(rucioClient, ftsContext, crabserver):
    """

    """
    last_line = 0
    if os.path.exists('task_process/transfers/last_transfer.txt'):
        with open("task_process/transfers/last_transfer.txt", "r") as _last:
            read = _last.readline()
            last_line = int(read)
            logging.info("last line is: %s", last_line)
            _last.close()

    # TODO: if the following fails check not to leave a corrupted file
    with open("task_process/transfers/last_transfer_new.txt", "w+") as _last:
        _, jobids = perform_transfers("task_process/transfers.txt", last_line, _last, ftsContext, rucioClient, crabserver)
        _last.close()
        os.rename("task_process/transfers/last_transfer_new.txt", "task_process/transfers/last_transfer.txt")

    with open("task_process/transfers/fts_jobids.txt", "a") as _jobids:
        for job in jobids:
            _jobids.write(str(job)+"\n")
        _jobids.close()

    return jobids


def algorithm():
    """

    script algorithm
    - instantiates FTS3 python easy client
    - delegate user proxy to fts if needed
    - check for fts jobs to monitor and update states in oracle
    - get last line from last_transfer.txt
    - gather list of file to transfers
        + group by source
        + submit ftsjob and save fts jobid
        + update info in oracle
    - append new fts job ids to fts_jobids.txt
    """

    logging.info("using user's proxy from %s", proxy)
    ftsContext = fts3.Context(FTS_ENDPOINT, proxy, proxy, verify=True)
    logging.info("Delegating proxy to FTS...")
    delegationId = fts3.delegate(ftsContext, lifetime=timedelta(hours=48),  # pylint: disable=unused-variable
                                 delegate_when_lifetime_lt=timedelta(hours=24), force=False)
    # we never had problems with delegation since we put proper delegate_when in the above line
    # but if need to check delegation arise, it can be done with a query like
    # curl --cert proxy --key proxy https://fts3-cms.cern.ch:8446/delegation/<delegatinId>
    # see: https://fts3-docs.web.cern.ch/fts3-docs/fts-rest/docs/api.html#get-delegationdlgid

    # instantiate an object to talk with CRAB REST server
    try:
        crabserver = CRABRest(restInfo['host'], localcert=proxy, localkey=proxy,
                              userAgent='CRABSchedd')
        crabserver.setDbInstance(restInfo['dbInstance'])
    except Exception:
        logging.exception("Failed to set connection to crabserver")
        return

    with open("task_process/transfers.txt") as _list:
        _data = _list.readlines()[0]
        try:
            doc = json.loads(_data)
            username = doc["username"]
            taskname = doc["taskname"]
            destination = doc["destination"]
        except Exception as ex:
            msg = "Username gathering failed with\n%s" % str(ex)
            logging.warn(msg)
            raise ex

    try:
        logging.info("Initializing Rucio client")
        os.environ["X509_USER_PROXY"] = proxy
        os.environ["X509_USER_PROXY"] = proxy
        rucioClient = rucioclient(account=username, auth_type='x509_proxy')
        #rucioClient = CRABDataInjector(taskname, destination, account=username,
        #                               scope="user."+username, auth_type='x509_proxy')
    except Exception as exc:
        msg = "Rucio initialization failed with\n%s" % str(exc)
        logging.warn(msg)
        raise exc

    jobs_ongoing = state_manager(ftsContext, crabserver)
    new_jobs = submission_manager(rucioClient, ftsContext, crabserver)

    logging.info("Transfer jobs ongoing: %s, new: %s ", jobs_ongoing, new_jobs)

    return


if __name__ == "__main__":
    try:
        algorithm()
    except Exception:
        logging.exception("error during main loop")
    logging.info("transfer_inject.py exiting")
