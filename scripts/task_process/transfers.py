#!/usr/bin/python
"""

"""
from __future__ import division, print_function
import json
import logging
import threading
import os
import time
import subprocess

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Storage.TrivialFileCatalog import readTFC
import fts3.rest.client.easy as fts3
from datetime import timedelta
from RESTInteractions import HTTPRequests
from ServerUtilities import  encodeRequest

if not os.path.exists('task_process/transfers'):
    os.makedirs('task_process/transfers')

logging.basicConfig(
    filename='task_process/transfers/transfer_inject.log',
    level=logging.DEBUG,
    format='%(asctime)s[%(relativeCreated)6d]%(threadName)s: %(message)s'
)

if os.path.exists('task_process/rest_filetransfers.txt'): 
    with open("task_process/rest_filetransfers.txt", "r") as _rest:
        rest_filetransfers = _rest.readline().split('\n')[0]
        proxy = os.getcwd() + "/" + _rest.readline()
        print("Proxy: %s", proxy)


def execute_command(command, logger, timeout):
    """
    _execute_command_
    Funtion to manage commands.
    """

    stdout, stderr, rc = None, None, 99999
    proc = subprocess.Popen(
            command, shell=True, cwd=os.environ['PWD'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
    )

    t_beginning = time.time()
    seconds_passed = 0
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command )
            return stdout, rc

        time.sleep(0.1)

    stdout, stderr = proc.communicate()
    rc = proc.returncode

    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))

    return stdout, rc


def get_tfc_rules(phedex, site):
    """
    Get the TFC regexp for a given site.
    """
    tfc_file = None
    try:
        phedex.getNodeTFC(site)
    except Exception as e:
        logging.exception('PhEDEx exception: %s', e)
    try:
        tfc_file = phedex.cacheFileName('tfc',
                                        inputdata={'node': site})
    except Exception as e:
        logging.exception('PhEDEx cache exception: %s', e)
    return readTFC(tfc_file)


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    :param l: list to splitt in chunks
    :param n: chunk size
    :return: yield the next list chunk
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def mark_transferred(ids):
    """
    Mark the list of files as tranferred
    :param ids: list of Oracle file ids to update
    :return: 0 success, 1 failure
    """
    try:
        oracleDB = HTTPRequests(rest_filetransfers,
                                proxy,
                                proxy)
        logging.debug("Marking done %s", ids)

        data = dict()
        data['asoworker'] = 'asoless'
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["DONE" for _ in ids]

        oracleDB.post('/filetransfers',
                      data=encodeRequest(data))
        logging.debug("Marked good %s", ids)
    except Exception:
        logging.exception("Error updating documents")
        return 1
    return 0


def mark_failed(ids, failures_reasons):
    """
    Mark the list of files as failed
    :param ids: list of Oracle file ids to update
    :param failures_reasons: list of strings with transfer failure messages
    :return: 0 success, 1 failure
    """
    try:
        oracleDB = HTTPRequests(rest_filetransfers,
                                proxy,
                                proxy)
        data = dict()
        data['asoworker'] = 'asoless'
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["FAILED" for _ in ids]
        data['list_of_failure_reason'] = failures_reasons
        data['list_of_retry_value'] = [0 for _ in ids]

        oracleDB.post('/filetransfers',
                      data=encodeRequest(data))
        logging.debug("Marked failed %s", ids)
    except Exception:
        logging.exception("Error updating documents")
        return 1
    return 0


def remove_files(pfn):

    command = 'env -i X509_USER_PROXY=%s gfal-rm -v -t 180 %s'  % \
              (proxy, pfn)
    logging.debug("Running remove command %s" % command)
    stdout, rc = execute_command(command, logging, 3600)
    if rc:
        logging.info("Deletion command failed with output %s" % (stdout))
    else:
        logging.info("File Deleted.")
    return


class check_states_thread(threading.Thread):
    """
    get transfers state per jobid
    """
    def __init__(self, threadLock, log, fts, jobid, jobs_ongoing, done_id, failed_id, failed_reasons):
        """

        :param threadLock:
        :param log:
        :param fts:
        :param jobid:
        :param jobs_ongoing:
        :param done_id:
        :param failed_id:
        :param failed_reasons:
        """
        threading.Thread.__init__(self)
        self.fts = fts
        self.jobid = jobid
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
            status = self.fts.get("jobs/"+self.jobid)[0]
        except Exception as ex:
            self.log.exception("failed to retrieve status for %s " % self.jobid)
            self.jobs_ongoing.append(self.jobid)
            self.threadLock.release()
            return

        self.log.info("State of job %s: %s" % (self.jobid, status["job_state"]))

        # TODO: if in final state get with list_files=True and the update_states
        if status["job_state"] in ['FINISHED', 'FINISHEDDIRTY', "FAILED", "CANCELED"]:
            file_statuses = self.fts.get("jobs/%s/files" % self.jobid)[0]

            self.done_id[self.jobid] = []
            self.failed_id[self.jobid] = []
            self.failed_reasons[self.jobid] = []

            for file_status in file_statuses:
                _id = file_status['file_metadata']['oracleId']
                tx_state = file_status['file_state']

                if tx_state == 'FINISHED':
                    self.done_id[self.jobid].append(_id)
                else:
                    self.failed_id[self.jobid].append(_id)
                    if file_status['reason']:
                        self.log.info('Failure reason: ' + file_status['reason'])
                        self.failed_reasons[self.jobid].append(file_status['reason'])
                    else:
                        self.log.exception('Failure reason not found')
                        self.failed_reasons[self.jobid].append('unable to get failure reason')
                # TODO: wait for gfal installed on schedds
                # try:
                #     remove_files(file_status['source_surl'])
                # except:
                #     self.log.exception('Failed to remove temp files')

        self.threadLock.release()


class submit_thread(threading.Thread):
    """

    """
    def __init__(self, threadLock, log, context, files, source, jobids, toUpdate):
        """

        :param threadLock:
        :param log:
        :param context:
        :param files:
        :param source:
        :param jobids:
        :param toUpdate:
        """
        threading.Thread.__init__(self)
        self.log = log
        self.threadLock = threadLock
        self.files = files
        self.source = source
        self.jobids = jobids
        self.context = context
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
                                               metadata={'oracleId': lfn[2]}
                                               )
                             )
        self.log.info("Submitting %s transfers to FTS server" % len(self.files))

        # Submit fts job
        job = fts3.new_job(transfers,
                           overwrite=True,
                           verify_checksum=True,
                           # TODO: add user DN to metadata
                           metadata={"issuer": "ASO",
                                     "userDN": self.files[0][4],
                                     "taskname": self.files[0][5]},
                           copy_pin_lifetime=-1,
                           bring_online=None,
                           source_spacetoken=None,
                           spacetoken=None,
                           max_time_in_queue=600,
                           retry=3,
                           retry_delay=600
                           )
        # reuse=True
        # TODO: fts retries?? check delay

        jobid = fts3.submit(self.context, job)

        self.jobids.append(jobid)

        # TODO: manage exception here, what we should do?
        fileDoc = dict()
        fileDoc['asoworker'] = 'asoless'
        fileDoc['subresource'] = 'updateTransfers'
        fileDoc['list_of_ids'] = [x[2] for x in self.files]
        fileDoc['list_of_transfer_state'] = ["SUBMITTED" for _ in self.files]
        fileDoc['list_of_fts_instance'] = ['https://fts3.cern.ch:8446/' for _ in self.files]
        fileDoc['list_of_fts_id'] = [jobid for _ in self.files]

        self.log.info("Marking submitted %s files" % (len(fileDoc['list_of_ids'])))

        self.toUpdate.append(fileDoc)
        self.threadLock.release()


def submit(phedex, context, toTrans):
    """
    submit tranfer jobs

    - group files to be transferred by source site
    - prepare jobs chunks of max 200 transfers
    - submit fts job

    :param context: fts client context
    :param toTrans: [source pfn, destination pfn, oracle file id, source site]
    :return: list of jobids submitted
    """
    threadLock = threading.Lock()
    threads = []
    jobids = []
    to_update = []

    oracleDB = HTTPRequests(rest_filetransfers,
                            proxy,
                            proxy)

    sources = list(set([x[3] for x in toTrans]))

    for source in sources:

        ids = [x[2] for x in toTrans if x[3] == source]
        username = toTrans[0][5]
        taskname = toTrans[0][6]
        src_lfns = [x[0] for x in toTrans if x[3] == source]
        dst_lfns = [x[1] for x in toTrans if x[3] == source]

        sorted_source_pfns = []
        sorted_dest_pfns = []

        try:
            for chunk in chunks(src_lfns, 10):
                unsorted_source_pfns = [[k[1], str(x)] for k, x in phedex.getPFN(source, chunk).items()]
                for order_lfn in chunk:
                    for lfn, pfn in unsorted_source_pfns:
                        if order_lfn == lfn:
                            sorted_source_pfns.append(pfn)
                            break

            for chunk in chunks(dst_lfns, 10):
                unsorted_dest_pfns = [[k[1], str(x)] for k, x in phedex.getPFN(toTrans[0][4], chunk).items()]
                for order_lfn in chunk:
                    for lfn, pfn in unsorted_dest_pfns:
                        if order_lfn == lfn:
                            sorted_dest_pfns.append(pfn)
                            break
        except Exception as ex:
            logging.error("Failed to map lfns to pfns: %s", ex)
            mark_failed(ids, ["Failed to map lfn to pfn: " + str(ex) for _ in ids])

        source_pfns = sorted_source_pfns
        dest_pfns = sorted_dest_pfns

        tx_from_source = [[x[0], x[1], x[2], source, username, taskname] for x in zip(source_pfns, dest_pfns, ids)] 

        for files in chunks(tx_from_source, 200):
            thread = submit_thread(threadLock, logging, context, files, source, jobids, to_update)
            thread.start()
            threads.append(thread)

    for t in threads:
        t.join()

    for fileDoc in to_update:
        _ = oracleDB.post('/filetransfers',
                          data=encodeRequest(fileDoc))
        logging.info("Marked submitted %s files", fileDoc['list_of_ids'])

    return jobids


def perform_transfers(inputFile, lastLine, _lastFile, context, phedex):
    """
    get transfers and update last read line number
    :param inputFile:
    :param lastLine:
    :return:
    """

    #threadLock = threading.Lock()
    #threads = []
    transfers = []
    logging.info("starting from line: %s", lastLine)

    with open(inputFile) as _list:
        for _data in _list.readlines()[lastLine:]:
            try:
                lastLine += 1
                doc = json.loads(_data)
            except:
                continue
            transfers.append([doc["source_lfn"],
                              doc["destination_lfn"],
                              doc["id"],
                              doc["source"],
                              doc["destination"],
                              doc["username"],
                              doc["taskname"]])

        jobids = []
        if len(transfers) > 0:
            jobids = submit(phedex, context, transfers)

            for jobid in jobids:
                logging.info("Monitor link: https://fts3.cern.ch:8449/fts3/ftsmon/#/job/"+jobid)

            # TODO: send to dashboard

        _lastFile.write(str(lastLine))

    return transfers, jobids


def state_manager(fts):
    """

    """
    threadLock = threading.Lock()
    threads = []
    jobs_done = []
    jobs_ongoing = []
    failed_id = {}
    failed_reasons = {}
    done_id = {}

    # TODO: puo esser utile togliere questo file? mmm forse no

    if os.path.exists('task_process/transfers/fts_jobids.txt'):
        with open("task_process/transfers/fts_jobids.txt", "r") as _jobids:
            lines = _jobids.readlines()
            for line in lines:
                if line:
                    jobid = line.split('\n')[0]
                if jobid:
                    thread = check_states_thread(threadLock, logging, fts, jobid, jobs_ongoing, done_id, failed_id, failed_reasons)
                    thread.start()
                    threads.append(thread)
            _jobids.close()

        for t in threads:
            t.join()

        try:
            for jobID, _ in done_id.iteritems():
                logging.info('Marking job %s files done and %s files failed for job %s', len(done_id[jobID]), len(failed_id[jobID]), jobID)

                if len(done_id[jobID]) > 0:
                    doneReady = mark_transferred(done_id[jobID])
                else:
                    doneReady = 0
                if len(failed_id[jobID]) > 0:
                    failedReady = mark_failed(failed_id[jobID], failed_reasons[jobID])
                else:
                    failedReady = 0

                if doneReady == 0 and failedReady == 0:
                    jobs_done.append(jobID)
                    jobs_ongoing.remove(jobID)
                else:
                    jobs_ongoing.append(jobID)
        except Exception:
            logging.exception('Failed to update states')
    else:
        logging.warning('No FTS job ID to monitor yet')

    with open("task_process/transfers/fts_jobids_new.txt", "w+") as _jobids:
        for line in jobs_ongoing:
            logging.info("Writing: %s", line)
            _jobids.write(line+"\n")

    os.rename("task_process/transfers/fts_jobids_new.txt", "task_process/transfers/fts_jobids.txt")

    return jobs_ongoing


def submission_manager(phedex, context):
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
        _, jobids = perform_transfers("task_process/transfers.txt", last_line, _last, context, phedex)
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
    - create fts REST HTTPRequest
    - delegate user proxy to fts if needed
    - check for fts jobs to monitor and update states in oracle
    - get last line from last_transfer.txt
    - gather list of file to transfers
        + group by source
        + submit ftsjob and save fts jobid
        + update info in oracle
    - append new fts job ids to fts_jobids.txt
    """

    # TODO: pass by configuration
    fts = HTTPRequests('fts3.cern.ch:8446/',
                       proxy,
                       proxy)

    context = fts3.Context('https://fts3.cern.ch:8446', proxy, proxy, verify=True)
    logging.debug("Delegating proxy: "+fts3.delegate(context, lifetime=timedelta(hours=48), force=False))

    try:
        phedex = PhEDEx(responseType='xml',
                        httpDict={'key': proxy, 'cert': proxy, 'pycurl':True})
    except Exception as e:
        logging.exception('PhEDEx exception: %s', e)
        return

    jobs_ongoing = state_manager(fts)
    new_jobs = submission_manager(phedex, context)

    logging.debug("Transfer jobs ongoing: %s, %s ", jobs_ongoing, new_jobs)

    return


if __name__ == "__main__":
    try:
        algorithm()
    except Exception:
        logging.exception("error during main loop")
    logging.debug("transfer_inject.py exiting")

