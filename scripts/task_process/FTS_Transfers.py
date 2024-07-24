#!/usr/bin/python
# pylint: disable=line-too-long, broad-except, invalid-name
"""
for lack of a better place, I record here the current situation with
FTS python easy binding documentation, which is needed to understand
the code used in this file.
Reference: https://cern.service-now.com/service-portal?id=ticket&table=incident&n=INC3888594

Message from Mihai Patrascoiu:
Hello Stefano,
 
The ".md" file is there. The HTML file is there as well ( https://fts3-docs.web.cern.ch/fts3-docs/fts-rest/docs/easy/submit.html ).
But there's no direct link to them in the navigation menu on the left.
 
We have our main documentation repository ( https://gitlab.cern.ch/fts/documentation ), but some parts, such as the easy-bindings, come from the old FTS-REST project ( https://gitlab.cern.ch/fts/fts-rest/-/tree/develop/docs ). It's not ideal, but it worked and we left it untouched. (this should explain why the easy-bindings are not in the "documentation" repo)
 
Unfortunately, the documentation CI relies on old technologies. The building still happens on CC7 image + Gitbook (deprecated). At some point in time, it stopped successfully rendering the documentation files that came from the old FTS-REST project. We just left it like that.
 
When we can redo the whole documentation, the foreign parts scattered in the old FTS-REST project will be moved to the main "documentation" repository.
 
You can always find a direct link to our documentation from the main FTS page:
https://fts.web.cern.ch/fts/ > Docs (in the top navigation bar)
 
Below is the listing of the easy-bindings directory.
You can access those files via the web page if you know the exact location.
```
$ tree < https://fts3-docs.web.cern.ch/fts3-docs/fts-rest/docs/easy/ >
├── examples
│   ├── cancel.py
│   ├── listing.py
│   ├── snapshot.py
│   ├── status.py
│   ├── submit.py
│   └── whoami.py
├── index.html
├── submit.html
└── submit.md
```
 
I hope this answers your questions. For the foreseeable future, the current situation will have to do.
 
Cheers,
Mihai
"""
import json
import logging
import os
import subprocess
from datetime import datetime, timedelta
from http.client import HTTPException

import fts3.rest.client.easy as fts3

from rucio.client import Client as rucioclient
from rucio.rse.rsemanager import find_matching_scheme

from RESTInteractions import CRABRest
from ServerUtilities import encodeRequest

FTS_ENDPOINT = "https://fts3-cms.cern.ch:8446/"
FTS_MONITORING = "https://fts3-cms.cern.ch:8449/"

if not os.path.exists('task_process/transfers'):
    os.makedirs('task_process/transfers')

logging.basicConfig(
    filename='task_process/transfers/transfer_inject.log',
    level=logging.INFO,
    format='%(asctime)s: %(message)s'
)

if os.path.exists('task_process/RestInfoForFileTransfers.json'):
    with open('task_process/RestInfoForFileTransfers.json', encoding='utf-8') as fp:
        restInfo = json.load(fp)
        proxy = os.getcwd() + "/" + str(restInfo['proxyfile'])  # make sure no to unicode to FTS clients
        # rest_filetransfers = restInfo['host'] + '/crabserver/' + restInfo['dbInstance']
        os.environ["X509_USER_PROXY"] = proxy

# if os.path.exists('task_process/rest_filetransfers.txt'):
#    with open("task_process/rest_filetransfers.txt", "r") as _rest:
#        rest_filetransfers = _rest.readline().split('\n')[0]
#        proxy = os.getcwd() + "/" + _rest.readline()
#        print("Proxy: %s" % proxy)

asoworker = 'schedd'
ftsReuse = os.path.exists('USE_FTS_REUSE')


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

        data = {}
        data['asoworker'] = asoworker
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["DONE" for _ in ids]

        crabserver.post('filetransfers', data=encodeRequest(data))
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
        data = {}
        data['asoworker'] = asoworker
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["FAILED" for _ in ids]
        reasons = [r.replace(',', '.') for r in failures_reasons]  # commas would confuse REST
        data['list_of_failure_reason'] = reasons
        data['list_of_retry_value'] = [0 for _ in ids]

        crabserver.post('filetransfers', data=encodeRequest(data))
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
        timeout = f"{len(pfns) * 3}m"  # default is 3minutes per file to be removed
    command = f"env -i X509_USER_PROXY={proxy} timeout {timeout} gfal-rm -v -t 180 {pfns} >> {logFile} 2>&1 &"
    logging.debug("Running remove command %s", command)
    subprocess.call(command, shell=True)

    return


def check_FTSJob(logger, ftsContext, jobid, jobsEnded, jobs_ongoing, done_id, failed_id, failed_reasons):
    """
    get transfers state per jobid

    INPUT PARAMS
    :param logger: a logging object
    :param ftsContext:
    :param jobid:
    OUTPUT PARAMS
    :prarm jobsEnded:
    :param jobs_ongoing:
    :param done_id:
    :param failed_id:
    :param failed_reasons:
    - check if the fts job is in final state (FINISHED, FINISHEDDIRTY, CANCELED, FAILED)
    - get file transfers states and get corresponding oracle ID from FTS file metadata
    - update states on oracle
    """

    logger.info(f"Getting state of job {jobid}")

    jobs_ongoing.append(jobid)
    file_statuses = {}

    try:
        status = fts3.get_job_status(ftsContext, jobid, list_files=False)
        if status["job_state"] in ['ACTIVE', 'FINISHED', 'FINISHEDDIRTY', "FAILED", "CANCELED"]:
            file_statuses = fts3.get_job_status(ftsContext, jobid, list_files=True)['files']
    except HTTPException as hte:
        logger.exception(f"failed to retrieve status for {jobid}")
        logger.exception(f"httpExeption headers {hte.headers}")
        if hte.status == 404:
            logger.exception(f"{jobid} not found in FTS3 DB")
            jobs_ongoing.remove(jobid)
        return
    except Exception:
        logger.exception(f"failed to retrieve status for {jobid}")
        return

    logger.info("State of job %s: %s", jobid, status["job_state"])

    if status["job_state"] in ('FINISHED', 'FINISHEDDIRTY', "FAILED", "CANCELED"):
        jobsEnded.append(jobid)
    if file_statuses:
        done_id[jobid] = []
        failed_id[jobid] = []
        failed_reasons[jobid] = []
        files_to_remove = []
        fileIds_to_remove = []

        # get the job content from local file
        jobContentFileName = 'task_process/transfers/' + jobid + '.json'
        with open(jobContentFileName, 'r', encoding='utf-8') as fh:
            fileIds = json.load(fh)

        for file_status in file_statuses:
            _id = file_status['file_metadata']['oracleId']
            if _id not in fileIds:
                # this file xfer has been handled already in a previous iteration
                # nothing to do
                continue

            tx_state = file_status['file_state']

            # xfers have only 3 terminal states: FINISHED, FAILED, and CANCELED see
            # https://fts3-docs.web.cern.ch/fts3-docs/docs/state_machine.html
            if tx_state == 'FINISHED':
                logger.info('file XFER OK will remove %s', file_status['source_surl'])
                done_id[jobid].append(_id)
                files_to_remove.append(file_status['source_surl'])
                fileIds_to_remove.append(_id)
            elif tx_state in ('FAILED', 'CANCELED'):
                logger.info('file XFER FAIL will remove %s', file_status['source_surl'])
                failed_id[jobid].append(_id)
                if file_status['reason']:
                    logger.info('Failure reason: ' + file_status['reason'])
                    failed_reasons[jobid].append(file_status['reason'])
                else:
                    logger.exception('Failure reason not found')
                    failed_reasons[jobid].append('unable to get failure reason')
                files_to_remove.append(file_status['source_surl'])
                fileIds_to_remove.append(_id)
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
                    failed_id[jobid].append(_id)
                    logger.info('Failure reason: stuck inside FTS')
                    failed_reasons[jobid].append(file_status['reason'])
        if files_to_remove:
            list_of_surls = ''   # gfal commands take list of SURL as a list of blank-separated strings
            for f in files_to_remove:
                list_of_surls += str(f) + ' '  # convert JSON u'srm://....' to plain srm://...
            removeLogFile = './task_process/transfers/remove_files.log'
            msg = str(datetime.now()) + f": Will remove: {list_of_surls}"
            with open(removeLogFile, 'a', encoding='utf-8') as removeLog:
                removeLog.write(msg)
            remove_files_in_bkg(list_of_surls, removeLogFile)
            # remove those file Id's from the list and update the json disk file
            fileIds = list(set(fileIds) - set(fileIds_to_remove))
            jobContentTmp = jobContentFileName + '.tmp'
            with open(jobContentTmp, 'w', encoding='utf-8') as fh:
                json.dump(fileIds, fh)
            os.rename(jobContentTmp, jobContentFileName)


def submitToFTS(logger, ftsContext, files, jobids, toUpdate):
    """
    actual FTS job submission
    INPUT PARAMS:
    :param logger: logging object
    :param ftsContext: FTS context
    :param files: [
           [source_pfn,
            dest_pfn,
            file oracle id,
            source site,
            dest RSE,
            username,
            taskname,
            file size,
            checksum],
           ...]
    OUTPUT PARAMS:
    :param jobids: collect the list of job ids when submitted
    :param toUpdate: list of oracle ids to update

    RETURNS: the submitted jobid (a string)
    """

    transfers = []
    for lfn in files:
        src_rse = lfn[3] + '_Temp'
        dst_rse = lfn[4]
        transfers.append(fts3.new_transfer(lfn[0],
                                           lfn[1],
                                           filesize=lfn[7],
                                           metadata={'oracleId': lfn[2], 'src_rse': src_rse, 'dst_rse': dst_rse}
                                           )
                         )
    logger.info(f"Submitting {len(files)} transfers to FTS server")
    for lfn in files:
        logger.info("%s %s %s %s", lfn[0], lfn[1], lfn[6], {'oracleId': lfn[2]})

    # Submit fts job
    job = fts3.new_job(transfers,
                       overwrite=True,
                       verify_checksum=True,
                       metadata={"issuer": "ASO",
                                 "userDN": files[0][5],
                                 "taskname": files[0][6]},
                       copy_pin_lifetime=-1,
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

    jobid = fts3.submit(ftsContext, job)

    jobids.append(jobid)

    fileDoc = {}
    fileDoc['asoworker'] = asoworker
    fileDoc['subresource'] = 'updateTransfers'
    fileDoc['list_of_ids'] = [x[2] for x in files]
    fileDoc['list_of_transfer_state'] = ["SUBMITTED" for _ in files]
    fileDoc['list_of_fts_instance'] = [FTS_ENDPOINT for _ in files]
    fileDoc['list_of_fts_id'] = [jobid for _ in files]

    logger.info(f"Will mark as submitted {len(fileDoc['list_of_ids'])} files")
    toUpdate.append(fileDoc)

    return jobid


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
    jobids = []
    to_update = []

    # some things are the same for all files, pick them from the first one
    username = toTrans[0][5]
    scope = "user." + username
    taskname = toTrans[0][6]
    dst_rse = toTrans[0][4]
    a_dst_lfn = toTrans[0][1]
    a_src_lfn = toTrans[0][0]

    # this is not old ASO anymore. All files here have same destination
    # and LFN's and PFN's can only differ from each other in the directory counter and actual file name
    # no reason to lookup lfn2pfn by individual file
    # remember that src and dst LFN's are in general different (/store/user vs. /store/temp/user)
    # so we assume: (P/L)FN = (P/L)FN_PREFIX/FILEID
    # where: FILEID = xxxx/filename.filetype and xxxx is a 0000, 0001, 0002 etc. for output or
    # xxxx is 0000/log, 00001/log etc. for log if user selected TransferLogs=True i.e. LFN's are like
    # /store/temp/user/.../0000/output_1.root or /store/temp/user/.../0000/log/cmsRun_1.log.tar.gz

    if a_dst_lfn.split("/")[-2] == 'log':
        dst_lfn_prefix = '/'.join(a_dst_lfn.split("/")[:-3])
    else:
        dst_lfn_prefix = '/'.join(a_dst_lfn.split("/")[:-2])
    dst_did = scope + ':' + dst_lfn_prefix + '/0000/file.root'

    if a_src_lfn.split("/")[-2] == 'log':
        src_lfn_prefix = '/'.join(a_src_lfn.split("/")[:-3])
    else:
        src_lfn_prefix = '/'.join(a_src_lfn.split("/")[:-2])
    src_did = scope + ':' + src_lfn_prefix + '/0000/file.root'

    sourceSites = list(set(x[3] for x in toTrans))

    for source in sourceSites:
        logging.info("Processing transfers from: %s", source)

        # find all files for this source
        toTransFromThisSource = [x for x in toTrans if x[3] == source]
        # find matching transfer protocol and proper PFN prefixes
        src_rse = source
        try:
            dst_scheme, src_scheme, _, _ = find_matching_scheme(
                {"protocols": rucioClient.get_protocols(dst_rse)},
                {"protocols": rucioClient.get_protocols(src_rse)},
                "third_party_copy_read",
                "third_party_copy_write",
            )
            dst_pfn_template = rucioClient.lfns2pfns(dst_rse, [dst_did],
                                                     operation="third_party_copy_write", scheme=dst_scheme)
            src_pfn_template = rucioClient.lfns2pfns(src_rse, [src_did],
                                                     operation="third_party_copy_read", scheme=src_scheme)
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
            jobid = f[2]
            size = f[7]
            checksums = f[8]
            # src_lfn = f[0]  # currently unused
            dst_lfn = f[1]
            if dst_lfn.split("/")[-2] == 'log':
                fileid = '/'.join(dst_lfn.split("/")[-3:])
            else:
                fileid = '/'.join(dst_lfn.split("/")[-2:])
            src_pfn = src_pfn_prefix + '/' + fileid
            dst_pfn = dst_pfn_prefix + '/' + fileid

            xfer = [src_pfn, dst_pfn, jobid, source, dst_rse, username, taskname, size, checksums['adler32'].rjust(8, '0')]
            tx_from_source.append(xfer)

        xfersPerFTSJob = 50 if ftsReuse else 200
        for files in chunks(tx_from_source, xfersPerFTSJob):
            ftsJobId = submitToFTS(logging, ftsContext, files, jobids, to_update)
            # save oracleIds of files in this job in a local file
            jobContentFileName = 'task_process/transfers/' + ftsJobId + '.json'
            with open(jobContentFileName, 'w', encoding='utf-8') as fh:
                json.dump([f[2] for f in files], fh)

    for fileDoc in to_update:
        _ = crabserver.post('filetransfers', data=encodeRequest(fileDoc))
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

    # read one doc from each line in input file
    # doc is a dictionary :
    #{
    # "username": "belforte",
    # "taskname": "240515_151607:belforte_crab_20240515_171602",
    # "start_time": 1715795671,
    # "destination": "T2_CH_CERN",
    # "destination_lfn": "/store/user/belforte/[...].root",
    # "source": "T2_UK_SGrid_RALPP",
    # "source_lfn": "/store/temp/user/belforte.4bba3c2d14d54a938291c268ff4eba8b575b197f/[...].root",
    # "filesize": 166575,
    # "publish": 0,
    # "transfer_state": "NEW",
    # "publication_state": "NOT_REQUIRED",
    # "job_id": "2",
    # "job_retry_count": 0,
    # "type": "output",
    # "publishname": "autotest-1715786162-00000000000000000000000000000000",
    # "checksums": {
    #   "adler32": "1c0775fa",
    #   "cksum": "1309875024"
    # }

    with open(inputFile, encoding='utf-8') as _list:
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
    jobs_done = []
    jobs_ongoing = []
    jobsEnded = []
    failed_id = {}
    failed_reasons = {}
    done_id = {}

    # TODO: puo esser utile togliere questo file? mmm forse no

    if os.path.exists('task_process/transfers/fts_jobids.txt'):
        with open("task_process/transfers/fts_jobids.txt", "r", encoding='utf-8') as _jobids:
            lines = _jobids.readlines()
            for line in list(set(lines)):
                if line:
                    jobid = line.split('\n')[0]
                if jobid:
                    check_FTSJob(logging, ftsContext, jobid, jobsEnded, jobs_ongoing, done_id, failed_id, failed_reasons)
            _jobids.close()

        # the loop above has filled:
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
        except Exception:
            logging.exception('Failed to update states')
    else:
        logging.warning('No FTS job ID to monitor yet')

    with open("task_process/transfers/fts_jobids_new.txt", "w+", encoding='utf-8') as _jobids:
        for line in jobs_ongoing:
            logging.info("Writing: %s", line)
            _jobids.write(line + "\n")

    os.rename("task_process/transfers/fts_jobids_new.txt", "task_process/transfers/fts_jobids.txt")

    return jobs_ongoing


def submission_manager(rucioClient, ftsContext, crabserver):
    """

    """
    last_line = 0
    if os.path.exists('task_process/transfers/last_transfer.txt'):
        with open("task_process/transfers/last_transfer.txt", "r", encoding='utf-8') as _last:
            read = _last.readline()
            last_line = int(read)
            logging.info("last line is: %s", last_line)
            _last.close()

    # TODO: if the following fails check not to leave a corrupted file
    with open("task_process/transfers/last_transfer_new.txt", "w+", encoding='utf-8') as _last:
        _, jobids = perform_transfers("task_process/transfers.txt", last_line, _last, ftsContext, rucioClient, crabserver)
        _last.close()
        os.rename("task_process/transfers/last_transfer_new.txt", "task_process/transfers/last_transfer.txt")

    with open("task_process/transfers/fts_jobids.txt", "a", encoding='utf-8') as _jobids:
        for job in jobids:
            _jobids.write(str(job) + "\n")
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

    with open("task_process/transfers.txt", encoding='utf-8') as _list:
        _data = _list.readlines()[0]
        try:
            doc = json.loads(_data)
            username = doc["username"]
            # taskname = doc["taskname"]  # currently unused
            # destination = doc["destination"]  # currently unused
        except Exception as ex:
            msg = f"Username gathering failed with\n{str(ex)}"
            logging.warning(msg)
            raise ex

    try:
        logging.info("Initializing Rucio client")
        os.environ["X509_USER_PROXY"] = proxy
        os.environ["X509_USER_PROXY"] = proxy
        rucioClient = rucioclient(account=username, auth_type='x509_proxy')
    except Exception as exc:
        msg = f"Rucio initialization failed with\n{str(ex)}"
        logging.warning(msg)
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
