#!/usr/bin/python
"""
Script algorithm
    - check for file reports from post-job on a local file
    - if present register Rucio dataset and rule for this task
    - Start from the last file processed (stored on last_transfer.txt)
    - gather list of file to transfers
        + register temp and direct staged files
        + update info in oracle
    - monitor the Rucio replica locks for the datasets
        + update info in oracle accordingly
"""
from __future__ import absolute_import, division, print_function
import json
import logging
import os

from TransferInterface.RegisterFiles import submit
from TransferInterface.MonitorTransfers import monitor


if not os.path.exists('task_process/transfers'):
    os.makedirs('task_process/transfers')

logging.basicConfig(
    filename='task_process/transfers/transfer_inject.log',
    level=logging.DEBUG,
    format='%(asctime)s[%(relativeCreated)6d]%(threadName)s: %(message)s'
)
  

def perform_transfers(inputFile, lastLine, direct=False):
    """
    get transfers submitted and save the last read line of the transfer list

    :param inputFile: file name containing post job files ready
    :type inputFile: str
    :param lastLine: last line processed
    :type lastLine: int
    :param direct: job output stored on temp or directly, defaults to False
    :param direct: bool, optional
    :return: (username,taskname) or None in case of critical error
    :rtype: tuple or None
    """

    if not os.path.exists(inputFile):
        return None, None

    # Get proxy and rest endpoint information
    proxy = None
    if os.path.exists('task_process/rest_filetransfers.txt'):
        with open("task_process/rest_filetransfers.txt", "r") as _rest:
            rest_filetransfers = _rest.readline().split('\n')[0]
            proxy = os.getcwd() + "/" + _rest.readline()
            logging.info("Proxy: %s", proxy)
            os.environ["X509_USER_PROXY"] = proxy

    # If there are no user proxy yet, just wait for the first pj of the task to finish
    if not proxy:
        logging.info('No proxy available yet - waiting for first post-job')
        return None

    logging.info("starting from line: %s" % lastLine)

    # define ntuples and column names
    # TODO: make use of dict instead of this
    file_to_submit = []
    to_submit_columns = ["source_lfn",
                         "destination_lfn",
                         "id",
                         "source",
                         "destination",
                         "checksums",
                         "filesize",
                         "publishname"
                         ]
    transfers = []
    user = None
    taskname = None
    destination = None

    # get username and taskname form input file list
    with open(inputFile) as _list:
        doc = json.loads(_list.readlines()[0])
        user = doc['username']

    # Save needed info in ordered lists
    with open(inputFile) as _list:
        for _data in _list.readlines()[lastLine:]:
            file_to_submit = []
            try:
                lastLine += 1
                doc = json.loads(_data)
            except Exception:
                continue
            for column in to_submit_columns:
                # Save everything other than checksums and publishnames
                # They will be managed below
                if column not in ['checksums', 'publishname']:
                    file_to_submit.append(doc[column])
                # Change publishname for task with publication disabled 
                # as discussed in https://github.com/dmwm/CRABServer/pull/6038#issuecomment-618654580
                if column == "publishname":
                    taskname = doc["publishname"].replace('-00000000000000000000000000000000', '/rucio/USER')
                    file_to_submit.append(taskname)
                # Save adler checksum in a form accepted by Rucio
                if column == "checksums":
                    file_to_submit.append(doc["checksums"]["adler32"].rjust(8,'0'))
            transfers.append(file_to_submit)
            destination = doc["destination"]

    # Pass collected info to submit function
    if len(transfers) > 0:
        # Store general job metadata
        job_data = {'taskname': taskname,
                    'username': user,
                    'destination': destination+'_Test',
                    'proxy': proxy,
                    'rest': rest_filetransfers}
        # Split the processing for the directly staged files
        if not direct:
            try:
                # Start the submission process  that will be managed
                # in src/python/TransferInterface/RegisterFiles.py
                success = submit((transfers, to_submit_columns), job_data, logging)
            except Exception:
                logging.exception('Submission process failed.')

            # if succeeded go on and update the last processed file
            # otherwise retry at the next round of task process
            if success:
                # update last read line
                with open("task_process/transfers/last_transfer_new.txt", "w+") as _last:
                    _last.write(str(lastLine))
                os.rename("task_process/transfers/last_transfer_new.txt", "task_process/transfers/last_transfer.txt")

        elif direct:

            # In case of direct stageout do the same as above but with flag direct=True
            try:
                success = submit((transfers, to_submit_columns), job_data, logging, direct=True)
            except Exception:
                logging.exception('Registering direct stage files failed.')

            if success:
                # update last read line
                with open("task_process/transfers/last_transfer_direct_new.txt", "w+") as _last:
                    _last.write(str(lastLine))
                os.rename("task_process/transfers/last_transfer_direct_new.txt", "task_process/transfers/last_transfer_direct.txt")

    return user, taskname


def monitor_manager(user, taskname):
    """Monitor Rucio replica locks for user task

    :param user: user HN name
    :type user: str
    :param taskname: name of the task
    :type taskname: str
    :return: exit code 0 or None
    :rtype: int
    """

    # Get proxy and rest endpoint information
    proxy = None
    if os.path.exists('task_process/rest_filetransfers.txt'):
        with open("task_process/rest_filetransfers.txt", "r") as _rest:
            _rest.readline().split('\n')[0]
            proxy = os.getcwd() + "/" + _rest.readline()
            logging.info("Proxy: %s", proxy)
            os.environ["X509_USER_PROXY"] = proxy

    # Same as submission process
    # If no proxy present yet, wait for first postjob to finish
    if not proxy:
        logging.info('No proxy available yet - waiting for first post-job')
        return None

    # Start the monitoring process, managed in:
    # src/python/TransferInterface/MonitorTransfers.py
    try:
        monitor(user, taskname, logging)
    except Exception:
        logging.exception('Monitor process failed.')

    return 0


def submission_manager():
    """
    Wrapper for Rucio submission algorithm. 

    :return: results of perform_transfers function
    :rtype: tuple or None
    """
    last_line = 0
    if os.path.exists('task_process/transfers/last_transfer.txt'):
        with open("task_process/transfers/last_transfer.txt", "r") as _last:
            read = _last.readline()
            last_line = int(read)
            logging.info("last line is: %s", last_line)

    # TODO: check if everything is aligned here with the FTS script
    # expecially the safety of the locks

    # Perform transfers of files that are not directly staged 
    r = perform_transfers("task_process/transfers.txt",
                          last_line)

    # Manage the direct stageout cases
    if os.path.exists('task_process/transfers/last_transfer_direct.txt'):
        with open("task_process/transfers/last_transfer_direct.txt", "r") as _last:
            read = _last.readline()
            last_line = int(read)
            logging.info("last line is: %s", last_line)
    else:
        with open("task_process/transfers/last_transfer_direct.txt", "w+") as _last:
            last_line = 0 
            _last.write(str(last_line))
            logging.info("last line direct is: %s", last_line)

    if os.path.exists('task_process/transfers_direct.txt'):
        perform_transfers("task_process/transfers_direct.txt",
                          last_line, direct=True)

    return r


def algorithm():
    """
    Script algorithm
    - check for file reports from post-job on a local file
    - if present register Rucio dataset and rule for this task
    - Start from the last file processed (stored on last_transfer.txt)
    - gather list of file to transfers
        + register temp and direct staged files
        + update info in oracle
    - monitor the Rucio replica locks for the datasets
        + update info in oracle accordingly
    """

    user = None
    try:
        user, taskname = submission_manager()
    except Exception:
        logging.exception('Submission proccess failed.')

    if not user:
        logging.info('Nothing to monitor yet.')
        return
    try:
        monitor_manager(user, taskname)
    except Exception:
        logging.exception('Monitor proccess failed.')

    return


if __name__ == "__main__":
    logging.info(">>>> New process started  <<<<")
    try:
        algorithm()
    except Exception:
        logging.exception("error during main loop")
    logging.debug("transfers.py exiting")
