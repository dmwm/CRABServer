#!/usr/bin/python
"""
Script algorithm
    - 
"""
from __future__ import absolute_import, division, print_function
import json
import logging
import os

from rucio.client.client import Client

from RESTInteractions import CRABRest

CRABSERVER = None

REPLICAS_CHUNK_SIZE = 100

RUCIO_SCOPE = "user."
RUCIO_CLIENT = Client()
RUCIO_CLIENT_CONFIGURED = False

CURRENT_DATASETS = None

PUBLISHNAME = None

# TODO: try
with open("task_process/transfers/last_transfer.txt", "r") as _last:
    LAST_LINE = _last.readline

# TODO: try
with open("task_process/transfers/last_transfer_direct.txt", "r") as _last:
    LAST_LINE_DIRECT = _last.readline


if not os.path.exists('task_process/transfers'):
    os.makedirs('task_process/transfers')

logging.basicConfig(
    filename='task_process/transfers/transfer_inject.log',
    level=logging.DEBUG,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)

def init_crabrest_client():
    try:
       if os.path.exists('task_process/RestInfoForFileTransfers.json'):
        with open('task_process/RestInfoForFileTransfers.json') as fp:
            restInfo = json.load(fp)
            proxy = os.getcwd() + "/" + restInfo['proxyfile']
            #rest_filetransfers = restInfo['host'] + '/crabserver/' + restInfo['dbInstance']
            os.environ["X509_USER_PROXY"] = proxy

    # If there are no user proxy yet, just wait for the first pj of the task to finish
    if not proxy:
        logging.info('No proxy available yet - waiting for first post-job')
        return None

    try:
        CRABSERVER = CRABRest(restInfo['host'], localcert=proxy, localkey=proxy,
                              userAgent='CRABSchedd')
        CRABSERVER.setDbInstance(restInfo['dbInstamce'])
    except Exception:
        logging.exception("Failed to set connection to crabserver")
        return

def init_rucio_client ():
    """
    Initiate the Rucio client if not already set:
        - check if RUCIO_CLIENT is already initialized
            - if not try to instantiate now
                - get username from ask_process/transfers.txt
                - get the proxy location
                - instatiate client
    """
        logging.info("Checking if there are any files in task_process/transfers.txt")
        try:
            with open("task_process/transfers.txt") as _list:
                doc = json.loads(_list.readlines()[0])
                user = doc['username']
        except Exception as ex:
            logging.exception("task_process/transfers.txt does not exist. Probably no completed jobs in the task yet.", ex)    
            return

        # TODO: logging and try
        logging.info("Checking if task_process/RestInfoForFileTransfers.json")
        try:
            if os.path.exists('task_process/RestInfoForFileTransfers.json'):
                with open('task_process/RestInfoForFileTransfers.json') as fp:
                    restInfo = json.load(fp)
                    proxy = os.getcwd() + "/" + restInfo['proxyfile']
                    os.environ["X509_USER_PROXY"] = proxy
        except Exception as ex:
            logging.exception("task_process/RestInfoForFileTransfers.json can't be read. Probably no completed jobs in the task yet.", ex)    
            return

        if not RUCIO_CLIENT_CONFIGURED:
            logging.warning("Rucio client not configured, I'm initiating it right now")
            logging.info("I'm going to configure Rucio client for %s", user)

            RUCIO_SCOPE += user
            logging.debug("Account %s scope: %s with creds %s" % (user, RUCIO_SCOPE, proxy))
            try:
                RUCIO_CLIENT = Client(account=user, auth_type="x509_proxy")
                RUCIO_CLIENT_CONFIGURED = True
            except Exception as ex:
                logging.error("Something went wrong when initializing rucio client: %s", ex)
                return

def generate_container_name():
    # generate DBS name

    PUBLISHNAME = 

    return    

def create_container():
    """
    - one rule with lifetime (1m) for replicas on TEMP RSE
    - one rule with no lifetime (1m) for replicas on DEST RSE
    """

    return

def create_datasets():

    CURRENT_DATASETS = dataset

    return

def create_transfer_dicts(input_dict={}):

    xdict = {
        "source_lfn": input_dict["source_lfn"],
        "destination_lfn": input_dict["destination_lfn"],
        "id": input_dict["id"],
        "source": input_dict["source"],
        "destination": input_dict["destination"],
        "checksum": input_dict["checksums"]["adler32"].rjust(8,'0'),
        "filesize": input_dict["filesize"],
        "publishname":     PUBLISHNAME

    }

    return xdict

def register_replicas(input_replicas=[]):
    # TODO: collect by SOURCE --> one dict per source
    # TODO: exclude direct files!
    replicas = []
    for chunk in chunks(input_replicas, REPLICAS_CHUNK_SIZE):
        for data in chunk:
            # add_replicas(rse, files, ignore_availability=True)
            #     Bulk add file replicas to a RSE.
            #     Parameters
            #             rse – the RSE name.
            #             files – The list of files. This is a list of DIDs like : [{‘scope’: <scope1>, ‘name’: <name1>}, {‘scope’: <scope2>, ‘name’: <name2>}, …]
            #             ignore_availability – Ignore the RSE blacklisting.
            #     Returns
            #             True if files were created successfully.

            replicas.apped(
                {

                }
            )

    # add to CURRENT_DATASET

    # TODO: close CURRENT_DATASET if needed
    # -if everything full create new one
    return

def register_direct_files():

    DIRECT_FILES = []

    return

def monitor_locks_status():

    return

def main():
    """
    Script algorithm
    - check if rucio client is good
        - that means that at least a file report from post-job is there
    - register Rucio datasets and containers + container rule for this task
        - one rule with lifetime (1m) for replicas on TEMP RSE
        - one rule with no lifetime (1m) for replicas on DEST RSE
    - Start from the last file processed (stored on last_transfer.txt)
    - gather list of file to transfers
        + register temp files (no direct stageout should be used for RUCIO)
        + fill the first dataset not closed
        + update info in oracle
    - monitor the Rucio replica locks by the datasets
        + update info in oracle accordingly
    """
    transfers_dict = []

    with open("task_process/transfers.txt") as _list:
        for _data in _list.readlines()[int(LAST_LINE):]:
            file_to_submit = []
            try:
                LAST_LINE += 1
                doc = json.loads(_data)
                transfers_dict.append(create_transfer_dict(dict=doc))

        direct_files = []
        if os.path.exists('task_process/transfers/registered_direct_files.txt'):
            with open("task_process/transfers/registered_direct_files.txt", "r") as list_file:
                direct_files = [x.split('\n')[0] for x in list_file.readlines()]

        generate_container_name()
        create_container()
        create_datasets()

        register_direct_files(direct_files)
        register_replicas(transfers_dict)

        monitor_locks_status()

    return


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
    #if os.path.exists('task_process/rest_filetransfers.txt'):
    #    with open("task_process/rest_filetransfers.txt", "r") as _rest:
    #        rest_filetransfers = _rest.readline().split('\n')[0]
    #        proxy = os.getcwd() + "/" + _rest.readline()
    #        logging.info("Proxy: %s", proxy)
    #        os.environ["X509_USER_PROXY"] = proxy
    if os.path.exists('task_process/RestInfoForFileTransfers.json'):
        with open('task_process/RestInfoForFileTransfers.json') as fp:
            restInfo = json.load(fp)
            proxy = os.getcwd() + "/" + restInfo['proxyfile']
            #rest_filetransfers = restInfo['host'] + '/crabserver/' + restInfo['dbInstance']
            os.environ["X509_USER_PROXY"] = proxy

    # If there are no user proxy yet, just wait for the first pj of the task to finish
    if not proxy:
        logging.info('No proxy available yet - waiting for first post-job')
        return None

    try:
        crabserver = CRABRest(restInfo['host'], localcert=proxy, localkey=proxy,
                              userAgent='CRABSchedd')
        crabserver.setDbInstance(restInfo['dbInstamce'])
    except Exception:
        logging.exception("Failed to set connection to crabserver")
        return

    logging.info("starting from line: %s", lastLine)

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
        taskname = doc["publishname"].replace('-00000000000000000000000000000000', '/rucio/USER#000000')

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
                    taskname = doc["publishname"].replace('-00000000000000000000000000000000', '/rucio/USER#000000')
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
                    'destination': destination,
                    'proxy': proxy,
                    'crabserver': crabserver}
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

    # Get proxy for talking to Rucio
    proxy = None
#    if os.path.exists('task_process/rest_filetransfers.txt'):
#        with open("task_process/rest_filetransfers.txt", "r") as _rest:
#            proxy = os.getcwd() + "/" + _rest.readline()
#            logging.info("Proxy: %s", proxy)
#            os.environ["X509_USER_PROXY"] = proxy

    if os.path.exists('task_process/RestInfoForFileTransfers.json'):
        with open('task_process/RestInfoForFileTransfers.json') as fp:
            restInfo = json.load(fp)
            proxy = os.getcwd() + "/" + restInfo['proxyfile']
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

    logging.info("Perform transfers: %s", r)
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




if __name__ == "__main__":
    logging.info(">>>> New process started  <<<<")
    try:
        main()
    except Exception:
        logging.exception("error during main loop")
    logging.debug("transfers.py exiting")
