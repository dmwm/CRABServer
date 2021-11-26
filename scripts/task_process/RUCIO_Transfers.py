#!/usr/bin/python
"""
Script algorithm
    - 
"""
from __future__ import absolute_import, division, print_function
import json
import logging
import os
import uuid

from rucio.client.client import Client

from RESTInteractions import CRABRest

CRABSERVER = None

DATASET_FILE_LIMIT = 100
REPLICAS_CHUNK_SIZE = 20

RUCIO_SCOPE = "user."
RUCIO_CLIENT = Client()
RUCIO_CLIENT_CONFIGURED = False

CURRENT_DATASET = None

PUBLISHNAME = None
DESTINATION = None

REPLICAS = []


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


def check_or_create_container():
    """
    - one rule with no lifetime for replicas on DEST RSE
    """

    # create container called PUBLISHNAME

    # TODO: try and explicit excption. go on only if alreadyExists error
    try:
        RUCIO_CLIENT.get_did(RUCIO_SCOPE, PUBLISHNAME)
    except:
        try:
            RUCIO_CLIENT.add_container(RUCIO_SCOPE, PUBLISHNAME)
            RUCIO_CLIENT.add_replication_rule(RUCIO_SCOPE+":"+PUBLISHNAME, copies=1, rse_expression=DESTINATION)
        except:
            raise

    return


def check_or_create_current_dataset(force_create=False):
    """Check if there are open datasets and then start from there
    """

    if force_create = False:
        # TODO: try
        # check if there are any open datasets
        datasets = RUCIO_CLIENT.list_content(RUCIO_SCOPE, PUBLISHNAME)
        # get open datasets
        # if more than one, close the most occupied
        closed_ds = []
        for d in datasets:
            # check if closed and append

        if len(closed_ds) == 0:
            # create a new dataset
            RUCIO_CLIENT.add_dataset(RUCIO_SCOPE, CURRENT_DATASET)

            # attach dataset to the container
            RUCIO_CLIENT.attach_dids(RUCIO_SCOPE, PUBLISHNAME, RUCIO_SCOPE+ ":" + CURRENT_DATASET) 
        elif len(closed_ds)  > 1:
            # close the most occupied and take the other as the current one
        elif len(closed_ds)  == 1:
            # set it as the current

    else:    
        CURRENT_DATASET = PUBLISHNAME+"#%s" % uuid.uuid4()
        # create a new dataset
        RUCIO_CLIENT.add_dataset(RUCIO_SCOPE, CURRENT_DATASET)

        # attach dataset to the container
        RUCIO_CLIENT.attach_dids(RUCIO_SCOPE, PUBLISHNAME, CURRENT_DATASET)

    return


def create_transfer_dict(input_dict={}):

    xdict = {
        "source_lfn": input_dict["source_lfn"],
        "destination_lfn": input_dict["destination_lfn"],
        "id": input_dict["id"],
        "source": input_dict["source"]+"_Temp",
        "destination": input_dict["destination"],
        "checksum": input_dict["checksums"]["adler32"].rjust(8,'0'),
        "filesize": input_dict["filesize"],
        "publishname":     PUBLISHNAME
    }

    return xdict


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    :param l: list to splitt in chunks
    :param n: chunk size
    :return: yield the next list chunk
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_pfns(rse, lfns):

    pfns = []

    for chunk in chunks(lfns, 10):
        # lfns2pfns: (rse, lfns, protocol_domain='ALL', operation=None, scheme=None)
        pfns += RUCIO_CLIENT.lfns2pfns(rse,  lfns)

    # returning {"rse":{"lfn":"pfn"}}

    map_dict = {}
    for lfn, pfn in zip(lfns, pfns):
        map_dict.update({lfn: pfn})
    pfn_map = {rse: map_dict}

    return pfn_map


def manage_replicas_for_remote_remote_direct(transfer_dicts, direct_lfns):
    """Generate a replica list starting from 2 subset, one for directly staged file and one for the files in temp RSEs

    """
    replicas = {}

    remote_xdicts = []
    direct_xdicts = []

    rses_direct = []
    rses_remote = []
    rse_and_lfns = []

    pfn_map = {}

    # first of all lets separate direct from remote dicts
    for xdict in transfer_dicts:
        if xdict["destination_lfn"] in direct_lfns:
            direct_xdicts.append(xdict)
            rses_direct.append(xdict["destination"])
        else:
            remote_xdicts.append(xdict)
            rses_remote.append(xdict["source"])
            rse_and_lfns.append([xdict["source"], xdict["source_lfn"]])

    rses_direct = list(dict.fromkeys(rses_direct))
    for rse in rses_direct:
        replicas.update({rse: []})

    rses_remote = list(dict.fromkeys(rses_remote))
    for rse in rses_remote:      
        replicas.update({rse: []})
        # collect info for bulk pfn extraction
        # pfn_map = {"rse":{"lfn":"pfn"}}
        lfns = []
        for source, lfn in rse_and_lfns:
            if source == rse:
                lfns.append(lfn)
        
        # TODO: try
        pfn_map.update(get_pfns(rse, lfns))

    # Generate remote replicas dict
    #{"T2_IT_Pisa_Temp": [{‘scope’: <scope1>, ‘name’: <name1>}, {‘scope’: <scope2>, ‘name’: <name2>}, …]}
    # collect also lfns to convert into pfns in bulk
    for xdict in remote_xdicts:
        rse = xdict["source"]
        source_lfn = xdict["source_lfn"]
        destination_lfn = xdict["destination_lfn"]
        size = xdict["filesize"]
        checksum = xdict["checksum"]
        replica = {'scope': RUCIO_SCOPE, 'pfn': pfn_map[rse][source_lfn], 'name': destination_lfn, 'bytes': size, 'adler32': checksum}
        replicas[rse].append(replica)

   # Generate direct replicas list
    #{"T2_IT_Pisa": [{‘scope’: <scope1>, ‘name’: <name1>}, {‘scope’: <scope2>, ‘name’: <name2>}, …]}
    # no checksums
    for xdict in direct_xdicts:
        rse = xdict["source"]
        destination_lfn = xdict["destination_lfn"]
        size = xdict["filesize"]
        replica = {'scope': RUCIO_SCOPE, 'pfn': pfn_map[rse][source_lfn], 'name': destination_lfn, 'bytes': size}
        replicas[rse].append(replica)

    return replicas


def register_replicas(input_replicas=[]):

    # list of dids registration that succeeded or failed
    success = []
    failed = []

    # Per rse and per chunks

    for rse, replicas in input_replicas.items():

        for chunk in chunks(replicas, REPLICAS_CHUNK_SIZE):
            # add_replicas(rse, files, ignore_availability=True)
            #     Bulk add file replicas to a RSE.
            #     Parameters
            #             rse – the RSE name.
            #             files – The list of files. This is a list of DIDs like : [{‘scope’: <scope1>, ‘name’: <name1>}, {‘scope’: <scope2>, ‘name’: <name2>}, …]
            #             ignore_availability – Ignore the RSE blacklisting.
            #     Returns
            #             True if files were created successfully.

            # TODO: try
            if not RUCIO_CLIENT.add_replicas(rse, chunk):
                # TODO: add to failed list
            else: 
                dids = [RUCIO_SCOPE+":"+x["name"] for x in chunk]

                # keep file in place at least one rule with lifetime (1m) for replicas on TEMP RSE
                # 2629800 seconds in a month
                RUCIO_CLIENT.add_replication_rule(dids, 1, rse, purge_replicas=True, lifetime=2629800)

                # add to CURRENT_DATASET
                RUCIO_CLIENT.attach_dids(RUCIO_SCOPE, CURRENT_DATASET, dids)

                # check the current number of files in the dataset
                if len(list(RUCIO_CLIENT.list_content(RUCIO_SCOPE, CURRENT_DATASET))) > DATASET_FILE_LIMIT:
                    # TODO: close CURRENT_DATASET if needed (check if over limit)
                    # -if everything full create new one
                    RUCIO_CLIENT.close(RUCIO_SCOPE, CURRENT_DATASET)
                    check_or_create_current_dataset(force_create=True)

    # TODO: return a list of success and one for failed
    return


def monitor_locks_status():
    # Monitor by datasets

    # remove rule for file if transferred
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
    transfers_dicts = []

    with open("task_process/transfers.txt") as _list:
        for _data in _list.readlines()[int(LAST_LINE):]:
            if not DESTINATION:
                DESTINATION = _data["destination"]
            if not PUBLISHNAME:
                # translate publish name in case DBS publication is disabled
                PUBLISHNAME = _data["publishname"].replace('-00000000000000000000000000000000', '/rucio/USER')
            try:
                LAST_LINE += 1
                doc = json.loads(_data)
                transfers_dicts.append(create_transfer_dict(dict=doc))
            except:
                raise

    if os.path.exists('task_process/transfers/registered_direct_files.txt'):
        with open("task_process/transfers/registered_direct_files.txt", "r") as list_file:
            # list of directly staged files in transfer_dicts format
            direct_lfns = [x.split('\n')[0] for x in list_file.readlines()]
            REPLICAS = manage_replicas_for_remote_remote_direct(transfers_dicts, direct_lfns)

    check_or_create_container()
    check_or_create_current_dataset()

    register_replicas(REPLICAS)

    # TODO: update DB

    monitor_locks_status()

    # TODO: update DB

    # TODO: write LASTLINE to file
    # first in temp and then copy to avoid inconsistency for kill while processing

    return



## TODO: remove this... only for reference so far
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




