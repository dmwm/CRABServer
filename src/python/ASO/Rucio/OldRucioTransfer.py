#!/usr/bin/python
# flake8: noqa: E501
"""
Script algorithm
    - init RUCIO and CRAB clients
    - check for file reports from post-job on a local file
    - if present register Rucio container for this task
    - check if any open dataset is available for the container
        + in not, create one with rule for 1 replica at destination RSE
    - Start from the last file processed (stored on last_transfer.txt)
    - Gather list of file to transfers and prepare a RUCIO replica dict
        + register files staged in Temp RSEs
        + update info in oracle
        + if dataset contains more than dataset_file_limit files, close it and recreate a new one
    - monitor the Rucio replica locks for the datasets
        + update info in oracle accordingly
"""
#transfers.txt
#{
#    "id": "7e6d075f7434f1307a764d491d86ab1192554b76106d139846810834",
#    "username": "tseethon",
#    "taskname": "230227_174038:tseethon_crab_rucio_transfer_test12_20230227_184034",
#    "start_time": 1677520683,
#    "destination": "T2_CH_CERN",
#    "destination_lfn": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1677519634/230227_174038/0000/output_2.root",
#    "source": "T3_US_FNALLPC",
#    "source_lfn": "/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1677519634/230227_174038/0000/output_2.root",
#    "filesize": 630710,
#    "publish": 0,
#    "transfer_state": "NEW",
#    "publication_state": "NOT_REQUIRED",
#    "job_id": "2",
#    "job_retry_count": 1,
#    "type": "output",
#    "publishname": "autotest-1677519634-00000000000000000000000000000000",
#    "checksums": {
#        "adler32": "5cbf440e",
#        "cksum": "3473488862"
#    },
#    "outputdataset": "/GenericTTbar/tseethon-autotest-1677519634-94ba0e06145abd65ccb1d21786dc7e1d/USER"
#}

from __future__ import absolute_import, division, print_function
import json
import logging
import os
import re
import uuid

from rucio.client.client import Client

from RESTInteractions import CRABRest
from ServerUtilities import encodeRequest
from rucio.common.exception import DataIdentifierAlreadyExists, InvalidObject, FileAlreadyExists, ReplicaNotFound

import cProfile


class globs:
    # RUCIO and CRABRest clients
    crabserver: CRABRest = None
    rucio_client: Client = None

    # Utility variables
    dataset_file_limit: int = 9
    replicas_chunk_size: int = 2
    last_line: int = 0

    # RUCIO/Task variables
    rucio_scope: str = "user."  # the username will be added in the code
    current_dataset: str = None
    logs_dataset: str = None
    publishname: str = None
    destination: str = None
    replicas: dict = {}  # will be {'tempRse':[rd,...,rd]} rd is dict. with keys: adler32w, bytes, name, pfn, scope}
                         # name is the final LFN. scope is not needed since is the same for all files

    # Mapping variables
    id2lfn_map: dict = {}
    lfn2id_map: dict = {}


# Initialize glob dataclass
glob = globs()


# TODO: review info level logging information

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
)

try:
    with open("task_process/transfers/last_transfer.txt", "r") as _last:
        glob.last_line = int(_last.readline())
except Exception as ex:
    logging.info(
        "task_process/transfers/last_transfer.txt does not exists. Starting from the first ever file to transfer")
    logging.exception("")

if not os.path.exists('task_process/transfers'):
    os.makedirs('task_process/transfers')


def init_crabrest_client():
    """
    Initialize client for CRAB REST
    """
    crabrest_logger = logging.getLogger('crabrest_init')
    # try:
    if os.path.exists('task_process/RestInfoForFileTransfers.json'):
        with open('task_process/RestInfoForFileTransfers.json') as fp:
            restInfo = json.load(fp)
            proxy = os.getcwd() + "/" + restInfo['proxyfile']
            # rest_filetransfers = restInfo['host'] + '/crabserver/' + restInfo['dbInstance']
            os.environ["X509_USER_PROXY"] = proxy
    else:
        crabrest_logger.error("No RestInfoForFileTransfers.json file ready")
        raise Exception(
            "No RestInfoForFileTransfers.json file ready. Waiting for first job to finish")
    # If there are no user proxy yet, just wait for the first pj of the task to finish
    if not proxy:
        crabrest_logger.warning(
            'No proxy available yet - waiting for first post-job')
        return

    crabrest_logger.info("Loading crab rest client")
    try:
        glob.crabserver = CRABRest(
            restInfo['host'],
            localcert=proxy,
            localkey=proxy,
            userAgent='CRABSchedd'
        )
        glob.crabserver.setDbInstance(restInfo['dbInstance'])
    except Exception:
        crabrest_logger.exception("Failed to set connection to crabserver")
        return


def init_rucio_client():
    """
    Initiate the Rucio client:
        - check for username in task_process/transfers.txt
        - get the proxy location
        - instatiate client
        - try rucio whoami
    """
    rucio_logger = logging.getLogger('rucio_client')
    rucio_logger.info(
        "Checking if there are any files in task_process/transfers.txt")

    try:
        with open("task_process/transfers.txt") as _list:
            doc = json.loads(_list.readlines()[0])
            user = doc['username']
            if not glob.destination:
                glob.destination = doc["destination"]
            if not glob.publishname:
                glob.publishname = doc["outputdataset"]
    except Exception as ex:
        rucio_logger.exception(
            "task_process/transfers.txt does not exist. Probably no completed jobs in the task yet.", ex)
        return

    glob.logs_dataset = glob.publishname + "#LOGS"

    rucio_logger.info(
        "Checking if task_process/RestInfoForFileTransfers.json exists")
    try:
        if os.path.exists('task_process/RestInfoForFileTransfers.json'):
            with open('task_process/RestInfoForFileTransfers.json') as fp:
                restInfo = json.load(fp)
                proxy = os.getcwd() + "/" + restInfo['proxyfile']
                os.environ["X509_USER_PROXY"] = proxy
    except Exception as ex:
        rucio_logger.exception(
            "task_process/RestInfoForFileTransfers.json can't be read. Probably no completed jobs in the task yet.", ex)
        return

    if not glob.rucio_client:
        rucio_logger.warning(
            "Rucio client not configured, I'm initiating it right now")
        rucio_logger.info("I'm going to configure Rucio client for %s", user)

        glob.rucio_scope += user
        rucio_logger.debug("Account %s scope: %s with creds %s" %
                           (user, glob.rucio_scope, proxy))

        try:
            rc = Client(
                account=user,
                auth_type="x509_proxy",
                logger=rucio_logger
            )
            glob.rucio_client = rc
            glob.rucio_client.whoami()
        except Exception as ex:
            rucio_logger.error(
                "Something went wrong when initializing rucio client: %s", ex)
            return


def check_or_create_container():
    """
    - check if container already exists
    - otherwise create it
    """

    container_exists = False

    try:
        glob.rucio_client.add_container(glob.rucio_scope, glob.publishname)
        container_exists = True
        logging.info("%s container created" % glob.publishname)
    except DataIdentifierAlreadyExists:
        logging.info(
            "%s container already exists, doing nothing", glob.publishname)
        container_exists = True
    except Exception as ex:
        logging.error(ex)

    return container_exists


def check_or_create_current_dataset(force_create: bool = False):
    """
    Check if there are open datasets and then start from there:
    - if open with less than max file go ahead and use it
    - if max file is reached, close the currente ds and open a new one
    - if force_create = True, create a new one anyway
    """
    checkds_logger = logging.getLogger("check_or_create_current_dataset")

    # TODO: create #LOGS dataset if does not exists
    # Can we simply avoid transferring LOGS with RUCIO?
    dataset_exists = False

    if not force_create:
        try:
            datasets = glob.rucio_client.list_content(
                glob.rucio_scope, glob.publishname)
        except Exception as ex:
            checkds_logger.error("Failed to list container content", ex)
            return dataset_exists

        # get open datasets
        # if more than one, close the most occupied
        open_ds = []

        dids = []
        logs_ds_exists = False

        for d in datasets:
            if d['name'] == glob.logs_dataset:
                logs_ds_exists = True
            dids.append(d)

        # If a ds for logs does not exists, create one
        if not logs_ds_exists:
            try:
                glob.rucio_client.add_dataset(glob.rucio_scope, glob.logs_dataset)
                ds_did = {'scope': glob.rucio_scope, 'type': "DATASET", 'name': glob.logs_dataset}
                glob.rucio_client.add_replication_rule([ds_did], 1, glob.destination)
                # attach dataset to the container
                glob.rucio_client.attach_dids(glob.rucio_scope, glob.logs_dataset, [ds_did])
            except Exception as ex:
                checkds_logger.exception("Failed to create and attach a logs RUCIO dataset %s" % ex)


        if len(dids) > 0:
            try:
                metadata = glob.rucio_client.get_metadata_bulk(dids)
            except InvalidObject:
                # Cover the case for which the dataset has been created but has 0 files
                # FIX: probably a bug on get_metadata_bulk that crash if any of the did has size 0
                metadata = []
                for did in dids:
                    metadata.append(glob.rucio_client.get_metadata(
                        glob.rucio_scope, did["name"]))
            except Exception as ex:
                checkds_logger.exception(
                    "Failed to get metadata in bulk for dids: ", ex)
                return dataset_exists

            for md in metadata:
                if md["is_open"]:
                    open_ds.append(md["name"])

        if len(open_ds) == 0:
            checkds_logger.warning("No dataset available yet, creating one")
            glob.current_dataset = glob.publishname+"#%s" % uuid.uuid4()
            # create a new dataset
            try:
                glob.rucio_client.add_dataset(glob.rucio_scope, glob.current_dataset)
                ds_did = {'scope': glob.rucio_scope,
                          'type': "DATASET", 'name': glob.current_dataset}
                glob.rucio_client.add_replication_rule([ds_did], 1, glob.destination)
                # attach dataset to the container
                glob.rucio_client.attach_dids(
                    glob.rucio_scope, glob.publishname, [ds_did])
                dataset_exists = True
            except Exception as ex:
                checkds_logger.exception(
                    "Failed to create and attach a new RUCIO dataset %s" % ex)
        elif len(open_ds) > 1:
            checkds_logger.info(
                "Found more than one open dataset, closing the one with more files and using the other as the current one")
            # TODO: close the most occupied and take the other as the current one -
            # so far we take the first and then let the Publisher close the dataset when task completed
            glob.current_dataset = open_ds[0]
            dataset_exists = True
        elif len(open_ds) == 1:
            checkds_logger.info(
                "Found exactly one open dataset, setting it as the current dataset: %s", open_ds[0])
            glob.current_dataset = open_ds[0]

            dataset_exists = True

    else:
        checkds_logger.info("Forced creation of a new dataset.")
        glob.current_dataset = glob.publishname+"#%s" % uuid.uuid4()
        # create a new dataset
        try:
            glob.rucio_client.add_dataset(glob.rucio_scope, glob.current_dataset)
            ds_did = {'scope': glob.rucio_scope,
                      'type': "DATASET", 'name': glob.current_dataset}
            glob.rucio_client.add_replication_rule([ds_did], 1, glob.destination)
            # attach dataset to the container
            glob.rucio_client.attach_dids(glob.rucio_scope, glob.publishname, [ds_did])
            dataset_exists = True
        except Exception as ex:
            checkds_logger.error(
                "Failed to create and attach a new RUCIO dataset", ex)

    return dataset_exists


def create_transfer_dict(input_dict: dict = {}):
    """
    Populate dictionaries with transfer information from transfers.txt json

    :param input_dict: dictionary coming from  transfers.txt json, defaults to {}
    :type input_dict: dict, optional
    :return: transfer dictionary
    :rtype: dict
    """

    xdict = {
        "source_lfn": input_dict["source_lfn"],
        "destination_lfn": input_dict["destination_lfn"],
        "id": input_dict["id"],
        "source": input_dict["source"]+"_Temp",
        "destination": input_dict["destination"],
        "checksum": input_dict["checksums"]["adler32"].rjust(8, '0'),
        "filesize": input_dict["filesize"],
        "publishname":     glob.publishname
    }

    #print(input_dict["source_lfn"], input_dict["checksums"]["adler32"].rjust(8,'0'))
    return xdict


def chunks(lst: list = None, n: int = 1):
    """
    Yield successive n-sized chunks from l.
    :param l: list to splitt in chunks
    :param n: chunk size
    :return: yield the next list chunk
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


MAX_CHAIN_DEPTH = 5


def tfc_lfn2pfn(lfn, tfc, proto, depth=0):
    """
    Performs the actual tfc lfn2pfn matching
    """

    if depth > MAX_CHAIN_DEPTH:
        raise Exception("Max depth reached matching lfn %s and protocol %s with tfc %s" %
                        lfn, proto, tfc)

    for rule in tfc:
        if rule['proto'] == proto:
            if 'chain' in rule:
                lfn = tfc_lfn2pfn(lfn, tfc, rule['chain'], depth + 1)

            regex = re.compile(rule['path'])
            if regex.match(lfn):
                return regex.sub(rule['out'].replace('$', '\\'), lfn)

    if depth > 0:
        return lfn

    raise ValueError(
        "lfn %s with proto %s cannot be matched by tfc %s" % (lfn, proto, tfc))


def get_pfns(rse: str, lfns: list):
    """
    get pfns from RSE name and list of lfns

    :param rse: RSE name
    :type rse: str
    :param lfns: list of lnfs
    :type lfns: list[str]
    :return: dictionary {rse: {lfn1: pfn1, lfn2: pfn2, ...}}
    :rtype: dict
    """
    # Get the rse prefix from the first entry, then apply to all

    pfns = []
    # print(rse)
    #pfn_0 = glob.rucio_client.lfns2pfns(rse.split("_Temp")[0], [glob.rucio_scope + ":" + lfns[0]], operation="read")

    map_dict = {}
    try:
        rgx = glob.rucio_client.get_protocols(
            rse.split("_Temp")[0], protocol_domain='ALL', operation="read")[0]

        if not rgx['extended_attributes'] or 'tfc' not in rgx['extended_attributes']:
            pfn_0 = glob.rucio_client.lfns2pfns(
                rse.split("_Temp")[0], [glob.rucio_scope + ":" + lfns[0]], operation="read")
            pfns.append(pfn_0[glob.rucio_scope + ":" + lfns[0]])
            prefix = pfn_0[glob.rucio_scope + ":" + lfns[0]].split(lfns[0])[0]
            # print(pfn_0)
            for lfn in lfns:
                map_dict.update({lfn: prefix+lfn})
        else:
            for lfn in lfns:
                if 'tfc' in rgx['extended_attributes']:
                    tfc = rgx['extended_attributes']['tfc']
                    tfc_proto = rgx['extended_attributes']['tfc_proto']

                    # matching the lfn into a pfn
                    map_dict.update({lfn: tfc_lfn2pfn(lfn, tfc, tfc_proto)})

    except TypeError:
        raise TypeError('Cannot determine PFN for LFN %s:%s at %s with proto %s'
                        % glob.rucio_scope, lfn, rse, rgx)

    pfn_map = {rse: map_dict}
    return pfn_map


def prepare_replicas(transfer_dicts: list):
    """
    Generate a RUCIO replica list of dict starting from the files in temp RSEs

    Store it in glob class at glob.replicas

    """

    remote_xdicts = []

    rses_remote = []
    rse_and_lfns = []

    pfn_map = {}

    # first of all lets separate direct from remote dicts
    for xdict in transfer_dicts:
        remote_xdicts.append(xdict)
        rses_remote.append(xdict["source"])
        rse_and_lfns.append([xdict["source"], xdict["source_lfn"]])

    rses_remote = list(dict.fromkeys(rses_remote))
    for rse in rses_remote:
        glob.replicas.update({rse: []})
        # collect info for bulk pfn extraction
        # pfn_map = {"rse":{"lfn":"pfn"}}
        lfns = []
        for source, lfn in rse_and_lfns:
            if source == rse:
                lfns.append(lfn)

        try:
            pfn_map.update(get_pfns(rse, lfns))
        except Exception as ex:
            raise ex

    # TODO: split logs from output!

    # Generate remote replicas dict
    # collect also lfns to convert into pfns in bulk
    for xdict in remote_xdicts:
        rse = xdict["source"]
        source_lfn = xdict["source_lfn"]
        destination_lfn = xdict["destination_lfn"]
        size = xdict["filesize"]
        checksum = xdict["checksum"]
        replica = {'scope': glob.rucio_scope, 'pfn': pfn_map[rse][source_lfn],
                   'name': destination_lfn, 'bytes': size, 'adler32': checksum}
        glob.replicas[rse].append(replica)
        # print(replica['adler32'])
    return


def map_lfns_to_oracle_ids():
    """
    Generate a map from lfns to oracle ids
    """

    if os.path.exists('task_process/transfers.txt'):
        with open('task_process/transfers.txt', 'r') as _list:
            for _data in _list.readlines():
                try:
                    doc = json.loads(_data)

                    glob.lfn2id_map.update({doc['id']: doc['destination_lfn']})
                except Exception as ex:
                    raise ex

    return


def map_oracle_ids_to_lfns():
    """
    Generate a map from oracle ids to lfns
    SB: THIS IS ACTUALLY A MAP FROM LFN to ID !!
    glob.id2lfn_map{lfn:id,...,lfn:id}
    """

    if os.path.exists('task_process/transfers.txt'):
        with open('task_process/transfers.txt', 'r') as _list:
            for _data in _list.readlines():
                try:
                    doc = json.loads(_data)
                    glob.id2lfn_map.update({doc['destination_lfn']: doc['id']})
                except Exception as ex:
                    raise ex

    return


def register_replicas(input_replicas: dict) -> tuple:
    """
    Take RUCIO replica dictionary and register replica in RUCIO
    (attaching them to the glob.current_dataset)

    Eventually check if we went further the glob.dataset_file_limit
    in such case close current dataset and open a new one

    Store the line number of the current processing

    Return a list of success and fail ids
    """
    recrep_logger = logging.getLogger("register_replicas")

    # list of dids registration that succeeded or failed
    success = []
    failed = []

    # Per rse and per chunks
    #replica = {'scope': glob.rucio_scope, 'pfn': pfn_map[rse][source_lfn],
    #           'name': destination_lfn, 'bytes': size, 'adler32': checksum}

    for rse, replicas in input_replicas.items():

        for chunk in chunks(replicas, glob.replicas_chunk_size):
            # for ch in chunk:
            #    print(ch['name'], ch['pfn'],ch['adler32'])
            try:
                if not glob.rucio_client.add_replicas(rse, chunk):
                    failed.append([x["name"] for x in chunk])
                else:
                    dids = [{'scope': glob.rucio_scope, 'type': "FILE",
                             'name': x["name"]} for x in chunk]

                # keep file in place at least one rule with lifetime (1m) for replicas on TEMP RSE
                # 2629800 seconds in a month
                #glob.rucio_client.add_replicationrule(dids, 1, rse, purge_replicas=True, lifetime=2629800)

                # add to _current_dataset
                glob.rucio_client.attach_dids(
                    glob.rucio_scope, glob.current_dataset, dids)

                # TODO: close if update comes > 4h, or is it a Publisher task?
                success += [{'lfn':x["name"], 'dbsBlock':glob.current_dataset, 'complete':'NO'} for x in chunk]
            except FileAlreadyExists:
                recrep_logger.info(
                    "files were already registered, going ahead checking if attached to the dataset status update and monitor")
                try:
                    glob.rucio_client.add_files_to_datasets(
                        [{'scope': glob.rucio_scope, 'name': glob.current_dataset, 'dids': dids}], ignore_duplicate=True)
                except:
                    recrep_logger.exception(
                        "Failing to attach replica %s to dataset" % dids)
                    failed += [x["name"] for x in chunk]
                    continue
                recrep_logger.debug("files already registered and attached are: %s" %
                                    [x["name"] for x in chunk])
                success += [{'lfn':x["name"], 'block':glob.current_dataset, 'complete':'NO'} for x in chunk]
            except Exception as ex:
                recrep_logger.exception("Failing managing replicas %s" % [x["name"] for x in chunk])
                failed += [x["name"] for x in chunk]
                continue
            # check the current number of files in the dataset
            if len(list(glob.rucio_client.list_content(glob.rucio_scope, glob.current_dataset))) >= glob.dataset_file_limit:
                # -if everything full create new one
                glob.rucio_client.close(glob.rucio_scope, glob.current_dataset)
                check_or_create_current_dataset(force_create=True)
    # update last read line
    with open("task_process/transfers/last_transfer_new.txt", "w+") as _last:
        _last.write(str(glob.last_line))
    os.rename("task_process/transfers/last_transfer_new.txt",
              "task_process/transfers/last_transfer.txt")

    return success, failed


def monitor_locks_status():
    """
    Get rules for the RUCIO container

    Check all locks for all the rules

    Generate a list of completed,failed replicas

    Eventually attach ruleId for the locks in replicating state
    """
    monitor_logger = logging.getLogger("monitor_locks_status")

    # get list of files already updated
    already_processed_list = []
    list_update = []
    list_good = []
    list_failed = []
    list_stuck = []
    list_failed_tmp = []

    # get container rules
    try:
        for ds in glob.rucio_client.list_content(glob.rucio_scope, glob.publishname):
            rules = glob.rucio_client.list_did_rules(glob.rucio_scope, ds['name'])

            for r in rules:
                ruleID = r['id']
                # print(ruleID)
                try:
                    locks_generator = glob.rucio_client.list_replica_locks(
                        r['id'])
                except Exception:
                    monitor_logger.exception('Unable to get replica locks')
                    return [], [], []

                # analyze replica locks info for each file
                for file_ in locks_generator:
                    monitor_logger.debug("LOCK %s", file_)
                    filename = file_['name']
                    result = { 'filename': filename, 'dbsBlock': ds['name'], 'complete': 'OK' if r['state'] == 'OK' else 'NO'}

                    # skip files already processed
                    if filename in already_processed_list:
                        continue

                    if filename not in glob.id2lfn_map:
                        # This is needed because in Rucio we allow user to publish 2 different tasks
                        # within the same Rucio dataset
                        monitor_logger.debug(
                            "Skipping file from previous tasks: %s", filename)
                        continue
                    status = file_['state']
                    monitor_logger.debug("state %s", status)
                    sitename = file_['rse']

                    if status == "OK":
                        list_good.append(result)
                    # No need to retry job at this point --> DELEGATE TO RUCIO
                    # We did not use list_failed anymore
                    # if status == "STUCK":
                    #     did = {'scope': glob.rucio_scope, 'name': filename }
                    #     monitor_logger.debug("Getting source RSE information for %s" % filename)
                    #     replica_info =  glob.rucio_client.list_replicas([did])
                    #     pfns = []
                    #     sources = []
                    #     for rep in replica_info:
                    #         did.update({'bytes': rep['bytes']})
                    #         for src, files in rep['rses'].items():
                    #             sources.append(src)
                    #             for pfn in files:
                    #                pfns.append(pfn)
                    #     monitor_logger.debug("Sources for %s is %s" % (filename, sources))
                    #     monitor_logger.info("Detaching stuck did %s from %s" % (did['name'], ds['name']))
                    #     try:
                    #         glob.rucio_client.detach_dids(glob.rucio_scope, ds['name'], [did])
                    #         for source in sources:
                    #             monitor_logger.debug("Deleting %s from %s" % (filename, source))
                    #             # TODO: not clear yet if we need to remove replicas
                    #             #glob.rucio_client.delete_replicas(source, [did])
                    #             glob.rucio_client.declare_bad_file_replicas(pfns, "STUCK crab transfer for rule %s" % ruleID)
                    #         list_failed_tmp.append((filename, "Transfer Stuck, with error: %s" % r['error'], sitename))
                    #     except Exception as ex:
                    #         monitor_logger.error("Failed to remove stuck replica: %s" % ex)
                    if status in ["REPLICATING", "STUCK"]:
                        #   TODO:   if now - replica["created_at"] > 12h:
                        #   delete replica and detach from dataset --> treat as STUCK
                        try:
                            list_update.append((filename, ruleID))
                        except Exception:
                            monitor_logger.exception("Replica lock not found")

                # Expose RUCIO rule ID in case of failure (if available)
                for name_ in [x[0] for x in list_failed_tmp]:
                    list_failed.append((name_, "Rule ID: %s" % ruleID))

    except:
        monitor_logger.exception("Failed to monitor rules")

    list_failed = list_failed_tmp + list_stuck
    return list_good, list_failed, list_update


def make_filedoc_for_db(
        ids: list,
        states: list,
        dbsBlocknames: list = None,
        blockCompletes: list = None,
        reasons: list = None,
        rule_ids: list = None
):
    """
    prepare dictionary in a proper form to be passed for CRAB REST call
    """
    fileDoc = {}

    if len(ids) != len(states):
        raise Exception("Lenght of ids list != lenght states list")

    fileDoc['asoworker'] = 'rucio'
    fileDoc['subresource'] = 'updateTransfers'
    fileDoc['list_of_ids'] = ids
    fileDoc['list_of_transfer_state'] = states
    fileDoc['list_of_dbs_blockname'] = dbsBlocknames
    fileDoc['list_of_block_complete'] = blockCompletes
    fileDoc['list_of_fts_instance'] = [
        'https://fts3-cms.cern.ch:8446/' for _ in ids]
    if reasons:
        if len(reasons) != len(ids):
            raise
        fileDoc['list_of_failure_reason'] = reasons
        # No need for retry -> delegate to RUCIO
        fileDoc['list_of_retry_value'] = [0 for _ in ids]
    if rule_ids:
        fileDoc['list_of_fts_id'] = [x for x in rule_ids]
    else:
        fileDoc['list_of_fts_id'] = ['NA' for _ in ids]

    return fileDoc


def update_db(fileDocs: list):
    """
    take a list of files and status --> update oracle
    """
    updatedb_logger = logging.getLogger("update_db")
    for fileDoc in fileDocs:
        updatedb_logger.debug("updating doc: %s" % fileDoc)
        try:
            glob.crabserver.post(
                api='filetransfers',
                data=encodeRequest(fileDoc)
            )
        except Exception as ex:
            updatedb_logger.exception("Error updating documents")
            raise ex

        if fileDoc['list_of_dbs_blockname'] or fileDoc['list_of_block_complete']:
            fileDoc['subresource'] = 'updateRucioInfo'
            try:
                glob.crabserver.post(
                    api='filetransfers',
                    data=encodeRequest(fileDoc)
                )
            except Exception as ex:
                updatedb_logger.exception("Error updating documents")
                raise ex

    return True


def main():
    """
    Script algorithm
    - check if rucio client is good
        - that means that at least a file report from post-job is there
    - register Rucio datasets and containers + container rule for this task
    - Start from the last file processed (stored on last_transfer.txt)
    - gather list of file to transfers   + register temp files (no direct stageout should be used for RUCIO)
        + register temp files (no direct stageout should be used for RUCIO)
        + fill the first dataset not closed
        + update info in oracle
    - monitor the Rucio replica locks by the datasets
        + update info in oracle accordingly
    """
    main_logger = logging.getLogger("main")
    transfers_dicts = []

    # do 1

    try:
        init_crabrest_client()
        init_rucio_client()
    except Exception as ex:
        main_logger.exception("Initialization failed.")
        return

    if not check_or_create_container():
        raise Exception("Failed to create container")

    if not check_or_create_current_dataset():
        raise Exception("Failed to check or create valid RUCIO dataset")

    with open("task_process/transfers.txt") as _list:
        print(glob.last_line)
        for _data in _list.readlines()[int(glob.last_line):]:
            try:
                glob.last_line += 1
                doc = json.loads(_data)
                transfers_dicts.append(create_transfer_dict(input_dict=doc))
            except Exception as ex:
                raise ex

    try:
        prepare_replicas(transfers_dicts)
    except Exception as ex:
        main_logger.error("Failed to prepare temp replica dicts")
        raise ex

    # TODO: if replica and rule exists go ahead anyway --> it means that db update was failling at the previous try
    success_from_registration, failed_from_registration = register_replicas(
        glob.replicas)

    try:
        map_lfns_to_oracle_ids()
        map_oracle_ids_to_lfns()
    except Exception as ex:
        main_logger.exception("Failed to map ids to lfns")
        raise ex

    to_update_success_docs = make_filedoc_for_db(
        ids=[glob.id2lfn_map[x['lfn']] for x in success_from_registration],
        states=["SUBMITTED" for x in success_from_registration],
        dbsBlocknames=[x['dbsBlock'] for x in success_from_registration],
        blockCompletes=[x['complete'] for x in success_from_registration],
        reasons=None
    )

    to_update_failed_docs = make_filedoc_for_db(
        ids=[glob.id2lfn_map[x] for x in failed_from_registration],
        states=["FAILED" for x in failed_from_registration],
        reasons=[
            "Failed to register files within RUCIO" for x in failed_from_registration]
    )

    try:
        update_db([to_update_success_docs, to_update_failed_docs])
    except Exception as ex:
        raise ex


    # do 2
    try:
        success_from_monitor, failed_from_monitor, ruleid_update = monitor_locks_status()
    except Exception as ex:
        raise ex

    #print(success_from_monitor, failed_from_monitor, ruleid_update)

    try:
        fileDocs_success_monitor = make_filedoc_for_db(
            ids=[glob.id2lfn_map[x['filename']] for x in success_from_monitor],
            states=["DONE" for x in success_from_monitor],
            dbsBlocknames=[x['dbsBlock'] for x in success_from_monitor],
            blockCompletes= [x['complete'] for x in success_from_monitor],
        )
        fileDocs_failed_monitor = make_filedoc_for_db(
            ids=[glob.id2lfn_map[x[0]] for x in failed_from_monitor],
            states=["FAILED" for x in failed_from_monitor],
            reasons=[x[1] for x in failed_from_monitor],
        )
        fileDocs_ruleid_monitor = make_filedoc_for_db(
            ids=[glob.id2lfn_map[x[0]] for x in ruleid_update],
            states=["SUBMITTED" for x in ruleid_update],
            rule_ids=[x[1] for x in ruleid_update]
        )
        update_db([fileDocs_success_monitor,
                   fileDocs_failed_monitor, fileDocs_ruleid_monitor])
    except Exception as ex:
        raise ex

    return
