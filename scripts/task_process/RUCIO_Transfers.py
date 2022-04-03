#!/usr/bin/python
# flake8: noqa: E501 
"""
Script algorithm
    - ...
"""
from __future__ import absolute_import, division, print_function
import json
import logging
import os
import re
import uuid

#from dataclasses import dataclass

from rucio.client.client import Client

from RESTInteractions import CRABRest
from ServerUtilities import encodeRequest
from rucio.common.exception import DataIdentifierAlreadyExists, InvalidObject, FileAlreadyExists, ReplicaNotFound

import cProfile

#@dataclass
class globals:
    # RUCIO and CRABRest clients
    crabserver: CRABRest = None
    rucio_client: Client = None

    # Utility variables
    dataset_file_limit: int = 100
    replicas_chunk_size: int = 20
    last_line: int = 0

    # RUCIO/Task variables
    rucio_scope: str = "user."
    current_dataset: str = None
    logs_dataset: str = None
    publishname: str = None
    destination: str = None
    replicas: dict = {} 

    # Mapping variables
    id2lfn_map: dict = {} 
    lfn2id_map: dict = {}

# Initialize global dataclass
g = globals()


# Setup logging, transfer dirs and get last processed line. 
#os.environ["X509_CERT_DIR"] = os.getcwd()

logging.basicConfig(
    filename='task_process/transfer_rucio.log',
    level=logging.DEBUG,
    format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
)

try:
    with open("task_process/transfers/last_transfer.txt", "r") as _last:
        g.last_line = int(_last.readline())
except Exception as ex:
    logging.info("task_process/transfers/last_transfer.txt does not exists. Starting from the first ever file to transfer")
    logging.exception("")

if not os.path.exists('task_process/transfers'):
    os.makedirs('task_process/transfers')


def init_crabrest_client():
    """[summary]

    :return: [description]
    :rtype: [type]
    """
    crabrest_logger = logging.getLogger('crabrest_init')
    #try:
    if os.path.exists('task_process/RestInfoForFileTransfers.json'):
        with open('task_process/RestInfoForFileTransfers.json') as fp:
            restInfo = json.load(fp)
            proxy = os.getcwd() + "/" + restInfo['proxyfile']
            # rest_filetransfers = restInfo['host'] + '/crabserver/' + restInfo['dbInstance']
            os.environ["X509_USER_PROXY"] = proxy
    else:
        crabrest_logger.error("No RestInfoForFileTransfers.json file ready")
        raise Exception("No RestInfoForFileTransfers.json file ready. Waiting for first job to finish")
    # If there are no user proxy yet, just wait for the first pj of the task to finish
    if not proxy:
        crabrest_logger.warning('No proxy available yet - waiting for first post-job')
        return None

    crabrest_logger.info("Loading crab rest client")
    try:
        g.crabserver = CRABRest(
            restInfo['host'],
            localcert=proxy,
            localkey=proxy,
            userAgent='CRABSchedd'
            )
        g.crabserver.setDbInstance(restInfo['dbInstance'])
    except Exception:
        crabrest_logger.exception("Failed to set connection to crabserver")
        return


def init_rucio_client():
    """
    Initiate the Rucio client if not already set:
        - check if RUCIO_CLIENT is already initialized
            - if not try to instantiate now
                - get username from ask_process/transfers.txt
                - get the proxy location
                - instatiate client
    """
    rucio_logger = logging.getLogger('rucio_client')
    rucio_logger.info("Checking if there are any files in task_process/transfers.txt")

    try:
        with open("task_process/transfers.txt") as _list:
            doc = json.loads(_list.readlines()[0])
            user = doc['username']
            if not g.destination:
                g.destination = doc["destination"]
            if not g.publishname:
                # translate publish name in case DBS publication is disabled
                g.publishname = doc["publishname"].replace('-00000000000000000000000000000000', '/rucio/USER')
    except Exception as ex:
        rucio_logger.exception("task_process/transfers.txt does not exist. Probably no completed jobs in the task yet.", ex)
        return
    
    g.logs_dataset = g.publishname + "#LOGS"

    rucio_logger.info("Checking if task_process/RestInfoForFileTransfers.json exists")
    try:
        if os.path.exists('task_process/RestInfoForFileTransfers.json'):
            with open('task_process/RestInfoForFileTransfers.json') as fp:
                restInfo = json.load(fp)
                proxy = os.getcwd() + "/" + restInfo['proxyfile']
                os.environ["X509_USER_PROXY"] = proxy
    except Exception as ex:
        rucio_logger.exception("task_process/RestInfoForFileTransfers.json can't be read. Probably no completed jobs in the task yet.", ex)
        return

    if not g.rucio_client:
        rucio_logger.warning("Rucio client not configured, I'm initiating it right now")
        rucio_logger.info("I'm going to configure Rucio client for %s", user)

        g.rucio_scope += user
        rucio_logger.debug("Account %s scope: %s with creds %s" % (user, g.rucio_scope, proxy))

        try:
            rc = Client(
                account=user,
                auth_type="x509_proxy",
                logger=rucio_logger
            )
            g.rucio_client = rc
            g.rucio_client.whoami()
        except Exception as ex:
            rucio_logger.error("Something went wrong when initializing rucio client: %s", ex)
            return


def check_or_create_container():
    """
    - check if container already exists
    - otherwise create it
    """
    
    container_exists = False

    try:
        g.rucio_client.add_container(g.rucio_scope, g.publishname)
        # container_did = {'scope': g.rucio_scope, 'type': "container", 'name': g.publishname}
        container_exists = True
        logging.info("%s container created" % g.publishname)
    except DataIdentifierAlreadyExists:
        logging.info("%s container already exists, doing nothing", g.publishname)
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

    dataset_exists = False

    if not force_create:
        try:
            datasets = g.rucio_client.list_content(g.rucio_scope, g.publishname)
        except Exception as ex:
            checkds_logger.error("Failed to list container content", ex)
            return dataset_exists

        # get open datasets
        # if more than one, close the most occupied
        open_ds = []

        dids = []
        logs_ds_exists = False

        for d in datasets:
            if d['name'] == g.logs_dataset:
                logs_ds_exists = True

            dids.append(d)

        # If a ds for logs does not exists, create one
        g.logs_dataset = "/" + g.publishname+"#LOGS"
        try:
            g.rucio_client.add_dataset(g.rucio_scope, g.logs_dataset)
            ds_did = {'scope': g.rucio_scope, 'type': "DATASET", 'name': g.logs_dataset}
            g.rucio_client.add_replication_rule([ds_did], 1, g.destination)
            # attach dataset to the container
            g.rucio_client.attach_dids(g.rucio_scope, g.logs_dataset, [ds_did])
        except Exception as ex:
            checkds_logger.exception("Failed to create and attach a logs RUCIO dataset %s" % ex)        


        if len(dids) > 0:
            try:
                metadata = g.rucio_client.get_metadata_bulk(dids)
            except InvalidObject:
                # Cover the case for which the dataset has been created but has 0 files
                # FIX: probably a bug on get_metadata_bulk that crash if any of the did has size 0
                metadata = []
                for did in dids:
                    metadata.append(g.rucio_client.get_metadata(g.rucio_scope, did["name"]))
            except Exception as ex:
                checkds_logger.exception("Failed to get metadata in bulk for dids: ", ex)
                return dataset_exists

            for md in metadata:
                if md["is_open"]:
                    open_ds.append(md["name"])

        if len(open_ds) == 0:
            checkds_logger.warning("No dataset available yet, creating one")
            g.current_dataset = "/" + g.publishname+"#%s" % uuid.uuid4()
            # create a new dataset
            try:
                g.rucio_client.add_dataset(g.rucio_scope, g.current_dataset)
                ds_did = {'scope': g.rucio_scope, 'type': "DATASET", 'name': g.current_dataset}
                g.rucio_client.add_replication_rule([ds_did], 1, g.destination)
                # attach dataset to the container
                g.rucio_client.attach_dids(g.rucio_scope, g.publishname, [ds_did])
                dataset_exists = True
            except Exception as ex:
                checkds_logger.exception("Failed to create and attach a new RUCIO dataset %s" % ex)
        elif len(open_ds) > 1:
            checkds_logger.info("Found more than one open dataset, closing the one with more files and using the other as the current one")
            # TODO: close the most occupied and take the other as the current one
            g.current_dataset = open_ds[0]
            dataset_exists = True
        elif len(open_ds) == 1:
            checkds_logger.info("Found exactly one open dataset, setting it as the current dataset: %s", open_ds[0])
            # TODO: check N files
            g.current_dataset = open_ds[0]

            dataset_exists = True

    else:    
            checkds_logger.info("Forced creation of a new dataset.")
            g.current_dataset = g.publishname+"#%s" % uuid.uuid4()
            # create a new dataset
            try:
                g.rucio_client.add_dataset(g.rucio_scope, g.current_dataset, rules=[{"copies": 1, "rse_expression": g.destination}])
                ds_did = {'scope': g.rucio_scope, 'type': "dataset", 'name': g.current_dataset}
                # attach dataset to the container
                g.rucio_client.attach_dids(g.rucio_scope, g.publishname, [ds_did])
                dataset_exists = True
            except Exception as ex:
                checkds_logger.error("Failed to create and attach a new RUCIO dataset", ex)

    return dataset_exists


def create_transfer_dict(input_dict: dict = {}):
    """[summary]

    :param input_dict: [description], defaults to {}
    :type input_dict: dict, optional
    :return: [description]
    :rtype: [type]
    """

    xdict = {
        "source_lfn": input_dict["source_lfn"],
        "destination_lfn": input_dict["destination_lfn"],
        "id": input_dict["id"],
        "source": input_dict["source"]+"_Temp",
        "destination": input_dict["destination"],
        "checksum": input_dict["checksums"]["adler32"].rjust(8,'0'),
        "filesize": input_dict["filesize"],
        "publishname":     g.publishname
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
    Performs the actual tfc matching
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

    raise ValueError("lfn %s with proto %s cannot be matched by tfc %s" % (lfn, proto, tfc))

def get_pfns(rse: str, lfns: list):
    """[summary]

    :param rse: [description]
    :type rse: [type]
    :param lfns: [description]
    :type lfns: [type]
    :return: [description]
    :rtype: [type]
    """
    # Get the rse prefix from the first entry, then apply to all

    pfns = []
    #print(rse)
    #pfn_0 = g.rucio_client.lfns2pfns(rse.split("_Temp")[0], [g.rucio_scope + ":" + lfns[0]], operation="read")

    map_dict = {}
    try:
      rgx = g.rucio_client.get_protocols(rse.split("_Temp")[0], protocol_domain='ALL', operation="read")[0]
      
      if not rgx['extended_attributes'] or 'tfc' not in rgx['extended_attributes']:
          pfn_0 = g.rucio_client.lfns2pfns(rse.split("_Temp")[0], [g.rucio_scope + ":" + lfns[0]], operation="read")
          pfns.append(pfn_0[g.rucio_scope + ":" + lfns[0]])
          prefix = pfn_0[g.rucio_scope + ":" + lfns[0]].split(lfns[0])[0]
          #print(pfn_0)
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
                        % g.rucio_scope, lfn, rse, rgx)
        
    pfn_map = {rse: map_dict}
    return pfn_map


def prepare_replicas(transfer_dicts: list):
    """
    Generate a replica list starting from the files in temp RSEs

    """
    # TODO: manage logs separately

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
        g.replicas.update({rse: []})
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

    # Generate remote replicas dict
    # collect also lfns to convert into pfns in bulk
    for xdict in remote_xdicts:
        rse = xdict["source"]
        source_lfn = xdict["source_lfn"]
        destination_lfn = xdict["destination_lfn"]
        size = xdict["filesize"]
        checksum = xdict["checksum"]
        replica = {'scope': g.rucio_scope, 'pfn': pfn_map[rse][source_lfn], 'name': destination_lfn, 'bytes': size, 'adler32': checksum}
        g.replicas[rse].append(replica)
        #print(replica['adler32'])


def map_lfns_to_oracle_ids():
    """[summary]
    """

    if os.path.exists('task_process/transfers.txt'):
        with open('task_process/transfers.txt', 'r') as _list:
            for _data in _list.readlines():
                try:
                    doc = json.loads(_data)
                    
                    g.lfn2id_map.update({doc['id']: doc['destination_lfn']})
                except Exception as ex:
                    raise ex

    return


def map_oracle_ids_to_lfns():
    """[summary]
    """

    if os.path.exists('task_process/transfers.txt'):
        with open('task_process/transfers.txt', 'r') as _list:
            for _data in _list.readlines():
                try:
                    doc = json.loads(_data)
                    g.id2lfn_map.update({doc['destination_lfn']: doc['id']})
                except Exception as ex:
                    raise ex

    return


def register_replicas(input_replicas: dict) -> tuple:
    """[summary]

    :param input_replicas: [description], defaults to []
    :type input_replicas: list, optional
    """
    recrep_logger = logging.getLogger("register_replicas")

    # list of dids registration that succeeded or failed
    success = []
    failed = []

    # Per rse and per chunks

    for rse, replicas in input_replicas.items():

        for chunk in chunks(replicas, g.replicas_chunk_size):
            #for ch in chunk:
            #    print(ch['name'], ch['pfn'],ch['adler32'])
            try:
                if not g.rucio_client.add_replicas(rse, chunk):
                    failed.append([x["name"] for x in chunk] )
                else: 
                    dids = [{'scope': g.rucio_scope, 'type': "FILE", 'name': x["name"]} for x in chunk]

                # keep file in place at least one rule with lifetime (1m) for replicas on TEMP RSE
                # 2629800 seconds in a month
                #g.rucio_client.add_replication_rule(dids, 1, rse, purge_replicas=True, lifetime=2629800)

                # add to _current_dataset
                g.rucio_client.attach_dids(g.rucio_scope, g.current_dataset, dids)
  
                # TODO: be sure that only new files are processed (either last line or sync file)

                # TODO: close if update comes > 4h
                success += [x["name"] for x in chunk]
            except FileAlreadyExists:
                recrep_logger.info("files were already registered, going ahead checking if attached to the dataset status update and monitor")
                try:
                    # attachment is: {‘scope’: scope, ‘name’: name, ‘dids’: dids} dids is: [{‘scope’: scope, ‘name’: name}, …]
                    g.rucio_client.add_files_to_datasets([{'scope': g.rucio_scope, 'name': g.current_dataset, 'dids': dids}], ignore_duplicate=True)
                except:
                    recrep_logger.exception("Failing to attach replica %s to dataset" % dids)
                    failed += [x["name"] for x in chunk]
                    continue
                recrep_logger.debug("files alread registered and attached are: %s" % [x["name"] for x in chunk] )
                success += [x["name"] for x in chunk]
            except Exception as ex:
                recrep_logger.exception("Failing managing replicas %s" % [x["name"] for x in chunk] )
                failed += [x["name"] for x in chunk]
                continue
            # check the current number of files in the dataset
            if len(list(g.rucio_client.list_content(g.rucio_scope, g.current_dataset))) > g.dataset_file_limit:
                # TODO: close _current_dataset if needed (check if over limit)
                # -if everything full create new one
                g.rucio_client.close(g.rucio_scope, g.current_dataset)
                check_or_create_current_dataset(force_create=True)
    # update last read line
    with open("task_process/transfers/last_transfer_new.txt", "w+") as _last:
        _last.write(str(g.last_line))
    os.rename("task_process/transfers/last_transfer_new.txt", "task_process/transfers/last_transfer.txt")        

    return success, failed


def monitor_locks_status():
    """[summary]
    """
    monitor_logger = logging.getLogger("monitor_locks_status")

    # get list of files already updated
    already_processed_list = []
    list_update_filt = []
    list_update = []
    list_good = []
    list_failed = []
    list_stuck = []
    list_failed_tmp = []
    

    # TODO: Keep track of what has been already marked. Avoiding double updates at next iteration
    #if os.path.exists("task_process/transfers/submitted_files.txt"):
    #    with open("task_process/transfers/submitted_files.txt", "r") as list_file:
    #        for _data in list_file.readlines():
    #            already_processed_list.append(_data.split("\n")[0])

    # get container rules
    try:
      for ds in g.rucio_client.list_content(g.rucio_scope, g.publishname):
        rules = g.rucio_client.list_did_rules(g.rucio_scope, ds['name'])

        # {u'locks_ok_cnt': 200, u'source_replica_expression': None, u'weight': None, u'purge_replicas': False, 
        #  u'rse_expression': u'T2_IT_Pisa=True', u'updated_at': datetime.datetime(2020, 10, 9, 9, 12, 26), 
        #  u'meta': None, u'child_rule_id': None, u'id': u'87946da28bfb4aeeaf71894a4606070d', u'ignore_account_limit': False,
        #  u'locks_stuck_cnt': 0, u'locks_replicating_cnt': 0, u'notification': u'NO', u'copies': 1, u'comments': None,
        #  u'split_container': False, u'priority': 3, u'state': u'OK', u'scope': u'user.dciangot', u'subscription_id': None, 
        # u'ignore_availability': False, u'stuck_at': None, u'error': None, u'eol_at': None, u'expires_at': None, u'did_type': u'DATASET', 
        # u'account': u'dciangot', u'locked': False, u'name': u'/NanoTestPost-preprod/rucio/USER#000000', 
        # u'created_at': datetime.datetime(2020, 9, 22, 7, 44, 47), u'activity': u'User Subscriptions', u'grouping': u'DATASET'}

        for r in rules: 
            ruleID = r['id']
            #print(ruleID)
            try:
                locks_generator = g.rucio_client.list_replica_locks(r['id'])  
            except Exception:
                monitor_logger.exception('Unable to get replica locks')
                return [], [], []

            # analyze replica locks info for each file
            for file_ in locks_generator:
                monitor_logger.debug("LOCK %s", file_)
                filename = file_['name']

                # skip files already processed
                if filename  in already_processed_list:
                    continue

                if filename not in g.id2lfn_map:
                    # This is needed because in Rucio we allow user to publish 2 different tasks
                    # within the same Rucio dataset
                    monitor_logger.debug("Skipping file from previous tasks: %s", filename)
                    continue
                status = file_['state']
                monitor_logger.debug("state %s", status)
                sitename = file_['rse']

                if status == "OK":
                    list_good.append(filename)
                if status == "STUCK":
                    #ftsJobID = g.rucio_client.list_request_by_did(filename, sitename, g.rucio_scope)["external_id"]
                    #print(ftsJobID)
                    list_failed_tmp.append((filename, "Transfer Stuck, with error: %s" % r['error'], sitename))
                if status == "REPLICATING":
                    try:
                        list_update.append((filename, ruleID))
                    except Exception:
                        monitor_logger.exception("Replica lock not found")

            # Expose RUCIO rule ID in case of failure (if available)   
            for name_ in [x[0] for x in list_failed_tmp]:
                list_failed.append((name_, "Rule ID: %s" % ruleID))

        #   TODO:   if now - replica["created_at"] > 12h:
        #   delete replica and detach from dataset --> treat as STUCK

        # TODO: Mark files of STUCK rules on the DB and remove them from dataset and rucio replicas
        #try:
        #    if len(list_stuck) > 0:
        #        list_stuck_name = [{'scope': g.scope, 'name': x[0]} for x in list_stuck]
        #        monitor_logger.debug("Detaching %s" % list_stuck_name)
        #        # TODO: name = DATASETNAME --> GET above
        #        g.rucio_client.cli.detach_dids(g.scope, name, list_stuck_name)
        #        sources = list(set([source_rse[x['name']] for x in list_stuck_name]))
        #        for source in sources:
        #            to_delete = [x for x in list_stuck_name if source_rse[x['name']] == source]
        #            monitor_logger.debug("Deleting %s from %s" % (to_delete, source))
        #            g.rucio_client.delete_replicas(source, to_delete)
        #except ReplicaNotFound:
        #  monitor_logger.exception("Failed to remove file from dataset")
    except:
        monitor_logger.exception("Failed to monitor rules") 

    list_failed = list_failed_tmp + list_stuck
    # TODO: write somewhere the list of rules not to check again
    # in task_process/transfers/submitted_files.txt
    return list_good, list_failed, list_update


def make_filedoc_for_db(
        ids: list,
        states: list,
        reasons: list = None,
        rule_ids: list = None
        ):
    """[summary]

    :param ids: [description]
    :type ids: [type]
    :param states: [description]
    :type states: [type]
    :param reasons: [description], defaults to None
    :type reasons: [type], optional
    :return: [description]
    :rtype: [type]
    """
    fileDoc = {}

    if len(ids) != len(states):
        raise Exception("Lenght of ids list != lenght states list")

    fileDoc['asoworker'] = 'rucio'
    fileDoc['subresource'] = 'updateTransfers'
    fileDoc['list_of_ids'] = ids
    fileDoc['list_of_transfer_state'] = states
    fileDoc['list_of_fts_instance'] = ['https://fts3-cms.cern.ch:8446/' for _ in ids]
    if reasons:
        if len(reasons) != len(ids):
            raise
        fileDoc['list_of_failure_reason'] = reasons
        fileDoc['list_of_retry_value'] = [0 for _ in ids]
    # TODO: do we need retry values??
    #if retry_values:
    #    if len(retry_values) != len(ids):
    #        raise
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
            g.crabserver.post(
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
        - one rule with lifetime (1m) for replicas on TEMP RSE
        - one rule with no lifetime (1m) for replicas on DEST RSE
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
        print(g.last_line)
        for _data in _list.readlines()[int(g.last_line):]:
            try:
                g.last_line += 1
                doc = json.loads(_data)
                transfers_dicts.append(create_transfer_dict(input_dict=doc))
            except Exception as ex:
                raise ex

    try:
        prepare_replicas(transfers_dicts)
    except Exception as ex: 
        main_logger.error("Failed to prepare temp replica dicts")
        raise ex

    #TODO: if replica and rule exists go ahead anyway --> it means that db update was failling at the previous try
    success_from_registration, failed_from_registration = register_replicas(g.replicas)

    try:
        map_lfns_to_oracle_ids()
        map_oracle_ids_to_lfns()
    except Exception as ex: 
        main_logger.exception("Failed to map ids to lfns")
        raise ex

    to_update_success_docs = make_filedoc_for_db(
            ids = [g.id2lfn_map[x] for x in success_from_registration],
            states = ["SUBMITTED" for x in success_from_registration],
            reasons = None
    )
        
    to_update_failed_docs =  make_filedoc_for_db( 
            ids = [g.id2lfn_map[x] for x in failed_from_registration],
            states = ["FAILED" for x in failed_from_registration],
            reasons = ["Failed to register files within RUCIO" for x in failed_from_registration]
    )    


    try:
        update_db([to_update_success_docs, to_update_failed_docs])
    except Exception as ex:
        raise ex

    # TODO: write LASTLINE
    # first in temp and then copy to avoid inconsistency for kill while processing

    try:
        success_from_monitor, failed_from_monitor, ruleid_update = monitor_locks_status()
    except Exception as ex:
        raise ex

    #print(success_from_monitor, failed_from_monitor, ruleid_update)
    #TODO: exclude already checked fiels

    try:
        fileDocs_success_monitor = make_filedoc_for_db(
            ids = [g.id2lfn_map[x] for x in success_from_monitor],
            states = ["DONE" for x in success_from_monitor],
        )
        fileDocs_failed_monitor = make_filedoc_for_db(
            ids = [g.id2lfn_map[x[0]] for x in failed_from_monitor],
            states = ["FAILED" for x in failed_from_monitor],
            reasons = [x[1] for x in failed_from_monitor],  
        )
        fileDocs_ruleid_monitor = make_filedoc_for_db(
            ids = [g.id2lfn_map[x[0]] for x in ruleid_update],
            states = ["SUBMITTED" for x in ruleid_update],
            rule_ids = [x[1] for x in ruleid_update]   
        )
        #print([fileDocs_success_monitor, fileDocs_failed_monitor, fileDocs_ruleid_monitor])
        update_db([fileDocs_success_monitor, fileDocs_failed_monitor, fileDocs_ruleid_monitor])
    except Exception as ex:
        raise ex

    # files_to_update = [
    #        fileDocs_success_registration,
    #        fileDocs_failed_registration,
    #        fileDocs_success_monitor,
    #        fileDocs_failed_monitor
    #    ]

    #update_files = update_db( files_to_update )

    # TODO: how about resubmit? will be same name?
    #    with open("task_process/transfers/submitted_files.txt", "a+") as list_file:
    #    for update in update_files:
    #        log.debug("{0}\n".format(str(update)))
    #        list_file.write("{0}\n".format(str(update)))

    # TODO: what if oracle push failed?????
    # they are all final states, so maybe dump them somewhere and retry them later?


    return


if __name__ == "__main__":
    main_logger = logging.getLogger("main")
    try:
        #cProfile.run('main()')
        main()
    except Exception as ex:
        print("error during main loop %s", ex)
        main_logger.exception("error during main loop")
    main_logger.info("transfer_inject.py exiting")
