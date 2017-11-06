import os
import uuid
import time
import logging
import sys
import json
import pycurl
import traceback
import urllib
import argparse
import re
import datetime
import dbs.apis.dbsClient as dbsClient
from utils import getProxy
from ServerUtilities import getHashLfn, PUBLICATIONDB_STATUSES, encodeRequest, oracleOutputMapping
from RESTInteractions import HTTPRequests


def Proxy(userDN, group, role, logger):
    userProxy = ''

    try:
        serviceCert = '/data/certs/hostcert.pem'
        serviceKey = '/data/certs/hostkey.pem'

        defaultDelegation = {'logger': logger,
                             'credServerPath': 'credentials',
                             'myProxySvr': 'myproxy.cern.ch',
                             'min_time_left': 36000,
                             'serverDN': "/DC=ch/DC=cern/OU=computers/CN=vocms0105.cern.ch",
                             'uisource': "/data/srv/tmp.sh"
                            }

        cache_area = 'https://vocms035.cern.ch/crabserver/dev/filemetadata'
        getCache = re.compile('https?://([^/]*)/.*')
        myproxyAccount = getCache.findall(cache_area)[0]
        defaultDelegation['myproxyAccount'] = myproxyAccount

        defaultDelegation['server_cert'] = serviceCert
        defaultDelegation['server_key'] = serviceKey

        valid = False
        defaultDelegation['userDN'] = userDN
        defaultDelegation['group'] = group
        defaultDelegation['role'] = role

        print group, role
        valid, proxy = getProxy(defaultDelegation, logger)
    except Exception as ex:
        msg = "Error getting the user proxy"
        print msg
        msg += str(ex)
        msg += str(traceback.format_exc())
        logger.error(msg)
    if valid:
        userProxy = proxy
    else:
        logger.error('Did not get valid proxy.')

    logger.info("userProxy: %s" % userProxy)

    return userProxy 

def format_file_3(file):
    """
    format file for DBS
    """
    nf = {'logical_file_name': file['lfn'],
          'file_type': 'EDM',
          'check_sum': unicode(file['cksum']),
          'event_count': file['inevents'],
          'file_size': file['filesize'],
          'adler32': file['adler32'],
          'file_parent_list': [{'file_parent_lfn': i} for i in set(file['parents'])],
         }
    file_lumi_list = []
    for run, lumis in file['runlumi'].items():
        for lumi in lumis:
            file_lumi_list.append({'lumi_section_num': int(lumi), 'run_num': int(run)})
    nf['file_lumi_list'] = file_lumi_list
    if file.get("md5") != "asda" and file.get("md5") != "NOTSET": # asda is the silly value that MD5 defaults to
        nf['md5'] = file['md5']
    return nf

def createBulkBlock(output_config, processing_era_config, primds_config, \
                    dataset_config, acquisition_era_config, block_config, files):
    """
    manage blocks
    """
    file_conf_list = []
    file_parent_list = []
    for file in files:
        file_conf = output_config.copy()
        file_conf_list.append(file_conf)
        file_conf['lfn'] = file['logical_file_name']
        for parent_lfn in file.get('file_parent_list', []):
            file_parent_list.append({'logical_file_name': file['logical_file_name'],
                                     'parent_logical_file_name': parent_lfn['file_parent_lfn']})
        del file['file_parent_list']
    blockDump = {
        'dataset_conf_list': [output_config],
        'file_conf_list': file_conf_list,
        'files': files,
        'processing_era': processing_era_config,
        'primds': primds_config,
        'dataset': dataset_config,
        'acquisition_era': acquisition_era_config,
        'block': block_config,
        'file_parent_list': file_parent_list
    }
    blockDump['block']['file_count'] = len(files)
    blockDump['block']['block_size'] = sum([int(file[u'file_size']) for file in files])
    return blockDump

def migrateByBlockDBS3(workflow, migrateApi, destReadApi, sourceApi, dataset, blocks = None):
    """
    Submit one migration request for each block that needs to be migrated.
    If blocks argument is not specified, migrate the whole dataset.
    """
    wfnamemsg = "%s: " % (workflow)
    logger = logging.getLogger(workflow)
    logging.basicConfig(filename=workflow+'.log', level=logging.INFO)

    if blocks:
        blocksToMigrate = set(blocks)
    else:
        # This is for the case to migrate the whole dataset, which we don't do
        # at this point Feb/2015 (we always pass blocks).
        # Make a set with the blocks that need to be migrated.
        blocksInDestDBS = set([block['block_name'] for block in destReadApi.listBlocks(dataset=dataset)])
        blocksInSourceDBS = set([block['block_name'] for block in sourceApi.listBlocks(dataset=dataset)])
        blocksToMigrate = blocksInSourceDBS - blocksInDestDBS
        msg = "Dataset %s in destination DBS with %d blocks; %d blocks in source DBS."
        msg = msg % (dataset, len(blocksInDestDBS), len(blocksInSourceDBS))
        logger.info(wfnamemsg+msg)
    numBlocksToMigrate = len(blocksToMigrate)
    if numBlocksToMigrate == 0:
        logger.info(wfnamemsg+"No migration needed.")
    else:
        msg = "Have to migrate %d blocks from %s to %s." % (numBlocksToMigrate, sourceApi.url, destReadApi.url)
        logger.info(wfnamemsg+msg)
        msg = "List of blocks to migrate:\n%s." % (", ".join(blocksToMigrate))
        logger.debug(wfnamemsg+msg)
        msg = "Submitting %d block migration requests to DBS3 ..." % (numBlocksToMigrate)
        logger.info(wfnamemsg+msg)
        numBlocksAtDestination = 0
        numQueuedUnkwonIds = 0
        numFailedSubmissions = 0
        migrationIdsInProgress = []
        for block in list(blocksToMigrate):
            # Submit migration request for this block.
            (reqid, atDestination, alreadyQueued) = requestBlockMigration(workflow, migrateApi, sourceApi, block)
            # If the block is already in the destination DBS instance, we don't need
            # to monitor its migration status. If the migration request failed to be
            # submitted, we retry it next time. Otherwise, save the migration request
            # id in the list of migrations in progress.
            if reqid is None:
                blocksToMigrate.remove(block)
                if atDestination:
                    numBlocksAtDestination += 1
                elif alreadyQueued:
                    numQueuedUnkwonIds += 1
                else:
                    numFailedSubmissions += 1
            else:
                migrationIdsInProgress.append(reqid)
        if numBlocksAtDestination > 0:
            msg = "%d blocks already in destination DBS." % numBlocksAtDestination
            logger.info(wfnamemsg+msg)
        if numFailedSubmissions > 0:
            msg = "%d block migration requests failed to be submitted." % numFailedSubmissions
            msg += " Will retry them later."
            logger.info(wfnamemsg+msg)
        if numQueuedUnkwonIds > 0:
            msg = "%d block migration requests were already queued," % numQueuedUnkwonIds
            msg += " but could not retrieve their request id."
            logger.info(wfnamemsg+msg)
        numMigrationsInProgress = len(migrationIdsInProgress)
        if numMigrationsInProgress == 0:
            msg = "No migrations in progress."
            logger.info(wfnamemsg+msg)
        else:
            msg = "%d block migration requests successfully submitted." % numMigrationsInProgress
            logger.info(wfnamemsg+msg)
            msg = "List of migration requests ids: %s" % migrationIdsInProgress
            logger.info(wfnamemsg+msg)
            # Wait for up to 300 seconds, then return to the main loop. Note that we
            # don't fail or cancel any migration request, but just retry it next time.
            # Migration states:
            #   0 = PENDING
            #   1 = IN PROGRESS
            #   2 = SUCCESS
            #   3 = FAILED (failed migrations are retried up to 3 times automatically)
            #   9 = Terminally FAILED
            # In the case of failure, we expect the publisher daemon to try again in
            # the future.
            numFailedMigrations = 0
            numSuccessfulMigrations = 0
            waitTime = 30
            numTimes = 10
            msg = "Will monitor their status for up to %d seconds." % waitTime * numTimes
            logger.info(wfnamemsg+msg)
            for _ in range(numTimes):
                msg = "%d block migrations in progress." % numMigrationsInProgress
                msg += " Will check migrations status in %d seconds." % waitTime
                logger.info(wfnamemsg+msg)
                time.sleep(waitTime)
                # Check the migration status of each block migration request.
                # If a block migration has succeeded or terminally failes, remove the
                # migration request id from the list of migration requests in progress.
                for reqid in list(migrationIdsInProgress):
                    try:
                        status = migrateApi.statusMigration(migration_rqst_id=reqid)
                        state = status[0].get('migration_status')
                        retry = status[0].get('retry_count')
                    except Exception as ex:
                        msg = "Could not get status for migration id %d:\n%s" % reqid, ex
                        logger.error(wfnamemsg+msg)
                    else:
                        if state == 2:
                            msg = "Migration id %d succeeded." % reqid
                            logger.info(wfnamemsg+msg)
                            migrationIdsInProgress.remove(reqid)
                            numSuccessfulMigrations += 1
                        if state == 9:
                            msg = "Migration id %d terminally failed." % reqid
                            logger.info(wfnamemsg+msg)
                            msg = "Full status for migration id %d:\n%s" % (reqid, str(status))
                            logger.debug(wfnamemsg+msg)
                            migrationIdsInProgress.remove(reqid)
                            numFailedMigrations += 1
                        if state == 3:
                            if retry < 3:
                                msg = "Migration id %d failed (retry %d), but should be retried." % (reqid, retry)
                                logger.info(wfnamemsg+msg)
                            else:
                                msg = "Migration id %d failed (retry %d)." % (reqid, retry)
                                logger.info(wfnamemsg+msg)
                                msg = "Full status for migration id %d:\n%s" % (reqid, str(status))
                                logger.debug(wfnamemsg+msg)
                                migrationIdsInProgress.remove(reqid)
                                numFailedMigrations += 1
                numMigrationsInProgress = len(migrationIdsInProgress)
                # Stop waiting if there are no more migrations in progress.
                if numMigrationsInProgress == 0:
                    break
            # If after the 300 seconds there are still some migrations in progress, return
            # with status 1.
            if numMigrationsInProgress > 0:
                msg = "Migration of %s is taking too long - will delay the publication." % dataset
                logger.info(wfnamemsg+msg)
                return 1, "Migration of %s is taking too long." % (dataset)
        msg = "Migration of %s has finished." % dataset
        logger.info(wfnamemsg+msg)
        msg = "Migration status summary (from %d input blocks to migrate):" % numBlocksToMigrate
        msg += " at destination = %d," % numBlocksAtDestination
        msg += " succeeded = %d," % numSuccessfulMigrations
        msg += " failed = %d," % numFailedMigrations
        msg += " submission failed = %d," % numFailedSubmissions
        msg += " queued with unknown id = %d." % numQueuedUnkwonIds
        logger.info(wfnamemsg+msg)
        # If there were failed migrations, return with status 2.
        if numFailedMigrations > 0 or numFailedSubmissions > 0:
            msg = "Some blocks failed to be migrated."
            logger.info(wfnamemsg+msg)
            return 2, "Migration of %s failed." % (dataset)
        # If there were no failed migrations, but we could not retrieve the request id
        # from some already queued requests, return with status 3.
        if numQueuedUnkwonIds > 0:
            msg = "Some block migrations were already queued, but failed to retrieve their request id."
            logger.info(wfnamemsg+msg)
            return 3, "Migration of %s in unknown status." % dataset
        if (numBlocksAtDestination + numSuccessfulMigrations) != numBlocksToMigrate:
            msg = "Something unexpected has happened."
            msg += " The numbers in the migration summary are not consistent."
            msg += " Make sure there is no bug in the code."
            logger.info(wfnamemsg+msg)
            return 4, "Migration of %s in some inconsistent status." % dataset
        msg = "Migration completed."
        logger.info(wfnamemsg+msg)
    try:
        migratedDataset = destReadApi.listDatasets(dataset=dataset, detail=True, dataset_access_type='*')
        if not migratedDataset or migratedDataset[0].get('dataset', None) != dataset:
            return 4, "Migration of %s in some inconsistent status." % dataset
    except Exception as ex:
        logger.exception("Migration check failed.")
        return 4, "Migration check failed. %s" % ex
    return 0, ""

def requestBlockMigration(workflow, migrateApi, sourceApi, block):
    """
    Submit migration request for one block, checking the request output.
    """
    logger = logging.getLogger(workflow)
    logging.basicConfig(filename=workflow+'.log', level=logging.INFO)

    wfnamemsg = "%s: " % workflow
    atDestination = False
    alreadyQueued = False
    reqid = None
    msg = "Submiting migration request for block %s ..." % block
    logger.info(wfnamemsg+msg)
    sourceURL = sourceApi.url
    data = {'migration_url': sourceURL, 'migration_input': block}
    try:
        result = migrateApi.submitMigration(data)
    except HTTPError as he:
        if "is already at destination" in he.msg:
            msg = "Block is already at destination."
            logger.info(wfnamemsg+msg)
            atDestination = True
        else:
            msg = "Request to migrate %s failed." % block
            msg += "\nRequest detail: %s" % data
            msg += "\nDBS3 exception: %s" % he.msg
            logger.error(wfnamemsg+msg)
    if not atDestination:
        msg = "Result of migration request: %s" % str(result)
        logger.debug(wfnamemsg+msg)
        reqid = result.get('migration_details', {}).get('migration_request_id')
        report = result.get('migration_report')
        if reqid is None:
            msg = "Migration request failed to submit."
            msg += "\nMigration request results: %s" % str(result)
            logger.error(wfnamemsg+msg)
        if "REQUEST ALREADY QUEUED" in report:
            # Request could be queued in another thread, then there would be
            # no id here, so look by block and use the id of the queued request.
            alreadyQueued = True
            try:
                status = migrateApi.statusMigration(block_name=block)
                reqid = status[0].get('migration_request_id')
            except Exception:
                msg = "Could not get status for already queued migration of block %s." % (block)
                logger.error(wfnamemsg+msg)
    return reqid, atDestination, alreadyQueued


def mark_good(workflow, files, oracleDB, logger):
    """
    Mark the list of files as tranferred
    """
    wfnamemsg = "%s: " % workflow
    last_update = int(time.time())
    for lfn in files:
        data = {}
        source_lfn = lfn
        docId = getHashLfn(source_lfn)
        msg = "Marking file %s as published." % lfn
        msg += " Document id: %s (source LFN: %s)." % (docId, source_lfn)
        logger.info(wfnamemsg+msg)
        data['asoworker'] = 'asodciangot1'
        data['subresource'] = 'updatePublication'
        data['list_of_ids'] = docId
        data['list_of_publication_state'] = 'DONE'
        data['list_of_retry_value'] = 1
        data['list_of_failure_reason'] = ''

        try:
            result = oracleDB.post('/crabserver/dev/filetransfers',
                                   data=encodeRequest(data))
            logger.debug("updated: %s %s " % (docId,result))
        except Exception as ex:
            logger.error("Error during status update: %s" %ex)


def mark_failed(workflow, files, oracleDB, logger, failure_reason="", force_failure=False ):
    """
    Something failed for these files so increment the retry count
    """
    wfnamemsg = "%s: " % workflow
    now = str(datetime.datetime.now())
    last_update = int(time.time())
    h = 0
    for lfn in files:
        h += 1
        logger.debug("Marking failed %s" % h)
        source_lfn = lfn
        docId = getHashLfn(source_lfn)
        logger.debug("Marking failed %s" % docId)
        try:
            docbyId = oracleDB.get('/crabserver/dev/fileusertransfers',
                                   data=encodeRequest({'subresource': 'getById', 'id': docId}))
        except Exception:
            logger.exception("Error updating failed docs.")
            continue
        document = oracleOutputMapping(docbyId, None)[0]
        logger.debug("Document: %s" % document)

        try:
            fileDoc = dict()
            fileDoc['asoworker'] = 'asodciangot1'
            fileDoc['subresource'] = 'updatePublication'
            fileDoc['list_of_ids'] = docId

            fileDoc['list_of_publication_state'] = 'FAILED'
            #if force_failure or document['publish_retry_count'] > self.max_retry:
            #    fileDoc['list_of_publication_state'] = 'FAILED'
            #else:
            #    fileDoc['list_of_publication_state'] = 'RETRY'
            # TODO: implement retry
            fileDoc['list_of_retry_value'] = 1
            fileDoc['list_of_failure_reason'] = failure_reason

            logger.debug("fileDoc: %s " % fileDoc)

            result = oracleDB.post('/crabserver/dev/filetransfers',
                                   data=encodeRequest(fileDoc))
            logger.debug("updated: %s " % docId)
        except Exception as ex:
            msg = "Error updating document: %s" % fileDoc
            msg += str(ex)
            msg += str(traceback.format_exc())
            logger.error(msg)
            continue


def publishInDBS3( taskname ):
    """

    """
    logger = logging.getLogger(taskname)
    logging.basicConfig(filename=taskname+'.log', level=logging.INFO)

    
    logger.info("Getting files to publish")

    toPublish = []
    # TODO move from new to done when processed
    with open("/tmp/"+taskname+".json") as f:
        toPublish = json.load(f)

    workflow = taskname 

    if not workflow:
        logger.info("NO TASKNAME: %s" % toPublish[0])
    for k, v in toPublish[0].iteritems():
        if k == 'taskname':
            logger.info("Starting: %s: %s" % (k, v))
    wfnamemsg = "%s: " % (workflow)

    user = toPublish[0]["User"]
    try:
        group = toPublish[0]["Group"]
        role = toPublish[0]["Role"]
    except:
        group = ""
        role = ""

    if not group or group in ['null']:
        group = ""
    if not role or role in ['null']:
        role = ""

    userDN = toPublish[0]["UserDN"]
    pnn = toPublish[0]["Destination"]
    logger.info(wfnamemsg+" "+user)

    READ_PATH = "/DBSReader"
    READ_PATH_1 = "/DBSReader/"

    opsProxy = '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy'

    # TODO: get user role and group
    try:
        proxy = Proxy(userDN, group, role, logger)
    except:
        logger.exception("Failed to retrieve user proxy")

    logger.info("userProxy_"+user)
    oracelInstance = "vocms035.cern.ch"
    oracleDB = HTTPRequests(oracelInstance,
                            opsProxy,
                            opsProxy)

    fileDoc = dict()
    fileDoc['subresource'] = 'search'
    fileDoc['workflow'] = workflow


    try:
        results = oracleDB.get('/crabserver/dev/task',
                               data=encodeRequest(fileDoc))
    except Exception as ex:
        logger.error("Failed to get acquired publications from oracleDB for %s: %s" % (workflow, ex))

    logger.info(results[0]['desc']['columns'])

    try:
        inputDatasetIndex = results[0]['desc']['columns'].index("tm_input_dataset")
        inputDataset = results[0]['result'][inputDatasetIndex]
        sourceURLIndex = results[0]['desc']['columns'].index("tm_dbs_url")
        sourceURL = results[0]['result'][sourceURLIndex]
        publish_dbs_urlIndex = results[0]['desc']['columns'].index("tm_publish_dbs_url")
        publish_dbs_url = results[0]['result'][publish_dbs_urlIndex]

        #sourceURL = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        if not sourceURL.endswith(READ_PATH) and not sourceURL.endswith(READ_PATH_1):
            sourceURL += READ_PATH


    except Exception:
        logger.exception("ERROR")
    ## When looking up parents may need to look in global DBS as well.
    globalURL = sourceURL
    globalURL = globalURL.replace('phys01', 'global')
    globalURL = globalURL.replace('phys02', 'global')
    globalURL = globalURL.replace('phys03', 'global')
    globalURL = globalURL.replace('caf', 'global')

    pr = os.environ.get("SOCKS5_PROXY")
    logger.info(wfnamemsg+"Source API URL: %s" % sourceURL)
    sourceApi = dbsClient.DbsApi(url=sourceURL, proxy=pr)
    logger.info(wfnamemsg+"Global API URL: %s" % globalURL)
    globalApi = dbsClient.DbsApi(url=globalURL, proxy=pr)

    WRITE_PATH = "/DBSWriter"
    MIGRATE_PATH = "/DBSMigrate"
    READ_PATH = "/DBSReader"

    if publish_dbs_url.endswith(WRITE_PATH):
        publish_read_url = publish_dbs_url[:-len(WRITE_PATH)] + READ_PATH
        publish_migrate_url = publish_dbs_url[:-len(WRITE_PATH)] + MIGRATE_PATH
    else:
        publish_migrate_url = publish_dbs_url + MIGRATE_PATH
        publish_read_url = publish_dbs_url + READ_PATH
        publish_dbs_url += WRITE_PATH

    try:
        logger.debug(wfnamemsg+"Destination API URL: %s" % publish_dbs_url)
        destApi = dbsClient.DbsApi(url=publish_dbs_url, proxy=pr)
        logger.debug(wfnamemsg+"Destination read API URL: %s" % publish_read_url)
        destReadApi = dbsClient.DbsApi(url=publish_read_url, proxy=pr)
        logger.debug(wfnamemsg+"Migration API URL: %s" % publish_migrate_url)
        migrateApi = dbsClient.DbsApi(url=publish_migrate_url, proxy=pr)
    except:
        logger.exception('Wrong DBS URL %s' % publish_dbs_url)
        return "FAILED"

    logger.info("inputDataset: %s" % inputDataset)
    noInput = len(inputDataset.split("/")) <= 3

    # TODO: fix dbs dep
    if not noInput:
        try:
            existing_datasets = sourceApi.listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
            primary_ds_type = existing_datasets[0]['primary_ds_type']
            # There's little chance this is correct, but it's our best guess for now.
            # CRAB2 uses 'crab2_tag' for all cases
            existing_output = destReadApi.listOutputConfigs(dataset=inputDataset)
        except:
            logger.exception('Wrong DBS URL %s' % publish_dbs_url)
            return "FAILED"
        if not existing_output:
            msg = "Unable to list output config for input dataset %s." % (inputDataset)
            logger.error(wfnamemsg+msg)
            global_tag = 'crab3_tag'
        else:
            global_tag = existing_output[0]['global_tag']
    else:
        msg = "This publication appears to be for private MC."
        logger.info(wfnamemsg+msg)
        primary_ds_type = 'mc'
        global_tag = 'crab3_tag'

    acquisition_era_name = "CRAB"
    processing_era_config = {'processing_version': 1, 'description': 'CRAB3_processing_era'}

    appName = 'cmsRun'
    appVer = toPublish[0]["swversion"]
    pset_hash = toPublish[0]['publishname'].split("-")[-1]
    gtag = str(toPublish[0]['globaltag'])
    if gtag == "None":
        gtag = global_tag
    try:
        if toPublish[0]['acquisitionera'] and not toPublish[0]['acquisitionera'] in ["null"]:
            acquisitionera = str(toPublish[0]['acquisitionera'])
        else:
            acquisitionera = acquisition_era_name
    except:
        acquisitionera = acquisition_era_name

    empty, primName, procName, tier = toPublish[0]['outdataset'].split('/')

    primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}
    msg = "About to insert primary dataset: %s" % (str(primds_config))
    logger.debug(wfnamemsg+msg)
    destApi.insertPrimaryDataset(primds_config)
    msg = "Successfully inserted primary dataset %s." % (primName)
    logger.debug(wfnamemsg+msg)

    final = {}
    failed = []
    failed_reason = ''
    publish_in_next_iteration = []
    published = []

    dataset = toPublish[0]['outdataset']
    # Find all (valid) files already published in this dataset.
    try:
        existingDBSFiles = destReadApi.listFiles(dataset=dataset, detail=True)
        existingFiles = [f['logical_file_name'] for f in existingDBSFiles]
        existingFilesValid = [f['logical_file_name'] for f in existingDBSFiles if f['is_file_valid']]
        msg = "Dataset %s already contains %d files" % (dataset, len(existingFiles))
        msg += " (%d valid, %d invalid)." % (len(existingFilesValid), len(existingFiles) - len(existingFilesValid))
        logger.info(wfnamemsg+msg)
        final['existingFiles'] = len(existingFiles)
    except Exception as ex:
        msg = "Error when listing files in DBS: %s" % (str(ex))
        msg += "\n%s" % (str(traceback.format_exc()))
        logger.error(wfnamemsg+msg)
        return "FAILED"

    # check if actions are needed
    workToDo = False

    for fileTo in toPublish:
        if fileTo['lfn'] not in existingFilesValid:
            workToDo = True

    if not workToDo:
        msg = "Nothing uploaded, %s has these files already or not enough files." % (dataset)
        logger.info(wfnamemsg+msg)
        return "NOTHING TO DO"

    acquisition_era_config = {'acquisition_era_name': acquisitionera, 'start_date': 0}

    output_config = {'release_version': appVer,
                     'pset_hash': pset_hash,
                     'app_name': appName,
                     'output_module_label': 'o',
                     'global_tag': global_tag,
                    }
    msg = "Published output config."
    logger.debug(wfnamemsg+msg)

    dataset_config = {'dataset': dataset,
                      'processed_ds_name': procName,
                      'data_tier_name': tier,
                      'acquisition_era_name': acquisitionera,
                      'dataset_access_type': 'VALID',
                      'physics_group_name': 'CRAB3',
                      'last_modification_date': int(time.time()),
                     }
    msg = "About to insert dataset: %s" % (str(dataset_config))
    logger.info(wfnamemsg+msg)
    del dataset_config['acquisition_era_name']

    # List of all files that must (and can) be published.
    dbsFiles = []
    dbsFiles_f = []
    # Set of all the parent files from all the files requested to be published.
    parentFiles = set()
    # Set of parent files for which the migration to the destination DBS instance
    # should be skipped (because they were not found in DBS).
    parentsToSkip = set()
    # Set of parent files to migrate from the source DBS instance
    # to the destination DBS instance.
    localParentBlocks = set()
    # Set of parent files to migrate from the global DBS instance
    # to the destination DBS instance.
    globalParentBlocks = set()

    # Loop over all files to publish.
    for file in toPublish:
        logger.info(file)
        # Check if this file was already published and if it is valid.
        if file['lfn'] not in existingFilesValid:
            # We have a file to publish.
            # Get the parent files and for each parent file do the following:
            # 1) Add it to the list of parent files.
            # 2) Find the block to which it belongs and insert that block name in
            #    (one of) the set of blocks to be migrated to the destination DBS.
            for parentFile in list(file['parents']):
                if parentFile not in parentFiles:
                    parentFiles.add(parentFile)
                    # Is this parent file already in the destination DBS instance?
                    # (If yes, then we don't have to migrate this block.)
                    blocksDict = destReadApi.listBlocks(logical_file_name=parentFile)
                    if not blocksDict:
                        # No, this parent file is not in the destination DBS instance.
                        # Maybe it is in the same DBS instance as the input dataset?
                        blocksDict = sourceApi.listBlocks(logical_file_name=parentFile)
                        if blocksDict:
                            # Yes, this parent file is in the same DBS instance as the input dataset.
                            # Add the corresponding block to the set of blocks from the source DBS
                            # instance that have to be migrated to the destination DBS.
                            localParentBlocks.add(blocksDict[0]['block_name'])
                        else:
                            # No, this parent file is not in the same DBS instance as input dataset.
                            # Maybe it is in global DBS instance?
                            blocksDict = globalApi.listBlocks(logical_file_name=parentFile)
                            if blocksDict:
                                # Yes, this parent file is in global DBS instance.
                                # Add the corresponding block to the set of blocks from global DBS
                                # instance that have to be migrated to the destination DBS.
                                globalParentBlocks.add(blocksDict[0]['block_name'])
                    # If this parent file is not in the destination DBS instance, is not
                    # the source DBS instance, and is not in global DBS instance, then it
                    # means it is not known to DBS and therefore we can not migrate it.
                    # Put it in the set of parent files for which migration should be skipped.
                    if not blocksDict:
                        parentsToSkip.add(parentFile)
                # If this parent file should not be migrated because it is not known to DBS,
                # we remove it from the list of parents in the file-to-publish info dictionary
                # (so that when publishing, this "parent" file will not appear as a parent).
                if parentFile in parentsToSkip:
                    msg = "Skipping parent file %s, as it doesn't seem to be known to DBS." % (parentFile)
                    logger.info(wfnamemsg+msg)
                    if parentFile in file['parents']:
                        file['parents'].remove(parentFile)
            # Add this file to the list of files to be published.
            dbsFiles.append(format_file_3(file))
            dbsFiles_f.append(file)
        #print file
        published.append(file['SourceLFN'])
    # Print a message with the number of files to publish.
    msg = "Found %d files not already present in DBS which will be published." % (len(dbsFiles))
    logger.info(wfnamemsg+msg)

    # If there are no files to publish, continue with the next dataset.
    if len(dbsFiles_f) == 0:
        msg = "Nothing to do for this dataset."
        logger.info(wfnamemsg+msg)
        return "NOTHING TO DO"

    # Migrate parent blocks before publishing.
    # First migrate the parent blocks that are in the same DBS instance
    # as the input dataset.
    if localParentBlocks:
        msg = "List of parent blocks that need to be migrated from %s:\n%s" % (sourceApi.url, localParentBlocks)
        logger.info(wfnamemsg+msg)
        statusCode, failureMsg = migrateByBlockDBS3(workflow,
                                                    migrateApi,
                                                    destReadApi,
                                                    sourceApi,
                                                    inputDataset,
                                                    localParentBlocks
                                                   )
        if statusCode:
            failureMsg += " Not publishing any files."
            logger.info(wfnamemsg+failureMsg)
            failed.extend([f['SourceLFN'] for f in dbsFiles_f])
            failure_reason = failureMsg
            published = [x for x in published[dataset] if x not in failed[dataset]]
            return "NOTHING TO DO"
    # Then migrate the parent blocks that are in the global DBS instance.
    if globalParentBlocks:
        msg = "List of parent blocks that need to be migrated from %s:\n%s" % (globalApi.url, globalParentBlocks)
        logger.info(wfnamemsg+msg)
        statusCode, failureMsg = migrateByBlockDBS3(workflow, migrateApi, destReadApi, globalApi, inputDataset, globalParentBlocks)
        if statusCode:
            failureMsg += " Not publishing any files."
            logger.info(wfnamemsg+failureMsg)
            failed.extend([f['SourceLFN'] for f in dbsFiles_f])
            failure_reason = failureMsg
            published = [x for x in published[dataset] if x not in failed[dataset]]
            return "NOTHING TO DO"
    # Publish the files in blocks. The blocks must have exactly max_files_per_block
    # files, unless there are less than max_files_per_block files to publish to
    # begin with. If there are more than max_files_per_block files to publish,
    # publish as many blocks as possible and leave the tail of files for the next
    # PublisherWorker call, unless forced to published.
    block_count = 0
    count = 0
    max_files_per_block = 100
    while True:
        block_name = "%s#%s" % (dataset, str(uuid.uuid4()))
        files_to_publish = dbsFiles[count:count+max_files_per_block]
        try:
            block_config = {'block_name': block_name, 'origin_site_name': pnn, 'open_for_writing': 0}
            msg = "Inserting files %s into block %s." % ([f['logical_file_name']
                                                          for f in files_to_publish], block_name)
            logger.info(wfnamemsg+msg)
            blockDump = createBulkBlock(output_config, processing_era_config,
                                        primds_config, dataset_config,
                                        acquisition_era_config, block_config, files_to_publish)
            #logger.debug(wfnamemsg+"Block to insert: %s\n %s" % (blockDump, destApi.__dict__ ))

            destApi.insertBulkBlock(blockDump)
            block_count += 1
        except Exception as ex:
            logger.error("Error for files: %s" % [f['SourceLFN'] for f in toPublish])
            failed.extend([f['SourceLFN'] for f in toPublish])
            msg = "Error when publishing (%s) " % ", ".join(failed)
            msg += str(ex)
            msg += str(traceback.format_exc())
            logger.error(wfnamemsg+msg)
            failure_reason = str(ex)
        count += max_files_per_block
        files_to_publish_next = dbsFiles_f[count:count+max_files_per_block]
        if len(files_to_publish_next) < max_files_per_block:
            publish_in_next_iteration.extend([f["SourceLFN"] for f in files_to_publish_next])
            break
    published = [x for x in published if x not in failed + publish_in_next_iteration]
    # Fill number of files/blocks published for this dataset.
    final['files'] = len(dbsFiles) - len(failed) - len(publish_in_next_iteration)
    final['blocks'] = block_count
    # Print a publication status summary for this dataset.
    msg = "End of publication status for dataset %s:" % (dataset)
    msg += " failed (%s) %s" % (len(failed), failed)
    msg += ", published (%s) %s" % (len(published), published)
    msg += ", publish_in_next_iteration (%s) %s" % (len(publish_in_next_iteration),
                                                    publish_in_next_iteration)
    msg += ", results %s" % (final)
    logger.info(wfnamemsg+msg)

     
    try:
        if published:
            mark_good(workflow, published, oracleDB, logger)
        if failed:
            logger.debug("Failed files: %s " % failed)
            mark_failed(workflow, failed, oracleDB, logger, failure_reason, True)
    except:
        logger.exception("Status update failed")

    return 0

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Publish datasets.')
    parser.add_argument('taskname', metavar='taskname', type=str, help='taskname')

    args = parser.parse_args()

    print publishInDBS3( args.taskname )

