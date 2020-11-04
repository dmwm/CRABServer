# pylint: disable=C0103, W0703, R0912, R0914, R0915
"""
this is a standalone script. It is spawned by PushisherMaster or could
be executed from CLI (in the Publisher environment) to retry or debug failures
"""
from __future__ import division
from __future__ import print_function
import os
import uuid
import time
import logging
import sys
import json
import traceback
import argparse

import dbs.apis.dbsClient as dbsClient
from ServerUtilities import getHashLfn, encodeRequest
from ServerUtilities import SERVICE_INSTANCES
from TaskWorker.WorkerExceptions import ConfigException
from RESTInteractions import HTTPRequests
from WMCore.Configuration import loadConfigurationFile


def format_file_3(file_):
    """
    format file for DBS
    """
    nf = {'logical_file_name': file_['lfn'],
          'file_type': 'EDM',
          'check_sum': unicode(file_['cksum']),
          'event_count': file_['inevents'],
          'file_size': file_['filesize'],
          'adler32': file_['adler32'],
          'file_parent_list': [{'file_parent_lfn': i} for i in set(file_['parents'])],
         }
    file_lumi_list = []
    for run, lumis in file_['runlumi'].items():
        for lumi in lumis:
            file_lumi_list.append({'lumi_section_num': int(lumi), 'run_num': int(run)})
    nf['file_lumi_list'] = file_lumi_list
    if file_.get("md5") != "asda" and file_.get("md5") != "NOTSET": # asda is the silly value that MD5 defaults to
        nf['md5'] = file_['md5']
    return nf


def createBulkBlock(output_config, processing_era_config, primds_config, \
                    dataset_config, acquisition_era_config, block_config, files):
    """
    manage blocks
    """
    file_conf_list = []
    file_parent_list = []
    for file_ in files:
        file_conf = output_config.copy()
        file_conf_list.append(file_conf)
        file_conf['lfn'] = file_['logical_file_name']
        for parent_lfn in file_.get('file_parent_list', []):
            file_parent_list.append({'logical_file_name': file_['logical_file_name'],
                                     'parent_logical_file_name': parent_lfn['file_parent_lfn']})
        del file_['file_parent_list']
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
    blockDump['block']['block_size'] = sum([int(file_[u'file_size']) for file_ in files])
    return blockDump


def migrateByBlockDBS3(taskname, migrateApi, destReadApi, sourceApi, dataset, blocks=None, verbose=False):
    """
    Submit one migration request for each block that needs to be migrated.
    If blocks argument is not specified, migrate the whole dataset.
    """
    #wfnamemsg = "%s: " % taskname
    logger = logging.getLogger(taskname)
    logging.basicConfig(filename=taskname+'.log', level=logging.INFO)

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
        logger.info(msg)
    numBlocksToMigrate = len(blocksToMigrate)
    if numBlocksToMigrate == 0:
        msg = "No migration needed."
        logger.info(msg)
    else:
        msg = "Have to migrate %d blocks from %s to %s." % (numBlocksToMigrate, sourceApi.url, destReadApi.url)
        logger.info(msg)
        if verbose:
            msg = "List of blocks to migrate:\n%s." % (", ".join(blocksToMigrate))
            logger.debug(msg)
        msg = "Submitting %d block migration requests to DBS3 ..." % (numBlocksToMigrate)
        logger.info(msg)
        numBlocksAtDestination = 0
        numQueuedUnkwonIds = 0
        numFailedSubmissions = 0
        migrationIdsInProgress = []
        for block in list(blocksToMigrate):
            # Submit migration request for this block.
            (reqid, atDestination, alreadyQueued) = requestBlockMigration(taskname, migrateApi, sourceApi, block)
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
            logger.info(msg)
        if numFailedSubmissions > 0:
            msg = "%d block migration requests failed to be submitted." % numFailedSubmissions
            msg += " Will retry them later."
            logger.info(msg)
        if numQueuedUnkwonIds > 0:
            msg = "%d block migration requests were already queued," % numQueuedUnkwonIds
            msg += " but could not retrieve their request id."
            logger.info(msg)
        numMigrationsInProgress = len(migrationIdsInProgress)
        if numMigrationsInProgress == 0:
            msg = "No migrations in progress."
            logger.info(msg)
        else:
            msg = "%d block migration requests successfully submitted." % numMigrationsInProgress
            logger.info(msg)
            msg = "List of migration requests ids: %s" % migrationIdsInProgress
            logger.info(msg)
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
            logger.info(msg)
            for _ in range(numTimes):
                msg = "%d block migrations in progress." % numMigrationsInProgress
                msg += " Will check migrations status in %d seconds." % waitTime
                logger.info(msg)
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
                        logger.error(msg)
                    else:
                        if state == 2:
                            msg = "Migration id %d succeeded." % reqid
                            logger.info(msg)
                            migrationIdsInProgress.remove(reqid)
                            numSuccessfulMigrations += 1
                        if state == 9:
                            msg = "Migration id %d terminally failed." % reqid
                            logger.info(msg)
                            msg = "Full status for migration id %d:\n%s" % (reqid, str(status))
                            logger.info(msg)
                            migrationIdsInProgress.remove(reqid)
                            numFailedMigrations += 1
                        if state == 3:
                            if retry < 3:
                                msg = "Migration id %d failed (retry %d), but should be retried." % (reqid, retry)
                                logger.info(msg)
                            else:
                                msg = "Migration id %d failed (retry %d)." % (reqid, retry)
                                logger.info(msg)
                                msg = "Full status for migration id %d:\n%s" % (reqid, str(status))
                                logger.info(msg)
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
                logger.info(msg)
                return 1, "Migration of %s is taking too long." % (dataset)
        msg = "Migration of %s has finished." % dataset
        logger.info(msg)
        msg = "Migration status summary (from %d input blocks to migrate):" % numBlocksToMigrate
        msg += " at destination = %d," % numBlocksAtDestination
        msg += " succeeded = %d," % numSuccessfulMigrations
        msg += " failed = %d," % numFailedMigrations
        msg += " submission failed = %d," % numFailedSubmissions
        msg += " queued with unknown id = %d." % numQueuedUnkwonIds
        logger.info(msg)
        # If there were failed migrations, return with status 2.
        if numFailedMigrations > 0 or numFailedSubmissions > 0:
            msg = "Some blocks failed to be migrated."
            logger.info(msg)
            return 2, "Migration of %s failed." % (dataset)
        # If there were no failed migrations, but we could not retrieve the request id
        # from some already queued requests, return with status 3.
        if numQueuedUnkwonIds > 0:
            msg = "Some block migrations were already queued, but failed to retrieve their request id."
            logger.info(msg)
            return 3, "Migration of %s in unknown status." % dataset
        if (numBlocksAtDestination + numSuccessfulMigrations) != numBlocksToMigrate:
            msg = "Something unexpected has happened."
            msg += " The numbers in the migration summary are not consistent."
            msg += " Make sure there is no bug in the code."
            logger.info(msg)
            return 4, "Migration of %s in some inconsistent status." % dataset
        msg = "Migration completed."
        logger.info(msg)
    try:
        migratedDataset = destReadApi.listDatasets(dataset=dataset, detail=True, dataset_access_type='*')
        if not migratedDataset or migratedDataset[0].get('dataset', None) != dataset:
            return 4, "Migration of %s in some inconsistent status." % dataset
    except Exception as ex:
        logger.exception("Migration check failed.")
        return 4, "Migration check failed. %s" % ex
    return 0, ""


def requestBlockMigration(taskname, migrateApi, sourceApi, block):
    """
    Submit migration request for one block, checking the request output.
    """
    logger = logging.getLogger(taskname)
#    logging.basicConfig(filename=taskname+'.log', level=logging.INFO, format=config.General.logMsgFormat)

    atDestination = False
    alreadyQueued = False
    reqid = None
    msg = "Submiting migration request for block %s ..." % block
    logger.info(msg)
    sourceURL = sourceApi.url
    data = {'migration_url': sourceURL, 'migration_input': block}
    try:
        result = migrateApi.submitMigration(data)
    except Exception as ex:
        if "is already at destination" in ex:
            msg = "Block is already at destination."
            logger.info(msg)
            atDestination = True
        else:
            msg = "Request to migrate %s failed." % block
            msg += "\nRequest detail: %s" % data
            msg += "\nDBS3 exception: %s" % ex
            logger.error(msg)
    if not atDestination:
        msg = "Result of migration request: %s" % str(result)
        logger.info(msg)
        reqid = result.get('migration_details', {}).get('migration_request_id')
        report = result.get('migration_report')
        if "Migration terminally failed" in report:
            migTime = time.gmtime(result['migration_details']['creation_date'])
            logger.debug("MigFailed at %s", migTime)
            # in May 2019 has a storm of failed migration which needed the following cleanup
            # keep the code in case we ever need to do the same again
            if migTime.tm_year == 2019 and migTime.tm_mon == 5 and migTime.tm_mday < 21:
                logger.debug("Failed migration %s requested on %s. Remove it",
                    reqid, time.ctime(result['migration_deta     ils']['creation_date']))
                mdic = {'migration_rqst_id': reqid} # pylint: disable=unused-variable
                migrateApi.removeMigration({'migration_rqst_id': reqid})
                logger.debug("  and submit again")
                result = migrateApi.submitMigration(data)
                reqid = result.get('migration_details', {}).get('migration_request_id')
                report = result.get('migration_report')
        if reqid is None:
            msg = "Migration request failed to submit."
            msg += "\nMigration request results: %s" % str(result)
            logger.error(msg)
        if "REQUEST ALREADY QUEUED" in report:
            # Request could be queued in another thread, then there would be
            # no id here, so look by block and use the id of the queued request.
            alreadyQueued = True
            try:
                status = migrateApi.statusMigration(block_name=block)
                reqid = status[0].get('migration_request_id')
            except Exception:
                msg = "Could not get status for already queued migration of block %s." % (block)
                logger.error(msg)
    return reqid, atDestination, alreadyQueued

def publishInDBS3(config, taskname, verbose):
    """
    Publish output from one task in DBS
    """

    def mark_good(files, crabServer, logger):
        """
        Mark the list of files as tranferred
        """

        msg = "Marking %s file(s) as published." % len(files)
        logger.info(msg)
        if dryRun:
            logger.info("DryRun: skip marking good file")
            return

        nMarked = 0
        for lfn in files:
            data = {}
            source_lfn = lfn
            docId = getHashLfn(source_lfn)
            data['asoworker'] = config.General.asoworker
            data['subresource'] = 'updatePublication'
            data['list_of_ids'] = docId
            data['list_of_publication_state'] = 'DONE'
            data['list_of_retry_value'] = 1
            data['list_of_failure_reason'] = ''

            try:
                result = crabServer.post(REST_filetransfers, data=encodeRequest(data))
                logger.debug("updated DocumentId: %s lfn: %s Result %s", docId, source_lfn, result)
            except Exception as ex:
                logger.error("Error updating status for DocumentId: %s lfn: %s", docId, source_lfn)
                logger.error("Error reason: %s", ex)

            nMarked += 1
            if nMarked % 10 == 0:
                logger.info('marked %d files', nMarked)

    def mark_failed(files, crabServer, logger, failure_reason=""):
        """
        Something failed for these files so increment the retry count
        """
        msg = "Marking %s file(s) as failed" % len(files)
        logger.info(msg)
        if dryRun:
            logger.debug("DryRun: skip marking failes files")
            return

        nMarked = 0
        for lfn in files:
            source_lfn = lfn
            docId = getHashLfn(source_lfn)
            data = dict()
            data['asoworker'] = config.General.asoworker
            data['subresource'] = 'updatePublication'
            data['list_of_ids'] = docId
            data['list_of_publication_state'] = 'FAILED'
            data['list_of_retry_value'] = 1
            data['list_of_failure_reason'] = failure_reason

            logger.debug("data: %s ", data)
            try:
                result = crabServer.post(REST_filetransfers, data=encodeRequest(data))
                logger.debug("updated DocumentId: %s lfn: %s Result %s", docId, source_lfn, result)
            except Exception as ex:
                logger.error("Error updating status for DocumentId: %s lfn: %s", docId, source_lfn)
                logger.error("Error reason: %s", ex)

            nMarked += 1
            if nMarked % 10 == 0:
                logger.info('marked %d files', nMarked)

    def createLogdir(dirname):
        """
        Create the directory dirname ignoring erors in case it exists. Exit if
        the directory cannot be created.
        """
        try:
            os.mkdir(dirname)
        except OSError as ose:
            if ose.errno != 17: #ignore the "Directory already exists error"
                print(str(ose))
                print("The task worker need to access the '%s' directory" % dirname)
                sys.exit(1)

    taskFilesDir = config.General.taskFilesDir
    dryRun = config.TaskPublisher.dryRun
    username = taskname.split(':')[1].split('_')[0]
    logdir = config.General.logsDir + '/tasks/' + username
    logfile = logdir + '/' + taskname + '.log'
    createLogdir(logdir)
    logger = logging.getLogger(taskname)
    logging.basicConfig(filename=logfile, level=logging.INFO, format=config.TaskPublisher.logMsgFormat)
    if verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("Getting files to publish")

    toPublish = []
    # TODO move from new to done when processed
    fname = taskFilesDir + taskname + ".json"
    with open(fname) as f:
        toPublish = json.load(f)

    if not toPublish:
        logger.info("Empty data file %s", fname)
        return "NOTHING TO DO"


    pnn = toPublish[0]["Destination"]

    # CRABServer REST API's (see CRABInterface)
    try:
        instance = config.General.instance
    except:
        msg = "No instance provided: need to specify config.General.instance in the configuration"
        raise ConfigException(msg)

    if instance in SERVICE_INSTANCES:
        logger.info('Will connect to CRAB service: %s', instance)
        restHost = SERVICE_INSTANCES[instance]['restHost']
        dbInstance = SERVICE_INSTANCES[instance]['dbInstance']
    else:
        msg = "Invalid instance value '%s'" % instance
        raise ConfigException(msg)
    if instance == 'other':
        logger.info('Will use restHost and dbInstance from config file')
        try:
            restHost = config.General.restHost
            dbInstance = config.General.dbInstance
        except:
            msg = "Need to specify config.General.restHost and dbInstance in the configuration"
            raise ConfigException(msg)

    restURInoAPI = '/crabserver/' + dbInstance
    logger.info('Will connect to CRAB Data Base via URL: https://%s/%s', restHost, restURInoAPI)

    # CRAB REST API's
    REST_filetransfers = restURInoAPI + '/filetransfers'
    REST_task = restURInoAPI + '/task'
    crabServer = HTTPRequests(url=restHost,
                              localcert=config.General.serviceCert,
                              localkey=config.General.serviceKey,
                              retry=3)


    data = dict()
    data['subresource'] = 'search'
    data['workflow'] = taskname

    try:
        results = crabServer.get(REST_task, data=encodeRequest(data))
    except Exception as ex:
        logger.error("Failed to get acquired publications from oracleDB for %s: %s", taskname, ex)
        return "FAILED"

    if verbose:
        logger.info(results[0]['desc']['columns'])

    try:
        inputDatasetIndex = results[0]['desc']['columns'].index("tm_input_dataset")
        inputDataset = results[0]['result'][inputDatasetIndex]
        sourceURLIndex = results[0]['desc']['columns'].index("tm_dbs_url")
        sourceURL = results[0]['result'][sourceURLIndex]
        publish_dbs_urlIndex = results[0]['desc']['columns'].index("tm_publish_dbs_url")
        publish_dbs_url = results[0]['result'][publish_dbs_urlIndex]

        if not sourceURL.endswith("/DBSReader") and not sourceURL.endswith("/DBSReader/"):
            sourceURL += "/DBSReader"
    except Exception:
        logger.exception("ERROR")
    # When looking up parents may need to look in global DBS as well.
    globalURL = sourceURL
    globalURL = globalURL.replace('phys01', 'global')
    globalURL = globalURL.replace('phys02', 'global')
    globalURL = globalURL.replace('phys03', 'global')
    globalURL = globalURL.replace('caf', 'global')

    # DBS client relies on X509 env. vars
    os.environ['X509_USER_CERT'] = config.General.serviceCert
    os.environ['X509_USER_KEY'] = config.General.serviceKey

    logger.info("Source API URL: %s", sourceURL)
    sourceApi = dbsClient.DbsApi(url=sourceURL)
    logger.info("Global API URL: %s", globalURL)
    globalApi = dbsClient.DbsApi(url=globalURL)

    if publish_dbs_url.endswith('/DBSWriter'):
        publish_read_url = publish_dbs_url[:-len('/DBSWriter')] + '/DBSReader'
        publish_migrate_url = publish_dbs_url[:-len('/DBSWriter')] + '/DBSMigrate'
    else:
        publish_migrate_url = publish_dbs_url + '/DBSMigrate'
        publish_read_url = publish_dbs_url + '/DBSReader'
        publish_dbs_url += '/DBSWriter'

    try:
        logger.info("Destination API URL: %s", publish_dbs_url)
        destApi = dbsClient.DbsApi(url=publish_dbs_url)
        logger.info("Destination read API URL: %s", publish_read_url)
        destReadApi = dbsClient.DbsApi(url=publish_read_url)
        logger.info("Migration API URL: %s", publish_migrate_url)
        migrateApi = dbsClient.DbsApi(url=publish_migrate_url)
    except Exception:
        logger.exception('Wrong DBS URL %s', publish_dbs_url)
        return "FAILED"

    logger.info("inputDataset: %s", inputDataset)
    noInput = len(inputDataset.split("/")) <= 3

    # TODO: fix dbs dep
    if not noInput:
        try:
            existing_datasets = sourceApi.listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
            primary_ds_type = existing_datasets[0]['primary_ds_type']
            # There's little chance this is correct, but it's our best guess for now.
            # CRAB2 uses 'crab2_tag' for all cases
            existing_output = destReadApi.listOutputConfigs(dataset=inputDataset)
        except Exception:
            logger.exception('Wrong DBS URL %s', publish_dbs_url)
            return "FAILED"
        if not existing_output:
            msg = "Unable to list output config for input dataset %s." % (inputDataset)
            logger.error(msg)
            global_tag = 'crab3_tag'
        else:
            global_tag = existing_output[0]['global_tag']
    else:
        msg = "This publication appears to be for private MC."
        logger.info(msg)
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
    except Exception:
        acquisitionera = acquisition_era_name

    _, primName, procName, tier = toPublish[0]['outdataset'].split('/')

    primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}
    msg = "About to insert primary dataset: %s" % (str(primds_config))
    logger.debug(msg)
    if dryRun:
        logger.info("DryRun: skip insertPrimaryDataset")
    else:
        destApi.insertPrimaryDataset(primds_config)
        msg = "Successfully inserted primary dataset %s." % (primName)
        logger.info(msg)

    final = {}
    failed = []
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
        logger.info(msg)
        final['existingFiles'] = len(existingFiles)
    except Exception as ex:
        msg = "Error when listing files in DBS: %s" % (str(ex))
        msg += "\n%s" % (str(traceback.format_exc()))
        logger.error(msg)
        return "FAILED"

    # check if actions are needed
    workToDo = False

    for fileTo in toPublish:
        #print(existingFilesValid)
        if fileTo['lfn'] not in existingFilesValid:
            workToDo = True
            break

    if not workToDo:
        msg = "Nothing uploaded, %s has these files already." % (dataset)
        logger.info(msg)
        logger.info('Make sure those files are marked as Done')
        # docId is the has of the source LFN i.e. the file in the tmp area at the running site
        files = [f['SourceLFN'] for f in toPublish]
        mark_good(files, crabServer, logger)

        return "NOTHING TO DO"

    acquisition_era_config = {'acquisition_era_name': acquisitionera, 'start_date': 0}

    output_config = {'release_version': appVer,
                     'pset_hash': pset_hash,
                     'app_name': appName,
                     'output_module_label': 'o',
                     'global_tag': global_tag,
                    }
    msg = "Published output config."
    logger.info(msg)

    dataset_config = {'dataset': dataset,
                      'processed_ds_name': procName,
                      'data_tier_name': tier,
                      'dataset_access_type': 'VALID',
                      'physics_group_name': 'CRAB3',
                      'last_modification_date': int(time.time()),
                     }
    msg = "About to insert dataset: %s" % (str(dataset_config))
    logger.info(msg)

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
    for file_ in toPublish:
        if verbose:
            logger.info(file_)
        # Check if this file was already published and if it is valid.
        if file_['lfn'] not in existingFilesValid:
            # We have a file to publish.
            # Get the parent files and for each parent file do the following:
            # 1) Add it to the list of parent files.
            # 2) Find the block to which it belongs and insert that block name in
            #    (one of) the set of blocks to be migrated to the destination DBS.
            for parentFile in list(file_['parents']):
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
                    logger.info(msg)
                    if parentFile in file_['parents']:
                        file_['parents'].remove(parentFile)
            # Add this file to the list of files to be published.
            dbsFiles.append(format_file_3(file_))
            dbsFiles_f.append(file_)
        #print file
        published.append(file_['SourceLFN'])
        #published.append(file_['lfn'].replace("/store","/store/temp"))

    # Print a message with the number of files to publish.
    msg = "Found %d files not already present in DBS which will be published." % (len(dbsFiles))
    logger.info(msg)

    # If there are no files to publish, continue with the next dataset.
    if not dbsFiles_f:
        msg = "No file to publish to do for this dataset."
        logger.info(msg)
        return "NOTHING TO DO"

    # Migrate parent blocks before publishing.
    # First migrate the parent blocks that are in the same DBS instance
    # as the input dataset.
    if localParentBlocks:
        msg = "List of parent blocks that need to be migrated from %s:\n%s" % (sourceApi.url, localParentBlocks)
        logger.info(msg)
        if dryRun:
            logger.info("DryRun: skipping migration request")
        else:
            statusCode, failureMsg = migrateByBlockDBS3(taskname,
                                                        migrateApi,
                                                        destReadApi,
                                                        sourceApi,
                                                        inputDataset,
                                                        localParentBlocks,
                                                        verbose
                                                       )
            if statusCode:
                failureMsg += " Not publishing any files."
                logger.info(failureMsg)
                failed.extend([f['SourceLFN'] for f in dbsFiles_f])
                #failed.extend([f['lfn'].replace("/store","/store/temp") for f in dbsFiles_f])
                failure_reason = failureMsg
                published = [x for x in published[dataset] if x not in failed[dataset]]
                return "NOTHING TO DO"
    # Then migrate the parent blocks that are in the global DBS instance.
    if globalParentBlocks:
        msg = "List of parent blocks that need to be migrated from %s:\n%s" % (globalApi.url, globalParentBlocks)
        logger.info(msg)
        if dryRun:
            logger.info("DryRun: skipping migration request")
        else:
            statusCode, failureMsg = migrateByBlockDBS3(taskname, migrateApi, destReadApi, globalApi,
                                                        inputDataset, globalParentBlocks, verbose)
            if statusCode:
                failureMsg += " Not publishing any files."
                logger.info(failureMsg)
                failed.extend([f['SourceLFN'] for f in dbsFiles_f])
                #failed.extend([f['lfn'].replace("/store","/store/temp") for f in dbsFiles_f])
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
    max_files_per_block = config.General.max_files_per_block
    while True:
        block_name = "%s#%s" % (dataset, str(uuid.uuid4()))
        files_to_publish = dbsFiles[count:count+max_files_per_block]
        try:
            block_config = {'block_name': block_name, 'origin_site_name': pnn, 'open_for_writing': 0}
            if verbose:
                msg = "Inserting files %s into block %s." % ([f['logical_file_name']
                                                              for f in files_to_publish], block_name)
                logger.info(msg)
            blockDump = createBulkBlock(output_config, processing_era_config,
                                        primds_config, dataset_config,
                                        acquisition_era_config, block_config, files_to_publish)
            #logger.debug("Block to insert: %s\n %s" % (blockDump, destApi.__dict__ ))

            if dryRun:
                logger.info("DryRun: skip insertBulkBlock")
            else:
                destApi.insertBulkBlock(blockDump)
            block_count += 1
        except Exception as ex:
            #logger.error("Error for files: %s" % [f['SourceLFN'] for f in toPublish])
            logger.error("Error for files: %s", [f['lfn'] for f in toPublish])
            failed.extend([f['SourceLFN'] for f in toPublish])
            #failed.extend([f['lfn'].replace("/store","/store/temp") for f in toPublish])
            msg = "Error when publishing (%s) " % ", ".join(failed)
            msg += str(ex)
            msg += str(traceback.format_exc())
            logger.error(msg)
            failure_reason = str(ex)
            fname = '/tmp/failed-block-at-%s.txt' % time.time()
            with open(fname, 'w') as fd:
                fd.write(blockDump)
            logger.error("FAILING BLOCK SAVED AS %s", file)
        count += max_files_per_block
        files_to_publish_next = dbsFiles_f[count:count+max_files_per_block]
        if len(files_to_publish_next) < max_files_per_block:
            publish_in_next_iteration.extend([f["SourceLFN"] for f in files_to_publish_next])
            #publish_in_next_iteration.extend([f["lfn"].replace("/store","/store/temp") for f in files_to_publish_next])
            break
    published = [x for x in published if x not in failed + publish_in_next_iteration]
    # Fill number of files/blocks published for this dataset.
    final['files'] = len(dbsFiles) - len(failed) - len(publish_in_next_iteration)
    final['blocks'] = block_count
    # Print a publication status summary for this dataset.
    msg = "End of publication status for dataset %s:" % (dataset)
    msg += " failed %s" % len(failed)
    if verbose:
        msg += ": %s" % failed
    msg += ", published %s" % len(published)
    if verbose:
        msg += ": %s" % published
    msg += ", publish_in_next_iteration %s" % len(publish_in_next_iteration)
    if verbose:
        msg += ": %s" % publish_in_next_iteration
    msg += ", results %s" % (final)
    logger.info(msg)

    try:
        if published:
            mark_good(published, crabServer, logger)
            data['workflow'] = taskname
            data['subresource'] = 'updatepublicationtime'
            crabServer.post(REST_task, data=encodeRequest(data))
        if failed:
            logger.debug("Failed files: %s ", failed)
            mark_failed(failed, crabServer, logger, failure_reason)
    except Exception as ex:
        logger.exception("Status update failed: %s", ex)

    return 0

def main():
    """
    starting from json file prepared by PusblishMaster with info on filed to be published, does
    the actual DBS publication
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--configFile', help='Publisher configuration file', default='PublisherConfig.py')
    parser.add_argument('--taskname', help='taskname', required=True)
    parser.add_argument('--verbose', help='Verbose mode, print dictionaries', action='store_true')
    parser.add_argument('--dry', help='Dry run mode, no changes done in DBS', action='store_true')
    args = parser.parse_args()
    configFile = os.path.abspath(args.configFile)
    taskname = args.taskname
    verbose = args.verbose
    dryRun = args.dry
    modeMsg = " in DRY RUN mode" if dryRun else ""
    config = loadConfigurationFile(configFile)
    if dryRun:
        config.TaskPublisher.dryRun = True

    print("Will run%s with:\nconfigFile: %s\ntaskname  : %s\n" % (modeMsg, configFile, taskname))

    result = publishInDBS3(config, taskname, verbose)
    print("Completed with result: %s" % result)

if __name__ == '__main__':
    main()
