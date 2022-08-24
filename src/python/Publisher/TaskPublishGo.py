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
from datetime import datetime
import logging
import sys
import json
import traceback
import argparse
import pprint

from dbs.apis.dbsClient import DbsApi
from ServerUtilities import getHashLfn, encodeRequest
from ServerUtilities import SERVICE_INSTANCES
from TaskWorker.WorkerExceptions import ConfigException
from RESTInteractions import CRABRest
from WMCore.Configuration import loadConfigurationFile

PARASITIC_TESTING = False

def format_file_3(file_):
    """
    format file for DBS
    """
    nf = {'logical_file_name': file_['lfn'],
          'file_type': 'EDM',
          'check_sum': str(file_['cksum']),  # historically CRAB FILEMTETADATADB has the wrong type (int)
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
    blockDump['block']['block_size'] = sum([int(file_['file_size']) for file_ in files])
    return blockDump


def migrateByBlockDBS3(taskname, migrateApi, destReadApi, sourceApi, dataset, blocks, migLogDir, verbose=False):
    """
    Submit one migration request for each block that needs to be migrated.
    If blocks argument is not specified, migrate the whole dataset.
    Returns a 2-element ntuple : (exitcode, message)
    exit codes:  0 OK, 1 taking too long, 2 failure
    """
    #wfnamemsg = "%s: " % taskname
    logger = logging.getLogger(taskname)
    logging.basicConfig(filename=taskname+'.log', level=logging.INFO)

    if blocks:
        blocksToMigrate = set(blocks)
    else:
        # migration of a full dataset is better accomplished with a separate method
        raise NotImplementedError

    numBlocksToMigrate = len(blocksToMigrate)
    if numBlocksToMigrate == 0:
        msg = "No migration needed."
        logger.info(msg)
        return 0, ""
    msg = "Have to migrate %d blocks from %s to %s." % (numBlocksToMigrate, sourceApi.url, destReadApi.url)
    logger.info(msg)
    if verbose:
        msg = "List of blocks to migrate:\n%s." % (", ".join(blocksToMigrate))
        logger.debug(msg)
    msg = "Submitting %d block migration requests to DBS3 ..." % (numBlocksToMigrate)
    logger.info(msg)
    numFailedSubmissions = 0
    migrationsInProgress = []
    for block in list(blocksToMigrate):
        # Submit migration request for this block.
        ok = requestBlockMigration(taskname, migrateApi, sourceApi, block, migLogDir)
        if ok:
            migrationsInProgress.append(block)
        else:
            numFailedSubmissions += 1

    numMigrationsInProgress = len(migrationsInProgress)
    msg = "%d block migration requests successfully submitted." % numMigrationsInProgress
    if numFailedSubmissions:
        msg = "%d block migration requests failed to be submitted." % numFailedSubmissions
    logger.info(msg)
    if not migrationsInProgress:
        return 2, "Migration of %s failed." % (dataset)

    # Wait for up to 300 seconds, then return to the main loop. Note that we
    # don't fail or cancel any migration request, but just retry it next time.
    # In the case of failure, we expect the publisher daemon to try again in
    # the future.
    numFailedMigrations = 0
    numSuccessfulMigrations = 0
    waitTime = 30
    numTimes = 10
    msg = "Will monitor their status for up to %d seconds." % (waitTime * numTimes)
    logger.info(msg)
    for _ in range(numTimes):
        msg = "%d block migrations in progress." % numMigrationsInProgress
        msg += " Will check migrations status in %d seconds." % waitTime
        logger.info(msg)
        time.sleep(waitTime)
        # Check the migration status of each block migration request.
        # If a block migration has succeeded or terminally failes, remove the
        # migration request id from the list of migration requests in progress.
        for block in migrationsInProgress.copy():  # make a copy to allow manipulating original list
            try:
                inProgress, atDestination, failed = checkBlockMigration(taskname, migrateApi, block, migLogDir)
            except Exception as ex:
                msg = "Could not get migration status for %s:\n%s" % (block, ex)
                logger.error(msg)
                continue  # will check status next time
            if atDestination:
                logger.info('Migration completed for %s', block)
                migrationsInProgress.remove(block)
                numSuccessfulMigrations += 1
            if failed:
                logger.error('Migration failed for %s', block)
                migrationsInProgress.remove(block)
                numFailedMigrations += 1
            if inProgress:
                pass  # will check again later
        numMigrationsInProgress = len(migrationsInProgress)  # update counter at the end of loop
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
    msg += " succeeded = %d," % numSuccessfulMigrations
    msg += " failed = %d," % numFailedMigrations
    msg += " submission failed = %d," % numFailedSubmissions
    logger.info(msg)
    # If there were failed migrations, return with status 2.
    if numFailedMigrations > 0 or numFailedSubmissions > 0:
        msg = "Some blocks failed to be migrated."
        logger.info(msg)
        return 2, "Migration of %s failed." % (dataset)
    msg = "Migration completed. Wait 5sec before verifying that migrated datesed is OK in destination DBS"
    logger.info(msg)
    time.sleep(5.0)
    migratedDataset = None
    try:
        migratedDataset = destReadApi.listDatasets(dataset=dataset, detail=True, dataset_access_type='*')
        if not migratedDataset or migratedDataset[0].get('dataset', None) != dataset:
            return 4, "Migration of %s in some inconsistent status." % dataset
    except Exception as ex:
        logger.exception("Migration check failed.")
        if migratedDataset:
            logger.error("listDatasets returned %s", migratedDataset)
        return 4, "Migration check failed. %s" % ex
    return 0, ""


def requestBlockMigration(taskname, migrateApi, sourceApi, block, migLogDir):
    """
    Submit migration request for one block, checking the request output.
    returns False if request could not be submitted, True otherwise
    migration status will have to be checked later using block name as key.
    If something went wrong, migration will have to be retried later
    """

    logger = logging.getLogger(taskname)
    #    logging.basicConfig(filename=taskname+'.log', level=logging.INFO, format=config.General.logMsgFormat)

    msg = "Submiting migration request for block %s ..." % block
    logger.info(msg)
    sourceURL = sourceApi.url
    data = {'migration_url': sourceURL, 'migration_input': block}
    try:
        result = migrateApi.submitMigration(data)
        # N.B. a migration request is supposed never to fail. Only failure to contact server should
        # result in HTTP or curl error/exceptions. Otherwise server will always return a list of dicionaries.
    except Exception as ex:
        msg = "Request to migrate %s failed." % block
        msg += "\nRequest detail: %s" % data
        msg += "\nDBS3 exception: %s" % ex
        logger.error(msg)
        return False
    # if HTTP call succeeded a migration request was sent for this block and all its ancestors
    logger.info('Migration request submitted. %d blocks will be migrated', len(result))
    return True

def checkBlockMigration(taskname, migrateApi, block, migLogDir):
    """
    returns a 3plet of booleans: (inProgress, atDestination, failed)
    WHen something goes wrong, reports all false
    """
    logger = logging.getLogger(taskname)
    # check status using block name

    atDestination = False
    failed = True
    inProgress = False
    try:
        result = migrateApi.statusMigration(block_name=block)
    except Exception as ex:
        msg = "Migration status query for block %s failed" % block
        msg += "\nDBS3 exception: %s" % ex
        logger.error(msg)
        failed = True
        return inProgress, atDestination, failed

    if result:
        status = result[0]['migration_status']
    else:
        # result can be [] if there's no migration for this block in the DB
        failed = True  # handle like failed, will be retried in next Publisher iteration
        return inProgress, atDestination, failed
    # reference https://github.com/dmwm/dbs2go/blob/master/docs/MigrationServer.md
    # Migration states:
    #   0 = PENDING
    #   1 = IN PROGRESS
    #   2 = SUCCESS
    #   3 = FAILED (failed migrations are retried up to 3 times automatically)
    #   4 = NOT ACCEPTED (block is already at destination)
    #   5 = QUEUED (migration server needs to compute list of ancestors to migrate)
    #   9 = Terminally FAILED
    inProgress = status in (0, 1, 5)
    atDestination = status in (2, 4)
    beingRetried = (status == 3)
    failed = (status == 9)
    if beingRetried:  # sanity check
        nRetries = status[0]['retry_count']
        if nRetries > 10:
            logger.error("too many (%d) retries. Treat as terminally failed", nRetries)
            failed = True
            return inProgress, atDestination, failed
    if failed:
        logger.error("migration terminally failed for %s\n", block)
        logger.error("migration status details:\n%s", status)
        failedMigrationsLog = os.path.join(migLogDir, 'TerminallyFailedLog.txt')
        logger.debug("Migration terminally failed, log to %s", failedMigrationsLog)
        creationDate = status['creation_date']
        # convert to human format
        migCreation = datetime.fromtimestamp(creationDate).strftime('%Y-%m-%d,%H:%M:%S')
        # FiledMigFile format is CSV: creationDate, block, taskname
        with open(failedMigrationsLog, 'a', encoding='utf8') as fp:
            line = "%s,%s,%s\n" % (migCreation, block, taskname)
            fp.write(line)
        return inProgress, atDestination, failed
    # all OK, we got a usable status information
    return inProgress, atDestination, failed


def publishInDBS3(config, taskname, verbose):
    """
    Publish output from one task in DBS
    """

    def mark_good(files, crabServer, logger):
        """
        Mark the list of files as tranferred
        """
        if PARASITIC_TESTING:
            # during parassitic testing we do not touch transfersdb, leave it to main thread
            return

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
            data['list_of_ids'] = [docId]
            data['list_of_publication_state'] = ['DONE']
            data['list_of_retry_value'] = [1]
            data['list_of_failure_reason'] = ['']

            try:
                result = crabServer.post(api='filetransfers', data=encodeRequest(data))
                logger.debug("updated DocumentId: %s lfn: %s Result %s", docId, source_lfn, result)
            except Exception as ex:
                logger.error("Error updating status for DocumentId: %s lfn: %s", docId, source_lfn)
                logger.error("Error reason: %s", ex)
                logger.error("Error reason: %s", ex)

            nMarked += 1
            if nMarked % 10 == 0:
                logger.info('marked %d files', nMarked)


    def mark_failed(files, crabServer, logger, failure_reason=""):
        """
        Something failed for these files so increment the retry count
        """
        if PARASITIC_TESTING:
            # during parassitic testing we do not touch transfersdb, leave it to main thread
            return

        msg = "Marking %s file(s) as failed" % len(files)
        logger.info(msg)
        if dryRun:
            logger.debug("DryRun: skip marking failes files")
            return

        nMarked = 0
        for lfn in files:
            source_lfn = lfn
            docId = getHashLfn(source_lfn)
            data = {}
            data['asoworker'] = config.General.asoworker
            data['subresource'] = 'updatePublication'
            data['list_of_ids'] = [docId]
            data['list_of_publication_state'] = ['FAILED']
            data['list_of_retry_value'] = [1]
            data['list_of_failure_reason'] = [failure_reason]

            logger.debug("data: %s ", data)
            try:
                result = crabServer.post(api='filetransfers', data=encodeRequest(data))
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

    def saveSummaryJson(logdir, summary):
        """
        Save a publication summary as JSON. Make a new file every time this script runs
        :param summary: a summary disctionary. Must at least have key 'taskname'
        :param logdir: the directory where to write the summary
        :return: the full path name of the written file
        """
        taskname = summary['taskname']
        counter = 1
        summaryFileName = os.path.join(logdir, taskname + '-1.json')
        while os.path.exists(summaryFileName):
            counter += 1
            summaryFileName = os.path.join(logdir, taskname + '-%d.json' % counter)
        with open(summaryFileName, 'w', encoding='utf8') as fd:
            json.dump(summary, fd)
        return summaryFileName

    taskFilesDir = config.General.taskFilesDir
    dryRun = config.TaskPublisher.dryRun
    username = taskname.split(':')[1].split('_')[0]
    logdir = os.path.join(config.General.logsDir, 'tasks', username)
    if PARASITIC_TESTING:
        logdir = os.path.join(config.General.logsDir, 'tasks-testbed', username)
    logfile = os.path.join(logdir, taskname + '.log')
    createLogdir(logdir)
    migrationLogDir = os.path.join(config.General.logsDir, 'migrations')
    if PARASITIC_TESTING:
        migrationLogDir = os.path.join(config.General.logsDir, 'migrations-testbed')
    createLogdir(migrationLogDir)
    logger = logging.getLogger(taskname)
    logging.basicConfig(filename=logfile, level=logging.INFO, format=config.TaskPublisher.logMsgFormat)
    if verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("Start new iteration on taskname:  %s\nGet files to publish", taskname)

    # prepare a dummy summary JSON file in case there's nothing to do
    nothingToDo = {}
    nothingToDo['taskname'] = taskname
    nothingToDo['result'] = 'OK'
    nothingToDo['reason'] = 'NOTHING TO DO'
    nothingToDo['publishedBlocks'] = 0
    nothingToDo['failedBlocks'] = 0
    nothingToDo['failedBlockDumps'] = []
    nothingToDo['publishedFiles'] = 0
    nothingToDo['failedFiles'] = 0
    nothingToDo['nextIterFiles'] = 0

    toPublish = []
    # TODO move from new to done when processed
    fname = taskFilesDir + taskname + ".json"
    with open(fname, 'r', encoding='utf8') as f:
        toPublish = json.load(f)

    if not toPublish:
        logger.info("Empty data file %s", fname)
        summaryFileName = saveSummaryJson(logdir, nothingToDo)
        return summaryFileName

    pnn = toPublish[0]["Destination"]
    dataset = toPublish[0]['outdataset']
    logger.info("Will publish user files in %s", dataset)

    # CRABServer REST API's (see CRABInterface)
    try:
        instance = config.General.instance
    except Exception as ex:
        msg = "No instance provided: need to specify config.General.instance in the configuration"
        raise ConfigException(msg) from ex

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
        except Exception as ex:
            msg = "Need to specify config.General.restHost and dbInstance in the configuration"
            raise ConfigException(msg) from ex

    restURInoAPI = '/crabserver/' + dbInstance
    logger.info('Will connect to CRAB Data Base via URL: https://%s%s', restHost, restURInoAPI)

    # CRAB REST API's
    crabServer = CRABRest(hostname=restHost, localcert=config.General.serviceCert,
                          localkey=config.General.serviceKey, retry=3,
                          userAgent='CRABPublisher')
    crabServer.setDbInstance(dbInstance=dbInstance)

    data = {}
    data['subresource'] = 'search'
    data['workflow'] = taskname

    try:
        results = crabServer.get(api='task', data=encodeRequest(data))
    except Exception as ex:
        logger.error("Failed to get acquired publications from oracleDB: %s", ex)
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error contacting CRAB REST'
        summaryFileName = saveSummaryJson(logdir, nothingToDo)
        return summaryFileName

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
    except Exception as ex:
        logger.exception("ERROR: %s", ex)

    # at the moment can't deal with parents in phys03 ( https://github.com/dmwm/dbs2go/issues/69 )
    if 'phys03' in sourceURL:
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Cannot migrate from Phys03 yet'
        summaryFileName = saveSummaryJson(logdir, nothingToDo)
        logger.error(nothingToDo['reason'])
        return summaryFileName

    # When looking up parents may need to look in global DBS as well.
    globalURL = sourceURL
    globalURL = globalURL.replace('phys01', 'global')
    globalURL = globalURL.replace('phys02', 'global')
    globalURL = globalURL.replace('phys03', 'global')
    globalURL = globalURL.replace('caf', 'global')

    # allow to use a DBS REST host different from cmsweb.cern.ch (which is the
    # default inserted by CRAB Client)
    sourceURL = sourceURL.replace('cmsweb.cern.ch', config.TaskPublisher.DBShost)
    globalURL = globalURL.replace('cmsweb.cern.ch', config.TaskPublisher.DBShost)
    publish_dbs_url = publish_dbs_url.replace('cmsweb.cern.ch', config.TaskPublisher.DBShost)

    # DBS client relies on X509 env. vars
    os.environ['X509_USER_CERT'] = config.General.serviceCert
    os.environ['X509_USER_KEY'] = config.General.serviceKey

    # create DBS API objects
    logger.info("DBS Source API URL: %s", sourceURL)
    sourceApi = DbsApi(url=sourceURL, debug=False)
    logger.info("DBS Global API URL: %s", globalURL)
    globalApi = DbsApi(url=globalURL, debug=False)

    if publish_dbs_url.endswith('/DBSWriter'):
        publish_read_url = publish_dbs_url[:-len('/DBSWriter')] + '/DBSReader'
        publish_migrate_url = publish_dbs_url[:-len('/DBSWriter')] + '/DBSMigrate'
    else:
        publish_migrate_url = publish_dbs_url + '/DBSMigrate'
        publish_read_url = publish_dbs_url + '/DBSReader'
        publish_dbs_url += '/DBSWriter'
    # setup for testing GO server on testbed
    publish_dbs_url = 'https://cmsweb-testbed.cern.ch/dbs/int/phys03/DBSWriter'
    publish_read_url = 'https://cmsweb-testbed.cern.ch/dbs/int/phys03/DBSReader'
    publish_migrate_url = 'https://cmsweb-testbed.cern.ch/dbs/int/phys03/DBSMigrate'
    try:
        logger.info("DBS Destination API URL: %s", publish_dbs_url)
        destApi = DbsApi(url=publish_dbs_url, debug=False)
        logger.info("DBS Destination read API URL: %s", publish_read_url)
        destReadApi = DbsApi(url=publish_read_url, debug=False)
        logger.info("DBS Migration API URL: %s", publish_migrate_url)
        migrateApi = DbsApi(url=publish_migrate_url, debug=False)
    except Exception as ex:
        logger.exception('Error creating DBS APIs, likely wrong DBS URL %s\n%s', publish_dbs_url, ex)
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error contacting DBS'
        summaryFileName = saveSummaryJson(logdir, nothingToDo)
        return summaryFileName

    logger.info("inputDataset: %s", inputDataset)
    noInput = len(inputDataset.split("/")) <= 3

    if not noInput:
        try:
            existing_datasets = sourceApi.listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
            primary_ds_type = existing_datasets[0]['primary_ds_type']
        except Exception as ex:
            logger.exception('Error looking up input dataset in %s\n%s', sourceApi.url, ex)
            nothingToDo['result'] = 'FAIL'
            nothingToDo['reason'] = 'Error looking up input dataset in DBS'
            summaryFileName = saveSummaryJson(logdir, nothingToDo)
            return summaryFileName

        try:
            existing_output = destReadApi.listOutputConfigs(dataset=inputDataset)
        except Exception as ex:
            logger.exception('Error from listOutputConfigs in %s\n%s', destReadApi.url, ex)
            nothingToDo['result'] = 'FAIL'
            nothingToDo['reason'] = 'Error looking up input dataset in DBS'
            summaryFileName = saveSummaryJson(logdir, nothingToDo)
            return summaryFileName
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
    except Exception as ex:
        acquisitionera = acquisition_era_name

    _, primName, procName, tier = toPublish[0]['outdataset'].split('/')

    primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}
    msg = "About to insert primary dataset"
    logger.debug(msg)
    if dryRun:
        logger.info("DryRun: skip insertPrimaryDataset")
    else:
        try:
            destApi.insertPrimaryDataset(primds_config)
        except Exception:
            logger.exception('Error inserting PrimaryDataset in %s', destApi.url)
            nothingToDo['result'] = 'FAIL'
            nothingToDo['reason'] = 'Error looking up input dataset in DBS'
            summaryFileName = saveSummaryJson(logdir, nothingToDo)
            return summaryFileName
        msg = "Successfully inserted primary dataset %s." % (primName)
        logger.info(msg)

    final = {}
    failed = []
    publish_in_next_iteration = []
    published = []

    # Find all files already published in this dataset.
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
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error listing existing files in DBS'
        summaryFileName = saveSummaryJson(logdir, nothingToDo)
        return summaryFileName

    # check if actions are needed
    workToDo = False

    for fileTo in toPublish:
        #print(existingFilesValid)
        if fileTo['lfn'] not in existingFiles:
            workToDo = True
            break

    if not workToDo:
        msg = "Nothing uploaded, output dataset has these files already."
        logger.info(msg)
        logger.info('Make sure those files are marked as Done')
        # docId is the has of the source LFN i.e. the file in the tmp area at the running site
        files = [f['SourceLFN'] for f in toPublish]
        mark_good(files, crabServer, logger)
        summaryFileName = saveSummaryJson(logdir, nothingToDo)
        return summaryFileName

    acquisition_era_config = {'acquisition_era_name': acquisitionera, 'start_date': 0}

    output_config = {'release_version': appVer,
                     'pset_hash': pset_hash,
                     'app_name': appName,
                     'output_module_label': 'o',
                     'global_tag': global_tag,
                    }

    dataset_config = {'dataset': dataset,
                      'processed_ds_name': procName,
                      'data_tier_name': tier,
                      'dataset_access_type': 'VALID',
                      'physics_group_name': 'CRAB3',
                      'last_modification_date': int(time.time()),
                     }

    logger.info("Output dataset config: %s", str(dataset_config))

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
                    # some parent files are illegal DBS names (GH issue #6771), skip them
                    try:
                        blocksDict = destReadApi.listBlocks(logical_file_name=parentFile)
                    except Exception:
                        file_['parents'].remove(parentFile)
                        continue
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

    # compute size of this publication request to guide us on picking max_files_per_block
    dbsFilesKBytes = len(json.dumps(dbsFiles)) // 1024
    if dbsFilesKBytes > 1024:
        dbsFilesSize = '%dMB' % (dbsFilesKBytes // 1024)
    else:
        dbsFilesSize = '%dKB' % dbsFilesKBytes
    nLumis = 0
    for file_ in dbsFiles:
        nLumis += len(file_['file_lumi_list'])

    # If there are no files to publish, continue with the next dataset.
    if not dbsFiles_f:
        msg = "No file to publish to do for this dataset."
        logger.info(msg)
        summaryFileName = saveSummaryJson(logdir, nothingToDo)
        return summaryFileName

    # Migrate parent blocks before publishing.
    # First migrate the parent blocks that are in the same DBS instance
    # as the input dataset.
    if localParentBlocks:
        msg = "List of parent blocks that need to be migrated from %s:\n%s" % (sourceApi.url, localParentBlocks)
        logger.info(msg)
        if dryRun:
            logger.info("DryRun: skipping migration request")
        else:
            try:
                statusCode, failureMsg = migrateByBlockDBS3(taskname, migrateApi, destReadApi, sourceApi,
                                                            inputDataset, localParentBlocks, migrationLogDir,
                                                            verbose)
            except Exception as ex:
                logger.exception('Exception raised inside migrateByBlockDBS3\n%s', ex)
                statusCode = 1
                failureMsg = 'Exception raised inside migrateByBlockDBS3'
            if statusCode:
                failureMsg += " Not publishing any files."
                logger.info(failureMsg)
                summaryFileName = saveSummaryJson(logdir, nothingToDo)
                return summaryFileName
    # Then migrate the parent blocks that are in the global DBS instance.
    if globalParentBlocks:
        msg = "List of parent blocks that need to be migrated from %s:\n%s" % (globalApi.url, globalParentBlocks)
        logger.info(msg)
        if dryRun:
            logger.info("DryRun: skipping migration request")
        else:
            try:
                statusCode, failureMsg = migrateByBlockDBS3(taskname, migrateApi, destReadApi, globalApi,
                                                            inputDataset, globalParentBlocks, migrationLogDir,
                                                            verbose)
            except Exception as ex:
                logger.exception('Exception raised inside migrateByBlockDBS3\n%s', ex)
                statusCode = 1
                failureMsg = 'Exception raised inside migrateByBlockDBS3'
            if statusCode:
                failureMsg += " Not publishing any files."
                logger.info(failureMsg)
                summaryFileName = saveSummaryJson(logdir, nothingToDo)
                return summaryFileName
    # Publish the files in blocks. The blocks must have exactly max_files_per_block
    # files, unless there are less than max_files_per_block files to publish to
    # begin with. If there are more than max_files_per_block files to publish,
    # publish as many blocks as possible and leave the tail of files for the next
    # PublisherWorker call, unless forced to published.
    nIter = 0
    block_count = 0
    count = 0
    publishedBlocks = 0
    failedBlocks = 0
    max_files_per_block = config.General.max_files_per_block
    #TODO here can tune max_file_per_block based on len(dbsFiles) and dbsFilesSize
    # start with a horrible hack for identified bad use cases:
    if '2018UL' in taskname or 'UL2018' in taskname:
        max_files_per_block = 10

    # make sure that a block never has too many lumis, see
    # https://github.com/dmwm/CRABServer/issues/6670#issuecomment-965837566
    maxLumisPerBlock = 1.e6  # 1 Million
    nBlocks = float(len(dbsFiles))/float(max_files_per_block)
    if nLumis > maxLumisPerBlock * nBlocks:
        logger.info('Trying to publish %d lumis in %d blocks', nLumis, nBlocks)
        reduction = (nLumis/maxLumisPerBlock)/nBlocks
        max_files_per_block = int(max_files_per_block/reduction)
        max_files_per_block = max(max_files_per_block, 1)  # sanity check
        logger.info('Reducing to %d files per block to keep nLumis/block below %s',
                    max_files_per_block, maxLumisPerBlock)

    dumpList = []   # keep a list of files where blocks which fail publication are dumped
    while True:
        nIter += 1
        block_name = "%s#%s" % (dataset, str(uuid.uuid4()))
        files_to_publish = dbsFiles[count:count+max_files_per_block]
        # makes sure variabled defined in try/except/finally are defined for later use
        t1 = 0
        blockSize = '0'
        didPublish = 'OK'
        failure_reason = ''
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
            blockSizeKBytes = len(json.dumps(blockDump)) // 1024
            if blockSizeKBytes > 1024:
                blockSize = '%dMB' % (blockSizeKBytes // 1024)
            else:
                blockSize = '%dKB' % blockSizeKBytes

            t1 = time.time()
            if dryRun:
                logger.info("DryRun: skip insertBulkBlock")
            else:
                destApi.insertBulkBlock(blockDump)
                didPublish = 'OK'
            block_count += 1
            publishedBlocks += 1
        except Exception as ex:
            didPublish = 'FAIL'
            #logger.error("Error for files: %s" % [f['SourceLFN'] for f in toPublish])
            logger.error("Error for files: %s", [f['lfn'] for f in toPublish])
            failed.extend([f['SourceLFN'] for f in toPublish])
            #failed.extend([f['lfn'].replace("/store","/store/temp") for f in toPublish])
            msg = "Error when publishing (%s) " % ", ".join(failed)
            msg += str(ex)
            msg += str(traceback.format_exc())
            logger.error(msg)
            failure_reason = str(ex)
            taskFilesDir = config.General.taskFilesDir
            fname = os.path.join(taskFilesDir, 'FailedBlocks', 'failed-block-at-%s.txt' % time.time())
            if PARASITIC_TESTING:
                fname = os.path.join(taskFilesDir, 'FailedBlocks-testbed', 'failed-block-at-%s.txt' % time.time())
            with open(fname, 'w', encoding='utf8') as fd:
                fd.write(pprint.pformat(blockDump))
            dumpList.append(fname)
            failedBlocks += 1
            logger.error("FAILING BLOCK DUE TO %s SAVED AS %s", str(ex), fname)
        finally:
            elapsed = int(time.time() - t1)
            msg = 'PUBSTAT: Nfiles=%4d, filestructSize=%6s, lumis=%7d, iter=%2d, blockSize=%6s, time=%3ds, status=%s, task=%s' % \
                  (len(dbsFiles), dbsFilesSize, nLumis, nIter, blockSize, elapsed, didPublish, taskname)
            logger.info(msg)
            logsDir = config.General.logsDir
            fname = os.path.join(logsDir, 'STATS.txt')
            with open(fname, 'a+', encoding='utf8') as fd:
                fd.write(str(msg+'\n'))

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
    msg = "End of publication status:"
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
            crabServer.post(api='task', data=encodeRequest(data))
        if failed:
            logger.debug("Failed files: %s ", failed)
            mark_failed(failed, crabServer, logger, failure_reason)
    except Exception as ex:
        logger.exception("Status update failed: %s", ex)

    summary = {}
    summary['taskname'] = taskname
    summary['result'] = 'OK' if not failed else 'FAIL'
    summary['reason'] = '' if not failed else 'DBS Publication Failure'
    summary['publishedBlocks'] = publishedBlocks
    summary['failedBlocks'] = failedBlocks
    summary['failedBlockDumps'] = dumpList
    summary['publishedFiles'] = len(published)
    summary['failedFiles'] = len(failed)
    summary['nextIterFiles'] = len(publish_in_next_iteration)

    summaryFileName = saveSummaryJson(logdir, summary)

    return summaryFileName


def main():
    """
    starting from json file prepared by PusblishMaster with info on filed to be published, does
    the actual DBS publication
    :return: prints various things to stdout, last string is the name of a JSON file with summary of the work done
            key          :    type, value
        taskname         : string, name of the task
        result           : string, 'OK' or 'FAIL'
        reason           : string, the failure reason, empty ('') if result=='OK'
        publishedBlocks  : integer, the number of published blocks
        failedBlocks     : integer, the number of blocks which failed to be published
        failedBlockDumps : list of strings, the one filename for each failed block containing the blockDump
                           as passed in input to the failing DBS API insertBulkBlock(blockDump)
        publishedFiles   : integer, the number of published files
        failedFiles      : integer, the number of failed in the blocks which failed to be published
        nextIterFiles    : integer, the number of files left to be handled in next iteration
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

    if verbose:
        print("Will run%s with:\nconfigFile: %s\ntaskname  : %s\n" % (modeMsg, configFile, taskname))

    result = publishInDBS3(config, taskname, verbose)
    print("Completed with result in %s" % result)

if __name__ == '__main__':
    main()
