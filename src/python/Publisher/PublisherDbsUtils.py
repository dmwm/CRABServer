"""
functions used both in Publisher_rucio and Publisher_schedd
which communicate with DBS
"""

import os
import logging
import json
import time
from datetime import datetime

from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import dbsClientException
from RestClient.ErrorHandling.RestClientExceptions import HTTPError

from TaskWorker.WorkerExceptions import CannotMigrateException

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
    return nf


def setupDbsAPIs(sourceURL=None, publishURL=None, DBSHost=None, logger=None):
    """
    takes as input the DBS URLs for the input dataset and the publish dataset
    as recorder in CRAB DB task table, e.g.
     tm_dbs_url	        https://cmsweb.cern.ch/dbs/prod/global/DBSReader
     tm_publish_dbs_url	https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter
    since Run2 we only publish in prod/phys03, but all in all it is good
    to keep the existing generality. We could e.d. use dbs/int or dbs/dev for testing
    tm_dbs_url usually is either prod/global or prod/phys03.
    returns DBSApis : a dictionary with the DBS API's needed to publish
    with keys: source, destRead, destWrite, global, migrate

    """

    DBSApis = {}

    # When looking up parents may need to look in global DBS as well.
    globalURL = sourceURL
    globalURL = globalURL.replace('phys01', 'global')
    globalURL = globalURL.replace('phys02', 'global')
    globalURL = globalURL.replace('phys03', 'global')
    globalURL = globalURL.replace('caf', 'global')

    # allow to use a DBS REST host different from cmsweb.cern.ch (which is the
    # default inserted by CRAB Client)
    if DBSHost:
        sourceURL = sourceURL.replace('cmsweb.cern.ch', DBSHost)
        globalURL = globalURL.replace('cmsweb.cern.ch', DBSHost)
        publishURL = publishURL.replace('cmsweb.cern.ch', DBSHost)

    # special case for testing, read from "standard" place and publish in int/phys03
    if DBSHost == 'cmsweb-testbed.cern.ch':
        sourceURL = sourceURL.replace(DBSHost, 'cmsweb.cern.ch')
        globalURL = globalURL.replace(DBSHost, 'cmsweb.cern.ch')
        publishURL = 'https://cmsweb-testbed.cern.ch:8443/dbs/int/phys03/DBSWriter'

    # create DBS API objects
    logger.info("DBS Source API URL: %s", sourceURL)
    sourceApi = DbsApi(url=sourceURL, debug=False)
    logger.info("DBS Global API URL: %s", globalURL)
    globalApi = DbsApi(url=globalURL, debug=False)

    if publishURL.endswith('/DBSWriter'):
        publish_read_url = publishURL[:-len('/DBSWriter')] + '/DBSReader'
        publish_migrate_url = publishURL[:-len('/DBSWriter')] + '/DBSMigrate'
    else:
        publish_migrate_url = publishURL + '/DBSMigrate'
        publish_read_url = publishURL + '/DBSReader'
        publishURL += '/DBSWriter'
    logger.info("DBS Destination API URL: %s", publishURL)
    destApi = DbsApi(url=publishURL, debug=False)
    logger.info("DBS Destination read API URL: %s", publish_read_url)
    destReadApi = DbsApi(url=publish_read_url, debug=False)
    logger.info("DBS Migration API URL: %s", publish_migrate_url)
    migrateApi = DbsApi(url=publish_migrate_url, debug=False)
    DBSApis['source'] = sourceApi
    DBSApis['destRead'] = destReadApi
    DBSApis['destWrite'] = destApi
    DBSApis['global'] = globalApi
    DBSApis['migrate'] = migrateApi

    return DBSApis


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


def migrateByBlockDBS3(taskname, migrateApi, destReadApi, sourceApi, blocks,
                       migLogDir, logger=None, verbose=False):
    """
    Submit one migration request for each block that needs to be migrated.
    If blocks argument is not specified, migrate the whole dataset.
    Returns a 2-element ntuple : (exitcode, message)
    exit codes:  0 OK, 1 taking too long, 2 failure
    """
    #wfnamemsg = "%s: " % taskname


    if blocks:
        blocksToMigrate = set(blocks)
        datasetToMigrate = list(blocks)[0].split('#')[0]
    else:
        # migration of a full dataset is better accomplished with a separate method
        raise NotImplementedError

    numBlocksToMigrate = len(blocksToMigrate)
    if numBlocksToMigrate == 0:
        msg = "No migration needed."
        logger.info(msg)
        return 0, ""
    msg = f"Have to migrate {numBlocksToMigrate} blocks from {sourceApi.url} to {destReadApi.url}."
    logger.info(msg)
    if verbose:
        msg = f"List of blocks to migrate:\n{', '.join(blocksToMigrate)}."
        logger.debug(msg)
    msg = f"Submitting {numBlocksToMigrate} block migration requests to DBS3 ..."
    logger.info(msg)
    numFailedSubmissions = 0
    migrationsInProgress = []
    for block in list(blocksToMigrate):
        # Submit migration request for this block.
        ok = requestBlockMigration(taskname, migrateApi, sourceApi, block)
        if ok:
            migrationsInProgress.append(block)
        else:
            numFailedSubmissions += 1

    numMigrationsInProgress = len(migrationsInProgress)
    msg = f"{numMigrationsInProgress} block migration requests successfully submitted."
    if numFailedSubmissions:
        msg = f"{numFailedSubmissions} block migration requests failed to be submitted."
    logger.info(msg)
    if not migrationsInProgress:
        return 2, f"Migration of {datasetToMigrate} failed."

    # Wait for up to 300 seconds, then return to the main loop. Note that we
    # don't fail or cancel any migration request, but just retry it next time.
    # In the case of failure, we expect the publisher daemon to try again in
    # the future.
    numFailedMigrations = 0
    numSuccessfulMigrations = 0
    waitTime = 30
    numTimes = 10
    msg = f"Will monitor their status for up to {waitTime * numTimes} seconds."
    logger.info(msg)
    for _ in range(numTimes):
        msg = f"{numMigrationsInProgress} block migrations in progress."
        msg += f" Will check migrations status in {waitTime} seconds."
        logger.info(msg)
        time.sleep(waitTime)
        # Check the migration status of each block migration request.
        # If a block migration has succeeded or terminally failes, remove the
        # migration request id from the list of migration requests in progress.
        for block in migrationsInProgress.copy():  # make a copy to allow manipulating original list
            try:
                inProgress, atDestination, failed = checkBlockMigration(taskname, migrateApi, block, migLogDir)
            except Exception as ex:
                msg = f"Could not get migration status for {block}:\n{ex}"
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
        msg = f"Migration of {datasetToMigrate} is taking too long - will delay the publication."
        logger.info(msg)
        return 1, f"Migration of {datasetToMigrate} is taking too long."
    msg = f"Migration of {datasetToMigrate} has finished."
    logger.info(msg)
    msg = f"Migration status summary (from {numBlocksToMigrate} input blocks to migrate):"
    msg += f" succeeded = {numSuccessfulMigrations},"
    msg += f" failed = {numFailedMigrations},"
    msg += f" submission failed = {numFailedSubmissions},"
    logger.info(msg)
    # If there were failed migrations, return with status 2.
    if numFailedMigrations > 0 or numFailedSubmissions > 0:
        msg = "Some blocks failed to be migrated."
        logger.info(msg)
        return 2, f"Migration of {datasetToMigrate} failed."
    msg = "Migration completed. Wait 5sec before verifying that migrated datesed is OK in destination DBS"
    logger.info(msg)
    time.sleep(5.0)
    migratedDataset = None
    try:
        migratedDataset = destReadApi.listDatasets(dataset=datasetToMigrate, detail=True, dataset_access_type='*')
        if not migratedDataset or migratedDataset[0].get('dataset', None) != datasetToMigrate:
            return 4, f"Migration of {datasetToMigrate} in some inconsistent status."
    except Exception as ex:
        logger.exception("Migration check failed.")
        if migratedDataset:
            logger.error("listDatasets returned %s", migratedDataset)
        return 4, f"Migration check failed. {ex}"
    return 0, ""


def requestBlockMigration(taskname, migrateApi, sourceApi, block):
    """
    Submit migration request for one block, checking the request output.
    returns False if request could not be submitted, True otherwise
    migration status will have to be checked later using block name as key.
    If something went wrong, migration will have to be retried later
    """

    logger = logging.getLogger(taskname)
    #    logging.basicConfig(filename=taskname+'.log', level=logging.INFO, format=config.General.logMsgFormat)

    msg = f"Submiting migration request for block {block} ..."
    logger.info(msg)
    sourceURL = sourceApi.url
    data = {'migration_url': sourceURL, 'migration_input': block}
    result = None
    try:
        result = migrateApi.submitMigration(data)
        # N.B. a migration request is supposed never to fail. Only failure to contact server should
        # result in HTTP or curl error/exceptions. Otherwise server will always return a list of dicionaries.
        # But there are cases where server replies with HTTP code other than 200 (e.g. 400 if migration
        # request is invalid), in those caes client raises exception which should be handled with proper care
    except dbsClientException as dbsEx:
        logger.error("HTTP call to server %s failed: %s", migrateApi.url, dbsEx)
        return False
    except HTTPError as httpErr:
        # this is a structured message from migrate server as per
        #  https://github.com/dmwm/dbs2go/blob/master/docs/DBSServer.md#dbs-errors
        # http call went through, simply server returned an HTTP code other than 200
        code = httpErr.code
        body = json.loads(httpErr.body)
        reason = body[0]['error']['reason']
        if code >= 500:
            # something bad happened inside server
            logger.error("HTTP error %d with msg %s", code, reason)
            return False
        if code == 400:
            # beware "not allowed for migration" error which is persistent
            msg = "Migration request refused by server."
            logger.error(msg)
            # if we simply report flase, Publisher will try and fail again, forever
            # some reasons for this are known
            if 'has status PRODUCTION' in reason:
                msg = 'Input dataset has status PRODUCTION'
            raise CannotMigrateException(msg) from httpErr

        if code > 400:
            # in this cases it is better to treat the migration request submission as successful
            # and go on with status checking via checkBlockMigration where various status codes
            # are properly handled
            logger.error("HTTP error %d", code)
            logger.error("A new migration request could not be submitted. Reason: %s", reason)
            return True
        logger.error("Unexpected HTTP error %d", code)
        return False
    except CannotMigrateException :
        raise
    except Exception as ex:
        msg = f"Request to migrate {block} failed."
        msg += f"\nRequest detail: {data}"
        msg += f"\nDBS3 exception: {ex}"
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
        msg = f"Migration status query for block {block} failed"
        msg += f"\nDBS3 exception: {ex}"
        logger.error(msg)
        failed = True
        return inProgress, atDestination, failed

    if result:
        status = result[0].get('migration_status')
        reqid = result[0].get('migration_request_id')
    else:
        # result can be [] if there's no migration for this block in the DB
        failed = True  # handle like failed, will be retried in next Publisher iteration
        return inProgress, atDestination, failed
    if status is None or reqid is None:
        msg = "Migration request failed to submit."
        msg += f"\nMigration request results: {result}"
        logger.error(msg)
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
        nRetries = result[0]['retry_count']
        if nRetries > 10:
            logger.error("too many (%d) retries. Treat as terminally failed", nRetries)
            failed = True
            return inProgress, atDestination, failed
    if failed:
        logger.error("migration terminally failed for %s\n", block)
        logger.error("migration status details:\n%s", result)
        failedMigrationsLog = os.path.join(migLogDir, 'TerminallyFailedLog.txt')
        logger.debug("Migration terminally failed, log to %s", failedMigrationsLog)
        creationDate = result[0]['creation_date']
        # convert to human format
        migCreation = datetime.fromtimestamp(creationDate).strftime('%Y-%m-%d,%H:%M:%S')
        # FiledMigFile format is CSV: reqid,creationDate, block, taskname
        with open(failedMigrationsLog, 'a', encoding='utf8') as fp:
            line = f"{reqid},{migCreation},{block},{taskname}\n"
            fp.write(line)
        return inProgress, atDestination, failed
    # all OK, we got a usable status information
    return inProgress, atDestination, failed


