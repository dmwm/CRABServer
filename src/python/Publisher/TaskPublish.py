# pylint: disable=C0103, W0703, R0912, R0914, R0915
"""
this is a standalone script. It is spawned by PushisherMaster or could
be executed from CLI (in the Publisher environment) to retry or debug failures
"""
import os
import uuid
import time
import json
import traceback
import argparse
import pprint

from WMCore.Configuration import loadConfigurationFile

from TaskWorker.WorkerExceptions import CannotMigrateException
from TaskWorker.WorkerUtilities import getCrabserver

from Publisher.PublisherUtils import setupLogging, prepareDummySummary, saveSummaryJson, \
    markGood, markFailed, getDBSInputInformation

from Publisher.PublisherDbsUtils import format_file_3, setupDbsAPIs, findParentBlocks, \
    createBulkBlock, migrateByBlockDBS3, prepareDbsPublishingConfigs


def publishInDBS3(config, taskname, verbose, console):
    """
    Publish output from one task in DBS
    """
    # a few dictionaries to pass global information around all these functions
    # initialized here to None simply as documentation
    log = {'logger': None, 'logdir': None, 'logTaskDir': None,
           'taskFilesDir': None, 'migrationLogDir': None}
    DBSApis = {'source': None, 'destRead': None, 'destWrite': None, 'global': None, 'migrate': None}
    nothingToDo = {}  # a pre-filled SummaryFile in case of no useful input or errors

    log = setupLogging(config, taskname, verbose, console)
    logger = log['logger']
    logdir = log['logdir']
    migrationLogDir = log['migrationLogDir']
    taskFilesDir = log['taskFilesDir']
    dryRun = config.TaskPublisher.dryRun
    logger.info("Start new iteration on taskname:  %s\nGet files to publish", taskname)

    # DBS client relies on X509 env. vars
    os.environ['X509_USER_CERT'] = config.TaskPublisher.cert
    os.environ['X509_USER_KEY'] = config.TaskPublisher.key

    # preapre an empy summary to be used in case of errors
    nothingToDo = prepareDummySummary(taskname)

    toPublish = []
    # TO DO move from new to done when processed
    fname = taskFilesDir + taskname + ".json"
    with open(fname, 'r', encoding='utf8') as f:
        toPublish = json.load(f)

    if not toPublish:
        logger.info("Empty data file %s", fname)
        summaryFileName = saveSummaryJson(nothingToDo, logdir)
        return summaryFileName

    pnn = toPublish[0]["Destination"]
    dataset = toPublish[0]['outdataset']
    logger.info("Will publish user files in %s", dataset)

    # CRABServer REST API's (see CRABInterface)
    # initialize CRABServer REST
    crabServer = getCrabserver(restConfig=config.REST, agentName='CRABPublisher', logger=logger)

    # retrieve info on DBS from TaskDB table
    try:
        (inputDataset, sourceURL, publishURL) = getDBSInputInformation(taskname, crabServer)
    except Exception as ex:
        logger.error("Failed to get task info oracleDB: %s", ex)
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error contacting CRAB REST'
        summaryFileName = saveSummaryJson(nothingToDo, log['logdir'])
        return summaryFileName
    logger.info("inputDataset: %s", inputDataset)

    # prepare DBS API's
    try:
        DBSApis = setupDbsAPIs(sourceURL=sourceURL, publishURL=publishURL,
                               DBSHost=config.TaskPublisher.DBShost, logger=logger)
    except Exception as ex:
        logger.exception('Error creating DBS APIs, likely wrong DBS URL %s\n%s', publishURL, ex)
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error contacting DBS'
        summaryFileName = saveSummaryJson(nothingToDo, log['logdir'])
        return summaryFileName

    sourceApi = DBSApis['source']
    globalApi = DBSApis['global']
    destApi = DBSApis['destWrite']
    destReadApi = DBSApis['destRead']
    migrateApi = DBSApis['migrate']

    logger.info("inputDataset: %s", inputDataset)

    final = {}
    failed = []
    publish_in_next_iteration = []
    published = []

    # Find all files already published in this dataset.
    try:
        existingDBSFiles = destReadApi.listFiles(dataset=dataset, detail=True)
        existingFiles = [f['logical_file_name'] for f in existingDBSFiles]
        existingFilesValid = [f['logical_file_name'] for f in existingDBSFiles if f['is_file_valid']]
        msg = f"Dataset {dataset} already contains {len(existingFiles)} files"
        msg += f" ({len(existingFilesValid)} valid, {len(existingFiles) - len(existingFilesValid)} invalid)."
        logger.info(msg)
        final['existingFiles'] = len(existingFiles)
    except Exception as ex:
        msg = f"Error when listing files in DBS: {ex}"
        msg += f"\n{traceback.format_exc()}"
        logger.error(msg)
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error listing existing files in DBS'
        summaryFileName = saveSummaryJson(nothingToDo, logdir)
        return summaryFileName

    # check if actions are needed
    workToDo = False

    for fileTo in toPublish:
        if fileTo['lfn'] not in existingFiles:
            workToDo = True
            break

    if not workToDo:
        msg = "Nothing uploaded, output dataset has these files already."
        logger.info(msg)
        logger.info('Make sure those files are marked as Done')
        # docId is the has of the source LFN i.e. the file in the tmp area at the running site
        files = [f['SourceLFN'] for f in toPublish]
        if not dryRun:
            markGood(files=files, crabServer=crabServer, asoworker=config.General.asoworker, logger=logger)
        summaryFileName = saveSummaryJson(nothingToDo, logdir)
        return summaryFileName

    # OK got something to do
    DBSConfigs = prepareDbsPublishingConfigs(aFile=toPublish[0], inputDataset=inputDataset,
                                              outputDataset=dataset, DBSApis=DBSApis, logger=logger)

    # List of all files that must (and can) be published.
    dbsFiles = []
    dbsFiles_f = []

    (localParentBlocks, globalParentBlocks) = findParentBlocks(
        toPublish, DBSApis=DBSApis, logger=logger, verbose=verbose)

    # Loop over all files to publish.
    for file_ in toPublish:
        if verbose:
            logger.info(file_)
        # Check if this file was already published and if it is valid.
        if file_['lfn'] not in existingFilesValid:
            # We have a file to publish.
            # Add this file to the list of files to be published.
            dbsFiles.append(format_file_3(file_))
            dbsFiles_f.append(file_)
        published.append(file_['SourceLFN'])

    # Print a message with the number of files to publish.
    msg = f"Found {len(dbsFiles)} files not already present in DBS which will be published."
    logger.info(msg)

    # compute size of this publication request to guide us on picking max_files_per_block
    dbsFilesKBytes = len(json.dumps(dbsFiles)) // 1024
    if dbsFilesKBytes > 1024:
        dbsFilesSize = f"{dbsFilesKBytes // 1024}MB"
    else:
        dbsFilesSize = f"{dbsFilesKBytes}KB"
    nLumis = 0
    for file_ in dbsFiles:
        nLumis += len(file_['file_lumi_list'])

    # If there are no files to publish, continue with the next dataset.
    if not dbsFiles_f:
        msg = "No file to publish to do for this dataset."
        logger.info(msg)
        summaryFileName = saveSummaryJson(nothingToDo, logdir)
        return summaryFileName

    # Migrate parent blocks before publishing.
    # First migrate the parent blocks that are in the same DBS instance
    # as the input dataset.
    if localParentBlocks:
        msg = "List of parent blocks that need to be migrated"
        msg += f" from {sourceApi.url}:\n{localParentBlocks}"
        logger.info(msg)
        if dryRun:
            logger.info("DryRun: skipping migration request")
        else:
            try:
                statusCode, failureMsg = migrateByBlockDBS3(taskname, migrateApi, destReadApi, sourceApi,
                                                            localParentBlocks, migrationLogDir,
                                                            logger=logger, verbose=verbose)
            except CannotMigrateException as ex:
                # there is nothing we can do in this case
                failureMsg = 'Cannot migrate. ' + str(ex)
                logger.info('%s. Mark all files as failed', failureMsg)
                if not dryRun:
                    markFailed([file['SourceLFN'] for file in toPublish],
                                crabServer=crabServer, asoworker=config.General.asoworker,
                                failureReason=failureMsg, logger=logger)
                nothingToDo['result'] = 'FAIL'
                nothingToDo['reason'] = failureMsg
                nothingToDo['failedFiles'] = len(dbsFiles)
                summaryFileName = saveSummaryJson(nothingToDo, logdir)
                return summaryFileName
            except Exception as ex:
                logger.exception('Exception raised inside migrateByBlockDBS3\n%s', ex)
                statusCode = 1
                failureMsg = 'Exception raised inside migrateByBlockDBS3'
            if statusCode:
                failureMsg += " Not publishing any files."
                logger.info(failureMsg)
                summaryFileName = saveSummaryJson(nothingToDo, logdir)
                return summaryFileName
    # Then migrate the parent blocks that are in the global DBS instance.
    if globalParentBlocks:
        msg = "List of parent blocks that need to be migrated from"
        msg += f" {globalApi.url}:\n{globalParentBlocks}"
        logger.info(msg)
        if dryRun:
            logger.info("DryRun: skipping migration request")
        else:
            try:
                statusCode, failureMsg = migrateByBlockDBS3(taskname, migrateApi, destReadApi, globalApi,
                                                            globalParentBlocks, migrationLogDir,
                                                            logger=logger, verbose=verbose)
            except Exception as ex:
                logger.exception('Exception raised inside migrateByBlockDBS3\n%s', ex)
                statusCode = 1
                failureMsg = 'Exception raised inside migrateByBlockDBS3'
            if statusCode:
                failureMsg += " Not publishing any files."
                logger.info(failureMsg)
                summaryFileName = saveSummaryJson(nothingToDo, logdir)
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
    # TO DO here can tune max_file_per_block based on len(dbsFiles) and dbsFilesSize
    # start with a horrible hack for identified bad use cases:
    if '2018UL' in taskname or 'UL2018' in taskname:
        max_files_per_block = 10

    # make sure that a block never has too many lumis, see
    # https://github.com/dmwm/CRABServer/issues/6670#issuecomment-965837566
    maxLumisPerBlock = 1.e6  # 1 Million
    nBlocks = float(len(dbsFiles)) / float(max_files_per_block)
    if nLumis > maxLumisPerBlock * nBlocks:
        logger.info('Trying to publish %d lumis in %d blocks', nLumis, nBlocks)
        reduction = (nLumis / maxLumisPerBlock) / nBlocks
        max_files_per_block = int(max_files_per_block / reduction)
        max_files_per_block = max(max_files_per_block, 1)  # sanity check
        logger.info('Reducing to %d files per block to keep nLumis/block below %s',
                    max_files_per_block, maxLumisPerBlock)

    dumpList = []   # keep a list of files where blocks which fail publication are dumped
    while True:
        nIter += 1
        block_name = f"{dataset}#{str(uuid.uuid4())}"
        files_to_publish = dbsFiles[count:count + max_files_per_block]
        # makes sure variabled defined in try/except/finally are defined for later use
        t1 = 0
        blockSize = '0'
        didPublish = 'OK'
        try:
            block_config = {'block_name': block_name, 'origin_site_name': pnn, 'open_for_writing': 0}
            if verbose:
                msg = f"Inserting files {[f['logical_file_name'] for f in files_to_publish]}"
                msg += f" into block {block_name}."
                logger.info(msg)

            blockDump = createBulkBlock(DBSConfigs['output_config'], DBSConfigs['processing_era_config'],
                                        DBSConfigs['primds_config'], DBSConfigs['dataset_config'],
                                        DBSConfigs['acquisition_era_config'],
                                        block_config, files_to_publish)
            blockSizeKBytes = len(json.dumps(blockDump)) // 1024
            if blockSizeKBytes > 1024:
                blockSize = f"{blockSizeKBytes // 1024}MB"
            else:
                blockSize = f"{blockSizeKBytes}KB"

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
            logger.error("Error for files: %s", [f['lfn'] for f in toPublish])
            failed.extend([f['SourceLFN'] for f in toPublish])
            msg = f"Error when publishing ({ ', '.join(failed)})"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logger.error(msg)
            taskFilesDir = config.General.taskFilesDir
            fname = os.path.join(taskFilesDir, 'FailedBlocks', f"failed-block-at-{time.time()}.txt")
            with open(fname, 'w', encoding='utf8') as fd:
                fd.write(pprint.pformat(blockDump))
            dumpList.append(fname)
            failedBlocks += 1
            logger.error("FAILING BLOCK DUE TO %s SAVED AS %s", str(ex), fname)
        finally:
            elapsed = int(time.time() - t1)
            msg = f"PUBSTAT: Nfiles={len(dbsFiles):{4}}, filestructSize={dbsFilesSize:{6}}"
            msg += f", lumis={nLumis:{7}}, iter={nIter:{2}}"
            msg += f", blockSize={blockSize:{6}}, time={elapsed:{3}}"
            msg += f", status={didPublish}, task={taskname}"
            logger.info(msg)
            logsDir = config.General.logsDir
            fname = os.path.join(logsDir, 'STATS.txt')
            with open(fname, 'a+', encoding='utf8') as fd:
                fd.write(str(msg + '\n'))

        count += max_files_per_block
        files_to_publish_next = dbsFiles_f[count:count + max_files_per_block]
        if len(files_to_publish_next) < max_files_per_block:
            publish_in_next_iteration.extend([f["SourceLFN"] for f in files_to_publish_next])
            break
    published = [x for x in published if x not in failed + publish_in_next_iteration]
    # Fill number of files/blocks published for this dataset.
    final['files'] = len(dbsFiles) - len(failed) - len(publish_in_next_iteration)
    final['blocks'] = block_count
    # Print a publication status summary for this dataset.
    msg = "End of publication status:"
    msg += f" failed {len(failed)}"
    if verbose:
        msg += f": {failed}"
    msg += f", published {len(published)}"
    if verbose:
        msg += f": {published}"
    msg += f", publish_in_next_iteration {len(publish_in_next_iteration)}"
    if verbose:
        msg += f": {publish_in_next_iteration}"
    msg += f", results {final}"
    logger.info(msg)

    try:
        if published:
            if not dryRun:
                markGood(files=published, crabServer=crabServer, asoworker=config.General.asoworker, logger=logger)
        if failed:
            logger.debug("Failed files: %s ", failed)
            if not dryRun:
                markFailed(failed, crabServer=crabServer, asoworker=config.General.asoworker, logger=logger)
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

    summaryFileName = saveSummaryJson(summary, logdir)

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
    parser.add_argument('--console', help='Console mode, send logging to stdout', action='store_true')
    args = parser.parse_args()
    configFile = os.path.abspath(args.configFile)
    taskname = args.taskname
    verbose = args.verbose
    dryRun = args.dry
    console = args.console
    modeMsg = " in DRY RUN mode" if dryRun else ""
    config = loadConfigurationFile(configFile)
    if dryRun:
        config.TaskPublisher.dryRun = True

    if verbose:
        print(f"Will run{modeMsg} with:\nconfigFile: {configFile}\ntaskname  : {taskname}\n")

    result = publishInDBS3(config, taskname, verbose, console)
    print(f"Completed with result in {result}")


if __name__ == '__main__':
    main()
