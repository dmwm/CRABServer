# pylint: disable=invalid-name, broad-except, too-many-branches
"""
this is a standalone script. It is spawned by PushisherMaster or could
be executed from CLI (in the Publisher environment) to retry or debug failures
"""
import os
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
    prepareDbsPublishingConfigs, createBulkBlock, migrateByBlockDBS3


def publishInDBS3(config, taskname, verbose, console):  # pylint: disable=too-many-statements, too-many-locals
    """
    Publish output from one task in DBS
    It must return the name of the SummaryFile where result of the pbulication attempt is saved
    Any exception must be catched
    """
    # a few dictionaries to pass global information around all these functions
    # initialized here to None simply as documentation
    log = {'logger': None, 'logdir': None, 'logTaskDir': None,
           'taskFilesDir': None, 'migrationLogDir': None}
    DBSApis = {'source': None, 'destRead': None, 'destWrite': None, 'global': None, 'migrate': None}
    nothingToDo = {}  # a pre-filled SummaryFile in case of no useful input or errors

    def publishOneBlockInDBS(blockDict=None, DBSConfigs=None, logger=None):
        """
        get one complete block info and publish it
        blockDict is a dictionary  {'block_name':name, 'files':[{},..,{}]}
        files is a list of dictionaries, one per file with keys:
        filetype, jobid, outdataset, inevents, publishname, lfn, runlumi,
        adler32, cksum, filesize, parents, state, created, destination, source_lfn
        DBSConfigs is a dictionary with common information to be inserted in DBS
        as returned returned by  prepareDbsPublishingConfigs

        it has 2 possible outcomes and returns a dictionary:
         if OK : {'status': 'OK', 'reason': None, 'dumpFile': None}
         if FAIL : {'status': 'FAIL', 'reason': reason, 'dumpFile': dumpFileName}
         When 'reason' is 'failedToInsertInDBS', 'dumpFile' is the full path to the
          file with the dump of the block. Otherwise is None.
        """

        # List of all files that must (and can) be published.
        dbsFiles = []  # list in the format DBS likes
        dictsOfFilesToBePublished = []  # list in the original dict from PublisherMasterRucio
        nLumis = 0  # not sure we need to track this now

        for file in blockDict['files']:
            # Check if this file was already published
            if file['lfn'] not in existingFiles:
                # Add this file to the list of files to be published.
                dbsFiles.append(format_file_3(file))
                dictsOfFilesToBePublished.append(file)

        if not dictsOfFilesToBePublished:
            msg = "Empty file list."
            logger.info(msg)
            return {'status': 'FAIL', 'reason': msg, 'dumpFile': None}

        (localParentBlocks, globalParentBlocks) = findParentBlocks(
            dictsOfFilesToBePublished, DBSApis=DBSApis, logger=logger, verbose=verbose)

        # Print a message with the number of files to publish.
        msg = f"Found {len(dbsFiles)} files not already present in DBS which will be published."
        logger.info(msg)

        # Migrate parent blocks before publishing.
        # First migrate the parent blocks that are in the same DBS instance
        # as the input dataset.
        if localParentBlocks:
            msg = f"List of parent blocks that need to be migrated from {DBSApis['source'].url}:"
            msg += f"\n {localParentBlocks}"
            logger.info(msg)
            if dryRun:
                logger.info("DryRun: skipping migration request")
            else:
                try:
                    statusCode, failureMsg = migrateByBlockDBS3(
                        taskname,
                        DBSApis['migrate'], DBSApis['destRead'], DBSApis['source'],
                        localParentBlocks,
                        log['migrationLogDir'], logger=logger, verbose=verbose)
                except CannotMigrateException as ex:
                    # there is nothing we can do in this case
                    failureMsg = 'Cannot migrate. ' + str(ex)
                    return {'status': 'FAIL', 'reason': failureMsg, 'dumpFile': None}
                except Exception as ex:
                    logger.exception('Exception raised inside migrateByBlockDBS3\n%s', ex)
                    statusCode = 1
                    failureMsg = 'Exception raised inside migrateByBlockDBS3'
                if statusCode:
                    failureMsg += " Not publishing any files."
                    logger.info(failureMsg)
                    return {'status': 'FAIL', 'reason': failureMsg, 'dumpFile': None}
        # Then migrate the parent blocks that are in the global DBS instance.
        if globalParentBlocks:
            msg = f"List of parent blocks that need to be migrated from {DBSApis['global'].url}:"
            msg += f"\n {globalParentBlocks}"
            logger.info(msg)
            if dryRun:
                logger.info("DryRun: skipping migration request")
            else:
                try:
                    statusCode, failureMsg = migrateByBlockDBS3(
                        taskname,
                        DBSApis['migrate'], DBSApis['destRead'], DBSApis['global'],
                        globalParentBlocks,
                        log['migrationLogDir'], logger=logger, verbose=verbose)
                except Exception as ex:
                    logger.exception('Exception raised inside migrateByBlockDBS3\n%s', ex)
                    statusCode = 1
                    failureMsg = 'Exception raised inside migrateByBlockDBS3'
                if statusCode:
                    failureMsg += " Not publishing any files."
                    logger.info(failureMsg)
                    return {'status': 'FAIL', 'reason': failureMsg, 'dumpFile': None}

        block_name = blockDict['block_name']
        originSite = blockDict['origin_site']
        files_to_publish = dbsFiles
        # makes sure variabled defined in try/except/finally are defined for later use
        t1 = 0
        block_config = {'block_name': block_name, 'origin_site_name': originSite, 'open_for_writing': 0}
        if verbose:
            msg = f"Inserting files {[f['logical_file_name'] for f in files_to_publish]}"
            msg += f" into block {block_name}."
            logger.info(msg)
        blockDump = createBulkBlock(DBSConfigs['output_config'], DBSConfigs['processing_era_config'],
                                    DBSConfigs['primds_config'], DBSConfigs['dataset_config'],
                                    DBSConfigs['acquisition_era_config'],
                                    block_config, files_to_publish)
        # logger.debug("Block to insert: %s\n %s" % (blockDump, destApi.__dict__ ))
        blockSizeKBytes = len(json.dumps(blockDump)) // 1024
        if blockSizeKBytes > 1024:
            blockSize = f"{blockSizeKBytes // 1024}MB"
        else:
            blockSize = f"{blockSizeKBytes}KB"
        # make sure these are initialized also in case we run with dryRun
        didPublish = 'OK'
        failureReason = ''
        failedBlockDumpFile = None
        if dryRun:
            logger.info("DryRun: skip insertBulkBlock")
        else:
            try:
                DBSApis['destWrite'].insertBulkBlock(blockDump)
                didPublish = 'OK'
                failedBlockDumpFile = None
                failureReason = None
            except Exception as ex:
                didPublish = 'FAIL'
                msg = f"Error when publishing {blockDict['block_name']}"
                logger.error(msg)
                failedBlockDumpFile = os.path.join(
                    log['taskFilesDir'], 'FailedBlocks', f"failed-block-at-{time.time()}.txt")
                with open(failedBlockDumpFile, 'w', encoding='utf8') as fd:
                    fd.write(pprint.pformat(blockDump))
                logger.error("FAILING BLOCK DUE TO %s SAVED AS %s", str(ex), failedBlockDumpFile)
                failureReason = 'failedToInsertInDBS'
            finally:
                elapsed = int(time.time() - t1)
                msg = f"PUBSTAT: Nfiles={len(dbsFiles):{4}}, lumis={nLumis:{7}}"
                msg += f", blockSize={blockSize:{6}}, time={elapsed:{3}}"
                msg += f", status={didPublish}, task={taskname}"
                logger.info(msg)
                logsDir = config.General.logsDir
                fname = os.path.join(logsDir, 'STATS.txt')
                with open(fname, 'a+', encoding='utf8') as fd:
                    fd.write(str(msg + '\n'))

        return {'status': didPublish, 'reason': failureReason, 'dumpFile': failedBlockDumpFile}

    # DBS client relies on X509 env. vars
    os.environ['X509_USER_CERT'] = config.TaskPublisher.cert
    os.environ['X509_USER_KEY'] = config.TaskPublisher.key

    log = setupLogging(config, taskname, verbose, console)
    logger = log['logger']

    dryRun = config.TaskPublisher.dryRun

    logger.info("Start new iteration on taskname:  %s\nGet files to publish", taskname)

    # preapre an empy summary to be used in case of errors
    nothingToDo = prepareDummySummary(taskname)

    # read JSON file with data to be published
    blocksToPublish = []
    fname = log['taskFilesDir'] + taskname + ".json"
    with open(fname, 'r', encoding='utf8') as f:
        blocksToPublish = json.load(f)
    if not blocksToPublish:
        logger.info("Empty data file %s", fname)
        summaryFileName = saveSummaryJson(nothingToDo, log['logdir'])
        return summaryFileName

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

    # pick a few params which are common to all blocks and files to be published
    aBlock = blocksToPublish[0]
    outputDataset = aBlock['block_name'].split('#')[0]

    logger.info("Will publish user files in %s", outputDataset)

    # Find all blocks and files already published in this dataset.
    try:
        existingDBSBlocks = DBSApis['destRead'].listBlocks(dataset=outputDataset, detail=True)
        existingDBSFiles = DBSApis['destRead'].listFiles(dataset=outputDataset, detail=True)
        existingBlocks = [f['block_name'] for f in existingDBSBlocks]
        existingFiles = [f['logical_file_name'] for f in existingDBSFiles]
        msg = f"Dataset {outputDataset} already contains {len(existingBlocks)} blocks"
        # msg += " (%d valid, %d invalid)." % (len(existingFile), len(existingFiles) - len(existingFile))
        logger.info(msg)
    except Exception as ex:
        msg = f"Error when listing blocks in DBS: {ex}"
        msg += f"\n{traceback.format_exc()}"
        logger.error(msg)
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error listing existing blocks in DBS'
        summaryFileName = saveSummaryJson(nothingToDo, log['logdir'])
        return summaryFileName

    # check if actions are needed
    workToDo = False

    for block in blocksToPublish:
        # print(existingFile)
        if block['block_name'] not in existingBlocks:
            workToDo = True
            break

    if not workToDo:
        msg = "Nothing todo, output dataset has these blocks already."
        logger.info(msg)
        logger.info('Make sure files in those are marked as Done')
        # docId is the hash of the source LFN i.e. the file in the tmp area at the running site
        files = [f['source_lfn'] for f in block['files'] for block in blocksToPublish]
        if not dryRun:
            markGood(files=files, crabServer=crabServer, asoworker=config.General.asoworker, logger=logger)
        summaryFileName = saveSummaryJson(nothingToDo, log['logdir'])
        return summaryFileName

    # OK got something to do !
    try:
        DBSConfigs = prepareDbsPublishingConfigs(blockDict=blocksToPublish[0], inputDataset=inputDataset,
                                                 outputDataset=outputDataset, DBSApis=DBSApis, logger=logger)
    except Exception as ex:
        logger.exception('Error looking up input dataset info:\n%s', ex)
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error looking up input dataset in DBS'
        summaryFileName = saveSummaryJson(nothingToDo, log['logdir'])
        return summaryFileName

    # counters
    publishedFiles = 0
    publishedBlocks = 0
    failedBlocks = 0
    failedFiles = 0

    # lists
    listOfPublishedLFNs = []
    listOfFailedLFNs = []

    dumpList = []  # keep a list of files where blocks which fail publication are dumped

    # Publish one block at a time
    for block in blocksToPublish:
        blockDict = {'block_name': block['block_name']}
        blockDict['origin_site'] = block['origin_site']
        blockDict['files'] = block['files']
        lfnsInBlock = [f['source_lfn'] for f in blockDict['files']]
        result = publishOneBlockInDBS(blockDict=blockDict, DBSConfigs=DBSConfigs, logger=logger)
        if result['status'] == 'OK':
            logger.info('Publish OK   for Block: %s', blockDict['block_name'])
            publishedBlocks += 1
            publishedFiles += len(blockDict['files'])
            if not dryRun:
                markGood(files=lfnsInBlock, crabServer=crabServer, asoworker=config.General.asoworker, logger=logger)
            listOfPublishedLFNs.extend(lfnsInBlock)
        elif result['status'] == 'FAIL':
            failedBlocks += 1
            logger.error('Publish FAIL for Block: %s', blockDict['block_name'])
            failedBlocks += 1
            failedFiles += len(blockDict['files'])
            if not dryRun:
                markFailed(files=lfnsInBlock, crabServer=crabServer, asoworker=config.General.asoworker, logger=logger)
            listOfFailedLFNs.extend(lfnsInBlock)
            if result['reason'] == 'failedToInsertInDBS':
                logger.error("Failed to insert block in DBS. Block Dump saved")
                dumpList.append(result['dumpFile'])
            else:
                logger.error("Could not publish block because %s", result['reason'])

    # Print a publication status summary for this dataset.
    msg = "End of publication status:"
    msg += f" failed blocks: {failedBlocks}"
    msg += f" success blocks: {publishedBlocks}"
    msg += f" failed files: {failedFiles}"
    msg += f" success files: {publishedFiles}"
    logger.info(msg)

    # save summary for PublisherMasterRucio
    summary = {}
    summary['taskname'] = taskname
    summary['result'] = 'OK' if not failedBlocks else 'FAIL'
    summary['reason'] = '' if not failedBlocks else 'DBS Publication Failure'
    summary['publishedBlocks'] = publishedBlocks
    summary['failedBlocks'] = failedBlocks
    summary['failedBlockDumps'] = dumpList
    summary['publishedFiles'] = publishedFiles
    summary['failedFiles'] = failedFiles

    summaryFileName = saveSummaryJson(summary, log['logdir'])

    return summaryFileName


def main():
    """
    starting from json file prepared by PusblishMasterRucio with info on filed to be published, does
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
        print(f"Will run {modeMsg} with:\nconfigFile: {configFile}\ntaskname  : {taskname}\n")

    summaryFile = publishInDBS3(config, taskname, verbose, console)
    print(f"Completed with result in {summaryFile}")


if __name__ == '__main__':
    main()
