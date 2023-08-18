# pylint: disable=invalid-name, invalid-name, broad-except, too-many-branches
"""
this is a standalone script. It is spawned by PushisherMaster or could
be executed from CLI (in the Publisher environment) to retry or debug failures
"""
import os
import time
import logging
import json
import traceback
import argparse
import pprint

from dbs.apis.dbsClient import DbsApi

from WMCore.Configuration import loadConfigurationFile

from ServerUtilities import encodeRequest
from TaskWorker.WorkerExceptions import CannotMigrateException
from TaskWorker.WorkerUtilities import getCrabserver

from PublisherUtils import format_file_3, createBulkBlock, migrateByBlockDBS3, \
    createLogdir, prepareDummySummary, saveSummaryJson, mark_good, mark_failed


def publishInDBS3(config, taskname, verbose, console):
    """
    Publish output from one task in DBS
    It must return the name of the SummaryFile where result of the pbulication attempt is saved
    Any exception must be catched
    """
    # a few dictionaries to pass global information around all these functions
    # initialized here to None simply as documentation
    log = {'logger': None, 'logdir': None, 'logTaskDir': None, 'taskFilesDir': None}
    DBSApis = {'source': None, 'destRead': None, 'destWrite': None, 'global': None, 'migrate': None}
    DBSConfigs = {'inputDataset': None, 'outputDataset': None, 'primds_config':None,
                  'dataset_config': None, 'output_config': None,
                  'processing_era_config': None, 'acquisition_era_config': None}
    nothingToDo = {}  # a pre-filled SummaryFile in case of no useful input or errors

    def setupLogging(config, taskname, console):
        # prepare log and work directories
        taskFilesDir = config.General.taskFilesDir
        username = taskname.split(':')[1].split('_')[0]
        logdir = os.path.join(config.General.logsDir, 'tasks', username)
        logfile = os.path.join(logdir, taskname + '.log')
        createLogdir(logdir)
        migrationLogDir = os.path.join(config.General.logsDir, 'migrations')
        createLogdir(migrationLogDir)
        # create logger
        if console:
            logger = logging.getLogger()
            logger.addHandler(logging.StreamHandler())
            logger.setLevel(logging.INFO)
        else:
            logger = logging.getLogger(taskname)
            logging.basicConfig(filename=logfile, level=logging.INFO, format=config.TaskPublisher.logMsgFormat)
        if verbose:
            logger.setLevel(logging.DEBUG)
        # pass info around
        log['logger'] = logger
        log['logdir'] = logdir
        log['migrationLogDir'] = migrationLogDir
        log['taskFilesDir'] = taskFilesDir

    def getDBSInputInformation(taskname=None, crabServer=None):
        # LOOK UP Input dataset for this task to know which DBS istance it was in
        data = {'subresource': 'search',
                'workflow': taskname}
        results = crabServer.get(api='task', data=encodeRequest(data))
        # TODO THERE are better ways to parse rest output into a dict !!
        inputDatasetIndex = results[0]['desc']['columns'].index("tm_input_dataset")
        inputDataset = results[0]['result'][inputDatasetIndex]
        sourceURLIndex = results[0]['desc']['columns'].index("tm_dbs_url")
        sourceURL = results[0]['result'][sourceURLIndex]
        publishURLIndex = results[0]['desc']['columns'].index("tm_publish_dbs_url")
        publishURL = results[0]['result'][publishURLIndex]
        if not sourceURL.endswith("/DBSReader") and not sourceURL.endswith("/DBSReader/"):
            sourceURL += "/DBSReader"
        return (inputDataset, sourceURL, publishURL)

    def setupDbsAPIs(sourceURL, publishURL):
        # When looking up parents may need to look in global DBS as well.
        logger = log['logger']

        globalURL = sourceURL
        globalURL = globalURL.replace('phys01', 'global')
        globalURL = globalURL.replace('phys02', 'global')
        globalURL = globalURL.replace('phys03', 'global')
        globalURL = globalURL.replace('caf', 'global')

        # allow to use a DBS REST host different from cmsweb.cern.ch (which is the
        # default inserted by CRAB Client)
        sourceURL = sourceURL.replace('cmsweb.cern.ch', config.TaskPublisher.DBShost)
        globalURL = globalURL.replace('cmsweb.cern.ch', config.TaskPublisher.DBShost)
        publishURL = publishURL.replace('cmsweb.cern.ch', config.TaskPublisher.DBShost)

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

    def findParentBlocks(listOfFileDicts):
        #sourceApi, globalApi, destApi, destReadApi, migrateApi = DBSApis
        logger = log['logger']

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

        for file in listOfFileDicts:
            if verbose:
                logger.info(file)
            # Get the parent files and for each parent file do the following:
            # 1) Add it to the list of parent files.
            # 2) Find the block to which it belongs and insert that block name in
            #    (one of) the set of blocks to be migrated to the destination DBS.
            for parentFile in list(file['parents']):
                if parentFile not in parentFiles:
                    parentFiles.add(parentFile)
                    # Is this parent file already in the destination DBS instance?
                    # (If yes, then we don't have to migrate this block.)
                    # some parent files are illegal DBS names (GH issue #6771), skip them
                    try:
                        blocksDict = DBSApis['destRead'].listBlocks(logical_file_name=parentFile)
                    except Exception:
                        file['parents'].remove(parentFile)
                        continue
                    if not blocksDict:
                        # No, this parent file is not in the destination DBS instance.
                        # Maybe it is in the same DBS instance as the input dataset?
                        blocksDict = DBSApis['source'].listBlocks(logical_file_name=parentFile)
                        if blocksDict:
                            # Yes, this parent file is in the same DBS instance as the input dataset.
                            # Add the corresponding block to the set of blocks from the source DBS
                            # instance that have to be migrated to the destination DBS.
                            localParentBlocks.add(blocksDict[0]['block_name'])
                        else:
                            # No, this parent file is not in the same DBS instance as input dataset.
                            # Maybe it is in global DBS instance?
                            blocksDict = DBSApis['global'].listBlocks(logical_file_name=parentFile)
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
                    msg = f"Skipping parent file {parentFile}, as it doesn't seem to be known to DBS."
                    logger.info(msg)
                    if parentFile in file['parents']:
                        file['parents'].remove(parentFile)
            return (localParentBlocks, globalParentBlocks)

    def prepareDbsPublishingConfigs(aBlock=None, aFile=None):
        # fills the dictionary with the various configs needed to publish one block
        # needs a few parameters from the a sample block/file to be published in the
        # input json file

        logger = log['logger']
        inputDataset = DBSConfigs['inputDataset']
        noInput = len(inputDataset.split("/")) <= 3

        if not noInput:
            existing_datasets = DBSApis['source'].listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
            primary_ds_type = existing_datasets[0]['primary_ds_type']
            existing_output = DBSApis['destRead'].listOutputConfigs(dataset=inputDataset)
            if not existing_output:
                msg = f"Unable to list output config for input dataset {inputDataset}"
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
        appVer = aBlock["swversion"]
        pset_hash = aFile['publishname'].split("-")[-1]
        gtag = str(aBlock['globaltag'])
        if gtag == "None":
            gtag = global_tag
        if aBlock['acquisitionera'] and not aBlock['acquisitionera'] in ["null"]:
            acquisitionera = str(aBlock['acquisitionera'])
        else:
            acquisitionera = acquisition_era_name

        _, primName, procName, tier = DBSConfigs['outputDataset'].split('/')
        primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}

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

        DBSConfigs['primds_config'] = primds_config
        DBSConfigs['dataset_config'] = dataset_config
        DBSConfigs['output_config'] = output_config
        DBSConfigs['processing_era_config'] = processing_era_config
        DBSConfigs['acquisition_era_config'] = acquisition_era_config


    def publishOneBlockInDBS(blockDict):
        """
        get one complete block info and publish it
        blockDict is a dictionary  {'blockname':name, 'files':[{},..,{}]}
        files is a list of dictionaries, one per file with keys:
        filetype, jobid, outdataset, inevents, publishname, lfn, runlumi,
        adler32, cksum, filesize, parents, state, created, destination, source_lfn
        it has 2 possible outcomes and returns a dictionary:
         if OK : {'status': 'OK', 'reason': None, 'dumpFile': None}
         if FAIL : {'status': 'FAIL', 'reason': reason, 'dumpFile': dumpFileName}
         When 'reason' is 'failedToInsertInDBS', 'dumpFile' is the full path to the
          file with the dump of the block. Otherwise is None.
        """

        logger = log['logger']

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

        (localParentBlocks, globalParentBlocks) = findParentBlocks(dictsOfFilesToBePublished)

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
                        taskname, DBSConfigs['inputDataset'],
                        DBSApis['migrate'], DBSApis['destRead'], DBSApis['source'],
                        localParentBlocks, log['migrationLogDir'], verbose)
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
                        taskname, DBSConfigs['inputDataset'],
                        DBSApis['migrate'], DBSApis['destRead'], DBSApis['global'],
                        globalParentBlocks, verbose)
                except Exception as ex:
                    logger.exception('Exception raised inside migrateByBlockDBS3\n%s', ex)
                    statusCode = 1
                    failureMsg = 'Exception raised inside migrateByBlockDBS3'
                if statusCode:
                    failureMsg += " Not publishing any files."
                    logger.info(failureMsg)
                    return {'status': 'FAIL', 'reason': failureMsg, 'dumpFile': None}

        block_name = blockDict['blockname']
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
        #logger.debug("Block to insert: %s\n %s" % (blockDump, destApi.__dict__ ))
        blockSizeKBytes = len(json.dumps(blockDump)) // 1024
        if blockSizeKBytes > 1024:
            blockSize = f"{blockSizeKBytes // 1024}MB"
        else:
            blockSize = f"{blockSizeKBytes}KB"
        if dryRun:
            logger.info("DryRun: skip insertBulkBlock")
        else:
            didPublish = 'FAIL'  # make sure this is initialized
            try:
                DBSApis['destWrite'].insertBulkBlock(blockDump)
                didPublish = 'OK'
                failedBlockDumpFile = None
                failureReason = None
            except Exception as ex:
                didPublish = 'FAIL'
                msg = f"Error when publishing {blockDict['name']}"
                logger.error(msg)
                failedBlockDumpFile = os.path.join(
                    log['taskFilesDir'], 'FailedBlocks', f"failed-block-at-{time.time()}.txt")
                with open(failedBlockDumpFile, 'w', encoding='utf8') as fd:
                    fd.write(pprint.pformat(blockDump))
                logger.error("FAILING BLOCK DUE TO %s SAVED AS %s", str(ex), failedBlockDumpFile)
                failureReason = 'failedToInsertInDBS'
            finally:
                elapsed = int(time.time() - t1)
                #msg = 'PUBSTAT: Nfiles=%4d, lumis=%7d, blockSize=%6s, time=%3ds, status=%s, task=%s' % \
                #      (len(dbsFiles), nLumis, blockSize, elapsed, didPublish, taskname)
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

    setupLogging(config, taskname, console)
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
    DBSConfigs['inputDataset'] = inputDataset

    # prepare DBS API's
    try:
        setupDbsAPIs(sourceURL, publishURL)
    except Exception as ex:
        logger.exception('Error creating DBS APIs, likely wrong DBS URL %s\n%s', publishURL, ex)
        nothingToDo['result'] = 'FAIL'
        nothingToDo['reason'] = 'Error contacting DBS'
        summaryFileName = saveSummaryJson(nothingToDo, log['logdir'])
        return summaryFileName

    # pick a couple params which are common to all blocks and files in the data file
    aBlock = blocksToPublish[0]
    aBlockName = aBlock['name']
    originSite = aBlock["origin_site"]
    dataset = aBlockName.split('#')[0]
    DBSConfigs['outputDataset'] = dataset
    aFile = aBlock['files'][0]
    logger.info("Will publish user files in %s", dataset)

    # Find all blocks and files already published in this dataset.
    try:
        existingDBSBlocks = DBSApis['destRead'].listBlocks(dataset=dataset, detail=True)
        existingDBSFiles = DBSApis['destRead'].listFiles(dataset=dataset, detail=True)
        existingBlocks = [f['block_name'] for f in existingDBSBlocks]
        existingFiles = [f['logical_file_name'] for f in existingDBSFiles]
        msg = f"Dataset {dataset} already contains {len(existingBlocks)} blocks"
        #msg += " (%d valid, %d invalid)." % (len(existingFile), len(existingFiles) - len(existingFile))
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
        #print(existingFile)
        if block['name'] not in existingBlocks:
            workToDo = True
            break

    if not workToDo:
        msg = "Nothing todo, output dataset has these blocks already."
        logger.info(msg)
        logger.info('Make sure files in those are marked as Done')
        # docId is the hash of the source LFN i.e. the file in the tmp area at the running site
        files = [f['source_lfn'] for f in block['files'] for block in blocksToPublish]
        if not dryRun:
            mark_good(files=files, crabServer=crabServer, asoworker=config.General.asoworker, logger=log['logger'])
        summaryFileName = saveSummaryJson(nothingToDo, log['logdir'])
        return summaryFileName

    # OK got something to do !
    try:
        prepareDbsPublishingConfigs(aBlock=aBlock, aFile=aFile)
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
        blockDict = {'blockname': block['name']}
        blockDict['originSite'] = originSite
        blockDict['files'] = block['files']
        lfnsInBlock = [f['source_lfn'] for f in blockDict['files']]
        result = publishOneBlockInDBS(blockDict)
        if result['status'] == 'OK':
            log['logger'].info('Publish OK   for Block: %s', blockDict['blockname'])
            publishedBlocks += 1
            publishedFiles += len(blockDict['files'])
            if not dryRun:
                mark_good(files=lfnsInBlock, crabServer=crabServer, asoworker=config.General.asoworker, logger=log['logger'])
            listOfPublishedLFNs.extend(lfnsInBlock)
        elif result['status'] == 'FAIL':
            failedBlocks += 1
            log['logger'].error('Publish FAIL for Block: %s', blockDict['blockname'])
            failedBlocks += 1
            failedFiles += len(blockDict['files'])
            if not dryRun:
                mark_failed(files=lfnsInBlock, crabServer=crabServer, asoworker=config.General.asoworker, logger=log['logger'])
            listOfFailedLFNs.extend(lfnsInBlock)
            if result['reason'] == 'failedToInsertInDBS':
                log['logger'].err("Failed to insert block in DBS. Block Dump saved")
                dumpList.append(result['dumpFile'])
            else:
                log['logger'].err("Could not publish block because %s", result['reason'])

    # Print a publication status summary for this dataset.
    msg = "End of publication status:"
    msg += f" failed blocks: {failedBlocks}"
    msg += f" succes blocks: {publishedBlocks}"
    msg += f" failed files: {failedFiles}"
    msg += f" succes files: {publishedFiles}"
    log['logger'].info(msg)

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
        print(f"Will run {modeMsg} with:\nconfigFile: {configFile}\ntaskname  : {taskname}\n")

    summaryFile = publishInDBS3(config, taskname, verbose, console)
    print(f"Completed with result in {summaryFile}")

if __name__ == '__main__':
    main()
