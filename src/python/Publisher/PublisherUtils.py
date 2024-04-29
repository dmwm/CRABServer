"""
functions used both in Publisher_rucio and Publisher_schedd
mostly things which communicate with DBS or CRAB REST
"""


import os
import sys
import logging
import json


from ServerUtilities import getHashLfn, encodeRequest


def createLogdir(dirname):
    """
    Create the directory dirname ignoring errors in case it exists. Exit if
    the directory cannot be created.
    """
    try:
        os.mkdir(dirname)
    except OSError as ose:
        if ose.errno != 17:  # ignore the "Directory already exists error"
            print(str(ose))
            print(f"The task worker need to access the '{dirname}' directory")
            sys.exit(1)


def setupLogging(config, taskname, verbose, console):
    # prepare log and work directories for TaskPublish
    # an initialize a logger
    # returs a dictionary log={logger, logdir, migrationlLogDir, taskFilesDir}
    log = {}
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
    return log


def prepareDummySummary(taskname):
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
    return nothingToDo


def saveSummaryJson(summary=None, logdir=None):
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
        summaryFileName = os.path.join(logdir, taskname + f"-{counter}.json")
    with open(summaryFileName, 'w', encoding='utf8') as fd:
        json.dump(summary, fd)
    return summaryFileName


def mark_good(files=None, crabServer=None, asoworker=None, logger=None):
    """
    Mark the list of files as published
    files must be a list of SOURCE_LFN's i.e. /store/temp/user/...
    """

    msg = f"Marking {len(files)} file(s) as published."
    logger.info(msg)

    nMarked = 0
    for lfn in files:
        source_lfn = lfn
        docId = getHashLfn(source_lfn)
        data = {}
        data['asoworker'] = asoworker
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


def mark_failed(files=None, crabServer=None, failure_reason="", asoworker=None, logger=None):
    """
    Something failed for these files.
    files must be a list of SOURCE_LFN's i.e. /store/temp/user/...
    """
    msg = f"Marking {len(files)} file(s) as failed"
    logger.info(msg)

    nMarked = 0
    for lfn in files:
        source_lfn = lfn
        docId = getHashLfn(source_lfn)
        data = {}
        data['asoworker'] = asoworker
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


def getDBSInputInformation(taskname=None, crabServer=None):
    # LOOK UP Input dataset info in CRAB DB task table
    # for this task to know which DBS istance it was in
    data = {'subresource': 'search',
            'workflow': taskname}
    results = crabServer.get(api='task', data=encodeRequest(data))
    # TODO THERE are better ways to parse rest output into a dict !!
    inputDatasetIndex = results[0]['desc']['columns'].index("tm_input_dataset")
    inputDataset = results[0]['result'][inputDatasetIndex]
    # in case input was a Rucio container, remove the scope if any
    inputDataset = inputDataset.split(':')[-1]
    sourceURLIndex = results[0]['desc']['columns'].index("tm_dbs_url")
    sourceURL = results[0]['result'][sourceURLIndex]
    publishURLIndex = results[0]['desc']['columns'].index("tm_publish_dbs_url")
    publishURL = results[0]['result'][publishURLIndex]
    if not sourceURL.endswith("/DBSReader") and not sourceURL.endswith("/DBSReader/"):
        sourceURL += "/DBSReader"
    return (inputDataset, sourceURL, publishURL)
