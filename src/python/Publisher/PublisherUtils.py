"""
functions used both in Publisher_rucio and Publisher_schedd
mostly things which communicate with DBS or CRAB REST
"""


import os
import sys
import time
import logging
from logging import FileHandler
from logging.handlers import TimedRotatingFileHandler
import json


from MultiProcessingLog import MultiProcessingLog
from ServerUtilities import getHashLfn, encodeRequest
from TaskWorker import __version__


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
    """
    prepare log and work directories for TaskPublish and initializes a logger
    returs a dictionary log={logger, logdir, migrationlLogDir, taskFilesDir}
    """
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


def setMasterLogger(logsDir, name='master'):
    """ Set the logger for the master process. The file used for it is logs/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    fileName = os.path.join(logsDir, 'processes', f"proc.c3id_{name}.pid_{os.getpid()}.txt")
    handler = TimedRotatingFileHandler(fileName, 'midnight', backupCount=30)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:" + name + ":%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def setSlaveLogger(logsDir, name):
    """ Set the logger for a single slave process. The file used for it is logs/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    fileName = os.path.join(logsDir, 'processes', f"proc.c3id_{name}.txt")
    # slaves are short lived, use one log file for each
    handler = FileHandler(fileName)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:" + "slave" + ":%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def setRootLogger(logsDir, logDebug=False, console=False):
    """Sets the root logger with the desired verbosity level
       The root logger logs to logs/log.txt and every single
       logging instruction is propagated to it (not really nice
       to read)

    :arg bool quiet: it tells if a quiet logger is needed
    :arg bool debug: it tells if needs a verbose logger
    :arg bool console: it tells if to direct all printoput to console rather then files, useful for debug
    :return logger: a logger with the appropriate logger level."""

    createLogdir(logsDir)
    createLogdir(os.path.join(logsDir, 'processes'))
    createLogdir(os.path.join(logsDir, 'tasks'))

    if console:
        # if we are testing log to the console is easier
        logging.getLogger().addHandler(logging.StreamHandler())
    else:
        logHandler = MultiProcessingLog(os.path.join(logsDir, 'log.txt'), when='midnight')
        logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
    loglevel = logging.INFO
    if logDebug:
        loglevel = logging.DEBUG
    logging.getLogger().setLevel(loglevel)
    logger = setMasterLogger(logsDir)
    logger.debug("PID %s.", os.getpid())
    logger.debug("Logging level initialized to %s.", loglevel)
    return logger


def logVersionAndConfig(config=None, logger=None):
    """
    log version number and major config. parameters
    args: config : a configuration object loaded from file
    args: logger : the logger instance to use
    """
    pubstartDict = {}
    pubstartDict['version'] = __version__
    pubstartDict['asoworker'] = config.General.asoworker
    pubstartDict['instance'] = config.General.instance
    if config.General.instance == 'other':
        pubstartDict['restHost'] = config.General.restHost
        pubstartDict['dbInstance'] = config.General.dbInstance
    pubstartDict['max_slaves'] = config.General.max_slaves
    pubstartDict['DBShost'] = config.TaskPublisher.DBShost
    pubstartDict['dryRun'] = config.TaskPublisher.dryRun
    #log entry below is used for logs parsing, therefore, changing it might require to update logstash configuration
    logger.info('PUBSTART: %s', json.dumps(pubstartDict))
    # multiple lines for humans to read
    for k, v in pubstartDict.items():
        logger.info('%s: %s', k, v)


def prepareDummySummary(taskname):
    """ prepare a dummy summary JSON file in case there's nothing to do """
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


def markGood(files=None, crabServer=None, asoworker=None, logger=None):
    """
    Mark the list of files as published
    files must be a list of SOURCE_LFN's i.e. /store/temp/user/...
    """

    msg = f"Marking {len(files)} file(s) as published."
    logger.info(msg)

    nMarked = 0
    for lfn in files:
        sourceLfn = lfn
        docId = getHashLfn(sourceLfn)
        data = {}
        data['asoworker'] = asoworker
        data['subresource'] = 'updatePublication'
        data['list_of_ids'] = [docId]
        data['list_of_publication_state'] = ['DONE']
        data['list_of_retry_value'] = [1]
        data['list_of_failure_reason'] = ['']

        try:
            result = crabServer.post(api='filetransfers', data=encodeRequest(data))
            logger.debug("updated DocumentId: %s lfn: %s Result %s", docId, sourceLfn, result)
        except Exception as ex:  # pylint: disable=broad-except
            logger.error("Error updating status for DocumentId: %s lfn: %s", docId, sourceLfn)
            logger.error("Error reason: %s", ex)
            logger.error("Error reason: %s", ex)

        nMarked += 1
        if nMarked % 10 == 0:
            logger.info('marked %d files', nMarked)
    logger.info('total of %s files, marked as Good', nMarked)


def markFailed(files=None, crabServer=None, failureReason="", asoworker=None, logger=None):
    """
    Something failed for these files.
    files must be a list of SOURCE_LFN's i.e. /store/temp/user/...
    """
    msg = f"Marking {len(files)} file(s) as failed"
    logger.info(msg)

    nMarked = 0
    for lfn in files:
        sourceLfn = lfn
        docId = getHashLfn(sourceLfn)
        data = {}
        data['asoworker'] = asoworker
        data['subresource'] = 'updatePublication'
        data['list_of_ids'] = [docId]
        data['list_of_publication_state'] = ['FAILED']
        data['list_of_retry_value'] = [1]
        data['list_of_failure_reason'] = [failureReason]

        logger.debug("data: %s ", data)
        try:
            result = crabServer.post(api='filetransfers', data=encodeRequest(data))
            logger.debug("updated DocumentId: %s lfn: %s Result %s", docId, sourceLfn, result)
        except Exception as ex:  # pylint: disable=broad-except
            logger.error("Error updating status for DocumentId: %s lfn: %s", docId, sourceLfn)
            logger.error("Error reason: %s", ex)

        nMarked += 1
        if nMarked % 10 == 0:
            logger.info('marked %d files', nMarked)
        logger.info('total of %s files, marked as Failed', nMarked)


def getInfoFromFMD(crabServer=None, taskname=None, lfns=None, logger=None):
    """
    Download and read the files describing what needs to be published
    do on a small number of LFNs at a time (numFilesAtOneTime) to avoid
    hitting the URL length limit in CMSWEB/Apache
    input: lfns : list of strings - a list of LFNs
           taskname : string - the name of the task, needed to retrieve FMD
           crabServer: an instance of CRABRest - as configured via WorkerUtilities/getCrabserver
           logger: a logger
    returns: a tuple: (FMDs, blackList)
             FMDs: a list of dictionaries, one per file, sorted by CRAB JobID
             blackList: boolean, True if this task has problem with FMD access and we need to blacklist it
    """
    out = []
    dataDict = {}
    blackList = False
    dataDict['taskname'] = taskname
    i = 0
    numFilesAtOneTime = 10
    logger.debug('FMDATA: will retrieve data for %d files', len(lfns))
    while i < len(lfns):
        dataDict['lfnList'] = lfns[i: i + numFilesAtOneTime]
        data = encodeRequest(dataDict)
        i += numFilesAtOneTime
        t1 = time.time()
        try:
            res = crabServer.get(api='filemetadata', data=data)
            # res is a 3-ple: (result, exit code, status)
            res = res[0]
            t2 = time.time()
            elapsed = int(t2 - t1)
            fmdata = int(len(str(res)) / 1e6)  # convert dict. to string to get actual length in HTTP call
            logger.debug('FMDATA: retrieved data for %d files', len(res['result']))
            logger.debug('FMDATA: retrieved: %d MB in %d sec for %s', fmdata, elapsed, taskname)
            if elapsed > 60 and fmdata > 100:  # more than 1 minute and more than 100MB
                blackList = True
                return ([], blackList)
        except Exception as ex:  # pylint: disable=broad-except
            t2 = time.time()
            elapsed = int(t2 - t1)
            logger.error("Error during metadata retrieving from crabserver:\n%s", ex)
            if elapsed > 290:
                blackList = True
            return ([], blackList)
        metadataList = [json.loads(md) for md in res['result']]  # CRAB REST returns a list of JSON objects
        for md in metadataList:
            out.append(md)

    logger.info('Got filemetadata for %d LFNs', len(out))
    # sort the list by jobId, makes it easier to compare https://stackoverflow.com/a/73050
    # sort by jobid as strings w/o converting to int becasue of https://github.com/dmwm/CRABServer/issues/7246
    sortedOut = sorted(out, key=lambda md: md['jobid'])
    return (sortedOut, blackList)


def getDBSInputInformation(taskname=None, crabServer=None):
    """
    LOOK UP Input dataset info in CRAB DB task table
    for this task to know which DBS istance it was in
    """

    data = {'subresource': 'search',
            'workflow': taskname}
    results = crabServer.get(api='task', data=encodeRequest(data))
    # NOTICE: There are better ways to parse rest output into a dict !!
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
