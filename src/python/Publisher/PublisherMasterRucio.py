# pylint: disable=C0103, W0703, R0912, R0914, R0915

"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. create a multiprocessing Pool of size N
4. spawn a process per task that publish their files
"""

from __future__ import division
from __future__ import print_function
import argparse
import logging
from logging import FileHandler
from logging.handlers import TimedRotatingFileHandler
import os
import traceback
import sys
import json
import pickle
import tempfile
from datetime import datetime
import time

from pathlib import Path
from multiprocessing import Process
from MultiProcessingLog import MultiProcessingLog

from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.Requests import Requests

from RESTInteractions import CRABRest
from ServerUtilities import getColumn, encodeRequest, oracleOutputMapping, executeCommand
from ServerUtilities import SERVICE_INSTANCES
from ServerUtilities import getProxiedWebDir
from ServerUtilities import getHashLfn
from TaskWorker import __version__
from TaskWorker.WorkerExceptions import ConfigException


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    :param l: list to splitt in chunks
    :param n: chunk size
    :return: yield the next list chunk
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]

def setMasterLogger(logsDir, name='master'):
    """ Set the logger for the master process. The file used for it is logs/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    fileName = os.path.join(logsDir, 'processes', "proc.c3id_%s.pid_%s.txt" % (name, os.getpid()))
    handler = TimedRotatingFileHandler(fileName, 'midnight', backupCount=30)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:"+name+":%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def setSlaveLogger(logsDir, name):
    """ Set the logger for a single slave process. The file used for it is logs/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    fileName = os.path.join(logsDir, 'processes', "proc.c3id_%s.txt" % name)
    #handler = TimedRotatingFileHandler(fileName, 'midnight', backupCount=30)
    # slaves are short lived, use one log file for each
    handler = FileHandler(fileName)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:"+"slave"+":%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class Master(object):
    """I am the main daemon kicking off all Publisher work via slave Publishers"""

    def __init__(self, confFile=None, quiet=False, debug=True, testMode=False):
        """
        Initialise class members

        :arg WMCore.Configuration config: input Publisher configuration
        :arg bool quiet: it tells if a quiet logger is needed
        :arg bool debug: it tells if needs a verbose logger
        :arg bool testMode: it tells if to run in test (no subprocesses) mode.
        """

        def createLogdir(dirname):
            """ Create the directory dirname ignoring erors in case it exists. Exit if
                the directory cannot be created.
            """
            try:
                os.makedirs(dirname)
            except OSError as ose:
                if ose.errno != 17: #ignore the "Directory already exists error"
                    print(str(ose))
                    print("The Publisher Worker needs to access the '%s' directory" % dirname)
                    sys.exit(1)

        def setRootLogger(logsDir, quiet=False, debug=True, console=False):
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
            if quiet:
                loglevel = logging.WARNING
            if debug:
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
            # one line for automatic parsing
            logger.info('PUBSTART: %s', json.dumps(pubstartDict))
            # multiple lines for humans to read
            for k, v in pubstartDict.items():
                logger.info('%s: %s', k, v)
            return

        self.configurationFile = confFile         # remember this, will have to pass it to TaskPublish
        config = loadConfigurationFile(confFile)
        self.config = config.General
        self.TPconfig = config.TaskPublisher

        # these are used for talking to DBS
        os.putenv('X509_USER_CERT', self.config.serviceCert)
        os.putenv('X509_USER_KEY', self.config.serviceKey)

        self.block_publication_timeout = self.config.block_closure_timeout
        self.lfn_map = {}
        self.force_publication = False
        self.force_failure = False
        self.TestMode = testMode
        self.taskFilesDir = self.config.taskFilesDir
        createLogdir(self.taskFilesDir)
        createLogdir(os.path.join(self.taskFilesDir, 'FailedBlocks'))
        # need a persistent place on disk to communicate among slaves
        self.blackListedTaskDir = os.path.join(self.taskFilesDir, 'BlackListedTasks')

        createLogdir(self.blackListedTaskDir)
        self.logger = setRootLogger(self.config.logsDir, quiet=quiet, debug=debug, console=self.TestMode)
        logVersionAndConfig(config, self.logger)

        try:
            instance = self.config.instance
        except:
            msg = "No instance provided: need to specify config.General.instance in the configuration"
            raise ConfigException(msg)

        if instance in SERVICE_INSTANCES:
            self.logger.info('Will connect to CRAB service: %s', instance)
            restHost = SERVICE_INSTANCES[instance]['restHost']
            dbInstance = SERVICE_INSTANCES[instance]['dbInstance']
        else:
            msg = "Invalid instance value '%s'" % instance
            raise ConfigException(msg)
        if instance == 'other':
            self.logger.info('Will use restHost and dbInstance from config file')
            try:
                restHost = self.config.restHost
                dbInstance = self.config.dbInstance
            except:
                msg = "Need to specify config.General.restHost and dbInstance in the configuration"
                raise ConfigException(msg)

        self.logger.info('Will connect to CRAB Data Base %s instance via URL: https://%s', dbInstance, restHost)

        # CRAB REST API's
        self.max_files_per_block = self.config.max_files_per_block
        self.crabServer = CRABRest(hostname=restHost, localcert=self.config.serviceCert,
                                   localkey=self.config.serviceKey, retry=3,
                                   userAgent='CRABPublisher')
        self.crabServer.setDbInstance(dbInstance=dbInstance)
        self.startTime = time.time()

        # tasks which are too loarge for us to deal with are
        self.taskBlackList = []

    def active_tasks(self, crabServer):
        """
        :param crabServer: CRABRest object to access proper REST as createdin __init__ method
        TODO  detail here the strucutre it returns
        TODO most keys here are unused !
        :return: a list of dictionaries [{'task':taskname, 'username':username , 'fileDicts':list_of_filedicts}, ...].
                 First two keys are obvious, 'fileDicts' is a list of dictionaries, one per file with keys:
                   'username', 'cache_url', 'source_lfn', 'publication_state', 'destination',
                   'last_update', 'input_dataset', 'dbs_url', 'aso_worker',
                   'transfer_state', 'destination_lfn', 'dbs_blockname', 'block_complete'
        """

        self.logger.debug("Retrieving publications from oracleDB")
        filesToPublish = []
        asoworkers = self.config.asoworker
        # it is far from obvious that we will ever have same Publisher code for multiple asoworkers.. anyhow
        # asoworkers can be a string or a list of strings
        # but if it is a string, do not turn it into a list of chars !
        asoworkers = [asoworkers] if isinstance(asoworkers, str) else asoworkers
        for asoworker in asoworkers:
            self.logger.info("Processing publication requests for asoworker: %s", asoworker)
            fileDoc = {}
            fileDoc['asoworker'] = asoworker
            fileDoc['subresource'] = 'acquirePublication'
            data = encodeRequest(fileDoc)
            try:
                result = crabServer.post(api='filetransfers', data=data)  # pylint: disable=unused-variable
            except Exception as ex:
                self.logger.error("Failed to acquire publications from crabserver: %s", ex)
                return []

            self.logger.debug("Retrieving max.100000 acquired publications from oracleDB")
            fileDoc = dict()
            fileDoc['asoworker'] = asoworker
            fileDoc['subresource'] = 'acquiredPublication'
            fileDoc['grouping'] = 0
            fileDoc['limit'] = 100000
            data = encodeRequest(fileDoc)
            try:
                # select files with transfer DONE and publication NEW and set publication to ACQUIRED
                results = crabServer.get(api='filetransfers', data=data)
            except Exception as ex:
                self.logger.error("Failed to acquire publications from crabserver: %s", ex)
                return []
            files = oracleOutputMapping(results)
            self.logger.info("%s acquired publications retrieved for asoworker %s", len(files), asoworker)
            filesToPublish.extend(files)

        # a list of unique tasknames
        unique_tasks = [taskName for taskName in set(x['taskname'] for x in filesToPublish)]

        # TODO will the following be more clear using list comprehension ?
        taskList = []
        for task in unique_tasks:
            taskDict = {'taskname': task}
            fileList = []
            for file in filesToPublish:
                if file['taskname'] == task:
                    taskDict['username'] = file['username']
                    taskDict['destination'] = file['destination']
                    fileList.append(file)
            taskDict['fileDicts'] = fileList
            taskList.append(taskDict)
        return taskList

    def getInfoFromFMD(self, workflow, lfns, logger):
        """
        Download and read the files describing what needs to be published
        do on a small number of LFNs at a time (numFilesAtOneTime) to avoid
        hitting the URL length limit in CMSWEB/Apache
        input: lfns : a list of LFNs
        returns: a list of dictionaries, one per file, sorted by CRAB JobID
        """
        out = []
        dataDict = {}
        dataDict['taskname'] = workflow
        i = 0
        numFilesAtOneTime = 10
        logger.debug('FMDATA: will retrieve data for %d files', len(lfns))
        while i < len(lfns):
            dataDict['lfnList'] = lfns[i:i+numFilesAtOneTime]
            data = encodeRequest(dataDict)
            i += numFilesAtOneTime
            try:
                t1 = time.time()
                res = self.crabServer.get(api='filemetadata', data=data)
                # res is a 3-plu: (result, exit code, status)
                res = res[0]
                t2 = time.time()
                elapsed = int(t2-t1)
                fmdata = int(len(str(res))/1e6) # convert dict. to string to get actual length in HTTP call
                logger.debug('FMDATA: retrieved data for %d files', len(res['result']))
                logger.debug('FMDATA: retrieved: %d MB in %d sec for %s', fmdata, elapsed, workflow)
                if elapsed > 60 and fmdata > 100:  # more than 1 minute and more than 100MB
                    self.taskBlackList.append(workflow)  # notify this slave
                    filepath = Path(os.path.join(self.blackListedTaskDir, workflow))
                    filepath.touch()  # notify other slaves
                    logger.debug('++++++++ BLACKLIST2 TASK %s ++', workflow)
            except Exception as ex:
                t2 = time.time()
                elapsed = int(t2-t1)
                logger.error("Error during metadata2 retrieving from crabserver:\n%s", ex)
                if elapsed > 290:
                    logger.debug('FMDATA gave error after > 290 secs. Most likely it timed out')
                    self.taskBlackList.append(workflow)  # notify this slave
                    filepath = Path(os.path.join(self.blackListedTaskDir, workflow))
                    filepath.touch()  # notify other slaves
                    logger.debug('++++++++ BLACKLIST TASK %s ++', workflow)
                return []
            metadataList = [json.loads(md) for md in res['result']]  # CRAB REST returns a list of JSON objects
            for md in metadataList:
                out.append(md)

        logger.info('Got filemetadata for %d LFNs', len(out))
        return out


    def runTaskPublish(self, workflow, logger):
        # find the location in the current environment of the script we want to run
        import Publisher.TaskPublishRucio as tp
        taskPublishScript = tp.__file__
        cmd = "python3 %s " % taskPublishScript
        cmd += " --configFile=%s" % self.configurationFile
        cmd += " --taskname=%s" % workflow
        if self.TPconfig.dryRun:
            cmd += " --dry"
        logger.info("Now execute: %s", cmd)
        stdout, stderr, exitcode = executeCommand(cmd)
        if exitcode != 0:
            errorMsg = 'Failed to execute command: %s.\n StdErr: %s.' % (cmd, stderr)
            raise Exception(errorMsg)
        else:
            logger.info('TaskPublishScript done : %s', stdout)

        jsonSummary = stdout.split()[-1]
        with open(jsonSummary, 'r') as fd:
            summary = json.load(fd)
        result = summary['result']
        reason = summary['reason']

        taskname = summary['taskname']
        if result == 'OK':
            if reason == 'NOTHING TO DO':
                logger.info('Taskname %s is OK. Nothing to do', taskname)
            else:
                msg = 'Taskname %s is OK. Published %d files in %d blocks.' % \
                      (taskname, summary['publishedFiles'], summary['publishedBlocks'])
                if summary['nextIterFiles']:
                    msg += ' %d files left for next iteration.' % summary['nextIterFiles']
                logger.info(msg)
        if result == 'FAIL':
            logger.error('Taskname %s : TaskPublish failed with: %s', taskname, reason)
            if reason == 'DBS Publication Failure':
                logger.error('Taskname %s : %d blocks failed for a total of %d files',
                             taskname, summary['failedBlocks'], summary['failedFiles'])
                logger.error('Taskname %s : Failed block(s) details have been saved in %s',
                             taskname, summary['failedBlockDumps'])



    def algorithm(self):
        """
        1. Get a list of files to publish from the REST and organize by taskname
        2. For each taks get a suitably sized input for publish
        3. Submit the publish to a subprocess
        """

        self.startTime = time.time()
        tasks = self.active_tasks(self.crabServer)

        # example code, uncomment to pick only one task
        # myTask='180912_142016:arizzi_crab_NanoDnRMXDYJetsToLL_M-105To160_TuneCUETP8M1_13TeV-amcaRunIISummer16MiniAODv2-PUMorio__heIV_v6-v22201'
        # tasksToDo=[]
        # for t in tasks:
        #  if t[0][3]==myTask:
        #  tasksToDo.append(t)
        # tasks = tasksToDo

        maxSlaves = self.config.max_slaves
        self.logger.info('kicking off pool for %s tasks using up to %s concurrent slaves', len(tasks), maxSlaves)
        self.taskBlackList = os.listdir(self.blackListedTaskDir)
        self.logger.debug('++++++ Using this taskBlackList: %s', self.taskBlackList)
        # print one line per task with the number of files to be published. Allow to find stuck tasks
        self.logger.debug(' # of acquired files : taskname')
        for task in tasks:
            taskName = task['taskname']
            acquiredFiles = len(task['fileDicts'])
            flag = '  OK' if acquiredFiles < 1000 else 'WARN' # mark suspicious tasks
            self.logger.debug('acquired_files: %s %5d : %s', flag, acquiredFiles, taskName)

        processes = []
        try:
            for task in tasks:
                taskname = str(task['taskname'])
                # this IF is for testing on preprod or dev DB's, which are full of old unpublished tasks
                if int(taskname[0:6]) < 230712:
                    self.logger.info("Skipped %s. Ignore tasks created before July 12 2023.", taskname)
                    continue
                username = task['username']
                if username in self.config.skipUsers:
                    self.logger.info("Skipped user %s task %s", username, taskname)
                    continue
                if self.TestMode:
                    self.startSlave(task)   # sequentially do one task after another
                    continue
                else:                       # deal with each task in a separate process
                    p = Process(target=self.startSlave, args=(task,))
                    p.start()
                    self.logger.info('Starting process %s  pid=%s', p, p.pid)
                    self.logger.info('PID %s will work on task %s', p.pid, taskname)
                    processes.append(p)
                if len(processes) == maxSlaves:
                    while len(processes) == maxSlaves:
                        # wait until one process has completed
                        time.sleep(10)
                        for proc in processes:
                            if not proc.is_alive():
                                self.logger.info('Terminated: %s pid=%s', proc, proc.pid)
                                processes.remove(proc)

        except Exception:
            self.logger.exception("Error during process mapping")
        self.logger.info('No more tasks to care for. Wait for remaining %d processes to terminate', len(processes))
        while (processes):
            time.sleep(10)
            for proc in processes:
                if not proc.is_alive():
                    self.logger.info('Terminated: %s pid=%s', proc, proc.pid)
                    processes.remove(proc)

        self.logger.info("Algorithm iteration completed")
        self.logger.info("Wait %d sec for next cycle", self.pollInterval())
        newStartTime = time.strftime("%H:%M:%S", time.localtime(time.time()+self.pollInterval()))
        # BEWARE: any change to message in next line needs to be synchronized with
        # a change in Publisher/stop.sh otherwise that script will break
        self.logger.info("Next cycle will start at %s", newStartTime)

    def startSlave(self, task):
        """
        start a slave process to deal with publication for a single task
        :param task: one tuple describing  a task as returned by  active_tasks()
        :return: 0  It will always terminate normally, if publication fails it will mark it in the DB
        """
        workflow = task['taskname']
        # - process logger
        logger = setSlaveLogger(self.config.logsDir, workflow)
        logger.info("Process %s is starting. PID %s", workflow, os.getpid())

        self.taskBlackList = os.listdir(self.blackListedTaskDir)
        logger.debug('==== inside SLAVE for %s using blacklist %s', workflow, self.taskBlackList)

        # fill list of blocks completely transferred and closed in Rucio, which can be published in DBS
        blocksToPublish = set()
        for fileDict in task['fileDicts']:
            if fileDict['block_complete'] == 'OK':
                blocksToPublish.add(fileDict['dbs_blockname'])

        # prepare 2 structures:
        # one dictionary for each blocksToPublish with list of files in that block
        # a list of all LFN's in those blocks to use for retrieving filemetadata
        lfnsToPublish = []
        FilesInfoFromTBDInBlock = {}
        for blockName in blocksToPublish:
            filesInfo = []
            for fileDict in task['fileDicts']:
                if fileDict['dbs_blockname'] == blockName:
                    filesInfo.append(fileDict)
                    lfnsToPublish.append(fileDict['destination_lfn'])
            FilesInfoFromTBDInBlock[blockName] = filesInfo

        print(f"Prepare publish info for {len(blocksToPublish)} blocks")


        # so far so good

        try:
            if workflow in self.taskBlackList:
                logger.debug('++++++++ TASK %s IN BLACKLIST,SKIP FILEMETADATA RETRIEVE AND MARK FILES AS FAILED',
                             workflow)
                self.markAsFailed(lfns=lfnsToPublish, reason='Blacklisted Task')
                return 0
            else:
                # get filemetadata info for all files which needs to be published
                infoFMD = self.getInfoFromFMD(workflow, lfnsToPublish, logger)
                # sort the list by lfn, makes it easier to compare https://stackoverflow.com/a/73050
                filesInfoFromFMD = sorted(infoFMD, key=lambda md: md['lfn'])

            print(filesInfoFromFMD[0])


            # TODO this shoudl be a function
            # prepare json to save, will be a list of blocks, one dictionary per block
            #
            # {'name':blockname,
            #    then a few parmeters common to the whole block (to the task! actuallY)
            #  'username', 'acquisitionera', 'swversion', globaltag', 'origin_site'
            # and finally a list of dictionariesx, one per file
            #    'files':[fileDict1, fileDict2,...] }
            # each fileDict has these keys, most values come straight from FileMetadata table
            # a (*) indicated values which in are obtained from info in transfersdb and now in block['filesInfo']
            # 'filetype', 'jobid', 'outdataset', 'inevents', 'publishname', 'lfn'
            # 'runlumi', 'adler32', 'cksum', 'filesize',
            # 'parents', 'state', 'created', 'destination'(*), 'source_lfn'(*)
            # info from FMD will be put in DBS (of course).
            # TODO 'state' appears unused. To be verifies
            # 'destination" will be put in DBS as origin_site
            # source_lfn" aka 'tmplfn' is needed to compute file name hash
            # so that the record in transferdb can be updated with publication
            # successful or not inside TaskPublishRUcio.py
            #

            blockDictsToPublish = []  # initialise the structure which will be dumped as JSON
            toPublish = []
            toFail = []

            for blockName in blocksToPublish:
                blockDict = {'name': blockName}
                blockDict['username'] = task['username']
                blockDict['origin_site'] = task['destination']
                filesInfo = []
                for fileInTDB in FilesInfoFromTBDInBlock[blockName]:
                    # this is the same for the wholw block, actually for the whole task
                    origin_site = fileInTDB['destination']
                    fileDict={'source_lfn': fileInTDB['source_lfn']}
                    metadataFound = False
                    for fmd in filesInfoFromFMD:
                        if fileInTDB['destination_lfn'] == fmd['lfn']:
                            metadataFound = True
                            toPublish.append(fmd['lfn'])
                            for key in ['inevents', 'filetype', 'publishname', 'lfn', 'jobid',
                                        'runlumi', 'adler32', 'cksum', 'filesize', 'parents', 'state', 'created'] :
                                fileDict[key] = fmd[key]
                            # following three vars. only need to be saved once, are the same for all FMD
                            # but let's stay simple even if a bit redundant
                            acquisitionera = fmd['acquisitionera']
                            swversion = fmd['swversion']
                            globaltag = fmd['globaltag']
                            break
                    if not metadataFound:
                        toFail.append(fileDict['lfn'])
                        continue
                    filesInfo.append(fileDict)
                blockDict['files'] = filesInfo
                blockDict['acquisitionera'] = acquisitionera
                blockDict['swversion'] = swversion
                blockDict['globaltag'] = globaltag
                blockDictsToPublish.append(blockDict)

            # save
            with open(self.taskFilesDir + workflow + '.json', 'w') as outfile:
                json.dump(blockDictsToPublish, outfile)

            logger.debug('Unitarity check: active_:%d toPublish:%d toFail:%d', len(lfnsToPublish), len(toPublish),
                         len(toFail))
            if len(toPublish) + len(toFail) != len(lfnsToPublish):
                logger.error("SOMETHING WRONG IN toPublish vs toFail !!")

            if toFail:
                logger.info('Did not find useful metadata for %d files. Mark as failed', len(toFail))
                nMarked = self.markAsFailed(lfns=toFail, reason='FileMetadata not found')
                logger.info('marked %d files as Failed', nMarked)

            # call taskPublishRucio
            #self.runTaskPublish(workflow, logger)

        except Exception as ex:
            logger.exception("Exception when calling TaskPublish!\n%s", str(ex))

        return 0

    def markAsFailed(self, lfns=None, reason=None):
        nMarked = 0
        for lfn in toFail:
            source_lfn = lfn
            docId = getHashLfn(source_lfn)
            data = dict()
            data['asoworker'] = self.config.asoworker
            data['subresource'] = 'updatePublication'
            data['list_of_ids'] = docId
            data['list_of_publication_state'] = 'FAILED'
            data['list_of_retry_value'] = 1
            data['list_of_failure_reason'] = reason
            try:
                result = self.crabServer.post(api='filetransfers', data=encodeRequest(data))
                # if nMarked % 10 == 0:
                # logger.debug("updated DocumentId: %s lfn: %s Result %s", docId, source_lfn, result)
            except Exception as ex:
                logger.error("Error updating status for DocumentId: %s lfn: %s", docId, source_lfn)
                logger.error("Error reason: %s", ex)
            nMarked += 1
        return nMarked


        """
        try:
            if self.force_publication:
                # - get info
                # SB Oh MY !!! this stuff stinks, smells of CoucheDB leftover, a plain dictionawry will do
                active_ = [{'key': [x['username'],
                                    x['taskname']],
                            'value': [x['destination'],
                                      x['source_lfn'],
                                      x['destination_lfn'],
                                      x['input_dataset'],
                                      x['dbs_url'],
                                      x['last_update'],
                                      x['dbs_blockname'],
                                      x['block_complete']
                                     ]}
                           for x in task[1] if x['transfer_state'] == 3 and x['publication_state'] not in [2, 3, 5]]

                lfn_ready = []
                wf_jobs_endtime = []
                pnn, input_dataset, input_dbs_url = "", "", ""
                for active_file in active_:
                    job_end_time = active_file['value'][5]
                    if job_end_time:
                        wf_jobs_endtime.append(int(job_end_time) - time.timezone)
                    source_lfn = active_file['value'][1]
                    dest_lfn = active_file['value'][2]
                    self.lfn_map[dest_lfn] = source_lfn
                    if not pnn or not input_dataset or not input_dbs_url:
                        pnn = str(active_file['value'][0])
                        input_dataset = str(active_file['value'][3])
                        input_dbs_url = str(active_file['value'][4])
                    lfn_ready.append(dest_lfn)

                username = task[0][0]

                # Get metadata
                toPublish = []
                toFail = []
                if workflow in self.taskBlackList:
                    logger.debug('++++++++ TASK %s IN BLACKLIST,SKIP FILEMETADATA RETRIEVE AND MARK FILES AS FAILED',
                                 workflow)
                else:
                    filesInfoFromFMD = self.getInfoFromFMD(workflow, lfn_ready, logger)
                for file_ in active_:
                    if workflow in self.taskBlackList:
                        toFail.append(file_["value"][1])  # mark all files as failed to avoid to look at them again
                        continue
                    metadataFound = False
                    for doc in filesInfoFromFMD:
                        # TODO SB OH MY.. another horrible CouchDB style thing !
                        # logger.info(type(doc))
                        # logger.info(doc)
                        if doc["lfn"] == file_["value"][2]:
                            doc["User"] = username
                            doc["UserDN"] = self.myDN
                            doc["Destination"] = file_["value"][0]
                            doc["SourceLFN"] = file_["value"][1]
                            doc["Block"] = file_["value"][6]
                            doc["BlockComplete"] = file["value"][7]
                            toPublish.append(doc)
                            metadataFound = True
                            break
                    # if we failed to find metadata mark publication as failed to avoid to keep looking
                    # at same files over and over
                    if not metadataFound:
                        toFail.append(file_["value"][1])
                with open(self.taskFilesDir + workflow + '.json', 'w') as outfile:
                    json.dump(toPublish, outfile)
                logger.debug('Unitarity check: active_:%d toPublish:%d toFail:%d', len(active_), len(toPublish), len(toFail))
                if len(toPublish) + len(toFail) != len(active_):
                    logger.error("SOMETHING WRONG IN toPublish vs toFail !!")
                if toFail:
                    logger.info('Did not find useful metadata for %d files. Mark as failed', len(toFail))
                    from ServerUtilities import getHashLfn
                    nMarked = 0
                    for lfn in toFail:
                        source_lfn = lfn
                        docId = getHashLfn(source_lfn)
                        data = dict()
                        data['asoworker'] = self.config.asoworker
                        data['subresource'] = 'updatePublication'
                        data['list_of_ids'] = docId
                        data['list_of_publication_state'] = 'FAILED'
                        data['list_of_retry_value'] = 1
                        data['list_of_failure_reason'] = 'File type not EDM or metadata not found'
                        try:
                            result = self.crabServer.post(api='filetransfers', data=encodeRequest(data))
                            #logger.debug("updated DocumentId: %s lfn: %s Result %s", docId, source_lfn, result)
                        except Exception as ex:
                            logger.error("Error updating status for DocumentId: %s lfn: %s", docId, source_lfn)
                            logger.error("Error reason: %s", ex)

                        nMarked += 1
                        #if nMarked % 10 == 0:
                    logger.info('marked %d files as Failed', nMarked)

                # find the location in the current environment of the script we want to run
                import Publisher.TaskPublish as tp
                taskPublishScript = tp.__file__
                cmd = "python3 %s " % taskPublishScript
                cmd += " --configFile=%s" % self.configurationFile
                cmd += " --taskname=%s" % workflow
                if self.TPconfig.dryRun:
                    cmd += " --dry"
                logger.info("Now execute: %s", cmd)
                stdout, stderr, exitcode = executeCommand(cmd)
                if exitcode != 0:
                    errorMsg = 'Failed to execute command: %s.\n StdErr: %s.' % (cmd, stderr)
                    raise Exception(errorMsg)
                else:
                    logger.info('TaskPublishScript done : %s', stdout)

                jsonSummary = stdout.split()[-1]
                with open(jsonSummary, 'r') as fd:
                    summary = json.load(fd)
                result = summary['result']
                reason = summary['reason']

                taskname = summary['taskname']
                if result == 'OK':
                    if reason == 'NOTHING TO DO':
                        logger.info('Taskname %s is OK. Nothing to do', taskname)
                    else:
                        msg = 'Taskname %s is OK. Published %d files in %d blocks.' % \
                              (taskname, summary['publishedFiles'], summary['publishedBlocks'])
                        if summary['nextIterFiles']:
                            msg += ' %d files left for next iteration.' % summary['nextIterFiles']
                        logger.info(msg)
                if result == 'FAIL':
                    logger.error('Taskname %s : TaskPublish failed with: %s', taskname, reason)
                    if reason == 'DBS Publication Failure':
                        logger.error('Taskname %s : %d blocks failed for a total of %d files',
                                     taskname, summary['failedBlocks'], summary['failedFiles'])
                        logger.error('Taskname %s : Failed block(s) details have been saved in %s',
                                     taskname, summary['failedBlockDumps'])
        except Exception as ex:
            logger.exception("Exception when calling TaskPublish!\n%s", str(ex))

        return 0
        """

    def pollInterval(self):
        """
        Decide when next polling cycle must start so that there is a start each
        self.config.pollInterval seconds
        :return: timeToWait: integer: the intervat time to wait before starting next cycle, in seconds
        """
        now = time.time()
        elapsed = int(now - self.startTime)
        timeToWait = self.config.pollInterval - elapsed
        timeToWait = max(timeToWait, 0)  # if too muach time has passed, start now !
        return timeToWait


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--config', help='Publisher config file', default='PublisherConfig.py')

    args = parser.parse_args()
    #need to pass the configuration file path to the slaves
    configurationFile = os.path.abspath(args.config)

    master = Master(confFile=configurationFile)
    while True:
        master.algorithm()
        time.sleep(master.pollInterval())
