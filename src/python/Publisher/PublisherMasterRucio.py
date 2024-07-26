# pylint: disable=invalid-name  # have a lot of snake_case variables here from "old times"

"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. create a multiprocessing Pool of size N
4. spawn a process per task that publish their files
"""

import argparse
import os
import json
import time

from pathlib import Path
from multiprocessing import Process

from WMCore.Configuration import loadConfigurationFile

from ServerUtilities import encodeRequest, oracleOutputMapping, executeCommand
from TaskWorker.WorkerUtilities import getCrabserver
from RucioUtils import getNativeRucioClient

from Publisher.PublisherUtils import createLogdir, setRootLogger, setSlaveLogger, logVersionAndConfig
from Publisher.PublisherUtils import getInfoFromFMD, markFailed


class Master():  # pylint: disable=too-many-instance-attributes
    """I am the main daemon kicking off all Publisher work via slave Publishers"""

    def __init__(self, confFile=None, sequential=False, logDebug=False, console=False):
        """
        Initialise class members

        :arg WMCore.Configuration config: input Publisher configuration
        :arg bool quiet: it tells if a quiet logger is needed
        :arg bool debug: it tells if needs a verbose logger
        :arg bool sequential: it tells if to run in test (no subprocesses) mode.
        """

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
        self.sequential = sequential
        self.taskFilesDir = self.config.taskFilesDir
        createLogdir(self.taskFilesDir)
        createLogdir(os.path.join(self.taskFilesDir, 'FailedBlocks'))
        # need a persistent place on disk to communicate among slaves
        self.blackListedTaskDir = os.path.join(self.taskFilesDir, 'BlackListedTasks')
        createLogdir(self.blackListedTaskDir)

        # if self.sequential is True, we want the log output to console
        self.logger = setRootLogger(self.config.logsDir, logDebug=logDebug, console=console)
        logVersionAndConfig(config, self.logger)

        # CRAB REST API
        self.crabServer = getCrabserver(restConfig=config.REST, agentName='CRABPublisher', logger=self.logger)

        # Rucio Client
        self.rucio = getNativeRucioClient(config=config.Rucio, logger=self.logger)

        self.startTime = time.time()

        # tasks which are too loarge for us to deal with
        self.taskBlackList = []

    def active_tasks(self, crabServer):
        """
        :param crabServer: CRABRest object to access proper REST as createdin __init__ method
        :return: a list of dictionaries, one for each task which has files waiting for publication
                in filetransfersdb: [{'task':taskname, 'username':username , 'scope':rucioScope,
                                     'fileDicts':list_of_filedicts}, ...].
                 First three keys are obvious, 'fileDicts' is a list of dictionaries, one per file with keys:
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
                # select files with transfer DONE and publication NEW and set publication to ACQUIRED
                result = crabServer.post(api='filetransfers', data=data)  # pylint: disable=unused-variable
            except Exception as ex:  # pylint: disable=broad-except
                self.logger.error("Failed to acquire publications from crabserver: %s", ex)
                return []

            self.logger.debug("Retrieving max.100000 acquired publications from oracleDB")
            fileDoc = {}
            fileDoc['asoworker'] = asoworker
            fileDoc['subresource'] = 'acquiredPublication'
            fileDoc['grouping'] = 0
            fileDoc['limit'] = 100000
            data = encodeRequest(fileDoc)
            try:
                results = crabServer.get(api='filetransfers', data=data)
            except Exception as ex:  # pylint: disable=broad-except
                self.logger.error("Failed to acquire publications from crabserver: %s", ex)
                return []
            files = oracleOutputMapping(results)
            self.logger.info("%s acquired publications retrieved for asoworker %s", len(files), asoworker)
            filesToPublish.extend(files)

        # a list of unique tasknames
        unique_tasks = list(set((x['taskname'] for x in filesToPublish)))

        taskList = []
        for task in unique_tasks:
            taskDict = {'taskname': task}
            fileList = []
            for file in filesToPublish:
                if file['taskname'] == task:
                    taskDict['username'] = file['username']
                    if file['dbs_blockname']:  # Ruio_ASO puts there a scope:name DID
                        (rucioScope, blockName) = file['dbs_blockname'].split(':')
                        taskDict['scope'] = rucioScope
                        file['dbs_blockname'] = blockName
                    taskDict['destination'] = file['destination']
                    fileList.append(file)
            taskDict['fileDicts'] = fileList
            taskList.append(taskDict)
        return taskList

    def runTaskPublish(self, workflow, logger):
        """
        forks a process which will run TaskPublishRucio.py
        """
        # find the location in the current environment of the script we want to run
        import Publisher.TaskPublishRucio as tp  # pylint: disable=import-outside-toplevel
        taskPublishScript = tp.__file__
        cmd = f"python3 {taskPublishScript}"
        cmd += f" --configFile={self.configurationFile}"
        cmd += f" --taskname={workflow}"
        if self.TPconfig.dryRun:
            cmd += " --dry"
        logger.info("Now execute: %s", cmd)
        stdout, stderr, exitcode = executeCommand(cmd)
        if exitcode != 0:
            errorMsg = f"Failed to execute command: {cmd}.\n StdErr:\n {stderr}."
            raise Exception(errorMsg)
        logger.info('TaskPublishScript done : %s', stdout)

        jsonSummary = stdout.split()[-1]
        with open(jsonSummary, 'r', encoding='utf8') as fd:
            summary = json.load(fd)
        result = summary['result']
        reason = summary['reason']

        taskname = summary['taskname']
        if result == 'OK':
            if reason == 'NOTHING TO DO':
                logger.info('Taskname %s is OK. Nothing to do', taskname)
            else:
                msg = f"Taskname {taskname} is OK."
                msg += f" Published {summary['publishedFiles']} files"
                msg += f" in {summary['publishedBlocks']} blocks."
                nextIterFiles = self.numAcquiredFiles - summary['publishedFiles']
                if nextIterFiles:
                    msg += f" {nextIterFiles} files left for next iteration."
                logger.info(msg)
        if result == 'FAIL':
            logger.error('Taskname %s : TaskPublish failed with: %s', taskname, reason)
            if reason == 'DBS Publication Failure':
                logger.error('Taskname %s : %d blocks failed for a total of %d files',
                             taskname, summary['failedBlocks'], summary['failedFiles'])
                logger.error('Taskname %s : Failed block(s) details have been saved in %s',
                             taskname, summary['failedBlockDumps'])

    def algorithm(self):  # pylint: disable=too-many-branches
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
        self.logger.info('++++++ Using this taskBlackList: %s', self.taskBlackList)
        # print one line per task with the number of files to be published. Allow to find stuck tasks
        self.logger.info(' # of acquired files : taskname')
        for task in tasks:
            taskName = task['taskname']
            acquiredFiles = len(task['fileDicts'])
            flag = '  OK' if acquiredFiles < 1000 else 'WARN'  # mark suspicious tasks
            self.logger.info('acquired_files: %s %5d : %s', flag, acquiredFiles, taskName)

        processes = []
        try:  # pylint: disable=too-many-nested-blocks
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
                if self.sequential:
                    self.startSlave(task)  # sequentially do one task after another
                    continue
                # deal with each task in a separate process
                p = Process(target=self.startSlave, args=(task,))
                p.start()
                self.logger.info('Starting process %s  pid=%s', p, p.pid)
                self.logger.info('PID %s will work on task %s', p.pid, taskname)
                processes.append(p)
                if len(processes) == maxSlaves:
                    while len(processes) == maxSlaves:
                        # wait until one process has completed
                        time.sleep(10)
                        for proc in processes.copy():
                            if not proc.is_alive():
                                self.logger.info('Terminated: %s pid=%s', proc, proc.pid)
                                processes.remove(proc)

        except Exception:  # pylint: disable=broad-except
            self.logger.exception("Error during process mapping")
        self.logger.info('No more tasks to care for. Wait for remaining %d processes to terminate', len(processes))
        while processes:
            time.sleep(10)
            for proc in processes.copy():
                if not proc.is_alive():
                    self.logger.info('Terminated: %s pid=%s', proc, proc.pid)
                    processes.remove(proc)

        self.logger.info("Algorithm iteration completed")
        self.logger.info("Wait %d sec for next cycle", self.pollInterval())
        newStartTime = time.strftime("%H:%M:%S", time.localtime(time.time() + self.pollInterval()))
        # BEWARE: any change to message in next line needs to be synchronized with
        # a change in Publisher/stop.sh otherwise that script will break
        self.logger.info("Next cycle will start at %s", newStartTime)

    def startSlave(self, task):  # pylint: disable=too-many-branches, too-many-locals
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
        numAcquiredFilesPerBlock = {}
        self.numAcquiredFiles = len(task['fileDicts'])  # pylint: disable=attribute-defined-outside-init
        for fileDict in task['fileDicts']:
            if fileDict['block_complete'] == 'OK':
                blockName = fileDict['dbs_blockname']
                if blockName not in numAcquiredFilesPerBlock:
                    numAcquiredFilesPerBlock[blockName] = 0
                numAcquiredFilesPerBlock[blockName] += 1
                blocksToPublish.add(blockName)

        # make sure that we have acquired all files in that block. ref #8491
        for blockName in blocksToPublish.copy():
            countOfAcquiredFilesInBlock = numAcquiredFilesPerBlock[blockName]
            countOfFilesInRucioDataset = len(list(self.rucio.list_content(scope=task['scope'], name=blockName)))
            if countOfAcquiredFilesInBlock != countOfFilesInRucioDataset:
                # wait until we have acquired all files which Rucio says are in this block
                blocksToPublish.remove(blockName)

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

        logger.info("Prepare publish info for %s blocks", len(blocksToPublish))

        # so far so good

        try:  # pylint: disable=too-many-nested-blocks
            if workflow in self.taskBlackList:
                logger.debug('++++++++ TASK %s IN BLACKLIST,SKIP FILEMETADATA RETRIEVE AND MARK FILES AS FAILED',
                             workflow)
                self.markAsFailed(lfns=lfnsToPublish, reason='Blacklisted Task')
                return 0
            # get filemetadata info for all files which needs to be published
            (filesInfoFromFMD, blackList) = getInfoFromFMD(
                crabServer=self.crabServer, taskname=workflow, lfns=lfnsToPublish, logger=logger)
            if blackList:
                self.taskBlackList.append(workflow)  # notify this slave
                filepath = Path(os.path.join(self.blackListedTaskDir, workflow))
                filepath.touch()  # notify other slaves
                logger.debug('++++++++ BLACKLIST TASK %s ++', workflow)

            blockDictsToPublish = []  # initialise the structure which will be dumped as JSON
            toPublish = []
            toFail = []

            for blockName in blocksToPublish:
                blockDict = {'block_name': blockName}
                blockDict['username'] = task['username']
                blockDict['origin_site'] = task['destination']
                filesInfo = []
                for fileInTDB in FilesInfoFromTBDInBlock[blockName]:
                    # this is the same for the wholw block, actually for the whole task
                    fileDict = {'source_lfn': fileInTDB['source_lfn']}
                    metadataFound = False
                    for fmd in filesInfoFromFMD:
                        if fileInTDB['destination_lfn'] == fmd['lfn']:
                            metadataFound = True
                            toPublish.append(fmd['lfn'])
                            for key in ['inevents', 'filetype', 'publishname', 'lfn', 'jobid', 'swversion',
                                        'runlumi', 'adler32', 'cksum', 'filesize', 'parents', 'state', 'created']:
                                fileDict[key] = fmd[key]
                            # following three vars. only need to be saved once, they are the same for all FMD
                            # but let's stay simple even if a bit redundant
                            acquisitionEra = fmd['acquisitionera']
                            globalTag = fmd['globaltag']
                            break
                    if not metadataFound:
                        toFail.append(fileDict['lfn'])
                        continue
                    filesInfo.append(fileDict)
                blockDict['files'] = filesInfo
                blockDict['acquisition_era_name'] = acquisitionEra
                blockDict['global_tag'] = globalTag
                blockDictsToPublish.append(blockDict)

            # save
            with open(self.taskFilesDir + workflow + '.json', 'w', encoding='utf8') as outfile:
                json.dump(blockDictsToPublish, outfile)

            logger.debug('Unitarity check: active_:%d toPublish:%d toFail:%d', len(lfnsToPublish), len(toPublish),
                         len(toFail))
            if len(toPublish) + len(toFail) != len(lfnsToPublish):
                logger.error("SOMETHING WRONG IN toPublish vs toFail !!")

            if toFail:
                logger.info('Did not find useful metadata for %d files. Mark as failed', len(toFail))
                self.markAsFailed(lfns=toFail, reason='FileMetadata not found')

            # call taskPublishRucio
            self.runTaskPublish(workflow, logger)

        except Exception as ex:  # pylint: disable=broad-except
            logger.exception("Exception when calling TaskPublish!\n%s", str(ex))

        return 0

    def markAsFailed(self, lfns=None, reason=None):
        """
        handy wrapper for PublisherUtils/markFailed
        """
        markFailed(files=lfns, crabServer=self.crabserver, failureReason=reason,
                             asoworker=self.config.asoworker, logger=self.logger)

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
    # need to pass the configuration file path to the slaves
    configurationFile = os.path.abspath(args.config)

    master = Master(confFile=configurationFile)
    while True:
        master.algorithm()
        time.sleep(master.pollInterval())
