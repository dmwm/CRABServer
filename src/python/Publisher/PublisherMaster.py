# pylint: disable=invalid-name  # have a lot of snake_case varaibles here from "old times"

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
import pickle
import tempfile
from datetime import datetime
import time
from pathlib import Path
from multiprocessing import Process

from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.Requests import Requests

from ServerUtilities import getColumn, encodeRequest, oracleOutputMapping, executeCommand
from ServerUtilities import getHashLfn
from ServerUtilities import getProxiedWebDir
from TaskWorker.WorkerUtilities import getCrabserver

from Publisher.PublisherUtils import createLogdir, setRootLogger, setSlaveLogger, logVersionAndConfig
from Publisher.PublisherUtils import getInfoFromFMD


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

        self.max_files_per_block = self.config.max_files_per_block
        self.startTime = time.time()

        # tasks which are too loarge for us to deal with are
        self.taskBlackList = []

    def active_tasks(self, crabServer):
        """
        :param crabServer: CRABRest object to access proper REST as createdin __init__ method
        TODO  detail here the strucutre it returns
        :return: a list tuples [(task,info)]. One element for each task which has jobs to be published
                 each list element has the format (task, info) where task and info are lists
                 task=[] a set fo 4 attributes formatted as a list of strings:
                    [username, usergroup, userrole, taskname]
                    this is a subset of the information present in each element of thee next list !
                 info=[filedic1, filedic2, filedic3...] a list of dictionaries,one per file with keys:
                   u'username', u'cache_url', u'source_lfn', u'publication_state', u'destination',
                   u'user_role', u'last_update', u'input_dataset', u'dbs_url', u'aso_worker',
                   u'user_group', u'taskname', u'transfer_state', u'destination_lfn'
        """

        self.logger.debug("Retrieving publications from oracleDB")
        filesToPublish = []
        asoworkers = self.config.asoworker
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

        # TO DO: join query for publisher (same of submitter)
        unique_tasks = [list(i) for i in set(tuple([x['username'],
                                                    x['user_group'],
                                                    x['user_role'],
                                                    x['taskname']]
                                                   ) for x in filesToPublish if x['transfer_state'] == 3)]

        info = []
        for task in unique_tasks:
            info.append([x for x in filesToPublish if x['taskname'] == task[3]])
        return list(zip(unique_tasks, info))

    def getTaskStatusFromSched(self, workflow, logger):
        """ find task status (need to know if it is terminal) """

        def translateStatus(statusToTr):
            """Translate from DAGMan internal integer status to a string.

            Uses parameter `dbstatus` to clarify if the state is due to the
            user killing the task. See:
            https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html#capturing-the-status-of-nodes-in-a-file
            """
            status = {0: 'PENDING', 1: 'SUBMITTED', 2: 'SUBMITTED', 3: 'SUBMITTED',
                      4: 'SUBMITTED', 5: 'COMPLETED', 6: 'FAILED'}[statusToTr]
            return status

        def collapseDAGStatus(dagInfo):
            """Collapse the status of one or several DAGs to a single one.
            Take into account that subdags can be submitted to the queue on the
            schedd, but not yet started.
            """
            status_order = ['PENDING', 'SUBMITTED', 'FAILED', 'COMPLETED']

            subDagInfos = dagInfo.get('SubDags', {})
            subDagStatus = dagInfo.get('SubDagStatus', {})
            # Regular splitting, return status of DAG
            if len(subDagInfos) == 0 and len(subDagStatus) == 0:
                return translateStatus(dagInfo['DagStatus'])

            def check_queued(statusOrSUBMITTED):
                # 99 is the status for a subDAG still to be submitted. An ad-hoc value
                # introduced in cache_status.py. If there are less
                # actual DAG status informations than expected DAGs, at least one
                # DAG has to be queued.
                if len(subDagInfos) < len([k for k in subDagStatus if subDagStatus[k] == 99]):
                    return 'SUBMITTED'
                return statusOrSUBMITTED

            # If the processing DAG is still running, we are 'SUBMITTED',
            # still.
            if len(subDagInfos) > 0:
                state = translateStatus(subDagInfos[0]['DagStatus'])
                if state == 'SUBMITTED':
                    return state
            # Tails active: return most active tail status according to
            # `status_order`
            if len(subDagInfos) > 1:
                states = [translateStatus(subDagInfos[k]['DagStatus']) for k in subDagInfos if k > 0]
                for iStatus in status_order:
                    if states.count(iStatus) > 0:
                        return check_queued(iStatus)
            # If no tails are active, return the status of the processing DAG.
            if len(subDagInfos) > 0:
                return check_queued(translateStatus(subDagInfos[0]['DagStatus']))
            return check_queued(translateStatus(dagInfo['DagStatus']))

        crabDBInfo, _, _ = self.crabServer.get(api='task', data={'subresource': 'search', 'workflow': workflow})
        dbStatus = getColumn(crabDBInfo, 'tm_task_status')
        if dbStatus == 'KILLED':
            return 'KILLED'
        proxiedWebDir = getProxiedWebDir(crabserver=self.crabServer, task=workflow, logFunction=logger)
        # Download status_cache file
        local_status_cache_fd, local_status_cache_pkl = tempfile.mkstemp(dir='/tmp', prefix='status-cache-', suffix='.pkl')
        url = proxiedWebDir + "/status_cache.pkl"
        # this host is dummy since we will pass full url to downloadFile but WMCore.Requests needs it
        host = 'https://cmsweb.cern.ch'
        cdict = {'cert': self.config.serviceCert, 'key': self.config.serviceKey}
        req = Requests(url=host, idict=cdict)
        _, ret = req.downloadFile(local_status_cache_pkl, url)
        if ret.status != 200:
            raise Exception(f"download attempt returned HTTP code {ret.status}")
        with open(local_status_cache_pkl, 'rb') as fp:
            statusCache = pickle.load(fp)
        os.close(local_status_cache_fd)
        os.remove(local_status_cache_pkl)
        # get DAG status from downloaded cache_status file
        statusCacheInfo = statusCache['nodes']
        dagInfo = statusCacheInfo['DagStatus']
        dagStatus = collapseDAGStatus(dagInfo)  # takes care of possible multiple subDAGs for automatic splitting
        status = dagStatus
        return status

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
            taskName = task[0][3]
            acquiredFiles = len(task[1])
            flag = '  OK' if acquiredFiles < 1000 else 'WARN'  # mark suspicious tasks
            self.logger.info('acquired_files: %s %5d : %s', flag, acquiredFiles, taskName)

        processes = []
        try:  # pylint: disable=too-many-nested-blocks
            for task in tasks:
                taskname = str(task[0][3])
                # this IF is for testing on preprod or dev DB's, which are full of old unpublished tasks
                if int(taskname[0:4]) < 2008:
                    self.logger.info("Skipped %s. Ignore tasks created before August 2020.", taskname)
                    continue
                username = task[0][0]
                if username in self.config.skipUsers:
                    self.logger.info("Skipped user %s task %s", username, taskname)
                    continue
                if self.sequential:
                    self.startSlave(task)   # sequentially do one task after another
                    continue
                # else deal with each task in a separate process
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

    def startSlave(self, task):  # pylint: disable=too-many-branches, too-many-locals, too-many-statements
        """
        start a slave process to deal with publication for a single task
        :param task: one tupla describing  a task as returned by  active_tasks()
        :return: 0  It will always terminate normally, if publication fails it will mark it in the DB
        """
        # - process logger
        logger = setSlaveLogger(self.config.logsDir, str(task[0][3]))
        logger.info("Process %s is starting. PID %s", task[0][3], os.getpid())

        self.force_publication = False
        workflow = str(task[0][3])

        self.taskBlackList = os.listdir(self.blackListedTaskDir)
        logger.debug('==== inside SLAVE for %s using blacklist %s', workflow, self.taskBlackList)

        if len(task[1]) > self.max_files_per_block:
            self.force_publication = True
            msg = f"All datasets have more than {self.max_files_per_block} ready files."
            msg += " No need to retrieve task status nor last publication time."
            logger.info(msg)
        else:
            msg = f"At least one dataset has less than {self.max_files_per_block} ready files. Retrieve task status"
            logger.info(msg)
            try:
                workflow_status = self.getTaskStatusFromSched(workflow, logger)
            except Exception as ex:  # pylint: disable=broad-except
                logger.warning('Error retrieving status cache from sched for %s:\n%s', workflow, str(ex))
                logger.warning('Assuming COMPLETED in order to force pending publications if any')
                workflow_status = 'COMPLETED'
            logger.info('Task status from DAG info: %s', workflow_status)
            # If the workflow status is terminal, go ahead and publish all the ready files
            # in the workflow.
            if workflow_status in ['COMPLETED', 'FAILED', 'KILLED', 'REMOVED', 'FAILED (KILLED)']:
                self.force_publication = True
                if workflow_status in ['KILLED', 'REMOVED']:
                    self.force_failure = True
                msg = "Considering task status as terminal. Will force publication."
                logger.info(msg)
            # Otherwise...
            else:    # TO DO put this else in a function like def checkForPublication()
                msg = "Task status is not considered terminal. Will check last publication time."
                logger.info(msg)
                # Get when was the last time a publication was done for this workflow (this
                # should be more or less independent of the output dataset in case there are
                # more than one).
                last_publication_time = None
                data = encodeRequest({'workflow': workflow, 'subresource': 'search'})
                try:
                    result = self.crabServer.get(api='task', data=data)
                    last_publication_time = getColumn(result[0], 'tm_last_publication')
                except Exception as ex:  # pylint: disable=broad-except
                    logger.error("Error during task info retrieving:\n%s", ex)
                if last_publication_time:
                    date = last_publication_time  # datetime in Oracle format
                    timetuple = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f").timetuple()
                    last_publication_time = time.mktime(timetuple)  # convert to seconds since Epoch (float)

                msg = f"Last publication time: {last_publication_time}."
                logger.debug(msg)
                # If this is the first time a publication would be done for this workflow, go
                # ahead and publish.
                if not last_publication_time:
                    self.force_publication = True
                    msg = "There was no previous publication. Will force publication."
                    logger.info(msg)
                # Otherwise...
                else:
                    last = last_publication_time
                    msg = f"Last published block time: {last}"
                    logger.debug(msg)
                    # If the last publication was long time ago (> our block publication timeout),
                    # go ahead and publish.
                    now = int(time.time()) - time.timezone
                    time_since_last_publication = now - last
                    hours = int(time_since_last_publication / 60 / 60)
                    minutes = int((time_since_last_publication - hours * 60 * 60) / 60)
                    timeout_hours = int(self.block_publication_timeout / 60 / 60)
                    timeout_minutes = int((self.block_publication_timeout - timeout_hours * 60 * 60) / 60)
                    msg = f"Last publication was {hours}h:{minutes}m ago"
                    if time_since_last_publication > self.block_publication_timeout:
                        self.force_publication = True
                        msg += f" (more than the timeout of {timeout_hours}h:{timeout_minutes}m)."
                        msg += " Will force publication."
                    else:
                        msg += F" (less than the timeout of {timeout_hours}h:{timeout_minutes}m)."
                        msg += " Not enough to force publication."
                    logger.info(msg)

        # logger.info(task[1])
        try:
            if self.force_publication:
                # - get info
                active_ = [{'key': [x['username'],
                                    x['user_group'],
                                    x['user_role'],
                                    x['taskname']],
                            'value': [x['destination'],
                                      x['source_lfn'],
                                      x['destination_lfn'],
                                      x['input_dataset'],
                                      x['dbs_url'],
                                      x['last_update']
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
                    # retrieve information from FileMetadata
                    (publDescFiles_list, blackList) = getInfoFromFMD(
                        crabServer=self.crabServer, taskname=workflow, lfns=lfn_ready, logger=logger)
                    if blackList:
                        self.taskBlackList.append(workflow)  # notify this slave
                        filepath = Path(os.path.join(self.blackListedTaskDir, workflow))
                        filepath.touch()  # notify other slaves
                        logger.debug('++++++++ BLACKLIST TASK %s ++', workflow)

                # now combine the info from FMD with the info from transfersdb (in active_)
                # to create the JSON files (one per task) to be put in Publisher_Files and
                # used by TaskPublish.py as input
                # current format for Publisher_schedd is a list of dictionaries, one per file
                # each dictionary has these keys, most values come straight from FileMetadata table
                # a (*) indicated values which in Publisher_schedd are obtained from info in transfersdb
                # which we now have in task['files']
                # 'taskname', 'filetype', 'jobid', 'outdataset', 'acquisitionera',
                # 'swversion', 'inevents', 'globaltag', 'publishname', 'location',
                # 'tmplocation', 'runlumi',
                #  'adler32', 'cksum', 'md5', 'lfn'(*), 'filesize',
                #  'parents', 'state', 'created', 'tmplfn', 'User'(*)
                #  'Destination'(*), 'SourceLFN(*)'
                # Clearly `taskname`, 'acquisitionera', 'swversion', 'globaltag' are common,
                # others could be different from one file to another (even if we do not support multiple
                # outputdataset at this moment

                for file_ in active_:
                    if workflow in self.taskBlackList:
                        toFail.append(file_["value"][1])  # mark all files as failed to avoid to look at them again
                        continue
                    metadataFound = False
                    for doc in publDescFiles_list:
                        # logger.info(type(doc))
                        # logger.info(doc)
                        if doc["lfn"] == file_["value"][2]:
                            doc["User"] = username
                            doc["Destination"] = file_["value"][0]
                            doc["SourceLFN"] = file_["value"][1]
                            toPublish.append(doc)
                            metadataFound = True
                            break
                    # if we failed to find metadata mark publication as failed to avoid to keep looking
                    # at same files over and over
                    if not metadataFound:
                        toFail.append(file_["value"][1])
                with open(self.taskFilesDir + workflow + '.json', 'w', encoding='utf-8') as outfile:
                    json.dump(toPublish, outfile)
                logger.debug('Unitarity check: active_:%d toPublish:%d toFail:%d', len(active_), len(toPublish), len(toFail))
                if len(toPublish) + len(toFail) != len(active_):
                    logger.error("SOMETHING WRONG IN toPublish vs toFail !!")
                if toFail:
                    logger.info('Did not find useful metadata for %d files. Mark as failed', len(toFail))
                    nMarked = 0
                    for lfn in toFail:
                        source_lfn = lfn
                        docId = getHashLfn(source_lfn)
                        data = {}
                        data['asoworker'] = self.config.asoworker
                        data['subresource'] = 'updatePublication'
                        data['list_of_ids'] = docId
                        data['list_of_publication_state'] = 'FAILED'
                        data['list_of_retry_value'] = 1
                        data['list_of_failure_reason'] = 'File type not EDM or metadata not found'
                        try:
                            result = self.crabServer.post(api='filetransfers', data=encodeRequest(data))
                        except Exception as ex:  # pylint: disable=broad-except
                            logger.error("Error updating status for DocumentId: %s lfn: %s", docId, source_lfn)
                            logger.error("Error reason: %s", ex)

                        nMarked += 1
                    logger.info('marked %d files as Failed', nMarked)

                # find the location in the current environment of the script we want to run
                import Publisher.TaskPublish as tp  # pylint: disable=import-outside-toplevel
                taskPublishScript = tp.__file__
                cmd = f"python3 {taskPublishScript} "
                cmd += f" --configFile={self.configurationFile}"
                cmd += f" --taskname={workflow}"
                if self.TPconfig.dryRun:
                    cmd += " --dry"
                logger.info("Now execute: %s", cmd)
                stdout, stderr, exitcode = executeCommand(cmd)
                if exitcode != 0:
                    errorMsg = f"Failed to execute command: {cmd}.\n StdErr:\n {stderr}\n"
                    raise Exception(errorMsg)
                logger.info('TaskPublishScript done : %s', stdout)

                jsonSummary = stdout.split()[-1]
                with open(jsonSummary, 'r', encoding='utf-8') as fd:
                    summary = json.load(fd)
                result = summary['result']
                reason = summary['reason']

                taskname = summary['taskname']
                if result == 'OK':
                    if reason == 'NOTHING TO DO':
                        logger.info('Taskname %s is OK. Nothing to do', taskname)
                    else:
                        msg = f"Taskname {taskname} is OK. Published {summary['publishedFiles']} "
                        msg += f"files in {summary['publishedBlocks']} blocks."
                        if summary['nextIterFiles']:
                            msg += f" {summary['nextIterFiles']} files left for next iteration."
                        logger.info(msg)
                if result == 'FAIL':
                    logger.error('Taskname %s : TaskPublish failed with: %s', taskname, reason)
                    if reason == 'DBS Publication Failure':
                        logger.error('Taskname %s : %d blocks failed for a total of %d files',
                                     taskname, summary['failedBlocks'], summary['failedFiles'])
                        logger.error('Taskname %s : Failed block(s) details have been saved in %s',
                                     taskname, summary['failedBlockDumps'])
        except Exception as ex:  # pylint: disable=broad-except
            logger.exception("Exception when calling TaskPublish!\n%s", str(ex))

        return 0

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
