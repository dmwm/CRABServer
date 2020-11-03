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
from logging.handlers import TimedRotatingFileHandler
import os
import subprocess
import traceback
import sys
import json
from datetime import datetime
import time
from multiprocessing import Process

from MultiProcessingLog import MultiProcessingLog

from WMCore.Configuration import loadConfigurationFile
#from WMCore.Services.pycurl_manager import RequestHandler
#from retry import retry
from RESTInteractions import HTTPRequests
from ServerUtilities import getColumn, encodeRequest, oracleOutputMapping
from ServerUtilities import SERVICE_INSTANCES
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

def setMasterLogger(name='master'):
    """ Set the logger for the master process. The file used for it is logs/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    handler = TimedRotatingFileHandler('logs/processes/proc.c3id_%s.pid_%s.txt' % (name, os.getpid()), 'midnight', backupCount=30)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:"+name+":%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

def setSlaveLogger(name):
    """ Set the logger for a single slave process. The file used for it is logs/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    handler = TimedRotatingFileHandler('logs/processes/proc.c3id_%s.pid_%s.txt' % (name, os.getpid()), 'midnight', backupCount=30)
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

        def setRootLogger(logsDir, quiet=False, debug=True, console=False):
            """Sets the root logger with the desired verbosity level
               The root logger logs to logs/twlog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :arg bool console: it tells if to direct all printoput to console rather then files, useful for debug
            :return logger: a logger with the appropriate logger level."""

            createLogdir(logsDir)
            createLogdir(logsDir + '/processes')
            createLogdir(logsDir + '/tasks')

            if console:
                # if we are testing log to the console is easier
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog(logsDir + '/log.txt', when='midnight')
                logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
                logHandler.setFormatter(logFormatter)
                logging.getLogger().addHandler(logHandler)
            loglevel = logging.INFO
            if quiet:
                loglevel = logging.WARNING
            if debug:
                loglevel = logging.DEBUG
            logging.getLogger().setLevel(loglevel)
            logger = setMasterLogger()
            logger.debug("PID %s.", os.getpid())
            logger.debug("Logging level initialized to %s.", loglevel)
            return logger

        self.logger = setRootLogger(self.config.logsDir, quiet=quiet, debug=debug, console=self.TestMode)

        from WMCore.Credential.Proxy import Proxy
        proxy = Proxy({'logger':self.logger})
        from ServerUtilities import tempSetLogLevel
        with tempSetLogLevel(self.logger, logging.ERROR):
            self.myDN = proxy.getSubjectFromCert(certFile=self.config.serviceCert)


        # CRABServer REST API's (see CRABInterface)
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

        restURInoAPI = '/crabserver/' + dbInstance
        self.logger.info('Will connect to CRAB Data Base via URL: https://%s/%s', restHost, restURInoAPI)

        # CRAB REST API's
        self.REST_filetransfers = restURInoAPI + '/filetransfers'
        #self.REST_usertransfers = restURInoAPI +  '/fileusertransfers'
        self.REST_filemetadata = restURInoAPI + '/filemetadata'
        self.REST_workflow = restURInoAPI + '/workflow'
        self.REST_task = restURInoAPI + '/task'
        self.max_files_per_block = self.config.max_files_per_block
        self.crabServer = HTTPRequests(url=restHost,
                                       localcert=self.config.serviceCert,
                                       localkey=self.config.serviceKey,
                                       retry=3)


        #try:
        #    self.connection = RequestHandler(config={'timeout': 900, 'connecttimeout' : 900})
        #except Exception as ex:
        #    msg = "Error initializing the connection handler"
        #    msg += str(ex)
        #    msg += str(traceback.format_exc())
        #    self.logger.debug(msg)

    def active_tasks(self, db):
        """
        :param db: HTTPRequest object to access proper REST as createdin __init__ method
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
        uri = self.REST_filetransfers
        asoworkers = self.config.asoworker
        # asoworkers can be a string or a list of strings
        # but if it is a string, do not turn it into a list of chars !
        asoworkers = [asoworkers] if isinstance(asoworkers, basestring) else asoworkers
        for asoworker in asoworkers:
            self.logger.info("Processing publication requests for asoworker: %s", asoworker)
            fileDoc = {}
            fileDoc['asoworker'] = asoworker
            fileDoc['subresource'] = 'acquirePublication'
            data = encodeRequest(fileDoc)
            try:
                result = db.post(uri=uri, data=data)
            except Exception as ex:
                self.logger.error("Failed to acquire publications from %s: %s", uri, ex)
                return []

            self.logger.debug("Retrieving max.100000 acquired publications from oracleDB")
            fileDoc = dict()
            fileDoc['asoworker'] = asoworker
            fileDoc['subresource'] = 'acquiredPublication'
            fileDoc['grouping'] = 0
            fileDoc['limit'] = 100000
            uri = self.REST_filetransfers
            data = encodeRequest(fileDoc)
            try:
                results = db.get(uri=uri, data=data)
            except Exception as ex:
                self.logger.error("Failed to acquire publications from %s: %s", uri, ex)
                return []
            files = oracleOutputMapping(results)
            self.logger.info("%s acquired publications retrieved for asoworker %s", len(files), asoworker)
            filesToPublish.extend(files)


        # TODO: join query for publisher (same of submitter)
        unique_tasks = [list(i) for i in set(tuple([x['username'],
                                                    x['user_group'],
                                                    x['user_role'],
                                                    x['taskname']]
                                                  ) for x in filesToPublish if x['transfer_state'] == 3)]

        info = []
        for task in unique_tasks:
            info.append([x for x in filesToPublish if x['taskname'] == task[3]])
        return zip(unique_tasks, info)

    def getPublDescFiles(self, workflow, lfn_ready, logger):
        """
        Download and read the files describing what needs to be published
        CRAB REST does not have any good way to select from the DB only what we need
        most efficient way is to get full list for the task, and then trim it here
        see: https://github.com/dmwm/CRABServer/issues/6124
        """
        out = []

        uri = self.REST_filemetadata
        dataDict = {}
        dataDict['taskname'] = workflow
        dataDict['filetype'] = 'EDM'
        data = encodeRequest(dataDict)
        try:
            res = self.crabServer.get(uri=uri, data=data)
            # res is a 3-plu: (result, exit code, status)
            res = res[0]
        except Exception as ex:
            logger.error("Error during metadata retrieving from %s: %s", uri, ex)
            return out

        metadataList = [json.loads(md) for md in res['result']]  # CRAB REST returns a list of JSON objects
        for md in metadataList:
            # pick only the metadata we need
            if md['lfn'] in lfn_ready:
                out.append(md)

        logger.info('Got filemetadata for %d LFNs', len(out))
        return out

    def algorithm(self):
        """
        1. Get a list of users with files to publish from the REST
        2. For each user get a suitably sized input for publish
        3. Submit the publish to a subprocess
        """
        tasks = self.active_tasks(self.crabServer)

        # example code, uncomment to pick only one task
        # myTask='180912_142016:arizzi_crab_NanoDnRMXDYJetsToLL_M-105To160_TuneCUETP8M1_13TeV-amcaRunIISummer16MiniAODv2-PUMorio__heIV_v6-v22201'
        # tasksToDo=[]
        # for t in tasks:
        #  if t[0][3]==myTask:
        #  tasksToDo.append(t)
        # tasks = tasksToDo

        maxSlaves = self.config.max_slaves
        self.logger.info('kicking off pool for %s tasks in batches of %s', len(tasks), maxSlaves)
        self.logger.debug('list of tasks %s', [x[0][3] for x in tasks])
        processes = []

        try:
            for task in tasks:
                taskname = str(task[0][3])
                # this IF is for testing on preprod or dev DB's, which are full of old unpublished tasks
                if int(taskname[0:4]) < 2008:
                    self.logger.info("Skipped %s. Ignore tasks created before August 2020.", taskname)
                    continue
                if self.TestMode:
                    self.startSlave(task)   # sequentially do one task after another
                    continue
                else:                       # deal with each task in a separate process
                    p = Process(target=self.startSlave, args=(task,))
                    p.start()
                    self.logger.info('Starting process %s  pid=%s', p, p.pid)
                    processes.append(p)
                if len(processes) == maxSlaves:
                    while len(processes) == maxSlaves:
                        # wait until one process has completed
                        time.sleep(10)
                        for proc in processes:
                            if not proc.is_alive():
                                processes.remove(proc)

        except Exception:
            self.logger.exception("Error during process mapping")

    def startSlave(self, task):
        """
        start a slave process to deal with publication for a single task
        :param task: one tupla describing  a task as returned by  active_tasks()
        :return: 0  It will always terminate normally, if publication fails it will mark it in the DB
        """
        # TODO: lock task!
        # - process logger
        logger = setSlaveLogger(str(task[0][3]))
        logger.info("Process %s is starting. PID %s", task[0][3], os.getpid())

        self.force_publication = False
        workflow = str(task[0][3])

        if len(task[1]) > self.max_files_per_block:
            self.force_publication = True
            msg = "All datasets have more than %s ready files." % (self.max_files_per_block)
            msg += " No need to retrieve task status nor last publication time."
            logger.info(msg)
        else:
            msg = "At least one dataset has less than %s ready files." % (self.max_files_per_block)
            logger.info(msg)
            # Retrieve the workflow status. If the status can not be retrieved, continue
            # with the next workflow.
            workflow_status = ''
            msg = "Retrieving status"
            logger.info(msg)
            uri = self.REST_workflow
            data = encodeRequest({'workflow': workflow})
            try:
                res = self.crabServer.get(uri=uri, data=data)
            except Exception as ex:
                logger.warn('Error retrieving status from %s for %s.', uri, workflow)
                return 0

            try:
                workflow_status = res[0]['result'][0]['status']
                msg = "Task status is %s." % workflow_status
                logger.info(msg)
            except ValueError:
                msg = "Workflow removed from WM."
                logger.error(msg)
                workflow_status = 'REMOVED'
            except Exception as ex:
                msg = "Error loading task status!"
                msg += str(ex)
                msg += str(traceback.format_exc())
                logger.error(msg)
            # If the workflow status is terminal, go ahead and publish all the ready files
            # in the workflow.
            if workflow_status in ['COMPLETED', 'FAILED', 'KILLED', 'REMOVED']:
                self.force_publication = True
                if workflow_status in ['KILLED', 'REMOVED']:
                    self.force_failure = True
                msg = "Considering task status as terminal. Will force publication."
                logger.info(msg)
            # Otherwise...
            else:
                msg = "Task status is not considered terminal."
                logger.info(msg)
                msg = "Getting last publication time."
                logger.info(msg)
                # Get when was the last time a publication was done for this workflow (this
                # should be more or less independent of the output dataset in case there are
                # more than one).
                last_publication_time = None
                data = encodeRequest({'workflow':workflow, 'subresource':'search'})
                try:
                    result = self.crabServer.get(self.REST_task, data)
                    logger.debug("task: %s ", str(result[0]))
                    last_publication_time = getColumn(result[0], 'tm_last_publication')
                except Exception as ex:
                    logger.error("Error during task doc retrieving: %s", ex)
                if last_publication_time:
                    date = last_publication_time # datetime in Oracle format
                    timetuple = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f").timetuple()  # convert to time tuple
                    last_publication_time = time.mktime(timetuple)      # convert to seconds since Epoch (float)

                msg = "Last publication time: %s." % str(last_publication_time)
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
                    msg = "Last published block time: %s" % last
                    logger.debug(msg)
                    # If the last publication was long time ago (> our block publication timeout),
                    # go ahead and publish.
                    now = int(time.time()) - time.timezone
                    time_since_last_publication = now - last
                    hours = int(time_since_last_publication/60/60)
                    minutes = int((time_since_last_publication - hours*60*60)/60)
                    timeout_hours = int(self.block_publication_timeout/60/60)
                    timeout_minutes = int((self.block_publication_timeout - timeout_hours*60*60)/60)
                    msg = "Last publication was %sh:%sm ago" % (hours, minutes)
                    if time_since_last_publication > self.block_publication_timeout:
                        self.force_publication = True
                        msg += " (more than the timeout of %sh:%sm)." % (timeout_hours, timeout_minutes)
                        msg += " Will force publication."
                    else:
                        msg += " (less than the timeout of %sh:%sm)." % (timeout_hours, timeout_minutes)
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
                publDescFiles_list = self.getPublDescFiles(workflow, lfn_ready, logger)
                for file_ in active_:
                    for doc in publDescFiles_list:
                        # logger.info(type(doc))
                        # logger.info(doc)
                        if doc["lfn"] == file_["value"][2]:
                            doc["User"] = username
                            doc["Group"] = file_["key"][1]
                            doc["Role"] = file_["key"][2]
                            doc["UserDN"] = self.myDN
                            doc["Destination"] = file_["value"][0]
                            doc["SourceLFN"] = file_["value"][1]
                            toPublish.append(doc)
                with open(self.taskFilesDir + workflow + '.json', 'w') as outfile:
                    json.dump(toPublish, outfile)

                # find the location in the current environment of the script we want to run
                import Publisher.TaskPublish as tp
                taskPublishScript = tp.__file__
                cmd = "python %s " % taskPublishScript
                cmd += " --configFile=%s" % self.configurationFile
                cmd += " --taskname=%s" % workflow
                if self.TPconfig.dryRun:
                    cmd += " --dry"
                logger.info("Now execute: %s", cmd)
                stdout = subprocess.check_output(cmd, shell=True)
                logger.info('TaskPublishScript completed. Output is: %s', stdout)

        except Exception:
            logger.exception("Exception when calling TaskPublish!")

        return 0

    def pollInterval(self):
        """
        :return: the poll Interval from thius master configuration
        """
        return self.config.pollInterval


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
