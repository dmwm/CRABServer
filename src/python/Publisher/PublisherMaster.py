# pylint: disable=C0103,W0105,broad-except,logging-not-lazy,W0702,C0301,R0902,R0914,R0912,R0915,C0112,C0111

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
import datetime
import time
from retry import retry
from multiprocessing import Process

from MultiProcessingLog import MultiProcessingLog
from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.pycurl_manager import RequestHandler
from RESTInteractions import HTTPRequests
#from Publisher.utils import getDNFromUserName
from ServerUtilities import getColumn, encodeRequest, oracleOutputMapping


def setProcessLogger(name):
    """ Set the logger for a single process. The file used for it is logs/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    handler = TimedRotatingFileHandler('logs/processes/proc.c3id_%s.pid_%s.txt' % (name, os.getpid()), 'midnight', backupCount=30)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:"+name+":%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


class Master(object):
    """I am the main daemon kicking off all Publisher work via slave Publishers"""

    def __init__(self, configurationFile, quiet=False, debug=True, testMode=False):
        """
        Initialise class members

        :arg WMCore.Configuration config: input Publisher configuration
        :arg bool quiet: it tells if a quiet logger is needed
        :arg bool debug: it tells if needs a verbose logger
        :arg bool testMode: it tells if to run in test (no subprocesses) mode.
        """

        self.configurationFile = configurationFile
        config = loadConfigurationFile(configurationFile)

        self.config = config.General
        # CRABServer REST API's (see CRABInterface)
        self.REST_filetransfers = '/crabserver/' + self.config.instance + '/filetransfers'
        #self.REST_usertransfers = '/crabserver/' + self.config.instance +  '/fileusertransfers'
        self.REST_filemetadata = '/crabserver/' + self.config.instance + '/filemetadata'
        self.REST_workflow = '/crabserver/' + self.config.instance + '/workflow'
        self.REST_task = '/crabserver/' + self.config.instance + '/task'
        
        self.max_files_per_block = self.config.max_files_per_block
        # these are used for talking to DBS
        os.putenv('X509_USER_CERT', self.config.serviceCert)
        os.putenv('X509_USER_KEY', self.config.serviceKey)
        self.block_publication_timeout = self.config.block_closure_timeout
        self.lfn_map = {}
        self.force_publication = False
        self.force_failure = False
        self.TestMode = testMode
        self.taskFilesDir = self.config.taskFilesDir
        try:
            os.makedirs(self.taskFilesDir)
        except OSError as ose:
            if ose.errno != 17: #ignore the "Directory already exists error"
                print(str(ose))
                print("The Publisher needs to access the '%s' directory" % dirname)
                sys.exit(1)

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

        def setRootLogger(quiet=False, debug=True, console=False):
            """Sets the root logger with the desired verbosity level
               The root logger logs to logs/twlog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :arg bool console: it tells if to direct all printoput to console rather then files, useful for debug
            :return logger: a logger with the appropriate logger level."""

            createLogdir('logs')
            createLogdir('logs/processes')
            createLogdir('logs/tasks')

            if console:
                # if we are testing log to the console is easier
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog('logs/log.txt', when='midnight')
                logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
                logHandler.setFormatter(logFormatter)
                logging.getLogger().addHandler(logHandler)
            loglevel = logging.INFO
            if quiet:
                loglevel = logging.WARNING
            if debug:
                loglevel = logging.DEBUG
            logging.getLogger().setLevel(loglevel)
            logger = setProcessLogger("master")
            logger.debug("PID %s.", os.getpid())
            logger.debug("Logging level initialized to %s.", loglevel)
            return logger

        self.logger = setRootLogger(quiet, True, console=self.TestMode)

        from WMCore.Credential.Proxy import Proxy
        proxy = Proxy({'logger':self.logger})
        from ServerUtilities import tempSetLogLevel
        with tempSetLogLevel(self.logger,logging.ERROR):
            self.myDN = proxy.getSubjectFromCert(certFile=self.config.serviceCert)

        self.logger.debug('Contacting Oracle via CRABREST:' + self.config.RestHostName)
        try:
            self.oracleDB = HTTPRequests(url=self.config.RestHostName,
                                         localcert=self.config.serviceCert,
                                         localkey=self.config.serviceKey,
                                         retry=3)
        except:
            self.logger.exception('Failed when contacting Oracle')
            raise

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
        uri = self.REST_filetransfers
        fileDoc = {}
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquirePublication'
        data=encodeRequest(fileDoc)
        results = ''
        try:
            results = db.post(uri=uri, data=data)
        except Exception as ex:
            self.logger.error("Failed to acquire publications from %s: %s", uri, ex)
            return []

        self.logger.debug("Retrieving max.100000 acquired publications from oracleDB")
        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquiredPublication'
        fileDoc['grouping'] = 0
        fileDoc['limit'] = 100000
        result = []
        uri=self.REST_filetransfers
        data=encodeRequest(fileDoc)

        try:
            results = db.get(uri=uri, data=data)
            result.extend(oracleOutputMapping(results))
        except Exception as ex:
            self.logger.error("Failed to acquire publications from %s: %s", uri, ex)
            return []

        self.logger.debug("publen: %s" % len(result))

        self.logger.debug("%s acquired publications retrieved" % len(result))

        # TODO: join query for publisher (same of submitter)
        unique_tasks = [list(i) for i in set(tuple([x['username'],
                                                    x['user_group'],
                                                    x['user_role'],
                                                    x['taskname']]
                                                   ) for x in result if x['transfer_state'] == 3)]

        info = []
        for task in unique_tasks:
            info.append([x for x in result if x['taskname'] == task[3]])
        return zip(unique_tasks, info)

    def getPublDescFiles(self, workflow, lfn_ready):
        """
        Download and read the files describing
        what needs to be published
        """
        data = {}
        data['taskname'] = workflow
        data['filetype'] = 'EDM'

        out = []

        # divide lfn per chunks, avoiding URI-too long exception
        def chunks(l, n):
            """
            Yield successive n-sized chunks from l.
            :param l: list to splitt in chunks
            :param n: chunk size
            :return: yield the next list chunk
            """
            for i in range(0, len(l), n):
                yield l[i:i + n]

        for lfn_ in chunks(lfn_ready, 50):
            data['lfn'] = lfn_
            data = encodeRequest({'lfn':lfn_}, listParams=["lfn"])
            uri = self.REST_filemetadata

            try:
                #res = self.oracleDB.get(uri=uri, data=encodeRequest(data, listParams=["lfn"]))
                res = self.oracleDB.get(uri=uri, data=data)
                res = res[0]
            except Exception as ex:
                self.logger.error("Error during metadata retrieving from %s: %s", uri, ex)
                continue

            print(len(res['result']))
            for obj in res['result']:
                if isinstance(obj, dict):
                    out.append(obj)
                else:
                    # print type(obj)
                    out.append(json.loads(str(obj)))

        return out

    def algorithm(self):
        """
        1. Get a list of users with files to publish from the couchdb instance
        2. For each user get a suitably sized input for publish
        3. Submit the publish to a subprocess
        """
        tasks = self.active_tasks(self.oracleDB)

        # example code, uncomment to pick only one task
        # myTask='180912_142016:arizzi_crab_NanoDnRMXDYJetsToLL_M-105To160_TuneCUETP8M1_13TeV-amcaRunIISummer16MiniAODv2-PUMorio__heIV_v6-v22201'
        # tasksToDo=[]
        # for t in tasks:
        #  if t[0][3]==myTask:
        #  tasksToDo.append(t)
        # tasks = tasksToDo

        self.logger.debug('kicking off pool %s' % [x[0][3] for x in tasks])
        processes = []

        try:
            for task in tasks:
                if self.TestMode:
                    self.startSlave(task)   # sequentially do one task after another
                else:                       # deal with each task in a separate process
                    p = Process(target=self.startSlave, args=(task,))
                    p.start()
                    processes.append(p)

            for proc in processes:
                proc.join()
        except:
            self.logger.exception("Error during process mapping")

    def startSlave(self, task):
        # TODO: lock task!
        # - process logger
        logger = setProcessLogger(str(task[0][3]))
        logger.info("Process %s is starting. PID %s", task[0][3], os.getpid())

        self.force_publication = False
        workflow = str(task[0][3])
        wfnamemsg = "%s: " % (workflow)

        if int(workflow[0:2]) < 20:
            msg = "Skipped. Ignore tasks created before 2020."
            logger.info(wfnamemsg+msg)
            return 0

        if len(task[1]) > self.max_files_per_block:
            self.force_publication = True
            msg = "All datasets have more than %s ready files." % (self.max_files_per_block)
            msg += " No need to retrieve task status nor last publication time."
            logger.info(wfnamemsg+msg)
        else:
            msg = "At least one dataset has less than %s ready files." % (self.max_files_per_block)
            logger.info(wfnamemsg+msg)
            # Retrieve the workflow status. If the status can not be retrieved, continue
            # with the next workflow.
            workflow_status = ''
            msg = "Retrieving status from %s" % (self.config.RestHostName)
            logger.info(wfnamemsg+msg)
            #urlPath = '/crabserver/' + self.config.instance + '/workflow'
            uri = self.REST_workflow
            data = encodeRequest({'workflow': workflow})
            try:
                #connection = HTTPRequests(self.config.RestHostName, self.config.serviceCert, self.config.serviceKey)
                res = self.oracleDB.get(uri=uri, data=data)
            except Exception as ex:
                # logger.info(wfnamemsg+encodeRequest(data))
                logger.warn('Error retrieving status from %s for %s.', uri, workflow)
                return 0

            try:
                workflow_status = res[0]['result'][0]['status']
                msg = "Task status is %s." % workflow_status
                logger.info(wfnamemsg+msg)
            except ValueError:
                msg = "Workflow removed from WM."
                logger.error(wfnamemsg+msg)
                workflow_status = 'REMOVED'
            except Exception as ex:
                msg = "Error loading task status!"
                msg += str(ex)
                msg += str(traceback.format_exc())
                logger.error(wfnamemsg+msg)
            # If the workflow status is terminal, go ahead and publish all the ready files
            # in the workflow.
            if workflow_status in ['COMPLETED', 'FAILED', 'KILLED', 'REMOVED']:
                self.force_publication = True
                if workflow_status in ['KILLED', 'REMOVED']:
                    self.force_failure = True
                msg = "Considering task status as terminal. Will force publication."
                logger.info(wfnamemsg+msg)
            # Otherwise...
            else:
                msg = "Task status is not considered terminal."
                logger.info(wfnamemsg+msg)
                msg = "Getting last publication time."
                logger.info(wfnamemsg+msg)
                # Get when was the last time a publication was done for this workflow (this
                # should be more or less independent of the output dataset in case there are
                # more than one).
                last_publication_time = None
                data = encodeRequest({'workflow':workflow, 'subresource':'search'})
                try:
                    result = self.oracleDB.get(self.config.REST_task, data)
                    logger.debug("task: %s " %  str(result[0]))
                    logger.debug("task: %s " %  getColumn(result[0], 'tm_last_publication'))
                except Exception as ex:
                    logger.error("Error during task doc retrieving: %s" %ex)
                if last_publication_time:
                    date = oracleOutputMapping(result)['last_publication']
                    seconds = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f").timetuple()
                    last_publication_time = time.mktime(seconds)

                msg = "Last publication time: %s." % str(last_publication_time)
                logger.debug(wfnamemsg+msg)
                # If this is the first time a publication would be done for this workflow, go
                # ahead and publish.
                if not last_publication_time:
                    self.force_publication = True
                    msg = "There was no previous publication. Will force publication."
                    logger.info(wfnamemsg+msg)
                # Otherwise...
                else:
                    last = last_publication_time
                    msg = "Last published block: %s" % (last)
                    logger.debug(wfnamemsg+msg)
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
                    logger.info(wfnamemsg+msg)

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
                    if job_end_time :
                        wf_jobs_endtime.append(int(job_end_time) - time.timezone)
                    source_lfn = active_file['value'][1]
                    dest_lfn = active_file['value'][2]
                    self.lfn_map[dest_lfn] = source_lfn
                    if not pnn or not input_dataset or not input_dbs_url:
                        pnn = str(active_file['value'][0])
                        input_dataset = str(active_file['value'][3])
                        input_dbs_url = str(active_file['value'][4])
                    lfn_ready.append(dest_lfn)

                #userDN = ''
                username = task[0][0]
                user_group = ""
                if task[0][1]:
                    user_group = task[0][1]
                user_role = ""
                if task[0][2]:
                    user_role = task[0][2]

                # Get metadata
                toPublish = []
                publDescFiles_list = self.getPublDescFiles(workflow, lfn_ready)
                for file_ in active_:
                    for _, doc in enumerate(publDescFiles_list):
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
                logger.info(". publisher.py %s" % (workflow))

                # find the location in the current environment of the script we want to run
                import Publisher.TaskPublish as tp
                taskPublishScript = tp.__file__
                subprocess.call(["python", taskPublishScript, "configurationFile=%s"%configurationFile, "taskname=%"%workflow])

        except:
            logger.exception("Exception!")


        return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--config', help='Publisher config file', default='PublisherConfig.py')

    args = parser.parse_args()
    #need to pass the configuration file path to the slaves
    configurationFile = os.path.abspath(args.config)

    master = Master(configurationFile)
    while(True):
        master.algorithm()
        time.sleep(configuration.General.pollInterval)
