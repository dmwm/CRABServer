#!/usr/bin/tenv python
#external dependencies
from __future__ import print_function

import json
import os
import shutil
import sys
import time
import logging

from http.client import HTTPException
from urllib.parse import urlencode
from MultiProcessingLog import MultiProcessingLog

#CRAB dependencies
from RESTInteractions import CRABRest
from ServerUtilities import newX509env
from ServerUtilities import SERVICE_INSTANCES
from TaskWorker import __version__
from TaskWorker.TestWorker import TestWorker
from TaskWorker.Worker import Worker, setProcessLogger
from TaskWorker.WorkerExceptions import ConfigException
from TaskWorker.Actions.Recurring.BaseRecurringAction import handleRecurring
from TaskWorker.Actions.Handler import handleResubmit, handleNewTask, handleKill
from CRABUtils.TaskUtils import getTasks, updateTaskStatus
import random

## NOW placing this here, then to be verified if going into Action.Handler, or TSM
## The meaning of the elements in the 3-tuples are as follows:
## 1st) the command to be executed on the task;
## 2nd) the work that should be do;
## 3nd) the new status that the task should get in case of failure.
STATE_ACTIONS_MAP = {'SUBMIT' : (handleNewTask, 'SUBMITFAILED'), 'KILL' : (handleKill, 'KILLFAILED'), 'RESUBMIT' : (handleResubmit, 'RESUBMITFAILED')}

MODEURL = {'cmsweb-dev': {'host': 'cmsweb-dev.cern.ch', 'instance':  'dev'},
           'cmsweb-test': {'host': 'cmsweb-test.cern.ch', 'instance': 'preprod'},
           'cmsweb-preprod': {'host': 'cmsweb-testbed.cern.ch', 'instance': 'preprod'},
           'cmsweb-prod': {'host': 'cmsweb.cern.ch', 'instance':  'prod'},
           'test' :{'host': None, 'instance': 'preprod'},
           'private': {'host': None, 'instance':  'dev'},
          }

def getRESTParams(config, logger):
    """ get REST host name and db instance from a config object
    returns a tuple of strings (host, dbinstance). If can't, raises exception"""
    # this is also a candidate for CRABUtils/TaskUtils.py

    try:
        instance = config.TaskWorker.instance
    except Exception as e:
        msg = "No instance provided: need to specify config.TaskWorker.instance in the configuration"
        raise ConfigException(msg) from e

    if instance in SERVICE_INSTANCES:
        logger.info('Will connect to CRAB service: %s', instance)
        restHost = SERVICE_INSTANCES[instance]['restHost']
        dbInstance = SERVICE_INSTANCES[instance]['dbInstance']
    else:
        msg = "Invalid instance value '%s'" % instance
        raise ConfigException(msg)
    if instance == 'other':
        logger.info('Will use restHost and dbInstance from config file')
        try:
            restHost = config.TaskWorker.restHost
            dbInstance = config.TaskWorker.dbInstance
        except Exception as e:
            msg = "Need to specify config.TaskWorker.restHost and dbInstance in the configuration"
            raise ConfigException(msg) from e
    return restHost, dbInstance


class MasterWorker(object):
    """I am the master of the TaskWorker"""

    def __init__(self, config, logWarning, logDebug, sequential=False, console=False, name='master'):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg bool logWarning: it tells if a quiet logger is needed
        :arg bool logDebug: it tells if needs a verbose logger
        :arg bool sequential: it tells if to run in sequential (no subprocesses) mode.
        :arg bool console: it tells if to log to console.
        :arg string name: defines a name for the log of this master process"""


        def createLogdir(dirname):
            """ Create the directory dirname ignoring errors in case it exists. Exit if
                the directory cannot be created.
            """
            try:
                os.mkdir(dirname)
            except OSError as ose:
                if ose.errno != 17: #ignore the "Directory already exists error"
                    print(str(ose))
                    print("The task worker need to access the '%s' directory" % dirname)
                    sys.exit(1)

        def createAndCleanLogDirectories(logsDir):
            # it can be named with the time stamp a TW started
            createLogdir(logsDir)
            createLogdir(logsDir+'/tasks')
            currentProcessesDir = logsDir + '/processes/'
            createLogdir(currentProcessesDir)
            # when running inside a container process logs will start with same
            # process numbers, i.e. same name, at any container restart.
            # to avoid clashes and confusion, we will put away all previous processes
            # logs when a TW instance starts. To this goal each TW which runs
            # creates a directory where new containers will move its logs, so
            # identify LastLogs_timestamp directory
            latestLogDir = None  # the logs directory could be empty
            files = os.listdir(currentProcessesDir)
            files.sort(reverse=True)   # if there are multiple Latest*, will hit the latest first
            for f in files:
                if f.startswith('Latest'):
                    latestLogDir = currentProcessesDir + f
                    break
            if files and latestLogDir:
                # rename from Latest to Old
                oldLogsDir = latestLogDir.replace('Latest', 'Old')
                shutil.move(latestLogDir, oldLogsDir)
            else:
                print("LatestLogDir not found in logs/processes, create a dummy dir to store old files")
                oldLogsDir = currentProcessesDir + 'OldLog-Unknwown'
                createLogdir(oldLogsDir)
            # move process logs for latest TW run to old directory
            for f in files:
                if f.startswith('proc.c3id'):
                    shutil.move(currentProcessesDir + f, oldLogsDir)

            # create a new LateastLogs directory where to store logs from this TaskWorker
            YYMMDD_HHMMSS = time.strftime('%y%m%d_%H%M%S', time.localtime())
            myDir = currentProcessesDir + 'LatestLogs-' + YYMMDD_HHMMSS
            createLogdir(myDir)

        def setRootLogger(logWarning, logDebug, console, name):
            """Sets the root logger with the desired verbosity level
               The root logger logs to logsDir/twlog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool logWarning: it tells if a quiet logger is needed
            :arg bool logDebug: it tells if needs a verbose logger
            :arg bool console: it tells if to log to console
            :arg string name: define a name for the log file of this master process
            :return logger: a logger with the appropriate logger level."""

            # this must only done for real Master, not when it is used by TapeRecallStatus
            logsDir = config.TaskWorker.logsDir
            if name == 'master':
                createAndCleanLogDirectories(logsDir)

            if console:
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog(logsDir+'/twlog.txt', when='midnight')
                logFormatter = \
                    logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
                logHandler.setFormatter(logFormatter)
                logging.getLogger().addHandler(logHandler)
            loglevel = logging.INFO
            if logWarning:
                loglevel = logging.WARNING
            if logDebug:
                loglevel = logging.DEBUG
            logging.getLogger().setLevel(loglevel)
            logger = setProcessLogger(name, logsDir)
            logger.info("PID %s.", os.getpid())
            logger.info("Logging level initialized to %s.", loglevel)
            return logger

        def logVersionAndConfig(config=None, logger=None):
            """
            log version number and major config. parameters
            args: config : a configuration object loaded from file
            args: logger : the logger instance to use
            """
            twstartDict = {}
            twstartDict['version'] = __version__
            twstartDict['DBSHostName'] = config.Services.DBSHostName
            twstartDict['name'] = config.TaskWorker.name
            twstartDict['instance'] = config.TaskWorker.instance
            if config.TaskWorker.instance == 'other':
                twstartDict['restHost'] = config.TaskWorker.restHost
                twstartDict['dbInstance'] = config.TaskWorker.dbInstance
            twstartDict['nslaves'] = config.TaskWorker.nslaves
            twstartDict['recurringActions'] = config.TaskWorker.recurringActions
            # one line for automatic parsing
            logger.info('TWSTART: %s', json.dumps(twstartDict))
            # multiple lines for humans to read
            for k, v in twstartDict.items():
                logger.info('%s: %s', k, v)
            return

        self.STOP = False
        self.TEST = sequential
        self.logger = setRootLogger(logWarning, logDebug, console, name)
        self.config = config
        self.restHost = None
        dbInstance = None

        logVersionAndConfig(self.config, self.logger)

        restHost, dbInstance = getRESTParams(self.config, self.logger)
        self.restHost = restHost
        self.dbInstance = dbInstance
        self.logger.info('Will connect via URL: https://%s/%s', self.restHost, self.dbInstance)

        #Let's increase the server's retries for recoverable errors in the MasterWorker
        #60 means we'll keep retrying for 1 hour basically (we retry at 20*NUMRETRY seconds, so at: 20s, 60s, 120s, 200s, 300s ...)
        self.crabserver = CRABRest(self.restHost, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey, retry=20,
                                   logger=self.logger, userAgent='CRABTaskWorker')
        self.crabserver.setDbInstance(self.dbInstance)
        self.logger.debug("Hostcert: %s, hostkey: %s", str(self.config.TaskWorker.cmscert), str(self.config.TaskWorker.cmskey))
        # Retries for any failures
        if not hasattr(self.config.TaskWorker, 'max_retry'):
            self.config.TaskWorker.max_retry = 0
        if not hasattr(self.config.TaskWorker, 'retry_interval'):
            self.config.TaskWorker.retry_interval = [retry*20*2 for retry in range(self.config.TaskWorker.max_retry)]
        if not len(self.config.TaskWorker.retry_interval) == self.config.TaskWorker.max_retry:
            raise ConfigException("No correct max_retry and retry_interval specified; len of retry_interval must be equal to max_retry.")
        # use the config to pass some useful global stuff to all workers
        # will use TaskWorker.cmscert/key to talk with CMSWEB
        self.config.TaskWorker.envForCMSWEB = newX509env(X509_USER_CERT=self.config.TaskWorker.cmscert,
                                                         X509_USER_KEY=self.config.TaskWorker.cmskey)

        if self.TEST:
            self.slaves = TestWorker(self.config, self.restHost, self.dbInstance)
        else:
            self.slaves = Worker(self.config, self.restHost, self.dbInstance)
        self.slaves.begin()
        recurringActionsNames = getattr(self.config.TaskWorker, 'recurringActions', [])
        self.recurringActions = [m for m in [self.getRecurringActionInst(name) for name in recurringActionsNames] if m is not None]


    def getRecurringActionInst(self, actionName):
        try:
            mod = __import__('TaskWorker.Actions.Recurring.%s' % actionName, fromlist=actionName)
        except ModuleNotFoundError:
            self.logger.error('Recurring Action module "%s" not found, skipping', actionName)
            return
        return getattr(mod, actionName)(self.config.TaskWorker.logsDir)


    def _externalScheduling(self, limit):
        """
        External scheduling method using round-robin algorithm to get tasks
        in waiting status and consider resource utilization for fair share.
        """
        self.logger.info("Starting external scheduling.")

        try:
            # Retrieve tasks with 'WAITING' status
            waiting_tasks = getTasks(crabserver=self.crabserver, status='WAITING', logger=self.logger, limit=limit)

            if not waiting_tasks:
                self.logger.info("No tasks in 'WAITING' status found.")
                return []

            # Organize tasks by user
            tasks_by_user = {}
            for task in waiting_tasks:
                user = task['tm_username']
                if user not in tasks_by_user:
                    tasks_by_user[user] = []
                tasks_by_user[user].append(task)

            # Perform round-robin selection among users
            users = list(tasks_by_user.keys())
            random.shuffle(users)  # To ensure fair round-robin each time
            selected_tasks = []

            for user in users:
                user_tasks = tasks_by_user[user]
                selected_tasks.extend(user_tasks[:limit // len(users)])

            # Create and populate task_count dictionary
            task_count = {'selected': {}, 'waiting': {}}

            for status, tasks in [('selected', selected_tasks), ('waiting', waiting_tasks)]:
                for task in tasks:
                    username = task['tm_username']
                    task_count[status][username] = task_count[status].get(username, 0) + 1

            # Prepare table headers and rows
            headers = ['Username', 'Waiting', 'Selected']
            rows = []

            # Collect all usernames to ensure every user appears in the table
            all_usernames = set(task_count['selected'].keys()).union(task_count['waiting'].keys())

            for username in all_usernames:
                waiting_count = task_count['waiting'].get(username, 0)
                selected_count = task_count['selected'].get(username, 0)
                rows.append([username, waiting_count, selected_count])

            # Determine the width of each column for formatting
            widths = [max(len(header) for header in headers)] + [max(len(str(row[i])) for row in rows) for i in range(1, len(headers))]

            # Prepare formatted table string
            table_header = ' | '.join(f'{header:<{width}}' for header, width in zip(headers, widths))
            table_separator = '-|-'.join('-' * width for width in widths)
            table_rows = '\n'.join(' | '.join(f'{str(cell):<{width}}' for cell, width in zip(row, widths)) for row in rows)

            # Combine header, separator, and rows into one string
            table = f"{table_header}\n{table_separator}\n{table_rows}"

            # Log the formatted table
            self.logger.info('\n%s', table)

            if getattr(self.config.TaskWorker, 'task_scheduling_dry_run', False):
                return waiting_tasks
            return selected_tasks

        except HTTPException as hte:
            msg = "HTTP Error during external scheduling: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
            return []

        except Exception as e: #pylint: disable=broad-except
            self.logger.exception("Unknown Exception occurred during external scheduling: %s", str(e))
            return []

    def _pruneTaskQueue(self):
        self.logger.info("Pruning the queue if required...logic tbd")

    def _reportQueueStatus(self):
        self.logger.info("Report Queue status... logic tbd")


    def _selectWork(self, limit):
        """This function calls external scheduling and updates task status for the selected tasks"""
        self.logger.info("Starting work selection process.")

        # Call the external scheduling method
        selected_tasks = self._externalScheduling(limit)

        if not selected_tasks:
            return False

        try:
            # Update the status of each selected task to 'NEW'
            for task in selected_tasks:
                task_name = task['tm_taskname']
                updateTaskStatus(crabserver=self.crabserver, taskName=task_name, status='NEW', logger=self.logger)
                self.logger.info("Task %s status updated to 'NEW'.", task_name)

            # Prune the task queue if necessary
            self._pruneTaskQueue()

            # Report queue status
            self._reportQueueStatus()

        except HTTPException as hte:
            msg = "HTTP Error during _selectWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
            return False

        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the _selectWork request.")
            return False

        return True

    def _lockWork(self, limit, getstatus, setstatus):
        """Today this is always returning true, because we do not want the worker to die if
           the server endpoint is not avaialable.
           Prints a log entry if answer is greater than 400:
            * the server call succeeded or
            * the server could not find anything to update or
            * the server has an internal error"""
        configreq = {'subresource': 'process', 'workername': self.config.TaskWorker.name, 'getstatus': getstatus, 'limit': limit, 'status': setstatus}

        try:
            #self.server.post(self.restURInoAPI + '/workflowdb', data=urlencode(configreq))
            self.crabserver.post(api='workflowdb', data=urlencode(configreq))
        except HTTPException as hte:
            msg = "HTTP Error during _lockWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
            return False
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the _lockWork request (prameters are %s)", configreq)
            return False

        return True

    def runCanary(self, limit):
        # Decide whether to use canary_name based on the canary_fraction value.
        # canary_fraction is a float value in the range [0.0-1.0]
        use_canary = random.random() < float(self.config.TaskWorker.canary_fraction)
        if not use_canary:
            return True

        # Build the configreq dictionary
        #This changes just the workername, status remains 'HOLDING'
        #The arguments that are not changed can't be skipped
        workername = self.config.TaskWorker.canary_name

        configreq = {
            'subresource': 'process',
            'workername': workername,
            'getstatus': 'HOLDING',
            'limit': limit,
            'status': 'HOLDING'
        }

        try:
            self.crabserver.post(api='workflowdb', data=urlencode(configreq))
        except HTTPException as hte:
            msg = "HTTP Error while trying to change TW name in runCanary step: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
            return False
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Error trying to change TW name in runCanary step")
            return False

        self.logger.info("TW changed from %s to %s during runCanary", self.config.TaskWorker.name, workername)
        return True


    def getWork(self, limit, getstatus, ignoreTWName=False):
        configreq = {'limit': limit, 'workername': self.config.TaskWorker.name, 'getstatus': getstatus}
        if ignoreTWName:
            configreq['workername'] = '%'

        pendingwork = []
        try:
            #pendingwork = self.server.get(self.restURInoAPI + '/workflowdb', data=configreq)[0]['result']
            pendingwork = self.crabserver.get(api='workflowdb', data=configreq)[0]['result']
        except HTTPException as hte:
            msg = "HTTP Error during getWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the getWork request (prameters are %s)", configreq)
        return pendingwork


    def quit_(self, dummyCode, dummyTraceback):  # pylint: disable=unused-argument
        self.logger.info("Received kill request. Setting STOP flag in the master process...")
        self.STOP = True


    def updateWork(self, taskname, command, status):
        """ Update taskname setting the status and the command for it
            Return True if the change succeded, False otherwise
        """

        configreq = {'workflow': taskname, 'command': command, 'status': status, 'subresource': 'state'}
        try:
            #self.server.post(self.restURInoAPI + '/workflowdb', data=urlencode(configreq))
            self.crabserver.post(api='workflowdb', data=urlencode(configreq))
        except HTTPException as hte:
            msg = "HTTP Error during updateWork: %s\n" % str(hte)
            msg += "HTTP Headers are %s: " % hte.headers
            self.logger.error(msg)
        except Exception: #pylint: disable=broad-except
            self.logger.exception("Server could not process the updateWork request (prameters are %s)", configreq)
        else:
            return True #success
        return False #failure


    def restartQueuedTasks(self):
        """ This method is used at the TW startup and it restarts QUEUED tasks
            setting them  back again to NEW.
        """
        limit = self.slaves.nworkers * 2
        total = 0
        current_tw_name = self.config.TaskWorker.name
        while True:
            pendingwork = self.getWork(limit=limit, getstatus='QUEUED')
            for task in pendingwork:
                if task['tw_name'] != current_tw_name:
                    self.logger.debug("Skipping task %s since it is assigned to another TW (%s).",task['tm_taskname'], task['tw_name'])
                    continue           
                self.logger.debug("Restarting QUEUED task %s", task['tm_taskname'])
                self.updateWork(task['tm_taskname'], task['tm_task_command'], 'NEW')
            if not pendingwork:
                self.logger.info("Finished restarting QUEUED tasks (total %s)", total)
                break #too bad "do..while" does not exist in python...
            else:
                total += len(pendingwork)
                self.logger.info("Restarted %s tasks (limit %s), getting next chunk of tasks", len(pendingwork), limit)


    def failBannedTask(self, task):
        """ This method is used at the TW startup and it fails NEW tasks which I do not like
        The method put those task to SUBMITFAILED, KILLFAILED, RESUBMITFAILED depending on the value of
         the command field.
        Initial implementation bans based on a list of usernames, other task attributes can
          be checked if needed by adding a bit of code
        Returns:
            True : if the task was declared bad and was failed
            False: for normal (good) tasks
        """
        taskname = task['tm_taskname']
        command = task['tm_task_command']
        bannedUsernames = getattr(self.config.TaskWorker, 'bannedUsernames', [])
        if task['tm_username'] in bannedUsernames:
            self.logger.debug("Forcefully failing task %s", taskname)
            if task['tm_task_command']:
                dummyWorktype, failstatus = STATE_ACTIONS_MAP[command]
            else:
                failstatus = 'FAILED'
            self.updateWork(taskname, command, failstatus)
            warning = 'username %s banned in CRAB TaskWorker configuration' % task['tm_username']
            configreq = {'subresource': 'addwarning',
                         'workflow': taskname,
                         'warning': warning}
            try:
                self.crabserver.post(api='task', data=urlencode(configreq))
            except Exception as e:  # pylint: disable=broad-except
                self.logger.error("Error uploading warning: %s", str(e))
                self.logger.warning("Cannot add a warning to REST interface. Warning message: %s", warning)
            return True
        return False

    def skipRejectedCommand(self, task):
        """ This method is used at the TW startup and is used to ignore requests for
        a command field in the reject list. It allows to configure the TW so to process
        some commands, not not all. E.g. to prevent users from doing more submit and/or resubmit
            True : if the task was declared bad and was failed
            False: for normal (good) tasks
        """
        taskname = task['tm_taskname']
        command = task['tm_task_command']
        rejectedCommands = getattr(self.config.TaskWorker, 'rejectedCommands', [])
        if command in rejectedCommands:
            self.logger.debug("Rejecting command %s", command)
            if command == 'SUBMIT':  # refuse i.e. mark as submission failed
                self.updateWork(taskname, command, 'SUBMITFAILED')
            if command == 'RESUBMIT':  # ignore, i.e. leave in status 'SUBMITTED'
                self.updateWork(taskname, command, 'SUBMITTED')
            if command == 'KILL':  # ignore, i.e. leave in status 'SUBMITTED'
                self.updateWork(taskname, command, 'SUBMITTED')
            warning = 'command %s disabled in CRAB TaskWorker configuration' % command
            configreq = {'subresource': 'addwarning', 'workflow': taskname, 'warning': warning}
            try:
                self.crabserver.post(api='task', data=urlencode(configreq))
            except Exception as e:  # pylint: disable=broad-except
                self.logger.error("Error uploading warning: %s", str(e))
                self.logger.warning("Cannot add a warning to REST interface. Warning message: %s", warning)
            return True
        return False


    def algorithm(self):
        """I'm the intelligent guy taking care of getting the work
           and distributing it to the slave processes."""

        self.logger.debug("Restarting QUEUED tasks before startup.")
        self.restartQueuedTasks()
        self.logger.debug("Master Worker Starting Main Cycle.")
        while not self.STOP:
            is_canary = self.config.TaskWorker.is_canary
            limit = self.slaves.queueableTasks()

            # _selectWork, _lockWork and runCanary steps are run only if TW is master (not canary)
            if is_canary:
                self.logger.info("This is canary TW %s running.", self.config.TaskWorker.name)
            else:
                canary_name = self.config.TaskWorker.canary_name
                self.logger.info("This is master TW %s running.", self.config.TaskWorker.name)
                selection_limit = self.config.TaskWorker.task_scheduling_limit
                if not self._selectWork(limit=selection_limit):
                    self.logger.warning("No tasks selected.")
                else:
                    self.logger.info("Work selected successfully.")
                if not self._lockWork(limit=limit, getstatus='NEW', setstatus='HOLDING'):
                    time.sleep(self.config.TaskWorker.polling)
                    continue
                if canary_name.startswith('crab'):
                    self.runCanary(limit=limit)
                  
            # getWork is run by both master TW and canary TW          
            pendingwork = self.getWork(limit=limit, getstatus='HOLDING')

            if pendingwork:
                keys = ['tm_task_command', 'tm_taskname']
                tasksInfo = [{k:v for k, v in task.items() if k in keys} for task in pendingwork]
                self.logger.info("Retrieved a total of %d works", len(pendingwork))
                self.logger.debug("Retrieved the following works: \n%s", str(tasksInfo))

            toInject = []
            for task in pendingwork:
                if self.failBannedTask(task):
                    continue
                if self.skipRejectedCommand(task):
                    continue
                if self.updateWork(task['tm_taskname'], task['tm_task_command'], 'QUEUED'):
                    worktype, failstatus = STATE_ACTIONS_MAP[task['tm_task_command']]
                    toInject.append((worktype, task, failstatus, None))
                else:
                    #The task stays in HOLDING and will be acquired again later
                    self.logger.info("Skipping %s since it could not be updated to QUEUED. Will be retried in the next iteration", task['tm_taskname'])

            self.slaves.injectWorks(toInject)

            for action in self.recurringActions:
                if action.isTimeToGo():
                    #Maybe we should use new slaves and not reuse the ones used for the tasks
                    # use an unique ID to track this action
                    actionName = str(action.__module__).split('.')[-1]
                    now = time.strftime("%y%m%d_%H%M%S", time.localtime())
                    actionName += '.' + now
                    self.logger.debug("Injecting recurring action: \n%s", actionName)
                    self.slaves.injectWorks([(handleRecurring, {'tm_username': 'recurring', 'tm_taskname' : actionName}, 'FAILED', action.__module__)])

            self.logger.info('Master Worker status:')
            self.logger.info(' - free slaves: %d', self.slaves.freeSlaves())
            self.logger.info(' - acquired tasks: %d', self.slaves.queuedTasks())
            if self.slaves.queuedTasks():
                working = self.slaves.listWorks()
                for wid, work in working.items():
                    self.logger.info(f"      wid {wid} : {work['workflow']}")
            self.logger.info(' - tasks pending in queue: %d', self.slaves.pendingTasks())

            time.sleep(self.config.TaskWorker.polling)

            dummyFinished = self.slaves.checkFinished()

        self.logger.debug("Master Worker Exiting Main Cycle.")
