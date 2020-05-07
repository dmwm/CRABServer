from __future__ import print_function
import os
import sys
import time
import logging
import htcondor
import traceback
import subprocess
import errno

hostname = os.uname()[1]
hostAllowRun = 'crab-prod-tw01.cern.ch'
if hostname != hostAllowRun:
    os._exit(0)

from datetime import datetime
from socket import gethostname
from pprint import pprint
import requests
from RESTInteractions import HTTPRequests
import json

fmt = "%Y-%m-%dT%H:%M:%S%z"
workdir = '/home/crab3/'
logdir  = '/home/crab3/logs/'
now = time.localtime()
logfile = 'GenMonit-%s%s.log' % (now.tm_year, now.tm_mon)

def send(document):
    """
    sends this document to Elastic Search via MONIT
    the document may contain InfluxDB data, but those will be ignored unless the end point
    in MONIT is changed. See main code body for more
    Currently there is no need for using InfluxDB, see discussion in
    https://its.cern.ch/jira/browse/CMSMONIT-72?focusedCommentId=2920389&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-2920389
    :param document:
    :return:
    """
    return requests.post('http://monit-metrics:10012/', data=json.dumps(document),
                         headers={"Content-Type": "application/json; charset=UTF-8"})


def send_and_check(document, should_fail=False):
    response = send(document)
    assert ((response.status_code in [200]) != should_fail), \
        'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)

class CRAB3CreateJson(object):

    def __init__(self, resthost, jsonDoc, logger=None):
        if not logger:
            self.logger = logging.getLogger(__name__)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logger

        self.jsonDoc = jsonDoc
        self.resthost = resthost
        self.pool = ''
        self.schedds = []
        self.resthost = "cmsweb.cern.ch"
        # use child collector on port 9620 to get schedd attributes
        collName = "cmsgwms-collector-global.cern.ch:9620,cmsgwms-collector-global.fnal.gov:9620"
        self.coll = htcondor.Collector(collName)

    def getCountTasksByStatus(self):
        try:
            resturi = "/crabserver/prod/task"
            configreq = {'minutes': "120", 'subresource': "counttasksbystatus"}
            server = HTTPRequests(self.resthost,
                                  "/data/certs/servicecert.pem",
                                  "/data/certs/servicekey.pem", retry = 3)
            result = server.get(resturi, data = configreq)
            return dict(result[0]['result'])
        except Exception:
            e = sys.exc_info()
            if hasattr(e,"headers"):
                self.logger.error(str(e.headers))
            self.logger.debug("Error in getCountTasksByStatus:\n%s" %e)
            pprint(e[1])
            traceback.print_tb(e[2])
            return []

    def getCountTasksByStatusAbs(self):
        try:
            resturi = "/crabserver/prod/task"
            #configreq = {'minutes': "1000000000", 'subresource': "counttasksbystatus"}
            configreq = {'minutes': "144000", 'subresource': "counttasksbystatus"} # query last 100 days only
            server = HTTPRequests(self.resthost, "/data/certs/servicecert.pem", "/data/certs/servicekey.pem", retry=10)
            result = server.get(resturi, data=configreq)
            return dict(result[0]['result'])
        except Exception:
            e = sys.exc_info()
            if hasattr(e,"headers"):
                self.logger.error(str(e.headers))
            self.logger.exception("Error in getCountTasksByStatusAbs:\n%s" %e)
            pprint(e[1])
            traceback.print_tb(e[2])
            return []

    def getScheddsInfo(self):
        data = []
        try:
            result = self.coll.query(htcondor.AdTypes.Schedd,
                                     'CMSGWMS_Type=?="crabschedd"',
                                     ['Name',
                                      'ShadowsRunning',
                                      'TotalSchedulerJobsRunning',
                                      'TotalIdleJobs',
                                      'TotalRunningJobs',
                                      'TotalHeldJobs',
                                      'TotalJobAds'])
            for schedd in result:
                data.append([schedd['Name'],
                             schedd['ShadowsRunning'],
                             schedd['TotalSchedulerJobsRunning'],
                             schedd['TotalIdleJobs'],
                             schedd['TotalRunningJobs'],
                             schedd['TotalHeldJobs'],
                             schedd['TotalJobAds']])
        except Exception, e:
            self.logger.debug("Error in getShadowsRunning: %s"%str(e))
        return data

    def execute(self):
        subprocesses_config = 6
        # In this case 5 + 1 MasterWorker process
        sub_grep_command="ps -ef | grep MasterWorker | grep -v 'grep' | wc -l"
        # If any subprocess is dead or not working, modify percentage of availability
        # If subprocesses are not working - service availability 0%
        process_count = int(subprocess.Popen(sub_grep_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read())

        if subprocesses_config == process_count:
            # This means that everything is fine
            self.jsonDoc['status'] = "available"
        else:
            self.jsonDoc['status'] = "degraded"

        # Get the number of tasks per status
        twStatus = self.getCountTasksByStatus()

        states_filter = ['SUBMITTED', 'FAILED', 'QUEUED', 'NEW', 'KILLED', 'KILLFAILED', 'RESUBMITFAILED',
                         'SUBMITFAILED', 'TAPERECALL']
        if len(twStatus) > 0:
            for state in twStatus.keys():
                if state in states_filter:
                    self.jsonDoc['current_task_states'].update({str(state): int(twStatus[state])})

        # get the absolut number of tasks per status
        twStatus = self.getCountTasksByStatusAbs()

        states_filter = ['KILL', 'RESUBMIT', 'NEW', 'QUEUED', 'KILLFAILED', 'RESUBMITFAILED', 'SUBMITFAILED',
                         'UPLOADED', 'TAPERECALL']
        if len(twStatus) > 0:
            for state in twStatus.keys():
                if state in states_filter:
                    self.jsonDoc['abs_task_states'].update({str(state): int(twStatus[state])})

        # get the number of condor_shadow processes per schedd
        ListOfSchedds = self.getScheddsInfo()
        totalRunningTasks = 0
        totalIdleTasks = 0
        totalRunningTP = 0
        # see https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
        pickSchedulerIdle = 'JobUniverse==7 && JobStatus==1'
        pickLocalRunning = 'JobUniverse==12 && JobStatus==2'

        if len(ListOfSchedds) > 0:
            metrics = []
            for oneSchedd in ListOfSchedds:
                scheddName = oneSchedd[0]
                # influxDB tags and fields are also added according to
                # https://monitdocs.web.cern.ch/monitdocs/ingestion/service_metrics.html#writing-to-influxdb
                # see https://github.com/dmwm/CRABServer/pull/6017  But they are irrelevant
                # as long as MONIT entry point data are sent to is an Elastic Search one. See comments
                # above in "send" function
                influxDb_measures = dict(shadows = int(oneSchedd[1]),
                                         running_schedulers = int(oneSchedd[2]),
                                         idle_jobs = int(oneSchedd[3]),
                                         running_jobs = int(oneSchedd[4]),
                                         held_jobs = int(oneSchedd[5]),
                                         all_jobs = int(oneSchedd[6]),
                                        )
                jsonDocSchedd = dict(
                                producer='crab',
                                type='schedd',
                                hostname=gethostname(),
                                name=scheddName,
                                idb_tags=["name"], # for InfluxDB
                                idb_fields=influxDb_measures.keys(), # for InfluxDB
                                )
                jsonDocSchedd.update(influxDb_measures)

                metrics.append(jsonDocSchedd)

                totalRunningTasks += int(oneSchedd[2])
                # if one schedd does not answer, go on and try the others
                try:
                    scheddAdd = self.coll.locate(htcondor.DaemonTypes.Schedd, scheddName)
                except:
                    continue
                schedd = htcondor.Schedd(scheddAdd)
                try:
                    idleDags = list(schedd.xquery(pickSchedulerIdle))
                except:
                    idleDags = []
                    pass
                try:
                    runningTPs = list(schedd.xquery(pickLocalRunning))
                except:
                    runningTPs = []
                    pass
                numDagIdle = len(idleDags)
                numTPRun = len(runningTPs)
                totalIdleTasks += numDagIdle
                totalRunningTP += numTPRun

            #print metrics
            try:
                send_and_check(metrics)
            except Exception as ex:
                print(ex)
        self.jsonDoc['total_running_tasks'] = totalRunningTasks
        self.jsonDoc['total_idle_tasks'] = totalIdleTasks
        self.jsonDoc['total_running_tp'] = totalRunningTP

        return self.jsonDoc

if __name__ == '__main__':
    """ Simple main to execute the action standalon. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change resthost, resturi, and twconfig.
        If you want to monitor your own machine, you have to enable it in puppet configuration.
    """

    start_time = time.time()
    resthost = 'cmsweb.cern.ch'
    logger = logging.getLogger()
    #handler = logging.StreamHandler(sys.stdout)
    handler = logging.FileHandler(logdir + logfile)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s",
                                  datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # see comments above about InfluxDB tags and fields, they are added but will not be used by MONIT
    influxDb_measures = dict(abs_task_states={},
                             current_task_states={},
                             total_running_tasks=0,
                             total_idle_tasks=0,
                             total_running_tp=0)
    jsonDoc = dict(
                   producer='crab',
                   type='taskworker',
                   hostname=gethostname(),
                   idb_fields=influxDb_measures.keys(), # for InfluxDB
                   )
    jsonDoc.update(influxDb_measures)

    pr = CRAB3CreateJson(resthost, jsonDoc, logger)

    # before running make sure no other instance of this script is running
    lockFile = workdir + 'CRAB3_SCHEDD_JSON.Lock'
    currentPid = str(os.getpid())

    # Check if lockfile already exists and if it does, check PID number in the lockfile if it is running
    if os.path.isfile(lockFile):
        if os.stat(lockFile).st_size == 0:
            logger.info("Lockfile is there but it is empty. Removing old lockfile.")
            os.remove(lockFile)
        else:
            with open(lockFile, 'r') as lf:
                pid = int(lf.read())
            try:
                os.kill(pid, 0)
            except OSError as e:
                if e.errno == errno.ESRCH:  # ESRCH - No such process
                    logger.info("Lockfile is there but program is not running. Removing old lockfile.")
                    os.remove(lockFile)
                elif e.errno == errno.EPERM:  # EPERM - Opration not permitted (i.e., process exists)
                    logger.info("Opration not permitted (i.e., process exists), abandon this run.")
                    exit()
                else:  # EINVAL - An invalid signal was specified.
                    logger.info("An invalid signal was specified. Removing old lockfile.")
                    os.remove(lockFile)
            else:
                logger.info("Lockfile is there and program is running, abandon this run.")
                exit()

    # Put PID in the lockfile
    with open(lockFile, 'w') as lf:
        lf.write(currentPid)
    
    logger.info('Lock created. Start data collection')            
    metrics = pr.execute()
    logger.info('Metrics collected. Send to MONIT.')
    
    #print metrics
    try:
        send_and_check([jsonDoc])
    except Exception as ex:
        print(ex)

    end_time = time.time()
    elapsed = end_time - start_time
    now = time.strftime("%H:%M:%S", time.gmtime(end_time))
    elapsed_min = "%3d:%02d" % divmod(elapsed, 60)
    logger.info('All done in %s minutes. Remove lock and exit' % elapsed_min)

    os.remove(lockFile)
