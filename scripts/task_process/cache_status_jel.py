#!/usr/bin/python
"""
VERSION OF CACHE_STATUS USING HTCONDOR JobEventLog API
THIS REQUIRES HTCONDOR 8.9.3 OR ABOVE
"""
from __future__ import print_function, division
import re
import time
import logging
import os
import ast
import glob
import copy
from shutil import move
import pickle
import htcondor
import classad

logging.basicConfig(filename='task_process/cache_status.log', level=logging.DEBUG)

NODE_DEFAULTS = {
    'Retries': 0,
    'Restarts': 0,
    'SiteHistory': [],
    'ResidentSetSize': [],
    'SubmitTimes': [],
    'StartTimes': [],
    'EndTimes': [],
    'TotalUserCpuTimeHistory': [],
    'TotalSysCpuTimeHistory': [],
    'WallDurations': [],
    'JobIds': []
}

STATUS_CACHE_FILE = "task_process/status_cache.txt"
LOG_PARSING_POINTERS_DIR = "task_process/jel_pickles/"
FJR_PARSE_RES_FILE = "task_process/fjr_parse_results.txt"

#
# insertCpu, parseJobLog, parsNodeStateV2 and parseErrorReport
# code copied from the backend HTCondorDataWorkflow.py with minimal changes.
#

cpuRe = re.compile(r"Usr \d+ (\d+):(\d+):(\d+), Sys \d+ (\d+):(\d+):(\d+)")


def insertCpu(event, info):
    """
    add CPU usage information to the job info record
    :param event: an event from HTCondor log
    :param info: the structure where information for a job is collected
    :return: nothing
    """
    if 'TotalRemoteUsage' in event:
        m = cpuRe.match(event['TotalRemoteUsage'])
        if m:
            g = [int(i) for i in m.groups()]
            user = g[0] * 3600 + g[1] * 60 + g[2]
            system = g[3] * 3600 + g[4] * 60 + g[5]
            info['TotalUserCpuTimeHistory'][-1] = user
            info['TotalSysCpuTimeHistory'][-1] = system
    else:
        if 'RemoteSysCpu' in event:
            info['TotalSysCpuTimeHistory'][-1] = float(event['RemoteSysCpu'])
        if 'RemoteUserCpu' in event:
            info['TotalUserCpuTimeHistory'][-1] = float(event['RemoteUserCpu'])


nodeNameRe = re.compile("DAG Node: Job(\d+(?:-\d+)?)")
nodeName2Re = re.compile("Job(\d+(?:-\d+)?)")

# this now takes as input an htcondor.JobEventLog object
# which as of HTCondor 8.9 can be saved/restored with memory of
# where it had reached in processing the job log file
def parseJobLog(jel, nodes, nodeMap):
    """
    parses new events in condor job log file and updates nodeMap
    :param jel: a condor JobEventLog object which provides an iterator over events
    :param nodes: the structure where we collect one job info for cache_status file
    :param nodeMap: the structure where collect summary of all events
    :return: nothing
    """
    count = 0
    for event in jel.events(0):
        count += 1
        eventtime = time.mktime(time.strptime(event['EventTime'], "%Y-%m-%dT%H:%M:%S"))
        if event['MyType'] == 'SubmitEvent':
            m = nodeNameRe.match(event['LogNotes'])
            if m:
                node = m.groups()[0]
                proc = event['Cluster'], event['Proc']
                info = nodes.setdefault(node, copy.deepcopy(NODE_DEFAULTS))
                info['State'] = 'idle'
                info['JobIds'].append("%d.%d" % proc)
                info['RecordedSite'] = False
                info['SubmitTimes'].append(eventtime)
                info['TotalUserCpuTimeHistory'].append(0)
                info['TotalSysCpuTimeHistory'].append(0)
                info['WallDurations'].append(0)
                info['ResidentSetSize'].append(0)
                info['Retries'] = len(info['SubmitTimes'])-1
                nodeMap[proc] = node
        elif event['MyType'] == 'ExecuteEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            nodes[node]['StartTimes'].append(eventtime)
            nodes[node]['State'] = 'running'
            nodes[node]['RecordedSite'] = False
        elif event['MyType'] == 'JobTerminatedEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            nodes[node]['EndTimes'].append(eventtime)
            # at times HTCondor does not log the ExecuteEvent and there's no StartTime
            if nodes[node]['StartTimes'] :
                nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
            else:
                nodes[node]['WallDurations'][-1] = 0
            insertCpu(event, nodes[node])
            if event['TerminatedNormally']:
                if event['ReturnValue'] == 0:
                    nodes[node]['State'] = 'transferring'
                else:
                    nodes[node]['State'] = 'cooloff'
            else:
                nodes[node]['State'] = 'cooloff'
        elif event['MyType'] == 'PostScriptTerminatedEvent':
            m = nodeName2Re.match(event['DAGNodeName'])
            if m:
                node = m.groups()[0]
                if event['TerminatedNormally']:
                    if event['ReturnValue'] == 0:
                        nodes[node]['State'] = 'finished'
                    elif event['ReturnValue'] == 2:
                        nodes[node]['State'] = 'failed'
                    else:
                        nodes[node]['State'] = 'cooloff'
                else:
                    nodes[node]['State'] = 'cooloff'
        elif event['MyType'] == 'ShadowExceptionEvent' or event["MyType"] == "JobReconnectFailedEvent" or event['MyType'] == 'JobEvictedEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            if nodes[node]['State'] != 'idle':
                nodes[node]['EndTimes'].append(eventtime)
                if nodes[node]['WallDurations'] and nodes[node]['EndTimes'] and nodes[node]['StartTimes']:
                    nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
                nodes[node]['State'] = 'idle'
                insertCpu(event, nodes[node])
                nodes[node]['TotalUserCpuTimeHistory'].append(0)
                nodes[node]['TotalSysCpuTimeHistory'].append(0)
                nodes[node]['WallDurations'].append(0)
                nodes[node]['ResidentSetSize'].append(0)
                nodes[node]['SubmitTimes'].append(-1)
                nodes[node]['JobIds'].append(nodes[node]['JobIds'][-1])
                nodes[node]['Restarts'] += 1
        elif event['MyType'] == 'JobAbortedEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            if nodes[node]['State'] == "idle" or nodes[node]['State'] == "held":
                nodes[node]['StartTimes'].append(-1)
                if not nodes[node]['RecordedSite']:
                    nodes[node]['SiteHistory'].append("Unknown")
            nodes[node]['State'] = 'killed'
            insertCpu(event, nodes[node])
        elif event['MyType'] == 'JobHeldEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            if nodes[node]['State'] == 'running':
                nodes[node]['EndTimes'].append(eventtime)
                if nodes[node]['WallDurations'] and nodes[node]['EndTimes'] and nodes[node]['StartTimes']:
                    nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
                insertCpu(event, nodes[node])
                nodes[node]['TotalUserCpuTimeHistory'].append(0)
                nodes[node]['TotalSysCpuTimeHistory'].append(0)
                nodes[node]['WallDurations'].append(0)
                nodes[node]['ResidentSetSize'].append(0)
                nodes[node]['SubmitTimes'].append(-1)
                nodes[node]['JobIds'].append(nodes[node]['JobIds'][-1])
                nodes[node]['Restarts'] += 1
            nodes[node]['State'] = 'held'
        elif event['MyType'] == 'JobReleaseEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            nodes[node]['State'] = 'idle'
        elif event['MyType'] == 'JobAdInformationEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            if (not nodes[node]['RecordedSite']) and ('JOBGLIDEIN_CMSSite' in event) and not event['JOBGLIDEIN_CMSSite'].startswith("$$"):
                nodes[node]['SiteHistory'].append(event['JOBGLIDEIN_CMSSite'])
                nodes[node]['RecordedSite'] = True
            insertCpu(event, nodes[node])
        elif event['MyType'] == 'JobImageSizeEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            nodes[node]['ResidentSetSize'][-1] = int(event['ResidentSetSize'])
            if nodes[node]['StartTimes']:
                nodes[node]['WallDurations'][-1] = eventtime - nodes[node]['StartTimes'][-1]
            insertCpu(event, nodes[node])
        elif event["MyType"] == "JobDisconnectedEvent" or event["MyType"] == "JobReconnectedEvent":
            # These events don't really affect the node status
            pass
        else:
            logging.warning("Unknown event type: %s", event['MyType'])

    logging.debug("There were %d events in the job log.", count)
    now = time.time()
    for node, info in nodes.items():
        if node == 'DagStatus':
            # StartTimes and WallDurations are not present, though crab status2 uses this record to get the DagStatus.
            continue
        lastStart = now
        if info['StartTimes']:
            lastStart = info['StartTimes'][-1]
        while len(info['WallDurations']) < len(info['SiteHistory']):
            info['WallDurations'].append(now - lastStart)
        while len(info['WallDurations']) > len(info['SiteHistory']):
            info['SiteHistory'].append("Unknown")

def parseErrorReport(data, nodes):
    """
    iterate over the jobs and set the error dict for those which are failed
    :param data:
    :param nodes:
    :return:
    """
    for jobid, statedict in nodes.iteritems():
        if 'State' in statedict and statedict['State'] == 'failed' and jobid in data:
            # data[jobid] is a dictionary with the retry number as a key and error summary information as a value.
            # Here we want to get the error summary information, and since values() returns a list
            # (even if there's only a single value) it has to be indexed to zero.
            statedict['Error'] = data[jobid].values()[0] #data[jobid] contains all retries. take the last one

def parseNodeStateV2(fp, nodes, level):
    """
    HTCondor 8.1.6 updated the node state file to be classad-based.
    This is a more flexible format that allows future extensions but, unfortunately,
    also requires a separate parser.
    """
    dagStatus = nodes.setdefault("DagStatus", {})
    dagStatus.setdefault("SubDagStatus", {})
    subDagStatus = dagStatus.setdefault("SubDags", {})
    for ad in classad.parseAds(fp):
        if ad['Type'] == "DagStatus":
            if level:
                statusDict = subDagStatus.setdefault(int(level), {})
                statusDict['Timestamp'] = ad.get('Timestamp', -1)
                statusDict['NodesTotal'] = ad.get('NodesTotal', -1)
                statusDict['DagStatus'] = ad.get('DagStatus', -1)
            else:
                dagStatus['Timestamp'] = ad.get('Timestamp', -1)
                dagStatus['NodesTotal'] = ad.get('NodesTotal', -1)
                dagStatus['DagStatus'] = ad.get('DagStatus', -1)
            continue
        if ad['Type'] != "NodeStatus":
            continue
        node = ad.get("Node", "")
        if node.endswith("SubJobs"):
            status = ad.get('NodeStatus', -1)
            dagname = "RunJobs{0}.subdag".format(nodeName2Re.match(node).group(1))
            # Add special state where we *expect* a submitted DAG for the
            # status command on the client
            if status == 5 and os.path.exists(dagname) and os.stat(dagname).st_size > 0:
                status = 99
            dagStatus["SubDagStatus"][node] = status
            continue
        if not node.startswith("Job"):
            continue
        nodeid = node[3:]
        status = ad.get('NodeStatus', -1)
        retry = ad.get('RetryCount', -1)
        msg = ad.get("StatusDetails", "")
        info = nodes.setdefault(nodeid, copy.deepcopy(NODE_DEFAULTS))
        if status == 1: # STATUS_READY
            if info.get("State") == "transferring":
                info["State"] = "cooloff"
            elif info.get('State') != "cooloff":
                info['State'] = 'unsubmitted'
        elif status == 2: # STATUS_PRERUN
            if retry == 0:
                info['State'] = 'unsubmitted'
            else:
                info['State'] = 'cooloff'
        elif status == 3: # STATUS_SUBMITTED
            if msg == 'not_idle':
                info.setdefault('State', 'running')
            else:
                info.setdefault('State', 'idle')
        elif status == 4: # STATUS_POSTRUN
            if info.get("State") != "cooloff":
                info['State'] = 'transferring'
        elif status == 5: # STATUS_DONE
            info['State'] = 'finished'
        elif status == 6: # STATUS_ERROR
            # Older versions of HTCondor would put jobs into STATUS_ERROR
            # for a short time if the job was to be retried.  Hence, we had
            # some status parsing logic to try and guess whether the job would
            # be tried again in the near future.  This behavior is no longer
            # observed; STATUS_ERROR is terminal.
            info['State'] = 'failed'

# --- New code ----

def storeNodesInfoInFile():
    """
    Open cache file and get the location until which the jobs_log was parsed last time
    :return: nothing
    """
    jobLogCheckpoint = None
    try:
        if os.path.exists(STATUS_CACHE_FILE) and os.stat(STATUS_CACHE_FILE).st_size > 0:
            logging.debug("cache file found, opening and reading")
            nodesStorage = open(STATUS_CACHE_FILE, "r")

            jobLogCheckpoint = nodesStorage.readline().strip()
            fjrParseResCheckpoint = int(nodesStorage.readline())
            nodes = ast.literal_eval(nodesStorage.readline())
            nodeMap = ast.literal_eval(nodesStorage.readline())
            nodesStorage.close()
        else:
            logging.debug("cache file not found, creating")
            jobLogCheckpoint = None
            fjrParseResCheckpoint = 0
            nodes = {}
            nodeMap = {}
    except Exception:
        logging.exception("error during status_cache handling")

    if jobLogCheckpoint:
        with open((LOG_PARSING_POINTERS_DIR+jobLogCheckpoint), 'r') as f:
            jel = pickle.load(f)
    else:
        jel = htcondor.JobEventLog('job_log')
    #jobsLog = open("job_log", "r")
    #jobsLog.seek(jobLogCheckpoint)

    parseJobLog(jel, nodes, nodeMap)
    # save jel object in a pickle file made unique by a timestamp
    newJelPickleName = 'jel-%d.pkl' % int(time.time())
    if not os.path.exists(LOG_PARSING_POINTERS_DIR):
        os.mkdir(LOG_PARSING_POINTERS_DIR)
    with open((LOG_PARSING_POINTERS_DIR+newJelPickleName), 'w') as f:
        pickle.dump(jel, f)
    newJobLogCheckpoint = newJelPickleName

    for fn in glob.glob("node_state*"):
        level = re.match(r'(\w+)(?:.(\w+))?', fn).group(2)
        with open(fn, 'r') as nodeState:
            parseNodeStateV2(nodeState, nodes, level)

    try:
        errorSummary, newFjrParseResCheckpoint = summarizeFjrParseResults(fjrParseResCheckpoint)
        if errorSummary and newFjrParseResCheckpoint:
            parseErrorReport(errorSummary, nodes)
    except IOError:
        logging.exception("error during error_summary file handling")

    # First write the new cache file under a temporary name, so that other processes
    # don't get an incomplete result. Then replace the old one with the new one.
    tempFilename = (STATUS_CACHE_FILE + ".%s") % os.getpid()

    nodesStorage = open(tempFilename, "w")
    nodesStorage.write(str(newJobLogCheckpoint) + "\n")
    nodesStorage.write(str(newFjrParseResCheckpoint) + "\n")
    nodesStorage.write(str(nodes) + "\n")
    nodesStorage.write(str(nodeMap) + "\n")
    nodesStorage.close()

    move(tempFilename, STATUS_CACHE_FILE)

def summarizeFjrParseResults(checkpoint):
    '''
    Reads the fjr_parse_results file line by line. The file likely contains multiple
    errors for the same jobId coming from different retries, we only care about
    the last error for each jobId. Since each postjob writes this information
    sequentially (job retry #2 will be written after job retry #1), overwrite
    whatever information there was before for each jobId.

    Return the updated error dictionary and also the location until which the
    fjr_parse_results file was read so that we can store it and
    don't have t re-read the same information next time the cache_status.py runs.
    '''

    if os.path.exists(FJR_PARSE_RES_FILE):
        with open(FJR_PARSE_RES_FILE, "r") as f:
            f.seek(checkpoint)
            content = f.readlines()
            newCheckpoint = f.tell()

        errDict = {}
        for line in content:
            fjrResult = ast.literal_eval(line)
            jobId = fjrResult.keys()[0]
            errDict[jobId] = fjrResult[jobId]
        return errDict, newCheckpoint
    else:
        return None, 0

def main():
    """
    parse condor job_log from last checkpoint until now and write summary in status_cache file
    :return:
    """
    try:
        storeNodesInfoInFile()
    except Exception:
        logging.exception("error during main loop")

main()

logging.debug("cache_status_jel.py exiting")
