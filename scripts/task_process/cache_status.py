#!/usr/bin/python
"""
VERSION OF CACHE_STATUS USING HTCONDOR JobEventLog API
THIS REQUIRES HTCONDOR 8.9.3 OR ABOVE
"""
import re
import time
import logging
import os
import ast
import glob
import copy
from shutil import move
import pickle
import json

import htcondor2 as htcondor
import classad2 as classad

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
PKL_STATUS_CACHE_FILE = "task_process/status_cache.pkl"
JSON_STATUS_CACHE_FILE = "task_process/status_cache.json"
LOG_PARSING_POINTERS_DIR = "task_process/jel_pickles/"
FJR_PARSE_RES_FILE = "task_process/fjr_parse_results.txt"


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


nodeNameRe = re.compile(r"DAG Node: Job(\d+(?:-\d+)?)")
nodeName2Re = re.compile(r"Job(\d+(?:-\d+)?)")

# this now takes as input an htcondor.JobEventLog object
# which as of HTCondor 8.9 can be saved/restored with memory of
# where it had reached in processing the job log file
def parseJobLog(jel, nodes, nodeMap):
    """
    parses new events in condor job log file and updates nodeMap
    :param jel: a condor JobEventLog object which provides an iterator over events
    :param nodes: the structure where we collect one job info for cache_status file
    :param nodeMap: a structure to map DAG node id (i.e. CRAB_Id) to condor jobid Cluster.Proc
                    first (Submit) event in job_log has a LogNotes attribute with the DAG id,
                    but subsequent ones are only identified via Cluster and Proc
    :return: nothing
    """
    count = 0
    for event in jel.events(0):
        count += 1
        eventtime = time.mktime(time.strptime(event['EventTime'], "%Y-%m-%dT%H:%M:%S"))
        if event['MyType'] == 'SubmitEvent':
            m = nodeNameRe.match(event['LogNotes'])  # True if LogNotes is like 'DAG Node: Job13'
            if m:
                node = m.groups()[0]  # the number after 'DAG Node: Job' e.g. '13' (as a string), i.e. CRAB_Id
                proc = event['Cluster'], event['Proc']  # SB: why a tuple instead of a string  like '10210368.0' ?
                info = nodes.setdefault(node, copy.deepcopy(NODE_DEFAULTS))  # adds key "node" and makes info=NODE_DEFAULTS
                info['State'] = 'idle'
                info['JobIds'].append("%d.%d" % proc)
                info['RecordedSite'] = False
                info['SubmitTimes'].append(eventtime)
                info['TotalUserCpuTimeHistory'].append(0)
                info['TotalSysCpuTimeHistory'].append(0)
                info['WallDurations'].append(0)
                info['ResidentSetSize'].append(0)
                info['Retries'] = len(info['SubmitTimes'])-1
                nodeMap[proc] = node  # nodeMap maps CRAB_Id '13' to (cluster, proc) i.e. condor jobId
        elif event['MyType'] == 'ExecuteEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            nodes[node]['StartTimes'].append(eventtime)
            nodes[node]['State'] = 'running'
            nodes[node]['RecordedSite'] = False
        elif event['MyType'] == 'JobTerminatedEvent':
            node = nodeMap[event['Cluster'], event['Proc']]
            nodes[node]['EndTimes'].append(eventtime)
            # at times HTCondor does not log the ExecuteEvent and there's no StartTime
            if nodes[node]['StartTimes'] and nodes[node]['StartTimes'][-1] > 0:
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
        elif event["MyType"] == "JobDisconnectedEvent" \
             or event["MyType"] == "JobReconnectedEvent" \
             or event["MyType"] == "FileTransferEvent" :
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
            if lastStart > 0:
                info['WallDurations'].append(now - lastStart)
            else:  # this means job did not start
                info['WallDurations'].append(0)
        while len(info['WallDurations']) > len(info['SiteHistory']):
            info['SiteHistory'].append("Unknown")

def parseErrorReport(data, nodes):
    """
    iterate over the jobs and set the error dict for those which are failed
    :param data: a dictionary as returned by summarizeFjrParseResults() : {jobid:errdict}
                 errdict is {crab_retry:error_summary} from PostJob/prepareErrorSummary
                 which writes one line for PostJoun run: {job_id : {crab_retry : error_summary}}
                 where crab_retry is a string and error_summary a list [exitcode, errorMsg, {}]
    :param nodes: a dictionary with format {jobid:statedict}
    :return: nothing, modifies nodes in place
    """
    for jobid, statedict in nodes.items():
        if 'State' in statedict and statedict['State'] == 'failed' and jobid in data:
            # pick error info from last retry (SB: AFAICT only last retry is listed anyhow)
            for key in data[jobid]:
                statedict['Error'] = data[jobid][key]

def parseNodeStateV2(fp, nodes, level):
    """
    HTCondor 8.1.6 updated the node state file to be classad-based.
    This is a more flexible format that allows future extensions but, unfortunately,
    also requires a separate parser.
    """
    # note that when nodes was read from cache, setdefault returns the current value
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
            # DagmanCreator uses Job{count}SubJobs as JobName for sub-dags
            # ref https://htcondor.readthedocs.io/en/latest/automated-workflows/dagman-using-other-dags.html#composing-workflows-from-dags
            # and https://htcondor.readthedocs.io/en/latest/automated-workflows/dagman-using-other-dags.html#a-dag-within-a-dag-is-a-subdag
            status = ad.get('NodeStatus', -1)
            dagname = "RunJobs{0}.subdag".format(nodeName2Re.match(node).group(1))
            # Add special state where we *expect* a submitted DAG for the
            # status command on the client. Note: our subdags are submitted with NOOP so
            # they complete immediately and the running Dag ends (status=5), the actual
            # submission of the subdags happens inside the PRE step in PreDag.py
            # Therefore we use the following set-status-to-99 trick to say
            #  "this dag is done, so look at subdags to tell if something is still running"
            if status == 5 and os.path.exists(dagname) and os.stat(dagname).st_size > 0:
                status = 99  # means: DAG completed after submitting a new DAG
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
            if retry == 0:
                info['State'] = 'unsubmitted'
            else:
                info['State'] = 'cooloff'
        elif status == 2: # STATUS_PRERUN
            if retry == 0:
                info['State'] = 'unsubmitted'
            else:
                info['State'] = 'cooloff'
        elif status == 3: # STATUS_SUBMITTED
            if msg == 'not_idle':
                info['State'] = 'running'
            else:
                info['State'] = 'idle'
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

def readOldStatusCacheFile():
    """
    it is enough to read the Pickle version, since we want to transition to that
    returns: a dictionary with keys: jobLogCheckpoint, fjrParseResCheckpoint, nodes, nodeMap
    """
    jobLogCheckpoint = None
    if os.path.exists(PKL_STATUS_CACHE_FILE) and os.stat(PKL_STATUS_CACHE_FILE).st_size > 0:
        logging.debug("cache file found, opening")
        try:
            with open(PKL_STATUS_CACHE_FILE, "rb") as fp:
                cacheDoc = pickle.load(fp)
            # protect against fake file with just bootstrapTime created by AdjustSites.py
            # note: python's dictionary.get(key) returns None if key is not in dictionary
            jobLogCheckpoint = cacheDoc.get('jobLogCheckpoint')
            fjrParseResCheckpoint = cacheDoc.get('fjrParseResCheckpoint')
            nodes = cacheDoc.get('nodes')
            nodeMap = cacheDoc.get('nodeMap')
        except Exception:  # pylint: disable=broad-except
            logging.exception("error during status_cache handling")
            jobLogCheckpoint = None

    if not jobLogCheckpoint:
        logging.debug("no usable cache file found, creating")
        fjrParseResCheckpoint = 0
        nodes = {}
        nodeMap = {}

    # collect all cache info in a single dictionary and return it to called
    cacheDoc = {}
    cacheDoc['jobLogCheckpoint'] = jobLogCheckpoint
    cacheDoc['fjrParseResCheckpoint'] = fjrParseResCheckpoint
    cacheDoc['nodes'] = nodes
    cacheDoc['nodeMap'] = nodeMap
    return cacheDoc

def parseCondorLog(cacheDoc):
    """
    do all real work and update checkpoints, nodes and nodemap dictionaries
    takes as input a cacheDoc dictionary with keys
      jobLogCheckpoint, fjrParseResCheckpoint, nodes, nodeMap
    and returns the same dictionary with updated information
    """

    jobLogCheckpoint = cacheDoc['jobLogCheckpoint']
    fjrParseResCheckpoint = cacheDoc['fjrParseResCheckpoint']
    nodes = cacheDoc['nodes']
    nodeMap = cacheDoc['nodeMap']
    if jobLogCheckpoint:
        # resume log parsing where we left
        with open((LOG_PARSING_POINTERS_DIR+jobLogCheckpoint), 'rb') as f:
            jel = pickle.load(f)
    else:
        # parse log from beginning
        jel = htcondor.JobEventLog('job_log')

    parseJobLog(jel, nodes, nodeMap)
    # save jel object in a pickle file made unique by a timestamp
    newJelPickleName = 'jel-%d.pkl' % int(time.time())
    if not os.path.exists(LOG_PARSING_POINTERS_DIR):
        os.mkdir(LOG_PARSING_POINTERS_DIR)
    with open((LOG_PARSING_POINTERS_DIR+newJelPickleName), 'wb') as f:
        pickle.dump(jel, f)
    newJobLogCheckpoint = newJelPickleName

    for fn in glob.glob("node_state*"):
        level = re.match(r'(\w+)(?:.(\w+))?', fn).group(2)
        with open(fn, 'r', encoding='utf-8') as nodeState:
            parseNodeStateV2(nodeState, nodes, level)

    try:
        errorSummary, newFjrParseResCheckpoint = summarizeFjrParseResults(fjrParseResCheckpoint)
        if errorSummary and newFjrParseResCheckpoint:
            parseErrorReport(errorSummary, nodes)
    except IOError:
        logging.exception("error during error_summary file handling")

    # collect all cache info in a single dictionary and return it to caller
    newCacheDoc = {}
    newCacheDoc['jobLogCheckpoint'] = newJobLogCheckpoint
    newCacheDoc['fjrParseResCheckpoint'] = newFjrParseResCheckpoint
    newCacheDoc['nodes'] = nodes
    newCacheDoc['nodeMap'] = nodeMap
    return newCacheDoc

def storeNodesInfoInPklFile(cacheDoc):
    """
    takes as input an cacheDoc dictionary with keys
      jobLogCheckpoint, fjrParseResCheckpoint, nodes, nodeMap
    """
    # First write the new cache file under a temporary name, so that other processes
    # don't get an incomplete result. Then replace the old one with the new one.
    tempFilename = (PKL_STATUS_CACHE_FILE + ".%s") % os.getpid()

    # persist cache info in py2-compatible pickle format
    with open(tempFilename, "wb") as fp:
        pickle.dump(cacheDoc, fp, protocol=2)
    # replace old file with new one
    move(tempFilename, PKL_STATUS_CACHE_FILE)

def storeNodesInfoInTxtFile(cacheDoc):
    """
    takes as input an cacheDoc dictionary with keys
      jobLogCheckpoint, fjrParseResCheckpoint, nodes, nodeMap
    """
    jobLogCheckpoint = cacheDoc['jobLogCheckpoint']
    fjrParseResCheckpoint = cacheDoc['fjrParseResCheckpoint']
    nodes = cacheDoc['nodes']
    nodeMap = cacheDoc['nodeMap']
    # First write the new cache file under a temporary name, so that other processes
    # don't get an incomplete result. Then replace the old one with the new one.
    tempFilename = (STATUS_CACHE_FILE + ".%s") % os.getpid()

    with open(tempFilename, "w", encoding='utf-8') as nodesStorage:
        nodesStorage.write(str(jobLogCheckpoint) + "\n")
        nodesStorage.write(str(fjrParseResCheckpoint) + "\n")
        nodesStorage.write(str(nodes) + "\n")
        nodesStorage.write(str(nodeMap) + "\n")

    move(tempFilename, STATUS_CACHE_FILE)

def storeNodesInfoInJSONFile(cacheDoc):
    """
    takes as input an cacheDoc dictionary with keys
      jobLogCheckpoint, fjrParseResCheckpoint, nodes, nodeMap
    """

    # First write a new cache file with a temporary name. Then replace old one with new.
    tempFilename = (JSON_STATUS_CACHE_FILE + ".%s") % os.getpid()
    # nodeMap keys are tuple, JSON does not like them. Anyhot this dict. appears unused by other code
    newDict = copy.deepcopy(cacheDoc)
    del newDict['nodeMap']
    # Avoid time information to enable comparison with *new*
    for node, nodeInfo in newDict['nodes'].items():
        if node != 'DagStatus':
            del nodeInfo['WallDurations']
    # remove checkpoints to enable comparison with *new*
    del newDict['jobLogCheckpoint']
    with open(tempFilename, "w", encoding='utf-8') as fp:
        json.dump(newDict, fp)
    move(tempFilename, JSON_STATUS_CACHE_FILE)


def summarizeFjrParseResults(checkpoint):
    """
    Reads the fjr_parse_results file line by line. The file likely contains multiple
    errors for the same jobId coming from different retries, we only care about
    the last error for each jobId. Since each postjob writes this information
    sequentially (job retry #2 will be written after job retry #1), overwrite
    whatever information there was before for each jobId.

    Return the updated error dictionary and also the location until which the
    fjr_parse_results file was read so that we can store it and
    don't have t re-read the same information next time the cache_status.py runs.

    SB: what this does is to convert file with one "JSON string per line"
    which represent a dictionary {jobid:errorDictionary}
    e.g.
    {"134": {"0": [50115, "BadFWJRXML", {}]}}
    {"1": {"0": [0, "OK", {}]}}
    ...
    where the same jobId may be repeated, into a single dictionay which for each
    jobId key contains only the last value of the errorDictionary
    for d in content:
      for k,v in d.items():
        errDict[k] = v

    Format and content of errorDictionary is described in parseErrorReport() method
    """

    if os.path.exists(FJR_PARSE_RES_FILE):
        with open(FJR_PARSE_RES_FILE, "r", encoding='utf-8') as f:
            f.seek(checkpoint)
            content = f.readlines()
            newCheckpoint = f.tell()
        errDict = {}
        for line in content:
            fjrResult = json.loads(line)
            for jobId,msg in fjrResult.items():
                errDict[jobId] = msg
        return errDict, newCheckpoint
    return None, 0

def reportDagStatusToDB(dagStatus):
    """
    argument
    dagStatus : a dictionary. Example for normal (no autom. splitt.) task:
     { "SubDagStatus": {}, "SubDags": {}, "Timestamp": 1741859306,
      "NodesTotal": 30, "DagStatus": 6 }
       Example for an automatic splitting task with not tails:
     {'SubDagStatus': {'Job0SubJobs': 99, 'Job1SubJobs': 5},
      'SubDags': {0: {'Timestamp': 1744826643, 'NodesTotal': 26, 'DagStatus': 5}},
       'Timestamp': 1744823021, 'NodesTotal': 6, 'DagStatus': 5}
    this is the value of the `DagStatus' key in the 'nodes' dictionary
    created inside parseNodeStateV2 function
    """
    statusName = collapseDAGStatus((dagStatus))
    logging.info("UPDATE DAG STATUS IN TASK DB")
    logging.info(f"Full dagStatus is {dagStatus}")
    logging.info(f"Will report {statusName}")

    with open(os.environ['_CONDOR_JOB_AD'], 'r', encoding='utf-8') as fd:
        ad = classad.parseOne(fd)
    host = ad['CRAB_RestHost']
    dbInstance = ad['CRAB_DbInstance']
    userProxy = ad['X509UserProxy']
    taskname = ad['CRAB_Reqname']
    logging.debug(f"host {host} dbInstance {dbInstance} cert {userProxy}")
    logging.debug(f"taskname {taskname}  DAGstatus {statusName}")
    from RESTInteractions import CRABRest  # pylint: disable=import-outside-toplevel
    from urllib.parse import urlencode  # pylint: disable=import-outside-toplevel
    crabserver = CRABRest(hostname=host, localcert=userProxy, localkey=userProxy, retry=0, userAgent='CRABSchedd')
    crabserver.setDbInstance(dbInstance)
    data = {'subresource': 'edit', 'column': 'tm_dagman_status',
            'value': statusName, 'workflow': taskname}
    try:
        R = crabserver.post(api='task', data=urlencode(data))
    except Exception as ex:  # pylint: disable=broad-except
        R = str(ex)
    logging.info(f"HTTP POST returned {R}")

    return

def collapseDAGStatus(dagInfo):
    """Collapse the status of one or several DAGs to a single one.

    Take into account that subdags can be submitted to the queue on the
    schedd, but not yet started.

    moved here from CRABClient/Command/status.py with small adaptions to work
    in the scheduler (HTCondor AP)
    originally developed by Matthias Wolf
    """
    status_order = ['SUBMITTED', 'FAILED', 'FAILED (KILLED)', 'COMPLETED']

    # subDagInfos is a dictionary with key the dag level ({count}) e.g.
    # {0: {'Timestamp': 1744883488, 'NodesTotal': 94, 'DagStatus': 3}}
    # indiates a running processing dag with 94 nodes
    subDagInfos = dagInfo.get('SubDags', {})
    # subDagStatus is a dictionary with key Job{count}SunJobs and value the status
    # e.g. {'Job0SubJobs': 99, 'Job1SubJobs': 2}q

    subDagStatus = dagInfo.get('SubDagStatus', {})
    if len(subDagInfos) == 0 and len(subDagStatus) == 0:
        # Regular splitting, return status of DAG
        return translateDagStatus(dagInfo['DagStatus'])

    def check_queued(statusOrSUBMITTED):
        # 99 is the status of a DAG which submitted one ore more subdags.
        # If there are less actual DAG status informations than expected DAGs, at least one
        # DAG has to be queued.
        # Stefano: now that we run in the AP we can possibly do better for detecting
        # that a condor_submit_dag has been issued but job did not start yet.
        # condor_dagmans run as job in scheduler universe (7) and e.g. expose CRAB_Reqname ad
        if len(subDagInfos) < len([k for k in subDagStatus if subDagStatus[k] == 99]):
            return 'SUBMITTED'
        return statusOrSUBMITTED

    # From now one deal with automatic splitting and the presence if subdags

    # If the processing DAG is still running, we are 'SUBMITTED',
    # still.
    if len(subDagInfos) > 0:
        state = translateDagStatus(subDagInfos[0]['DagStatus'])
        if state == 'SUBMITTED':
            return state
    # Tails active: return most active tail status according to
    # `status_order`
    if len(subDagInfos) > 1:
        states = [translateDagStatus(subDagInfos[k]['DagStatus']) for k in subDagInfos if k > 0]
        for iStatus in status_order:
            if states.count(iStatus) > 0:
                return check_queued(iStatus)
    # If no tails are active, return the status of the processing DAG.
    if len(subDagInfos) > 0:
        return check_queued(translateDagStatus(subDagInfos[0]['DagStatus']))
    return check_queued(translateDagStatus(dagInfo['DagStatus']))

def translateDagStatus(status):
    """
    from a number to a string which CRAB people can understand
    From
    https://htcondor.readthedocs.io/en task_process/cache_status.py
/latest/automated-workflows/dagman-information-files.html#current-node-status-file
    Most status values in there semms only relevant for nodes, but documentation does not differentiate
    0 (STATUS_NOT_READY): At least one parent has not yet finished or the node is a FINAL node.
    1 (STATUS_READY): All parents have finished, but the node is not yet running.
    2 (STATUS_PRERUN): The node’s PRE script is running.
    3 (STATUS_SUBMITTED): The node’s HTCondor job(s) are in the queue.
    4 (STATUS_POSTRUN): The node’s POST script is running.
    5 (STATUS_DONE): The node has completed successfully.
    6 (STATUS_ERROR): The node has failed.
    7 (STATUS_FUTILE): The node will never run because an ancestor node failed.
    """

    #
    # N.B. separate status=5 in SUCCESS or FAILED based on whether all job succeeded or not ?
    # or is already done by DAGMAN ?
    DAG_STATUS_TO_STRING = {
        0: 'SUBMITTED',  # clear for a node, but for DAG ? to be verified
        1: 'SUBMITTED',  # clear for a node, but for DAG ? to be verified
        3: 'RUNNING',
        4: 'RUNNING',
        5: 'COMPLETED',
        6: 'FAILED',  # one ore more jobs (DAG nodes) failed
        7: 'ERROR'  # as far as Stefano understand, this should never happen
    }
    # Do we report just DAG status, or a combined "global" status ?

    statusName = DAG_STATUS_TO_STRING[status]
    return statusName

def main():
    """
    parse condor job_log from last checkpoint until now and write summary in status_cache files
    :return:
    """
    try:
        logging.info(f"Start at {time.strftime('%d/%m/%y %X',time.localtime())}")

        oldInfo = readOldStatusCacheFile()
        updatedInfo = parseCondorLog(oldInfo)
        storeNodesInfoInPklFile(updatedInfo)
        # to keep the txt file locally, useful for debugging, when we remove the old code:
        storeNodesInfoInTxtFile(updatedInfo)
        storeNodesInfoInJSONFile(updatedInfo)

        # make sure that we only do this when status has changed, not every 5 minutes
        # even if...all in all.. one call per task every 5min is a drop in the ocean
        # isTimeToReport
        reportDagStatusToDB(updatedInfo['nodes']['DagStatus'])

    except Exception:  # pylint: disable=broad-except
        logging.exception("error during main loop")

main()

logging.debug("cache_status.py exiting")
