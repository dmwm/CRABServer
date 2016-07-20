#!/usr/bin/python
from __future__ import print_function, division
import re
import time
import logging
import os
import ast
import json
import sys
import classad
from shutil import move
# Need to import HTCondorUtils from a parent directory, not easy when the files are not in python packages.
# Solution by ajay, SO: http://stackoverflow.com/questions/11536764
# /attempted-relative-import-in-non-package-even-with-init-py/27876800#comment28841658_19190695
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import HTCondorUtils

logging.basicConfig(filename='task_process/cache_status.log', level=logging.DEBUG)

#
# insertCpu,
# parseJobLog and
# parseErrorReport code copied from the backend HTCondorDataWorkflow.py
#

cpu_re = re.compile(r"Usr \d+ (\d+):(\d+):(\d+), Sys \d+ (\d+):(\d+):(\d+)")
def insertCpu(event, info):
    if 'TotalRemoteUsage' in event:
        m = cpu_re.match(event['TotalRemoteUsage'])
        if m:
            g = [int(i) for i in m.groups()]
            user = g[0]*3600 + g[1]*60 + g[2]
            sys = g[3]*3600 + g[4]*60 + g[5]
            info['TotalUserCpuTimeHistory'][-1] = user
            info['TotalSysCpuTimeHistory'][-1] = sys
    else:
        if 'RemoteSysCpu' in event:
            info['TotalSysCpuTimeHistory'][-1] = float(event['RemoteSysCpu'])
        if 'RemoteUserCpu' in event:
            info['TotalUserCpuTimeHistory'][-1] = float(event['RemoteUserCpu'])

node_name_re = re.compile("DAG Node: Job(\d+(?:-\d)?)")
node_name2_re = re.compile("Job(\d+(?:-\d)?)")

def parseJobLog(fp, nodes, node_map):
    count = 0
    for event in HTCondorUtils.readEvents(fp):
        count += 1
        eventtime = time.mktime(time.strptime(event['EventTime'], "%Y-%m-%dT%H:%M:%S"))
        if event['MyType'] == 'SubmitEvent':
            m = node_name_re.match(event['LogNotes'])
            if m:
                node = m.groups()[0]
                proc = event['Cluster'], event['Proc']
                info = nodes.setdefault(node, {'Retries': 0, 'Restarts': 0, 'SiteHistory': [], 'ResidentSetSize': [], 'SubmitTimes': [], 'StartTimes': [],
                                               'EndTimes': [], 'TotalUserCpuTimeHistory': [], 'TotalSysCpuTimeHistory': [], 'WallDurations': [], 'JobIds': []})
                info['State'] = 'idle'
                info['JobIds'].append("%d.%d" % proc)
                info['RecordedSite'] = False
                info['SubmitTimes'].append(eventtime)
                info['TotalUserCpuTimeHistory'].append(0)
                info['TotalSysCpuTimeHistory'].append(0)
                info['WallDurations'].append(0)
                info['ResidentSetSize'].append(0)
                info['Retries'] = len(info['SubmitTimes'])-1
                node_map[proc] = node
        elif event['MyType'] == 'ExecuteEvent':
            node = node_map[event['Cluster'], event['Proc']]
            nodes[node]['StartTimes'].append(eventtime)
            nodes[node]['State'] = 'running'
            nodes[node]['RecordedSite'] = False
        elif event['MyType'] == 'JobTerminatedEvent':
            node = node_map[event['Cluster'], event['Proc']]
            nodes[node]['EndTimes'].append(eventtime)
            nodes[node]['WallDurations'][-1] = nodes[node]['EndTimes'][-1] - nodes[node]['StartTimes'][-1]
            insertCpu(event, nodes[node])
            if event['TerminatedNormally']:
                if event['ReturnValue'] == 0:
                    nodes[node]['State'] = 'transferring'
                else:
                    nodes[node]['State'] = 'cooloff'
            else:
                nodes[node]['State'] = 'cooloff'
        elif event['MyType'] == 'PostScriptTerminatedEvent':
            m = node_name2_re.match(event['DAGNodeName'])
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
            node = node_map[event['Cluster'], event['Proc']]
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
            node = node_map[event['Cluster'], event['Proc']]
            if nodes[node]['State'] == "idle" or nodes[node]['State'] == "held":
                nodes[node]['StartTimes'].append(-1)
                if not nodes[node]['RecordedSite']:
                    nodes[node]['SiteHistory'].append("Unknown")
            nodes[node]['State'] = 'killed'
            insertCpu(event, nodes[node])
        elif event['MyType'] == 'JobHeldEvent':
            node = node_map[event['Cluster'], event['Proc']]
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
            node = node_map[event['Cluster'], event['Proc']]
            nodes[node]['State'] = 'idle'
        elif event['MyType'] == 'JobAdInformationEvent':
            node = node_map[event['Cluster'], event['Proc']]
            if (not nodes[node]['RecordedSite']) and ('JOBGLIDEIN_CMSSite' in event) and not event['JOBGLIDEIN_CMSSite'].startswith("$$"):
                nodes[node]['SiteHistory'].append(event['JOBGLIDEIN_CMSSite'])
                nodes[node]['RecordedSite'] = True
            insertCpu(event, nodes[node])
        elif event['MyType'] == 'JobImageSizeEvent':
            node = node_map[event['Cluster'], event['Proc']]
            nodes[node]['ResidentSetSize'][-1] = int(event['ResidentSetSize'])
            if nodes[node]['StartTimes']:
                nodes[node]['WallDurations'][-1] = eventtime - nodes[node]['StartTimes'][-1]
            insertCpu(event, nodes[node])
        elif event["MyType"] == "JobDisconnectedEvent" or event["MyType"] == "JobReconnectedEvent":
            # These events don't really affect the node status
            pass
        else:
            logging.warning("Unknown event type: %s" % event['MyType'])

    logging.debug("There were %d events in the job log." % count)
    now = time.time()
    for node, info in nodes.items():
        last_start = now
        if info['StartTimes']:
            last_start = info['StartTimes'][-1]
        while len(info['WallDurations']) < len(info['SiteHistory']):
            info['WallDurations'].append(now - last_start)
        while len(info['WallDurations']) > len(info['SiteHistory']):
            info['SiteHistory'].append("Unknown")

def parseErrorReport(fp, nodes):
    def last(joberrors):
        return joberrors[max(joberrors, key=int)]
    data = json.load(fp)
    #iterate over the jobs and set the error dict for those which are failed
    for jobid, statedict in nodes.iteritems():
        if 'State' in statedict and statedict['State'] == 'failed' and jobid in data:
            statedict['Error'] = last(data[jobid]) #data[jobid] contains all retries. take the last one

def parseNodeStateV2(fp, nodes):
    """
    HTCondor 8.1.6 updated the node state file to be classad-based.
    This is a more flexible format that allows future extensions but, unfortunately,
    also requires a separate parser.
    """
    taskStatus = nodes.setdefault("DagStatus", {})
    for ad in classad.parseAds(fp):
        if ad['Type'] == "DagStatus":
            taskStatus['Timestamp'] = ad.get('Timestamp', -1)
            taskStatus['NodesTotal'] = ad.get('NodesTotal', -1)
            taskStatus['DagStatus'] = ad.get('DagStatus', -1)
            continue
        if ad['Type'] != "NodeStatus":
            continue
        node = ad.get("Node", "")
        if not node.startswith("Job"):
            continue
        nodeid = node[3:]
        status = ad.get('NodeStatus', -1)
        retry = ad.get('RetryCount', -1)
        msg = ad.get("StatusDetails", "")
        if status == 1: # STATUS_READY
            info = nodes.setdefault(nodeid, {})
            if info.get("State") == "transferring":
                info["State"] = "cooloff"
            elif info.get('State') != "cooloff":
                info['State'] = 'unsubmitted'
        elif status == 2: # STATUS_PRERUN
            info = nodes.setdefault(nodeid, {})
            if retry == 0:
                info['State'] = 'unsubmitted'
            else:
                info['State'] = 'cooloff'
        elif status == 3: # STATUS_SUBMITTED
            info = nodes.setdefault(nodeid, {})
            if msg == 'not_idle':
                info.setdefault('State', 'running')
            else:
                info.setdefault('State', 'idle')
        elif status == 4: # STATUS_POSTRUN
            info = nodes.setdefault(nodeid, {})
            if info.get("State") != "cooloff":
                info['State'] = 'transferring'
        elif status == 5: # STATUS_DONE
            info = nodes.setdefault(nodeid, {})
            info['State'] = 'finished'
        elif status == 6: # STATUS_ERROR
            info = nodes.setdefault(nodeid, {})
            # Older versions of HTCondor would put jobs into STATUS_ERROR
            # for a short time if the job was to be retried.  Hence, we had
            # some status parsing logic to try and guess whether the job would
            # be tried again in the near future.  This behavior is no longer
            # observed; STATUS_ERROR is terminal.
            info['State'] = 'failed'

# --- New code ----

def storeNodesInfoInFile():
    filename = "task_process/status_cache.txt"
    # Open cache file and get the location until which the jobs_log was parsed last time
    try:
        if os.path.exists(filename) and os.stat(filename).st_size > 0:
            logging.debug("cache file found, opening and reading")
            nodes_storage = open(filename, "r")

            last_read_until = int(nodes_storage.readline())
            nodes = ast.literal_eval(nodes_storage.readline())
            node_map = ast.literal_eval(nodes_storage.readline())
            nodes_storage.close()
        else:
            logging.debug("cache file not found, creating")
            last_read_until = 0
            nodes = {}
            node_map = {}
    except Exception:
        logging.exception("error during status_cache handling")
    jobsLog = open("job_log", "r")

    try:
        errorSummary = open("error_summary.json", "r")
        errorSummary.seek(0)
        parseErrorReport(errorSummary, nodes)
        errorSummary.close()
    except IOError:
        logging.exception("error during error_summary file handling")

    jobsLog.seek(last_read_until)

    parseJobLog(jobsLog, nodes, node_map)
    read_until = jobsLog.tell()
    jobsLog.close()

    node_state = open("node_state", "r")
    parseNodeStateV2(node_state, nodes)
    node_state.close()

    # First write the new cache file under a temporary name, so that other processes
    # don't get an incomplete result. Then replace the old one with the new one.
    temp_filename = (filename + ".%s") % os.getpid()

    nodes_storage = open(temp_filename, "w")
    nodes_storage.write(str(read_until) + "\n")
    nodes_storage.write(str(nodes) + "\n")
    nodes_storage.write(str(node_map) + "\n")
    nodes_storage.close()

    move(temp_filename, filename)

def main():
    try:
        storeNodesInfoInFile()
    except Exception:
        logging.exception("error during main loop")

main()

logging.debug("cache_status.py exiting")

