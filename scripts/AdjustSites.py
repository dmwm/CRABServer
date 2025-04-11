"""
This script is called by dag_bootstrap_startup.sh when the job is (re)submitted.
It not only does adjusting sites (blacklist/whitelist) but also:
    - It downloads sandbox from S3 (if not exist).
    - It creates the webdir if necessary
    - It updates both the webdir ant the proxied version of it on the REST task db
    - For resubmission: adjust the exit codes of the PJ in the RunJobs.dag.nodes.log files and
      the max retries in the RunJobs.dag files
"""
# this is full of snake_case variables from old times, which we do not care to change
# pylint: disable=invalid-name
# and yes, it is long and old but it may get simpler when we rewrite resubmission
# pylint: disable=too-many-branches

from __future__ import print_function

import pickle
import os
import re
import sys
import time
import glob
import shutil
import logging
import tarfile
from urllib.parse import urlencode
import traceback
from datetime import datetime
from http.client import HTTPException

from RESTInteractions import CRABRest
from ServerUtilities import getProxiedWebDir, getColumn, downloadFromS3

import htcondor2 as htcondor
import classad2 as classad

def printLog(msg):
    """ Utility function to print the timestamp in the log. Can be replaced
        with anything (e.g.: logging.info if we decided to set up a logger here)
    """
    print(f"{datetime.utcnow()}: {msg}")


def setupStreamLogger():
    """
    Setup logger object with stream handler. Needed by `downloadFromS3()`.
    :returns: stream logger object
    :rtype: logging.Logger
    """
    logHandler = logging.StreamHandler()
    logFormatter = logging.Formatter(
        "%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
    logHandler.setFormatter(logFormatter)
    logger = logging.getLogger('AdjustSites') # hardcode
    logger.addHandler(logHandler)
    logger.setLevel(logging.DEBUG)
    return logger


def adjustPostScriptExitStatus(resubmitJobIds, filename):
    """
    Edit the DAG .nodes.log file changing the POST script exit code from 0|2 to 1
    (i.e., in RetryJob terminology, from OK|FATAL_ERROR to RECOVERABLE_ERROR) for
    the job ids passed in the resubmitJobIds argument. This way DAGMan will retry
    these nodes when the DAG is resubmitted. In practice, search for this kind of
    sequence in the DAG .nodes.log file:
    ...
    016 (146493.000.000) 11/11 17:45:46 POST Script terminated.
        (1) Normal termination (return value [0|2])
        DAG Node: Job105
    ...
    for the job ids in resubmitJobIds and replace the return value to 1.
    If resubmitJobIds = True, only replace return values 2 (not 0) to 1.

    Note:
          When DAGMan runs in recovery mode, the DAG .nodes.log file is used to
    identify the nodes that have completed and should not be resubmitted.
          When DAGMan runs in rescue mode (assuming a rescue DAG file is present,
    which is not the case when a DAG is aborted, for example), all failed nodes
    are resubmitted. Nodes can be labeled as DONE in the rescue DAG file and
    DAGMan will not be rerun them, but then the node state would be set to DONE.
    """
    if not resubmitJobIds:
        return []
    printLog(f"Looking for resubmitJobIds {resubmitJobIds} in {filename}")
    resubmitAllFailed = (resubmitJobIds is True)
    terminator_re = re.compile(r"^\.\.\.$")
    # date field has two different format, this is for condor up to version 8.8.8
    event1_re = re.compile(r"016 \(-?\d+\.\d+\.\d+\) \d+/\d+ \d+:\d+:\d+ POST Script terminated.")
    # and this for 8.9.7
    event2_re = re.compile(r"016 \(-?\d+\.\d+\.\d+\) \d+-\d+-\d+ \d+:\d+:\d+ POST Script terminated.")
    if resubmitAllFailed:
        retvalue_re = re.compile(r"Normal termination \(return value 2\)")
    else:
        retvalue_re = re.compile(r"Normal termination \(return value [0|2]\)")
    node_re = re.compile(r"DAG Node: Job(\d+(-\d+)?)")
    ra_buffer = []
    alt = None
    output = ''
    adjustedJobIds = []
    for line in open(filename, 'r', encoding='utf-8').readlines():
        if len(ra_buffer) == 0:
            m = terminator_re.search(line)
            if m:
                ra_buffer.append(line)
            else:
                output += line
        elif len(ra_buffer) == 1:
            m1 = event1_re.search(line)
            m2 = event2_re.search(line)
            if m1 or m2:
                ra_buffer.append(line)
            else:
                for l in ra_buffer:
                    output += l
                output += line
                ra_buffer = []
        elif len(ra_buffer) == 2:
            m = retvalue_re.search(line)
            if m:
                ra_buffer.append("        (1) Normal termination (return value 1)\n")
                alt = line
            else:
                for l in ra_buffer:
                    output += l
                output += line
                ra_buffer = []
        elif len(ra_buffer) == 3:
            m = node_re.search(line)
            printLog(f"Matching the line '{line.strip()}' to groups: {m.groups()}")
            if m and (resubmitAllFailed or (m.groups()[0] in resubmitJobIds)):
                printLog(f"Successful match: {m.groups()[0]}, adjusting status and appending to adjustedJobIds")
                adjustedJobIds.append(m.groups()[0])
                for l in ra_buffer:
                    output += l
            else:
                for l in ra_buffer[:-1]:
                    output += l
                output += alt
            output += line
            ra_buffer = []
        else:
            output += line
    if ra_buffer:
        for l in ra_buffer:
            output += l
    # This is a curious dance!  If the user is out of quota, we don't want
    # to fail halfway into writing the file.  OTOH, we can't write into a temp
    # file and an atomic rename because the running shadows keep their event log
    # file descriptors open.  Accordingly, we write the file once (to see if we)
    # have enough quota space, then rewrite "the real file" after deleting the
    # temporary one.  There's a huge race condition here, but it seems to be the
    # best we can do given the constraints.  Note that we don't race with the
    # shadow as we have a write lock on the file itself.
    tmpfilename = filename + ".tmp"
    with  open(tmpfilename, "w", encoding='utf-8') as output_fd:
        output_fd.write(output)
    os.unlink(tmpfilename)
    with open(filename, "w", encoding='utf-8') as output_fd:
        output_fd.write(output)
    return adjustedJobIds


def getGlob(ad, normal, automatic):
    """ Function used to return the correct list of files to modify when we
        adjust the max retries and the PJ exit codes (automatic splitting has subdags)
    """
    if ad.get('CRAB_SplitAlgo') == 'Automatic':
        return glob.glob(automatic)
    return [normal]


def adjustMaxRetries(adjustJobIds, ad):
    """
    Edit the DAG file adjusting the maximum allowed number of retries to the current
    retry + CRAB_NumAutomJobRetries for the jobs specified in the jobIds argument
    (or for all jobs if jobIds = True). Incrementing the maximum allowed number of
    retries is a necessary condition for a job to be resubmitted.
    """
    printLog(f"Adjusting retries for job ids: {adjustJobIds}")
    if not adjustJobIds:
        return
    if not os.path.exists("RunJobs.dag"):
        return
    ## Get the latest retry count of each DAG node from the node status file.
    retriesDict = {}
    filenames = getGlob(ad, "node_state", "node_state.[1-9]*")
    for fn in filenames:
        with open(fn, 'r', encoding='utf-8') as fd:
            for nodeStatusAd in classad.parseAds(fd):
                if nodeStatusAd['Type'] != "NodeStatus":
                    continue
                node = nodeStatusAd.get('Node', '')
                if (not node.startswith("Job")) or node.endswith("SubJobs"):
                    continue
                jobId = node[3:]
                retriesDict[jobId] = int(nodeStatusAd.get('RetryCount', -1))
    ## Search for the RETRY directives in the DAG file for the job ids passed in the
    ## resubmitJobIds argument and change the maximum retries to the current retry
    ## count + CRAB_NumAutomJobRetries.
    retry_re = re.compile(r'RETRY Job(\d+(?:-\d+)?) (\d+) ')
    adjustAll = (adjustJobIds is True)
    numAutomJobRetries = int(ad.get('CRAB_NumAutomJobRetries', 2))
    filenames = getGlob(ad, "RunJobs.dag", "RunJobs[1-9]*.subdag")
    for fn in filenames:
        output = ""
        with open(fn, 'r', encoding='utf-8') as fd:
            for line in fd.readlines():
                match_retry_re = retry_re.search(line)
                if match_retry_re:
                    jobId = match_retry_re.groups()[0]
                    if adjustAll or (jobId in adjustJobIds):
                        if jobId in retriesDict and retriesDict[jobId] != -1:
                            lastRetry = retriesDict[jobId]
                            ## The 1 is to account for the resubmission itself; then, if the job fails, we
                            ## allow up to numAutomJobRetries automatic retries from DAGMan.
                            maxRetries = lastRetry + (1 + numAutomJobRetries)
                        else:
                            try:
                                maxRetries = int(match_retry_re.groups()[1]) + (1 + numAutomJobRetries)
                            except ValueError:
                                maxRetries = numAutomJobRetries
                        printLog(f"Adjusted maxRetries for {jobId} to {maxRetries}")
                        line = retry_re.sub(r'RETRY Job%s %d ' % (jobId, maxRetries), line)  # pylint: disable=consider-using-f-string
                output += line
        with open(fn, 'w', encoding='utf-8') as fd:
            fd.write(output)


def makeWebDir(ad):
    """
    Need a doc string here.
    """
    if 'AuthTokenId' in ad:
        path = os.path.expanduser(f"/home/grid/{ad['CRAB_UserHN']}/{ad['CRAB_ReqName']}")
    else:
        path = os.path.expanduser(f"~/{ad['CRAB_ReqName']}")
    try:
        ## Create the web directory.
        os.makedirs(path)
        os.makedirs(path + '/AutomaticSplitting')
        ## Copy the sandbox to the web directory.
        shutil.copy2(os.path.join(".", "sandbox.tar.gz"), os.path.join(path, "sandbox.tar.gz"))
        ## Copy the debug folder. It might not be available if an older (<3.3.1607) crabclient is used.
        if os.path.isfile(os.path.join(".", "debug_files.tar.gz")):
            shutil.copy2(os.path.join(".", "debug_files.tar.gz"), os.path.join(path, "debug_files.tar.gz"))

        ## Make all the necessary symbolic links in the web directory.
        sourceLinks = ["debug",
                       "RunJobs.dag", "RunJobs.dag.dagman.out", "RunJobs.dag.nodes.log",
                       "input_files.tar.gz", "run_and_lumis.tar.gz",
                       "input_dataset_lumis.json", "input_dataset_duplicate_lumis.json",
                       "aso_status.json", "error_summary.json", "site.ad.json"
                      ]
        for source in sourceLinks:
            link = source
            os.symlink(os.path.abspath(os.path.join(".", source)), os.path.join(path, link))
        ## Symlinks with a different link name than source name. (I would prefer to keep the source names.)
        os.symlink(os.path.abspath(os.path.join(".", "job_log")), os.path.join(path, "jobs_log.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "node_state")), os.path.join(path, "node_state.txt"))
        os.symlink(os.path.abspath(os.path.join(".", ".job.ad")), os.path.join(path, "job_ad.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "task_process/status_cache.txt")), os.path.join(path, "status_cache"))
        os.symlink(os.path.abspath(os.path.join(".", "task_process/status_cache.pkl")), os.path.join(path, "status_cache.pkl"))
        os.symlink(os.path.abspath(os.path.join(".", "prejob_logs/predag.0.txt")), os.path.join(path, "AutomaticSplitting_Log0.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "prejob_logs/predag.0.txt")), os.path.join(path, "AutomaticSplitting/DagLog0.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "prejob_logs/predag.1.txt")), os.path.join(path, "AutomaticSplitting/DagLog1.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "prejob_logs/predag.2.txt")), os.path.join(path, "AutomaticSplitting/DagLog2.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "InputFiles.tar.gz")), os.path.join(path, "InputFiles.tar.gz"))
        ## Symlinks to ease operator navigation across spool/web directories
        os.symlink(os.path.abspath("."), os.path.join(path, "SPOOL_DIR"))
        os.symlink(path, os.path.abspath(os.path.join(".", "WEB_DIR")))
    except Exception as ex: #pylint: disable=broad-except
        #Should we just catch OSError and IOError? Is that enough?
        printLog(f"Failed to copy/symlink files in the user web directory: {ex}")

    # prepare a startup cache_info file with time info for client to have something useful to print
    # in crab status while waiting for task_process to fill with actual jobs info. Do it in two ways
    # new way: a pickle file for python3 compatibility
    startInfo = {'bootstrapTime': {}}
    startInfo['bootstrapTime']['date'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    startInfo['bootstrapTime']['fromEpoch'] = int(time.time())
    with open(os.path.abspath(os.path.join(".", "task_process/status_cache.pkl")), 'wb') as fp:
        pickle.dump(startInfo, fp)
    # old way: a file with multiple lines and print-like output
    startInfo = "# Task bootstrapped at " + datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC") + "\n"
    startInfo += f"{int(time.time())}\n"  # machines will like seconds from Epoch more
    # prepare fake status_cache info to please current (v3.210127) CRAB Client
    fakeInfo = startInfo + "{"
    fakeInfo += "'DagStatus': {'SubDagStatus': {}, 'Timestamp': 0L, 'NodesTotal': 1L, 'SubDags': {}, 'DagStatus': 1L}"
    fakeInfo += "}\n{}\n"
    with open(os.path.abspath(os.path.join(".", "task_process/status_cache.txt")), 'w', encoding='utf-8') as fd:
        fd.write(fakeInfo)
    printLog("WEB_DIR created, sym links in place and status_cache initialized")

    try:
        storage_rules = htcondor.param['CRAB_StorageRules']
    except KeyError:
        printLog("CRAB_StorageRules param missing in HTCondor config. Can't create webdir URL")
    sinfo = storage_rules.split(",")
    storage_re = re.compile(sinfo[0])
    ad['CRAB_localWebDirURL'] = storage_re.sub(sinfo[1], path)


def uploadWebDir(crabserver, ad):
    """
    Need a doc string here.
    """
    data = {'subresource': 'addwebdir'}
    data['workflow'] = ad['CRAB_ReqName']
    data['webdirurl'] = ad['CRAB_localWebDirURL']

    try:
        printLog(f"Uploading webdir {data['webdirurl']} to the REST")
        crabserver.post(api='task', data=urlencode(data))
        return 0
    except HTTPException as hte:
        printLog(traceback.format_exc())
        printLog(hte.headers)
        printLog(hte.result)
        return 1


def saveProxiedWebdir(crabserver, ad):
    """ The function queries the REST interface to get the proxied webdir and sets
        a classad so that we report this to the dashboard instead of the regular URL.

        The webdir (if exists) is written to a file named 'webdir' so that
        prejobs can read it and report to dashboard. If the proxied URL does not exist
        (i.e.: schedd not at CERN), we report the usual webdir.

        See https://github.com/dmwm/CRABServer/issues/4883
    """
    # Get the proxied webdir from the REST itnerface
    task = ad['CRAB_ReqName']
    webDir_adName = 'CRAB_WebDirURL'
    ad[webDir_adName] = ad['CRAB_localWebDirURL']
    proxied_webDir = getProxiedWebDir(crabserver=crabserver, task=task, logFunction=printLog)
    if proxied_webDir:
        # Use a file to communicate webDir to the prejob
        with open("webdir", "w", encoding='utf-8') as fd:
            fd.write(proxied_webDir)
    else:
        printLog("Cannot get proxied webdir from the server. Maybe the schedd does not have one in the REST configuration?")


def clearAutomaticBlacklist():
    """
    Need a doc string here.
    """
    for filename in glob.glob("task_statistics.*"):
        try:
            os.unlink(filename)
        except Exception as e:  # pylint: disable=broad-except
            printLog(f"ERROR when clearing statistics: {e}")


def setupLog():
    """ Redirect the stdout and the stderr of the script to adjust_out.txt (unless the TEST_DONT_REDIRECT_STDOUT environment variable
        is set)
    """
    newstdout = "adjust_out.txt"
    printLog(f"Redirecting output to {newstdout}")
    logfd = os.open(newstdout, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    if not os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
        os.dup2(logfd, 1)
        os.dup2(logfd, 2)
    os.close(logfd)


def checkTaskInfo(taskDict, ad):
    """
    Function checks that given task is registered in the database with status
    SUBMITTED and with the same clusterId and schedd name in the database as in
    the condor ads where it is currently running.
    In case above condition is not met, script immediately terminates

    :param taskDict: task info return from REST.
    :type taskDict: dict
    :param ad: kv classad
    :type ad: HTC classad.ClassAd (which has the semantics of a dictionarty)
    """

    clusterIdOnSchedd = ad['ClusterId']

    taskStatusOnDB = getColumn(taskDict, 'tm_task_status')
    clusteridOnDB = getColumn(taskDict, 'clusterid')
    scheddOnDB = getColumn(taskDict, 'tm_schedd')

    scheddName = os.environ['schedd_name']

    msg = f"Task status on DB: {taskStatusOnDB}, clusterID on DB: {clusterIdOnSchedd}, schedd name on DB: {scheddOnDB}; "
    msg += f"\nclusterID on condor ads: {clusterIdOnSchedd}, schedd name on condor ads: {scheddName} "
    printLog(msg)

    if not (taskStatusOnDB == 'SUBMITTED' and scheddOnDB == scheddName and clusteridOnDB == str(clusterIdOnSchedd)):
        printLog('Exiting AdjustSites because this dagman does not match task information in TASKS DB')
        sys.exit(3)

def getSandbox(taskDict, crabserver, logger):
    """
    Getting user sandbox (sandbox.tar.gz) from S3. It will not redownload
    sandbox if file exists.
    This function contains side effect where sandbox.tar.gz(_tmp) are created in
    current directory.
    :param taskDict: task info return from REST.
    :type taskDict: dict
    :param crabserver: CRABRest object to talk with RESTCache API
    :type crabserver: RESTInteractions.CRABRest
    :param logger: downloadFromS3 requires a logger !
    :type logger: logging.logger object
    """
    sandboxTarBall = 'sandbox.tar.gz'
    sandboxTarBallTmp = sandboxTarBall + '_tmp'
    if os.path.exists(sandboxTarBall):
        printLog('sandbox.tar.gz already exist. Do nothing.')
        return

    # get info
    username = getColumn(taskDict, 'tm_username')
    sandboxName = getColumn(taskDict, 'tm_user_sandbox')

    # download
    try:
        downloadFromS3(crabserver=crabserver, objecttype='sandbox', username=username,
                       tarballname=sandboxName, filepath=sandboxTarBallTmp, logger=logger)
        shutil.move(sandboxTarBallTmp, sandboxTarBall)
    except Exception as ex:  # pylint: disable=broad-except
        logger.exception("The CRAB server backend could not download the input sandbox with your code " + \
                         "from S3.\nThis could be a temporary glitch; please try to submit a new task later " + \
                         "(resubmit will not work) and contact the experts if the error persists.\nError reason: %s", str(ex))
        sys.exit(4)

def getDebugFiles(taskDict, crabserver, logger):
    """
    Ops mon  (crabserver/ui) needs access to files from the debug_files.tar.gz
    Retrieve and expand debug_files.tar.gz from S3 in here for http access.
    It will not redownload tarball if file exists.
    This function contains side effect: debug_files.tar.gz(_tmp) and debug directory
    are created in current directory.
    :param taskDict: task info return from REST.
    :type taskDict: dict
    :param crabserver: CRABRest object to talk with RESTCache API
    :type crabserver: RESTInteractions.CRABRest
    :param logger: downloadFromS3 requires a logger !
    :type logger: logging.logger object
    """
    debugTarball = 'debug_files.tar.gz'
    if os.path.exists(debugTarball):
        printLog('sandbox.tar.gz already exist. Do nothing.')
        return

    # get info
    username = getColumn(taskDict, 'tm_username')
    sandboxName = getColumn(taskDict, 'tm_debug_files')

    # download
    debugTarballTmp = debugTarball + '_tmp'
    try:
        downloadFromS3(crabserver=crabserver, objecttype='sandbox', username=username,
                       tarballname=sandboxName, filepath=debugTarballTmp, logger=logger)
        shutil.move(debugTarballTmp, debugTarball)
    except Exception as ex:  # pylint: disable=broad-except
        logger.exception("CRAB server backend could not download the tarball with debug files " + \
                         "from S3, ops monitor will not work. Exception:\n %s", str(ex))
        return

    # extract files
    try:
        with tarfile.open(name=debugTarball, mode='r') as debugTar:
            debugTar.extractall()
    except Exception as ex:  # pylint: disable=broad-except
        logger.exception("CRAB server backend could not expand the tarball with debug files, " + \
                         "ops monitor will not work. Exception:\n %s", str(ex))
        return

    # Change permissions of extracted files to allow Ops mon to read them.
    for _, _, filenames in os.walk('debug'):
        for f in filenames:
            os.chmod('debug/' + f, 0o644)


def main():
    """
    Need a doc string here.
    """
    setupLog()

    if '_CONDOR_JOB_AD' not in os.environ or not os.path.exists(os.environ["_CONDOR_JOB_AD"]):
        printLog("Exiting AdjustSites since _CONDOR_JOB_AD is not in the environment or does not exist")
        sys.exit(0)

    printLog(f"Starting AdjustSites with _CONDOR_JOB_AD={os.environ['_CONDOR_JOB_AD']}")

    with open(os.environ['_CONDOR_JOB_AD'], 'r', encoding='utf-8') as fd:
        ad = classad.parseOne(fd)
    printLog(f"Parsed ad:\n{ad}\n")

    # instantiate a server object to talk with crabserver
    host = ad['CRAB_RestHost']
    dbInstance = ad['CRAB_DbInstance']
    cert = ad['X509UserProxy']
    crabserver = CRABRest(host, localcert=cert, localkey=cert, retry=3, userAgent='CRABSchedd')
    crabserver.setDbInstance(dbInstance)

    printLog("Sleeping 60 seconds to give TW time to update taskDB")
    time.sleep(60)  # give TW time to update taskDB #8411

    # get task info
    task = ad['CRAB_ReqName']
    data = {'subresource': 'search', 'workflow': task}
    try:
        dictresult, _, _ = crabserver.get(api='task', data=data)
    except HTTPException as hte:
        printLog(traceback.format_exc())
        printLog(hte.headers)
        printLog(hte.result)
        sys.exit(2)

    # check task status
    checkTaskInfo(taskDict=dictresult, ad=ad)
    # init logger required by downloadFromS3
    logger = setupStreamLogger()
    # get sandboxes
    getSandbox(taskDict=dictresult, crabserver=crabserver, logger=logger)
    getDebugFiles(taskDict=dictresult, crabserver=crabserver, logger=logger)

    # is this the first time this script runs for this task ? (it runs at each resubmit as well !)
    if not os.path.exists('WEB_DIR'):
        makeWebDir(ad)
        printLog("Webdir has been set up. Uploading the webdir URL to the REST")

        retries = 0
        exitCode = 1
        maxRetries = 3
        while retries < maxRetries and exitCode != 0:
            exitCode = uploadWebDir(crabserver, ad)
            if exitCode != 0:
                time.sleep(retries * 20)
            retries += 1
        if exitCode != 0:
            printLog(f"Exiting AdjustSites because the webdir upload failed {maxRetries} times.")
            sys.exit(1)
        printLog(f"Webdir URL has been uploaded, exit code is {exitCode}. Setting the classad for the proxied webdir")

        saveProxiedWebdir(crabserver, ad)
        printLog("Proxied webdir saved")

    printLog("Clearing the automatic blacklist and handling RunJobs.dag.nodes.log for resubmissions")

    clearAutomaticBlacklist()

    resubmitJobIds = []
    if 'CRAB_ResubmitList' in ad:
        resubmitJobIds = ad['CRAB_ResubmitList']
        try:
            resubmitJobIds = set(resubmitJobIds)
            resubmitJobIds = [str(i) for i in resubmitJobIds]
        except TypeError:
            resubmitJobIds = True

    # Hold and release processing and tail DAGs here so that modifications
    # to the submission and log files will be picked up.
    schedd = htcondor.Schedd()
    taskNameAd = classad.quote(ad.get("CRAB_ReqName"))
    tailconst = f"(CRAB_DAGType =?= \"TAIL\" && CRAB_ReqName =?= {taskNameAd})"
    tailconst += " || (TaskType =?= \"TAIL\" && CRAB_ReqName =?= {taskNameAd})"
    if resubmitJobIds and ad.get('CRAB_SplitAlgo') == 'Automatic':
        printLog("Holding processing and tail DAGs")
        schedd.edit(tailconst, "HoldKillSig", 'SIGKILL')
        schedd.act(htcondor.JobAction.Hold, tailconst)

    if resubmitJobIds:
        adjustedJobIds = []
        filenames = getGlob(ad, "RunJobs.dag.nodes.log", "RunJobs[1-9]*.subdag.nodes.log")
        for fn in filenames:
            if hasattr(htcondor, 'lock'):
                # While dagman is not running at this point, the schedd may be writing events to this
                # file; hence, we only edit the file while holding an appropriate lock.
                # Note this lock method didn't exist until 8.1.6; prior to this, we simply
                # run dangerously.
                with htcondor.lock(open(fn, 'a', encoding='utf-8'), htcondor.LockType.WriteLock):
                    adjustedJobIds.extend(adjustPostScriptExitStatus(resubmitJobIds, fn))
            else:
                adjustedJobIds.extend(adjustPostScriptExitStatus(resubmitJobIds, fn))
        ## Adjust the maximum allowed number of retries only for the job ids for which
        ## the POST script exit status was adjusted. Why only for these job ids and not
        ## for all job ids in resubmitJobIds? Because if resubmitJobIds = True, which as
        ## a general rule means "all failed job ids", we don't have a way to know if a
        ## job is in failed status or not just from the RunJobs.dag file, while job ids
        ## in adjustedJobIds correspond only to failed jobs.
        adjustMaxRetries(adjustedJobIds, ad)

    if resubmitJobIds and ad.get('CRAB_SplitAlgo') == 'Automatic':
        printLog("Releasing processing and tail DAGs")
        schedd.edit(tailconst, "HoldKillSig", 'SIGUSR1')
        schedd.act(htcondor.JobAction.Release, tailconst)

    printLog("Exiting AdjustSite")

if __name__ == '__main__':
    main()
