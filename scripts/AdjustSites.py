""" This script is called by dag_bootstrap_startup.sh when the job is (re)submitted and:
    - It creates the webdir if necessary
    - It updates both the webdir ant the proxied version of it on the REST task db
    - For resubmission: adjust the exit codes of the PJ in the RunJobs.dag.nodes.log files and
      the max retries in the RunJobs.dag files
"""

from __future__ import print_function
import os
import re
import sys
import time
import glob
import shutil
import urllib
import classad
import htcondor
import traceback
from datetime import datetime
from httplib import HTTPException

from RESTInteractions import HTTPRequests
from ServerUtilities import getProxiedWebDir


def printLog(msg):
    """ Utility function to print the timestamp in the log. Can be replaced
        with anything (e.g.: logging.info if we decided to set up a logger here)
    """
    print("%s: %s" % (datetime.utcnow(), msg))


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
    printLog("Looking for resubmitJobIds {0} in {1}".format(resubmitJobIds, filename))
    resubmitAllFailed = (resubmitJobIds is True)
    terminator_re = re.compile(r"^\.\.\.$")
    event_re = re.compile(r"016 \(-?\d+\.\d+\.\d+\) \d+/\d+ \d+:\d+:\d+ POST Script terminated.")
    if resubmitAllFailed:
        retvalue_re = re.compile(r"Normal termination \(return value 2\)")
    else:
        retvalue_re = re.compile(r"Normal termination \(return value [0|2]\)")
    node_re = re.compile(r"DAG Node: Job(\d+(-\d+)?)")
    ra_buffer = []
    alt = None
    output = ''
    adjustedJobIds = []
    for line in open(filename).readlines():
        if len(ra_buffer) == 0:
            m = terminator_re.search(line)
            if m:
                ra_buffer.append(line)
            else:
                output += line
        elif len(ra_buffer) == 1:
            m = event_re.search(line)
            if m:
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
            printLog("Matching the line '{0}' to groups: {1}".format(line.strip(), m.groups()))
            if m and (resubmitAllFailed or (m.groups()[0] in resubmitJobIds)):
                printLog("Successful match: {0}, adjusting status and appending to adjustedJobIds".format(m.groups()[0]))
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
    output_fd = open(tmpfilename, "w")
    output_fd.write(output)
    output_fd.close()
    os.unlink(tmpfilename)
    output_fd = open(filename, "w")
    output_fd.write(output)
    output_fd.close()
    return adjustedJobIds


def getGlob(ad, normal, automatic):
    """ Function used to return the correct list of files to modify when we
        adjust the max retries and the PJ exit codes (automatic splitting has subdags)
    """
    if ad.get('CRAB_SplitAlgo') == 'Automatic':
        return glob.glob(automatic)
    else:
        return [normal]


def adjustMaxRetries(adjustJobIds, ad):
    """
    Edit the DAG file adjusting the maximum allowed number of retries to the current
    retry + CRAB_NumAutomJobRetries for the jobs specified in the jobIds argument
    (or for all jobs if jobIds = True). Incrementing the maximum allowed number of
    retries is a necessary condition for a job to be resubmitted.
    """
    printLog("Adjusting retries for job ids: {0}".format(adjustJobIds))
    if not adjustJobIds:
        return
    if not os.path.exists("RunJobs.dag"):
        return
    ## Get the latest retry count of each DAG node from the node status file.
    retriesDict = {}
    filenames = getGlob(ad, "node_state", "node_state.[1-9]*")
    for fn in filenames:
        with open(fn, 'r') as fd:
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
        with open(fn, 'r') as fd:
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
                        printLog("Adjusted maxRetries for {0} to {1}".format(jobId, maxRetries))
                        line = retry_re.sub(r'RETRY Job%s %d ' % (jobId, maxRetries), line)
                output += line
        with open(fn, 'w') as fd:
            fd.write(output)


def makeWebDir(ad):
    """
    Need a doc string here.
    """
    path = os.path.expanduser("~/%s" % ad['CRAB_ReqName'])
    try:
        ## Create the web directory.
        os.makedirs(path)
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
                       "aso_status.json", "error_summary.json",
                      ]
        for source in sourceLinks:
            link = source
            os.symlink(os.path.abspath(os.path.join(".", source)), os.path.join(path, link))
        ## Symlinks with a different link name than source name. (I would prefer to keep the source names.)
        os.symlink(os.path.abspath(os.path.join(".", "job_log")), os.path.join(path, "jobs_log.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "node_state")), os.path.join(path, "node_state.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "site.ad")), os.path.join(path, "site_ad.txt"))
        os.symlink(os.path.abspath(os.path.join(".", ".job.ad")), os.path.join(path, "job_ad.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "task_process/status_cache.txt")), os.path.join(path, "status_cache"))
        os.symlink(os.path.abspath(os.path.join(".", "prejob_logs/predag.0.txt")), os.path.join(path, "AutomaticSplitting_Log0.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "InputFiles.tar.gz")), os.path.join(path, "InputFiles.tar.gz"))
        ## Symlinks to ease operator navigation across spool/web directories
        os.symlink(os.path.abspath("."), os.path.join(path, "SPOOL_DIR"))
        os.symlink(path, os.path.abspath(os.path.join(".", "WEB_DIR")))
    except Exception as ex: #pylint: disable=broad-except
        #Should we just catch OSError and IOError? Is that enough?
        printLog("Failed to copy/symlink files in the user web directory: %s" % str(ex))

    try:
        storage_rules = htcondor.param['CRAB_StorageRules']
    except KeyError:
        storage_rules = "^/home/remoteGlidein,http://submit-5.t2.ucsd.edu/CSstoragePath"
    sinfo = storage_rules.split(",")
    storage_re = re.compile(sinfo[0])
    ad['CRAB_localWebDirURL'] = storage_re.sub(sinfo[1], path)


def updateWebDir(ad):
    """
    Need a doc string here.
    """
    data = {'subresource': 'addwebdir'}
    host = ad['CRAB_RestHost']
    uri = ad['CRAB_RestURInoAPI'] + '/task'
    data['workflow'] = ad['CRAB_ReqName']
    data['webdirurl'] = ad['CRAB_localWebDirURL']
    cert = ad['X509UserProxy']

    try:
        printLog("Uploading webdir %s to %s" % (data['webdirurl'], host))
        server = HTTPRequests(host, cert, cert)
        server.post(uri, data=urllib.urlencode(data))
        return 0
    except HTTPException as hte:
        printLog(traceback.format_exc())
        printLog(hte.headers)
        printLog(hte.result)
        return 1


def saveProxiedWebdir(ad):
    """ The function queries the REST interface to get the proxied webdir and sets
        a classad so that we report this to the dashboard instead of the regular URL

        The proxied_url (if exist) is written to a file named proxied_webdir so that
        prejobs can read it and report to dashboard. If the url does not exist
        (i.e.: schedd not at CERN), the file is not written and we report the usual
        webdir

        See https://github.com/dmwm/CRABServer/issues/4883
    """
    # Get the proxied webdir from the REST itnerface
    task = ad['CRAB_ReqName']
    host = ad['CRAB_RestHost']
    uri = ad['CRAB_RestURInoAPI'] + '/task'
    cert = ad['X509UserProxy']
    webDir_adName = 'CRAB_WebDirURL'
    ad[webDir_adName] = ad['CRAB_localWebDirURL']
    proxied_webDir = getProxiedWebDir(task, host, uri, cert, logFunction=printLog)
    if proxied_webDir: # Prefer the proxied webDir to the non-proxied one
        ad[webDir_adName] = str(proxied_webDir)

    if ad[webDir_adName]:
        # This condor_edit is required because in the REST interface we look for the webdir if the DB upload failed (or in general if we use the "old logic")
        # See https://github.com/dmwm/CRABServer/blob/3.3.1507.rc8/src/python/CRABInterface/HTCondorDataWorkflow.py#L398
        dagJobId = '%d.%d' % (ad['ClusterId'], ad['ProcId'])
        try:
            htcondor.Schedd().edit([dagJobId], webDir_adName, '{0}'.format(ad.lookup(webDir_adName)))
        except RuntimeError as reerror:
            printLog(str(reerror))
    else:
        printLog("Cannot get proxied webdir from the server. Maybe the schedd does not have one in the REST configuration?")
        return 1

    return 0

def clearAutomaticBlacklist():
    """
    Need a doc string here.
    """
    for filename in glob.glob("task_statistics.*"):
        try:
            os.unlink(filename)
        except Exception as e:
            printLog("ERROR when clearing statistics: %s" % str(e))


def setupLog():
    """ Redirect the stdout and the stderr of the script to adjust_out.txt (unless the TEST_DONT_REDIRECT_STDOUT environment variable
        is set)
    """
    newstdout = "adjust_out.txt"
    printLog("Redirecting output to %s" % newstdout)
    logfd = os.open(newstdout, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    if not os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
        os.dup2(logfd, 1)
        os.dup2(logfd, 2)
    os.close(logfd)


def main():
    """
    Need a doc string here.
    """
    setupLog()

    if '_CONDOR_JOB_AD' not in os.environ or not os.path.exists(os.environ["_CONDOR_JOB_AD"]):
        printLog("Exiting AdjustSites since _CONDOR_JOB_AD is not in the environment or does not exist")
        sys.exit(0)

    printLog("Starting AdjustSites with _CONDOR_JOB_AD=%s" % os.environ['_CONDOR_JOB_AD'])

    with open(os.environ['_CONDOR_JOB_AD']) as fd:
        ad = classad.parseOld(fd)
    printLog("Parsed ad: %s" % ad)

    makeWebDir(ad)

    printLog("Webdir has been set up. Uploading the webdir URL to the REST")

    retries = 0
    exitCode = 1
    maxRetries = 3
    while retries < maxRetries and exitCode != 0:
        exitCode = updateWebDir(ad)
        if exitCode != 0:
            time.sleep(retries * 20)
        retries += 1

    if exitCode != 0:
        printLog("Exiting AdjustSites because the webdir upload failed %d times." % maxRetries)
        sys.exit(1)

    printLog("Webdir URL has been uploaded, exit code is %s. Setting the classad for the proxied webdir" % exitCode)

    saveProxiedWebdir(ad)

    printLog("Proxied webdir saved. Clearing the automatic blacklist and handling RunJobs.dag.nodes.log for resubmissions")

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
    tailconst = "TaskType =?= \"TAIL\" && CRAB_ReqName =?= %s" % classad.quote(ad.get("CRAB_ReqName"))
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
                with htcondor.lock(open(fn, 'a'), htcondor.LockType.WriteLock):
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

    if 'CRAB_SiteAdUpdate' in ad:
        newSiteAd = ad['CRAB_SiteAdUpdate']
        with open("site.ad") as fd:
            siteAd = classad.parse(fd)
        siteAd.update(newSiteAd)
        with open("site.ad", "w") as fd:
            fd.write(str(siteAd))

    if resubmitJobIds and ad.get('CRAB_SplitAlgo') == 'Automatic':
        printLog("Releasing processing and tail DAGs")
        schedd.edit(tailconst, "HoldKillSig", 'SIGUSR1')
        schedd.act(htcondor.JobAction.Release, tailconst)

    printLog("Exiting AdjustSite")

if __name__ == '__main__':
    main()

