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


if '_CONDOR_JOB_AD' not in os.environ or not os.path.exists(os.environ["_CONDOR_JOB_AD"]):
    sys.exit(0)


newstdout = "adjust_out.txt"
logfd = os.open(newstdout, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0644)
if not os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
    os.dup2(logfd, 1)
    os.dup2(logfd, 2)
os.close(logfd)


def printLog(msg):
    print("%s: %s" % (datetime.utcnow(), msg))


def adjustPostScriptExitStatus(resubmitJobIds):
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
    resubmitAllFailed = (resubmitJobIds == True)
    terminator_re = re.compile(r"^\.\.\.$")
    event_re = re.compile(r"016 \(-?\d+\.\d+\.\d+\) \d+/\d+ \d+:\d+:\d+ POST Script terminated.")
    if resubmitAllFailed:
        retvalue_re = re.compile(r"Normal termination \(return value 2\)")
    else:
        retvalue_re = re.compile(r"Normal termination \(return value [0|2]\)")
    node_re = re.compile(r"DAG Node: Job(\d+)")
    ra_buffer = []
    alt = None
    output = ''
    adjustedJobIds = []
    for line in open("RunJobs.dag.nodes.log").readlines():
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
            printLog("%s %s %s %s" % (line, m, m.groups(), resubmitJobIds))
            if m and (resubmitAllFailed or (m.groups()[0] in resubmitJobIds)):
                printLog("%s %s" % (m.groups()[0], resubmitJobIds))
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
    output_fd = open("RunJobs.dag.nodes.log.tmp", "w")
    output_fd.write(output)
    output_fd.close()
    os.unlink("RunJobs.dag.nodes.log.tmp")
    output_fd = open("RunJobs.dag.nodes.log", "w")
    output_fd.write(output)
    output_fd.close()
    return adjustedJobIds


def adjustMaxRetries(adjustJobIds, ad):
    """
    Edit the DAG file adjusting the maximum allowed number of retries to the current
    retry + CRAB_NumAutomJobRetries for the jobs specified in the jobIds argument
    (or for all jobs if jobIds = True). Incrementing the maximum allowed number of
    retries is a necessary condition for a job to be resubmitted.
    """
    if not adjustJobIds:
        return
    if not os.path.exists("RunJobs.dag"):
        return
    ## Get the latest retry count of each DAG node from the node status file.
    retriesDict = {}
    if os.path.exists("node_state"):
        with open("node_state", 'r') as fd:
            for nodeStatusAd in classad.parseAds(fd):
                if nodeStatusAd['Type'] != "NodeStatus":
                    continue
                node = nodeStatusAd.get('Node', '')
                if not node.startswith("Job"):
                    continue
                jobId = node[3:]
                retriesDict[jobId] = int(nodeStatusAd.get('RetryCount', -1))
    ## Search for the RETRY directives in the DAG file for the job ids passed in the
    ## resubmitJobIds argument and change the maximum retries to the current retry
    ## count + CRAB_NumAutomJobRetries.
    retry_re = re.compile(r'RETRY Job([0-9]+) ([0-9]+) ')
    output = ""
    adjustAll = (adjustJobIds == True)
    numAutomJobRetries = int(ad.get('CRAB_NumAutomJobRetries', 2))
    with open("RunJobs.dag", 'r') as fd:
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
                    line = retry_re.sub(r'RETRY Job%s %d ' % (jobId, maxRetries), line)
            output += line
    with open("RunJobs.dag", 'w') as fd:
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
        ## Make all the necessary symbolic links in the web directory.
        sourceLinks = ["debug",
                       "RunJobs.dag", "RunJobs.dag.dagman.out", "RunJobs.dag.nodes.log",
                       "input_files.tar.gz", "run_and_lumis.tar.gz",
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
    except Exception as ex:
        printLog("Failed to copy/symlink files in the user web directory: %s" % str(ex))
    try:
        storage_rules = htcondor.param['CRAB_StorageRules']
    except:
        storage_rules = "^/home/remoteGlidein,http://submit-5.t2.ucsd.edu/CSstoragePath"
    sinfo = storage_rules.split(",")
    storage_re = re.compile(sinfo[0])
    val = storage_re.sub(sinfo[1], path)
    ad['CRAB_UserWebDir'] = val
    #This condor_edit is required because in the REST interface we look for the webdir if the DB upload failed (or in general if we use the "old logic")
    #See https://github.com/dmwm/CRABServer/blob/3.3.1507.rc8/src/python/CRABInterface/HTCondorDataWorkflow.py#L398
    dagJobId = '%d.%d' % (ad['ClusterId'], ad['ProcId'])
    try:
        htcondor.Schedd().edit([dagJobId], 'CRAB_UserWebDir', ad.lookup('CRAB_UserWebDir'))
    except RuntimeError as reerror:
        printLog(str(reerror))


def updateWebDir(ad):
    """
    Need a doc string here.
    """
    data = {'subresource' : 'addwebdir'}
    host = ad['CRAB_RestHost']
    uri = ad['CRAB_RestURInoAPI'] + '/task'
    data['workflow'] = ad['CRAB_ReqName']
    data['webdirurl'] = ad['CRAB_UserWebDir']
    cert = ad['X509UserProxy']
    try:
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
    res = getProxiedWebDir(task, host, uri, cert, logFunction=printLog)

    # We need to use a file to communicate this to the prejob. I tried things like:
    #    htcondor.Schedd().edit([dagJobId], 'CRAB_UserWebDirPrx', ad.lookup('CRAB_UserWebDir'))
    # but the prejob read the classads from the file, not querying the schedd.and we can't update
    # the classad file since it's owned by user condor
    if res:
        with open("proxied_webdir", "w") as fd:
            fd.write(res)
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


def main():
    """
    Need a doc string here.
    """
    printLog("Starting AdjustSites")

    with open(os.environ['_CONDOR_JOB_AD']) as fd:
        ad = classad.parseOld(fd)
    printLog("Parsed ad: %s" % ad)

    makeWebDir(ad)

    printLog("Webdir has been set up. Uploading the webdir URL to the REST")

    retries = 0
    exitCode = 1
    while retries < 3 and exitCode != 0:
        exitCode = updateWebDir(ad)
        if exitCode != 0:
            time.sleep(retries * 20)
        retries += 1

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
    if resubmitJobIds:
        adjustedJobIds = []
        if hasattr(htcondor, 'lock'):
            # While dagman is not running at this point, the schedd may be writing events to this
            # file; hence, we only edit the file while holding an appropriate lock.
            # Note this lock method didn't exist until 8.1.6; prior to this, we simply
            # run dangerously.
            with htcondor.lock(open("RunJobs.dag.nodes.log", 'a'), htcondor.LockType.WriteLock):
                adjustedJobIds = adjustPostScriptExitStatus(resubmitJobIds)
        else:
            adjustedJobIds = adjustPostScriptExitStatus(resubmitJobIds)
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

    printLog("Exiting AdjustSite")

if __name__ == '__main__':
    main()

