"""
This contains some utility methods to share between the server and the client, or by the server components themself
"""

from __future__ import print_function

import os
import re
import datetime
import traceback
import subprocess
from httplib import HTTPException
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

FEEDBACKMAIL = 'hn-cms-computing-tools@cern.ch'


## These are all possible statuses of a task in the TaskDB.
## 'FAILED' is a status used to mark a task for which a failure occurred when
## doing a submission, resubmission or kill. It is not a status a task can get
## due to having (some) failed jobs.
TASKDBSTATUSES = ['NEW', 'HOLDING', 'QUEUED', 'UPLOADED', 'SUBMITTED', 'KILL', 'KILLED', 'KILLFAILED', 'RESUBMIT', 'RESUBMITTED', 'FAILED'] # would like to add RESUBMITFAILED

## These are all possible statuses of a task as returned by the `status' API.
## 'COMPLETED' is a status the `status' makes up combining information from the
## TaskDB and the DAG status.
TASKSTATUSES = TASKDBSTATUSES + ['COMPLETED']


def checkOutLFN(lfn, username):
    if lfn.startswith('/store/user/'):
        if lfn.split('/')[3] != username:
            return False
    elif lfn.startswith('/store/group/') or lfn.startswith('/store/local/'):
        if lfn.split('/')[3] == '':
            return False
    else:
        return False
    return True


def getProxiedWebDir(task, host, uri, cert, logFunction = print):
    """ The function smply queries the REST interface specified to get the proxied webdir to use
        for the task. Returns None in case the API could not find the url (either an error or the schedd
        is not configured)
    """
    #This import is here because SeverUtilities is also used on the worker nodes,
    #and I want to avoid the dependency to pycurl right now. We should actually add it one day
    #so that other code in cmscp that uses Requests.py from WMCore can be migrated to RESTInteractions
    from RESTInteractions import HTTPRequests
    data = { 'subresource' : 'webdirprx',
         'workflow' : task,
       }

    res = None
    try:
        server = HTTPRequests(host, cert, cert, retry = 2)
        dictresult, _, _ = server.get(uri, data = data) #the second and third parameters are deprecated
        res = dictresult['result'][0]
    except HTTPException as hte:
        logFunction(traceback.format_exc())
        logFunction(hte.headers)
        logFunction(hte.result)

    return res


#setDashboardLogs function is shared between the postjob and the job wrapper. Sharing it here
def setDashboardLogs(params, webdir, jobid, retry):
    log_files = [("job_out", "txt"), ("postjob", "txt")]
    for i, log_file in enumerate(log_files):
        log_file_basename, log_file_extension = log_file
        log_file_name = "%s/%s.%d.%d.%s" % (webdir, \
                                            log_file_basename, jobid, \
                                            retry, \
                                            log_file_extension)
        log_files[i] = log_file_name
    params['StatusLogFile'] = ",".join(log_files)


def getWebdirForDb(reqname, storage_rules):
    path = os.path.expanduser("~/%s" % reqname)
    sinfo = storage_rules.split(",")
    storage_re = re.compile(sinfo[0])
    val = storage_re.sub(sinfo[1], path)

    return val

def cmd_exist(cmd):
    """
    Check if linux command exist
    """
    try:
        null = open("/dev/null", "w")
        p = subprocess.Popen("/usr/bin/which %s" % cmd, stdout=null, stderr=null, shell=True)
        p.communicate()
        null.close()
        if p.returncode == 0:
            return True
    except OSError:
        return False
    return False

def getCheckWriteCommand(proxy, logger):
    """
    Return prepared gfal or lcg commands for checkwrite
    """
    cpCmd = None
    rmCmd = None
    append = ""
    if cmd_exist("gfal-copy") and cmd_exist("gfal-rm"):
        logger.info("Will use GFAL2 commands for checking write permissions")
        cpCmd = "env -i X509_USER_PROXY=%s gfal-copy -v -p -t 180 " % os.path.abspath(proxy)
        rmCmd = "env -i X509_USER_PROXY=%s gfal-rm -v -t 180 " % os.path.abspath(proxy)
        append = "file://"
    elif cmd_exist("lcg-cp") and cmd_exist("lcg-del"):
        logger.info("Will use LCG commands for checking write permissions")
        cpCmd = "lcg-cp -v -b -D srmv2 --connect-timeout 180 "
        rmCmd = "lcg-del --connect-timeout 180 -b -l -D srmv2 "
    return cpCmd, rmCmd, append

def createDummyFile(filename, logger):
    """
    Create dummy file for checking write permissions
    """
    abspath = os.path.abspath(filename)
    try:
        with open(abspath, 'w') as fd:
            fd.write('This is a dummy file created by the crab checkwrite command on %s' % str(datetime.datetime.now().strftime('%d/%m/%Y at %H:%M:%S')))
    except IOError as er:
        logger.info('Error: Failed to create file %s localy.' % filename)
        raise

def removeDummyFile(filename, logger):
    """
    Remove created dummy file
    """
    abspath = os.path.abspath(filename)
    try:
        os.remove(abspath)
    except:
        logger.info('Warning: Failed to delete file %s' % filename)

def getPFN(proxy, lfnsaddprefix, filename, sitename, logger):

        phedex = PhEDEx({"cert": proxy, "key": proxy, "logger": logger})
        lfnsadd = os.path.join(lfnsaddprefix, filename)
        try:
            pfndict = phedex.getPFN(nodes = [sitename], lfns = [lfnsadd])
            pfn = pfndict[(sitename, lfnsadd)]
            if not pfn:
                logger.info('Error: Failed to get PFN from the site. Please check the site status')
                return False
        except HTTPException as errormsg:
            logger.info('Error: Failed to contact PhEDEx or wrong PhEDEx node name is used')
            logger.info('Result: %s\nStatus :%s\nURL :%s' % (errormsg.result, errormsg.status, errormsg.url))
            raise HTTPException, errormsg
        return pfn

def executeCommand(command):
    """
    Execute passed bash command. There is no check for command success or failure. Who`s calling
    this command, has to check exitcode, out and err
    """
    process = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    out, err = process.communicate()
    exitcode = process.returncode
    return out, err, exitcode

def isFailurePermanent(reason, gridJob=False):
    """
    Method that decides whether a failure reason should be considered as a
    permanent failure and submit task or not.
    # TODO reuse it in PostJob
    """
    checkQuota = " Please check if you have access to write to destination site and your quota does not exceeded."
    refuseToSubmit = " CRAB3 refuses to send jobs to scheduler because of failure to transfer files to destination site."
    if gridJob:
        refuseToSubmit = ""
    permanentFailureReasons = {
                               ".*cancelled aso transfer after timeout.*" : "Transfer canceled due to timeout.",
                               ".*permission denied.*" : "Permission denied.",
                               ".*disk quota exceeded.*" : "Disk quota exceeded.",
                               ".*operation not permitted*" : "Operation not allowed.",
                               ".*mkdir\(\) fail.*" : "Can`t create directory.",
                               ".*open/create error.*" : "Can`t create directory/file on destination site.",
                               ".*mkdir\: cannot create directory.*" : "Can`t create directory.",
                               ".*does not have enough space.*" : "User quota exceeded.",
                               ".*reports could not open connection to.*" : "Storage element is not accessible.",
                               ".*530-login incorrect.*" : "Permission denied to write to destination site."
                              }
    reason = str(reason).lower()
    for failureReason in permanentFailureReasons:
        if re.match(failureReason, reason):
            reason = permanentFailureReasons[failureReason] + checkQuota + refuseToSubmit
            return True, reason
    return False, ""
