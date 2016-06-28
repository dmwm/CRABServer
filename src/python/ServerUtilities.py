"""
This contains some utility methods to share between the server and the client, or by the server components themself
"""

from __future__ import print_function
from __future__ import absolute_import

import os
import re
import time
import calendar
import datetime
import traceback
import subprocess
from httplib import HTTPException
from WMCore.WMExceptions import STAGEOUT_ERRORS

BOOTSTRAP_CFGFILE_DUMP = 'PSetDump.py'
FEEDBACKMAIL = 'hn-cms-computing-tools@cern.ch'

# Fatal error limits for job resource usage
# Defaults are used if unable to load from .job.ad
# Otherwise it uses these values.
MAX_WALLTIME = 21*60*60 + 30*60
MAX_MEMORY = 2*1024
MAX_DISK_SPACE = 20000000 # Disk usage is not used from .job.ad as CRAB3 is not seeting it. 20GB is max.

## Parameter used to set the LeaveJobInQueue and the PeriodicRemoveclassads.
## It's also used during resubmissions since we don't allow a resubmission during the last week
## Before changing this value keep in mind that old running DAGs have the old value in the CRAB_TaskSubmitTime
## classad expression but DagmanResubmitter uses this value to calculate if a resubmission is possible
TASKLIFETIME = 30*24*60*60
## Number of days where the resubmission is not possible if the task is expiring
NUM_DAYS_FOR_RESUBMITDRAIN = 7

## These are all possible statuses of a task in the TaskDB.
TASKDBSTATUSES_TMP = ['NEW', 'HOLDING', 'QUEUED']
TASKDBSTATUSES_FINAL = ['UPLOADED', 'SUBMITTED', 'SUBMITFAILED', 'KILLED', 'KILLFAILED', 'RESUBMITFAILED', 'FAILED']
TASKDBSTATUSES = TASKDBSTATUSES_TMP + TASKDBSTATUSES_FINAL

## These are all possible statuses of a task as returned by the `status' API.
TASKSTATUSES = TASKDBSTATUSES + ['COMPLETED', 'UNKNOWN', 'InTransition']


def USER_SANDBOX_EXCLUSIONS(tarmembers):
    """ The function is used by both the client and the crabcache to get a list of files to exclude during the
        calculation of the checksum of the user input sandbox.

        In particular the client tries to put the process object obtained with dumpPython in the sandbox, so that
        this can be used to calculate the checksum. If we used the process object saved with pickle.dump we would
        run into a problem since the same process objects have different dumps, see:
        https://github.com/dmwm/CRABServer/issues/4948#issuecomment-132984687
    """
    if BOOTSTRAP_CFGFILE_DUMP in map(lambda x: x.name, tarmembers):
        #exclude the pickle pset if the dumpPython PSet is there
        return ['PSet.py', 'PSet.pkl', 'debug/crabConfig.py', 'debug/originalPSet.py.py']
    else:
        return ['debug/crabConfig.py', 'debug/originalPSet.py.py']
    
def NEW_USER_SANDBOX_EXCLUSIONS(tarmembers):
    """ Exclusion function used with the new crabclient (>= 3.3.1607). Since the new client sandbox no longer
        contains the debug files, it's pointless to exclude them. Also, this function is used when getting
        the hash of the debug tarball (a new addition in 3.3.1607). If the debug files are excluded, the tarball
        would always have the same hash and stay the same, serving no purpose.
    """
    if BOOTSTRAP_CFGFILE_DUMP in map(lambda x: x.name, tarmembers):
        #exclude the pickle pset if the dumpPython PSet is there
        return ['PSet.py', 'PSet.pkl']
    else:
        return []


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
    """ The function simply queries the REST interface specified to get the proxied webdir to use
        for the task. Returns None in case the API could not find the url (either an error or the schedd
        is not configured)
    """
    #This import is here because SeverUtilities is also used on the worker nodes,
    #and I want to avoid the dependency to pycurl right now. We should actually add it one day
    #so that other code in cmscp that uses Requests.py from WMCore can be migrated to RESTInteractions
    from RESTInteractions import HTTPRequests
    data = {'subresource': 'webdirprx',
            'workflow': task,
           }
    res = None
    try:
        server = HTTPRequests(host, cert, cert, retry = 2)
        dictresult, _, _ = server.get(uri, data = data) #the second and third parameters are deprecated
        if dictresult.get('result'):
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


def insertJobIdSid(jinfo, jobid, workflow, jobretry):
    """
    Modifies passed dictionary (jinfo) to contain jobId and sid keys and values.
    Used when creating dashboard reports.
    """
    jinfo['jobId'] = "%s_https://glidein.cern.ch/%s/%s_%s" % (jobid, jobid, workflow.replace("_", ":"), jobretry)
    jinfo['sid'] = "https://glidein.cern.ch/%s%s" % (jobid, workflow.replace("_", ":"))


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
    except IOError:
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
    from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

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
        raise HTTPException(errormsg)
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
    """
    checkQuota = " Please check that you have write access to destination site and that your quota is not exceeded, use crab checkwrite for more informations."
    refuseToSubmit = " Can't submit task because write check at destination site fails."
    if gridJob:
        refuseToSubmit = ""
    for exitCode in STAGEOUT_ERRORS:
        for error in STAGEOUT_ERRORS[exitCode]:
            if re.match(error['regex'], reason.lower()):
                reason = error['error-msg'] + refuseToSubmit + checkQuota
                return error['isPermanent'], reason, exitCode
    return False, "", None


def parseJobAd(filename):
    jobAd = {}
    with open(filename) as fd:
        #classad.parseOld(fd)
        for adline in fd.readlines():
            info = adline.split(' = ', 1)
            if len(info) != 2:
                continue
            if info[1].startswith('undefined'):
                val = info[1].strip()
            elif info[1].startswith('"'):
                val = info[1].strip().replace('"', '')
            else:
                try:
                    val = int(info[1].strip())
                except ValueError:
                    continue
            jobAd[info[0]] = val
    return jobAd


def mostCommon(lst, default=0):
    try:
        return max(set(lst), key=lst.count)
    except ValueError:
        return default


def getTimeFromTaskname(taskname):
    """ Get the submission time from the taskname and return the seconds since epoch
        corresponding to it. The function is not currently used.
    """

    #validate taskname. In principle not necessary, but..
    if not isinstance(taskname, str):
        raise TypeError('In ServerUtilities.getTimeFromTaskname: "taskname" parameter must be a string')
    stime = taskname.split(':')[0] #s stands for string
    stimePattern = '^\d{6}_\d{6}$'
    if not re.match(stimePattern, stime):
        raise ValueError('In ServerUtilities.getTimeFromTaskname: "taskname" parameter must match %s' % stimePattern)
    #convert the time
    dtime = time.strptime(stime, '%y%m%d_%H%M%S') #d stands for data structured
    return calendar.timegm(dtime)
