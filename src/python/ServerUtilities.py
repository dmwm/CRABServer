"""
This contains some utility methods to share between the server and the client, or by the server components themself
"""

from __future__ import print_function
from __future__ import absolute_import

import os
import re
import time
import fcntl
import urllib
import hashlib
import calendar
import datetime
import traceback
import subprocess
import contextlib
from httplib import HTTPException

BOOTSTRAP_CFGFILE_DUMP = 'PSetDump.py'
FEEDBACKMAIL = 'hn-cms-computing-tools@cern.ch'

# Parameters for User File Cache
# 120 MB is the maximum allowed size of a single file
FILE_SIZE_LIMIT = 120*1048576
# 0.5MB is the maximum limit for file completely loaded into memory
FILE_MEMORY_LIMIT = 512*1024

# Fatal error limits for job resource usage
# Defaults are used if unable to load from .job.ad
# Otherwise it uses these values.
MAX_WALLTIME = 21*60*60 + 30*60
MAX_MEMORY = 2*1024
MAX_DISK_SPACE = 20000000 # Disk usage is not used from .job.ad as CRAB3 is not seeting it. 20GB is max.

MAX_IDLE_JOBS = 1000
MAX_POST_JOBS = 20

## Parameter used to set the LeaveJobInQueue and the PeriodicRemoveclassads.
## It's also used during resubmissions since we don't allow a resubmission during the last week
## Before changing this value keep in mind that old running DAGs have the old value in the CRAB_TaskSubmitTime
## classad expression but DagmanResubmitter uses this value to calculate if a resubmission is possible
TASKLIFETIME = 30*24*60*60
## Number of days where the resubmission is not possible if the task is expiring
NUM_DAYS_FOR_RESUBMITDRAIN = 7
## Maximum number of days a task can stay in TAPERECALL status for
MAX_DAYS_FOR_TAPERECALL = 30

## These are all possible statuses of a task in the TaskDB.
TASKDBSTATUSES_TMP = ['NEW', 'HOLDING', 'QUEUED']
TASKDBSTATUSES_FAILURES = ['SUBMITFAILED', 'KILLFAILED', 'RESUBMITFAILED', 'FAILED']
TASKDBSTATUSES_FINAL = ['UPLOADED', 'SUBMITTED', 'KILLED'] + TASKDBSTATUSES_FAILURES
TASKDBSTATUSES = TASKDBSTATUSES_TMP + TASKDBSTATUSES_FINAL

## These are all possible statuses of a task as returned by the `status' API.
TASKSTATUSES = TASKDBSTATUSES + ['COMPLETED', 'UNKNOWN', 'InTransition']


## These are all allowed transfer and publication status. P.S. See below for allowed statuses for user
## to put into database. For sure we don`t want to allow or make mistakes and add KILL status for files
## which were not transferred at all. Just a bit of security
TRANSFERDB_STATES = {0: "NEW",
                     1: "ACQUIRED",
                     2: "FAILED",
                     3: "DONE",
                     4: "RETRY",
                     5: "SUBMITTED",
                     6: "KILL",
                     7: "KILLED"}
TRANSFERDB_STATUSES = dict((v, k) for k, v in TRANSFERDB_STATES.iteritems())

PUBLICATIONDB_STATES = {0: "NEW",
                        1: "ACQUIRED",
                        2: "FAILED",
                        3: "DONE",
                        4: "RETRY",
                        5: "NOT_REQUIRED"}
PUBLICATIONDB_STATUSES = dict((v, k) for k, v in PUBLICATIONDB_STATES.iteritems())

## Whenever user is putting a new Doc, these statuses will be used for double checking
## and also for adding in database correct value
USER_ALLOWED_TRANSFERDB_STATES = {0: "NEW",
                                  3: "DONE"}
USER_ALLOWED_TRANSFERDB_STATUSES = dict((v, k) for k, v in TRANSFERDB_STATES.iteritems())

USER_ALLOWED_PUBLICATIONDB_STATES = {0: "NEW",
                                     4: "RETRY",
                                     5: "NOT_REQUIRED"}
USER_ALLOWED_PUBLICATIONDB_STATUSES = dict((v, k) for k, v in PUBLICATIONDB_STATES.iteritems())

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


def getTestDataDirectory():
    """ Get the /path/to/repository/test/data direcotry to locate files for unit tests
    """
    testdirList = __file__.split(os.sep)[:-3] + ["test", "data"]
    return os.sep.join(testdirList)

def isCouchDBURL(url):
    """ Return True if the url proviced is a couchdb one
    """
    return 'couchdb' in url


def truncateError(msg):
    """Truncate the error message to the first 7400 chars if needed, and add a message if we truncate it.
       See https://github.com/dmwm/CRABServer/pull/4867#commitcomment-12086393
    """
    MSG_LIMIT = 7500
    if len(msg) > MSG_LIMIT:
        truncMsg = msg[:MSG_LIMIT - 100]
        truncMsg += "\n[... message truncated to the first 7400 chars ...]"
        return truncMsg
    else:
        return msg


def checkOutLFN(lfn, username):
    """ Check if the lfn provided contains the username or is /store/local/ or /store/local/
    """
    if lfn.startswith('/store/user/'):
        if lfn.split('/')[3] != username:
            return False
    elif lfn.startswith('/store/group/') or lfn.startswith('/store/local/'):
        if lfn.split('/')[3] == '':
            return False
    else:
        return False
    return True


def getProxiedWebDir(task, host, uri, cert, logFunction=print):
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
        server = HTTPRequests(host, cert, cert, retry=2)
        dictresult, _, _ = server.get(uri, data=data) #the second and third parameters are deprecated
        if dictresult.get('result'):
            res = dictresult['result'][0]
    except HTTPException as hte:
        logFunction(traceback.format_exc())
        logFunction(hte.headers)
        logFunction(hte.result)

    return res


#setDashboardLogs function is shared between the postjob and the job wrapper. Sharing it here
def setDashboardLogs(params, webdir, jobid, retry):
    """ Method to prepare some common dictionary keys for dashboard reporting
    """
    log_files = [("job_out", "txt"), ("postjob", "txt")]
    for i, log_file in enumerate(log_files):
        log_file_basename, log_file_extension = log_file
        log_file_name = "%s/%s.%s.%d.%s" % (webdir, \
                                            log_file_basename, jobid, \
                                            retry, \
                                            log_file_extension)
        log_files[i] = log_file_name
    params['StatusLogFile'] = ",".join(log_files)


def insertJobIdSid(jinfo, jobid, workflow, jobretry):
    """ Modifies passed dictionary (jinfo) to contain jobId and sid keys and values.
        Used when creating dashboard reports.
    """
    jinfo['jobId'] = "%s_https://glidein.cern.ch/%s/%s_%s" % (jobid, jobid, workflow.replace("_", ":"), jobretry)
    jinfo['sid'] = "https://glidein.cern.ch/%s%s" % (jobid, workflow.replace("_", ":"))


def getWebdirForDb(reqname, storage_rules):
    """ Get the location of the webdir. This method is called on the schedd by AdjustSites
    """
    path = os.path.expanduser("~/%s" % reqname)
    sinfo = storage_rules.split(",")
    storage_re = re.compile(sinfo[0])
    val = storage_re.sub(sinfo[1], path)
    return val


def cmd_exist(cmd):
    """ Check if linux command exist
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
    """ Return prepared gfal or lcg commands for checkwrite
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
    """ Create dummy file for checking write permissions
    """
    abspath = os.path.abspath(filename)
    try:
        with open(abspath, 'w') as fd:
            fd.write('This is a dummy file created by the crab checkwrite command on %s' % str(datetime.datetime.now().strftime('%d/%m/%Y at %H:%M:%S')))
    except IOError:
        logger.info('Error: Failed to create file %s localy.' % filename)
        raise


def removeDummyFile(filename, logger):
    """ Remove created dummy file
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
        pfndict = phedex.getPFN(nodes=[sitename], lfns=[lfnsadd])
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
    """ Execute passed bash command. There is no check for command success or failure. Who`s calling
        this command, has to check exitcode, out and err
    """
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    exitcode = process.returncode
    return out, err, exitcode


def isFailurePermanent(reason, gridJob=False):
    """ Method that decides whether a failure reason should be considered as a
        permanent failure and submit task or not.
    """
    from WMCore.WMExceptions import STAGEOUT_ERRORS

    checkQuota = " Please check that you have write access to destination site and that your quota is not exceeded, use crab checkwrite for more informations."
    refuseToSubmit = " Can't submit task because write check at destination site fails."
    if gridJob:
        refuseToSubmit = ""
    for exitCode in STAGEOUT_ERRORS:
        for error in STAGEOUT_ERRORS[exitCode]:
            if re.match(error['regex'].lower(), reason.lower()):
                reason = error['error-msg'] + refuseToSubmit + checkQuota
                return error['isPermanent'], reason, exitCode
    return False, "", None


def parseJobAd(filename):
    """ Parse the jobad file provided as argument and return a dict representing it
    """
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
    """ Return the most common error among the list
    """
    try:
        return max(set(lst), key=lst.count)
    except ValueError:
        return default


@contextlib.contextmanager
def getLock(name):
    """ Create a "name".lock file and, using fcnt, lock it (or wait for the lock to be released) before proceeding)
    """
    with open(name + '.lock', 'a+') as fd:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield fd


def getHashLfn(lfn):
    """ Provide a hashed lfn from an lfn.
    """
    return hashlib.sha224(lfn).hexdigest()


def generateTaskName(username, requestname, timestamp=None):
    """ Generate a taskName which is saved in database
    """
    if not timestamp:
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
    taskname = "%s:%s_%s" % (timestamp, username, requestname)
    return taskname


# TODO: Remove this from CRABClient. This is kind of common for WMCore not only for CRAB. Maybe better place to have this in WMCore?
def encodeRequest(configreq, listParams=None):
    """ Used to encode the request from a dict to a string. Include the code needed for transforming lists in the format required by
        cmsweb, e.g.:   adduserfiles = ['file1','file2']  ===>  [...]adduserfiles=file1&adduserfiles=file2[...]
        The list of dictionary keys like adduserfiles above, which have a list as value, needs to be passed in the listParams argument  
    """
    listParams = listParams or []
    encodedLists = ''
    for lparam in listParams:
        if lparam in configreq:
            if len(configreq[lparam]) > 0:
                encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join(map(urllib.quote, configreq[lparam]))
            del configreq[lparam]
    if isinstance(configreq, dict):
        #Make sure parameters we encode are streing. Otherwise u'IamAstring' may become 'uIamAstring' in the DB
        for k, v in configreq.iteritems():
            if isinstance(v, basestring):
                configreq[k] = str(v)
            if isinstance(v, list):
                configreq[k] = [str(x) if isinstance(x, basestring) else x for x in v]
    encoded = urllib.urlencode(configreq) + encodedLists
    return str(encoded)


def oracleOutputMapping(result, key=None):
    """ If key is defined, it will use id as a key and will return dictionary which contains all items with this specific key
        Otherwise it will return a list of dictionaries.
        Up to caller to check for catch KeyError,ValueError and any other related failures.
        KeyError is raised if wrong key is specified.
        ValueError is raised if wrong format is provided. So far database provides tuple which has 1 dictionary.
    """
    if not(result, tuple):
        raise ValueError('Provided input is not valid tuple')
    if not(result[0], dict):
        raise ValueError('Provided input does not contain dictionary as first element')
    outputDict = {} if key else []
    for item in result[0]['result']:
        docInfo = dict(zip(result[0]['desc']['columns'], item))
        docOut = {}
        for dockey in docInfo:
            # Remove first 3 characters as they are tm_*
            docOut[dockey[3:]] = docInfo[dockey] # rm tm_ which is specific for database
        if key:
            if docOut[key] not in outputDict:
                outputDict[docOut[key]] = []
            outputDict[docOut[key]].append(docOut)
        else:
            outputDict.append(docOut)
    return outputDict


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

def checkTaskLifetime(submissionTime):
    """ Verify that at least 7 days are left before the task periodic remove expression
        evaluates to true. This is to let job finish and possibly not remove a task with
        running jobs.

        submissionTime are the seconds since epoch of the task submission time in the DB
    """

    msg = "ok"
    ## resubmitLifeTime is 23 days expressed in seconds
    resubmitLifeTime = TASKLIFETIME - NUM_DAYS_FOR_RESUBMITDRAIN * 24 * 60 * 60
    if time.time() > (submissionTime + resubmitLifeTime):
        msg = ("Resubmission of the task is not possble since less than %s days are left before the task is removed from the""schedulers.\n" % NUM_DAYS_FOR_RESUBMITDRAIN)
        msg += "A task expires %s days after its submission\n" % (TASKLIFETIME / (24 * 60 * 60))
        msg += "You can submit a 'recovery task' if you need to execute again the failed jobs\n"
        msg += "See https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ for more information about recovery tasks"
    return msg

def getEpochFromDBTime(startTime):
    """ Provide a meaningful description
    """
    return calendar.timegm(startTime.utctimetuple())

def getColumn(dictresult, columnName):
    """ Given a dict returned by REST calls, return the value of columnName
    """
    columnIndex = dictresult['desc']['columns'].index(columnName)
    value = dictresult['result'][columnIndex]
    if value == 'None':
        return None
    else:
        return value

class newX509env():
    """
    context manager to run some code with a new x509 environment
    exiting env is restored when code is exited, even if exited via exception
    The new environmnet will only contain X509_USER_.. variables explicitely
    listed as argument to the method.
    see: https://docs.python.org/2.7/reference/compound_stmts.html?highlight=try#the-with-statement
    and: http://preshing.com/20110920/the-python-with-statement-by-example/
    usage 1:
        with newX509env(X509_USER_PROXY=proxy,X509_USER_CERT=cert,X509_USER_KEY=key :
           do stuff
    usage 2:
        myEnv=newX509env(X509_USER_PROXY=myProxy,X509_USER_CERT=myCert,X509_USER_KEY=myKey)
        with myEnv:
            do stuff
    """

    def __init__(self, X509_USER_PROXY=None, X509_USER_CERT=None, X509_USER_KEY=None):
        # define the new environment
        self.oldProxy = None
        self.oldCert = None
        self.oldKey = None
        self.newProxy = X509_USER_PROXY
        self.newCert = X509_USER_CERT
        self.newKey = X509_USER_KEY

    def __enter__(self):
        # save current env
        self.oldProxy = os.getenv('X509_USER_PROXY')
        self.oldCert = os.getenv('X509_USER_CERT')
        self.oldKey = os.getenv('X509_USER_KEY')
        # Clean previous env. only delete env. vars if they were defined
        if self.oldProxy: del os.environ['X509_USER_PROXY']
        if self.oldCert:  del os.environ['X509_USER_CERT']
        if self.oldKey:   del os.environ['X509_USER_KEY']
        # set environment to whatever user wants
        if self.newProxy: os.environ['X509_USER_PROXY'] = self.newProxy
        if self.newCert:  os.environ['X509_USER_CERT'] = self.newCert
        if self.newKey:   os.environ['X509_USER_KEY'] = self.newKey

    def __exit__(self, a, b, c):
        # restore X509 environment
        if self.oldProxy:
            os.environ['X509_USER_PROXY'] = self.oldProxy
        else:
            if os.getenv('X509_USER_PROXY'): del os.environ['X509_USER_PROXY']
        if self.oldCert:
            os.environ['X509_USER_CERT'] = self.oldCert
        else:
            if os.getenv('X509_USER_CERT'): del os.environ['X509_USER_CERT']
        if self.oldKey:
            os.environ['X509_USER_KEY'] = self.oldKey
        else:
            if os.getenv('X509_USER_KEY'): del os.environ['X509_USER_KEY']


class tempSetLogLevel():
    """
        a simple context manager to temporarely change logging level
        making sure it is restored even if exception is raised
        USAGE:
            with tempSetLogLevel(logger=myLogger,level=logging.ERROR):
               do stuff
        both arguments are mandatory

        EXAMPLE:
        import logging
        logger=logging.getLogger()
        logger.addHandler(logging.StreamHandler())
        logger=logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger.debug("debug msg")
        with tempSetLogLevel(logger,logging.ERROR):
          try:
            i=1
            while True:
              logger.debug("msg inside loop at %d", i)
              if i==3: logger.error("got one error at %d", i)
              if i==5: raise
              i+=1
          except Exception as e:
            logger.error("got exception at %d", i)
            pass
        logger.debug("after the loop")

        RESULTS IN THIS PRINTOUT:
        debug msg
        got one error at 3
        got exception at 5
        after the loop
    """
    import logging
    def __init__(self, logger=None, level=None):
        self.previousLogLevel = None
        self.newLogLevel = level
        self.logger = logger
    def __enter__(self):
        self.previousLogLevel = self.logger.getEffectiveLevel()
        self.logger.setLevel(self.newLogLevel)
    def __exit__(self,a,b,c):
        self.logger.setLevel(self.previousLogLevel)
