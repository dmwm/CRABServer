"""
This contains some utility methods to share between the server and the client, or by the server components themself
IMPORTANT : SINCE THIS NEEDS TO RUN IN THE CLIENT, CODE NEEDS TO WORK
IN BOTH PYTHON2 AND PYTHON3 WITHOUT CALLS TO FUTURIZE OR OTHER UTILITIES NOT
PRESENT IN EARLIER CMSSW RELEASES
"""
# W0715: this is a weird and unclear message from pylint, better to ignore it
# W1514,W0707 for python2 compatibility. Will refactor later in https://github.com/dmwm/CRABServer/issues/7765 task.
# pylint: disable=W0715,W1514,W0707
# pylint: disable=consider-using-f-string   # for python2 compatibility as long as this is used in client too

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os
import sys
import re
import time
import fcntl
import hashlib
import calendar
import datetime
import traceback
import subprocess
import contextlib

if sys.version_info >= (3, 0):
    from http.client import HTTPException  # Python 3 and Python 2 in modern CMSSW
    from urllib.parse import urlencode, quote  # pylint: disable=no-name-in-module
    from past.builtins import basestring
if sys.version_info < (3, 0):
    from urllib import urlencode, quote
    from httplib import HTTPException  # old Python 2 version in CMSSW_7

BOOTSTRAP_CFGFILE_DUMP = 'PSetDump.py'
FEEDBACKMAIL = 'cmstalk+computing-tools@dovecotmta.cern.ch'

# Parameters for User File Cache
# 120 MB is the maximum allowed size of a single file
FILE_SIZE_LIMIT = 120 * 1048576
# 0.5MB is the maximum limit for file completely loaded into memory
FILE_MEMORY_LIMIT = 512 * 1024

# these are known, pre-defined nickames for "CRAB configuration" at large
# which correspond to a well known and specified REST host name and DataBase instance
SERVICE_INSTANCES = {'prod': {'restHost': 'cmsweb.cern.ch', 'dbInstance': 'prod'},
                     'preprod': {'restHost': 'cmsweb-testbed.cern.ch', 'dbInstance': 'preprod'},
                     'auth': {'restHost': 'cmsweb-auth.cern.ch', 'dbInstance': 'preprod'},
                     # Vijay's tmp env
                     'test1': {'restHost': 'cmsweb-test1.cern.ch', 'dbInstance': 'devfour'},
                     'test2': {'restHost': 'cmsweb-test2.cern.ch', 'dbInstance': 'dev'},
                     'test3': {'restHost': 'cmsweb-test3.cern.ch', 'dbInstance': 'dev'},
                     'test4': {'restHost': 'cmsweb-test4.cern.ch', 'dbInstance': 'dev'},
                     'test5': {'restHost': 'cmsweb-test5.cern.ch', 'dbInstance': 'dev'},
                     'test6': {'restHost': 'cmsweb-test6.cern.ch', 'dbInstance': 'dev'},
                     'test11': {'restHost': 'cmsweb-test11.cern.ch', 'dbInstance': 'devtwo'},
                     'test12': {'restHost': 'cmsweb-test12.cern.ch', 'dbInstance': 'devthree'},
                     'stefanovm': {'restHost': 'stefanovm.cern.ch', 'dbInstance': 'dev'},
                     'stefanovm2': {'restHost': 'stefanovm2.cern.ch', 'dbInstance': 'dev'},
                     'other': {'restHost': None, 'dbInstance': None},
                     }

# Limits for TaskWorker
MAX_LUMIS_IN_BLOCK = 100000  # 100K lumis to avoid blowing up memory


# Fatal error limits for job resource usage
# Defaults are used if unable to load from .job.ad
# Otherwise it uses these values.
MAX_WALLTIME = 21 * 60 * 60 + 30 * 60
MAX_MEMORY = 2 * 1024
# see https://github.com/dmwm/CRABServer/issues/5995
MAX_MEMORY_PER_CORE = 2500
MAX_MEMORY_SINGLE_CORE = 5000
MAX_DISK_SPACE = 20000000  # Disk usage is not used from .job.ad as CRAB3 is not seeting it. 20GB is max.

MAX_IDLE_JOBS = 1000
MAX_POST_JOBS = 20

# Parameter used to set the LeaveJobInQueue and the PeriodicRemoveclassads.
# It's also used during resubmissions since we don't allow a resubmission during the last week
# Before changing this value keep in mind that old running DAGs have the old value in the CRAB_TaskSubmitTime
# classad expression but DagmanResubmitter uses this value to calculate if a resubmission is possible
TASKLIFETIME = 30 * 24 * 60 * 60  # 30 days in seconds
# Number of days where the resubmission is not possible if the task is expiring
NUM_DAYS_FOR_RESUBMITDRAIN = 7
# Maximum number of days a task can stay in TAPERECALL status
MAX_DAYS_FOR_TAPERECALL = 15
# Threshold (in TB) to split a dataset among multiple sites when recalling from tape
MAX_TB_TO_RECALL_AT_A_SINGLE_SITE = 1000  # effectively no limit. See https://github.com/dmwm/CRABServer/issues/7610

# These are all possible statuses of a task in the TaskDB.
TASKDBSTATUSES_TMP = ['WAITING', 'NEW', 'HOLDING', 'QUEUED', 'TAPERECALL', 'KILLRECALL']
TASKDBSTATUSES_FAILURES = ['SUBMITFAILED', 'KILLFAILED', 'RESUBMITFAILED', 'FAILED']
TASKDBSTATUSES_FINAL = ['UPLOADED', 'SUBMITTED', 'KILLED'] + TASKDBSTATUSES_FAILURES
TASKDBSTATUSES = TASKDBSTATUSES_TMP + TASKDBSTATUSES_FINAL

# These are all possible statuses of a task as returned by the `status' API.
TASKSTATUSES = TASKDBSTATUSES + ['COMPLETED', 'UNKNOWN', 'InTransition']


# These are all allowed transfer and publication status. P.S. See below for allowed statuses for user
# to put into database. For sure we don`t want to allow or make mistakes and add KILL status for files
# which were not transferred at all. Just a bit of security
TRANSFERDB_STATES = {0: "NEW",
                     1: "ACQUIRED",
                     2: "FAILED",
                     3: "DONE",
                     4: "RETRY",
                     5: "SUBMITTED",
                     6: "KILL",
                     7: "KILLED"}
TRANSFERDB_STATUSES = dict((v, k) for k, v in TRANSFERDB_STATES.items())

PUBLICATIONDB_STATES = {0: "NEW",
                        1: "ACQUIRED",
                        2: "FAILED",
                        3: "DONE",
                        4: "RETRY",
                        5: "NOT_REQUIRED"}
PUBLICATIONDB_STATUSES = dict((v, k) for k, v in PUBLICATIONDB_STATES.items())

# Whenever user is putting a new Doc, these statuses will be used for double checking
# and also for adding in database correct value
USER_ALLOWED_TRANSFERDB_STATES = {0: "NEW",
                                  3: "DONE"}
USER_ALLOWED_TRANSFERDB_STATUSES = dict((v, k) for k, v in TRANSFERDB_STATES.items())

USER_ALLOWED_PUBLICATIONDB_STATES = {0: "NEW",
                                     4: "RETRY",
                                     5: "NOT_REQUIRED"}
USER_ALLOWED_PUBLICATIONDB_STATUSES = dict((v, k) for k, v in PUBLICATIONDB_STATES.items())

RUCIO_QUOTA_WARNING_GB = 10  # when available Rucio quota is less than this, warn users
RUCIO_QUOTA_MINIMUM_GB = 1  # when available Rucio quota is less thatn this, refuse submi

"""
STAGEOUT_ERRORS
key - exitCode which is also defined in WM_JOB_ERROR_CODES # TODO: to be used for reporting to dashboard and to end users in crab status command
value - is a list, which has dictionaries with the following content:
    regex - Error message which is exposed by gfal-copy/del.
    error-msg - Error msg which will be shown to users in crab status output
    isPermanent - True or False, which is used in CRAB3/ASO for following reasons:
           a) CRAB3 decides should it submit a task to gridScheduler; If it is not permanent,
              task will be submitted, but also error message will be shown in crab status output
           b) CRAB3 Postjob decides should it retry job or not;
           c) ASO decides should it resubmit transfer to FTS;
"""
STAGEOUT_ERRORS = {60317: [{"regex": ".*Cancelled ASO transfer after timeout.*",
                            "error-msg": "ASO Transfer canceled due to timeout.",
                            "isPermanent": True}
                          ],
                   60321: [{"regex": ".*reports could not open connection to.*",
                            "error-msg": "Storage element is not accessible.",
                            "isPermanent": False},
                           {"regex": ".*451 operation failed\\: all pools are full.*",
                            "error-msg": "Destination site does not have enough space on their storage.",
                            "isPermanent": True},
                           {"regex": ".*system error in connect\\: connection refused.*",
                            "error-msg": "Destination site storage refused connection.",
                            "isPermanent": False}
                          ],
                   60322: [{"regex": ".*permission denied.*",
                            "error-msg": "Permission denied.",
                            "isPermanent": True},
                           {"regex": ".*Permission refused.*",
                            "error-msg": "Permission denied.",
                            "isPermanent": True},
                           {"regex": ".*operation not permitted.*",
                            "error-msg": "Operation not allowed.",
                            "isPermanent": True},
                           {"regex": ".*mkdir\\(\\) fail.*",
                            "error-msg": "Can`t create directory.",
                            "isPermanent": True},
                           {"regex": ".*open/create error.*",
                            "error-msg": "Can`t create directory/file on destination site.",
                            "isPermanent": True},
                           {"regex": ".*mkdir\\: cannot create directory.*",
                            "error-msg": "Can`t create directory.",
                            "isPermanent": True},
                           {"regex": ".*530-login incorrect.*",
                            "error-msg": "Permission denied to write to destination site.",
                            "isPermanent": True},
                          ],
                   60323: [{"regex": ".*does not have enough space.*",
                            "error-msg": "User quota exceeded.",
                            "isPermanent": True},
                           {"regex": ".*disk quota exceeded.*",
                            "error-msg": "Disk quota exceeded.",
                            "isPermanent": True},
                           {"regex": ".*HTTP 507.*",
                            "error-msg": "HTTP 507: Disk quota exceeded or Disk full.",
                            "isPermanent": True},
                           {"regex": ".*status code 507.*",
                            "error-msg": "HTTP 507: Disk quota exceeded or Disk full.",
                            "isPermanent": True},
                          ]}


def NEW_USER_SANDBOX_EXCLUSIONS(tarmembers):  # This is only used in CRABClient !!! MOVE IT THERE
    """ Exclusion function used with the new crabclient (>= 3.3.1607). Since the new client sandbox no longer
        contains the debug files, it's pointless to exclude them. Also, this function is used when getting
        the hash of the debug tarball (a new addition in 3.3.1607). If the debug files are excluded, the tarball
        would always have the same hash and stay the same, serving no purpose.
    """
    if BOOTSTRAP_CFGFILE_DUMP in map(lambda x: x.name, tarmembers):
        # exclude the pickle pset if the dumpPython PSet is there
        return ['PSet.py', 'PSet.pkl']
    return []


def getTestDataDirectory():
    """ Get the /path/to/repository/test/data direcotry to locate files for unit tests
    """
    testdirList = __file__.split(os.sep)[:-3] + ["test", "data"]
    return os.sep.join(testdirList)


def truncateError(msg):
    """Truncate the error message to the first 1000 chars if needed, and add a message if we truncate it.
       See https://github.com/dmwm/CRABServer/pull/4867#commitcomment-12086393
    """
    msgLimit = 1100
    # hack to avoid passing HTTP error code to message parsing code in CRABClient/CrabRestInterface.py
    codeString = 'HTTP/1.'
    if codeString in msg:
        msg = msg.replace(codeString, 'HTTP 1.')
    if len(msg) > msgLimit:
        truncMsg = msg[:msgLimit - 100]
        truncMsg += "\n[... message truncated to the first {0} chars ...]".format(msgLimit-100)
        return truncMsg
    return msg


def checkOutLFN(lfn, username):
    """ Check if the lfn provided contains the username or is /store/local/ or /store/local/
    """
    if lfn.startswith('/store/user/rucio/'):
        if lfn.split('/')[4] != username:
            return False
    elif lfn.startswith('/store/user/'):
        if lfn.split('/')[3] != username:
            return False
    elif lfn.startswith('/store/test/rucio/user/'):
        if lfn.split('/')[5] != username:
            return False
    elif lfn.startswith('/store/test/rucio/int/user/'):
        if lfn.split('/')[6] != username:
            return False
    elif lfn.startswith('/store/group/') or lfn.startswith('/store/local/'):
        if lfn.split('/')[3] == '':
            return False
    else:
        return False
    return True


def getProxiedWebDir(crabserver=None, task=None, logFunction=print):
    """ The function simply queries the REST interface specified to get the proxied webdir to use
        for the task. Returns None in case the API could not find the url (either an error or the schedd
        is not configured)
    """
    data = {'subresource': 'webdirprx',
            'workflow': task,
           }
    res = None
    try:
        dictresult, _, _ = crabserver.get(api='task', data=data)  # the second and third parameters are deprecated
        if dictresult.get('result'):
            res = dictresult['result'][0]
    except HTTPException as hte:
        logFunction(traceback.format_exc())
        logFunction(hte.headers)
        logFunction(hte.result)

    return res


def insertJobIdSid(jinfo, jobid, workflow, jobretry):
    """ Modifies passed dictionary (jinfo) to contain jobId and sid keys and values.
        Used when creating dashboard reports.
    """
    jinfo['jobId'] = "%s_https://glidein.cern.ch/%s/%s_%s" % (jobid, jobid, workflow.replace("_", ":"), jobretry)
    jinfo['sid'] = "https://glidein.cern.ch/%s%s" % (jobid, workflow.replace("_", ":"))


def getWebdirForDb(reqname, storageRules):
    """ Get the location of the webdir. This method is called on the schedd by AdjustSites
    """
    path = os.path.expanduser("~/{0}".format(reqname))
    sinfo = storageRules.split(",")
    storageRegex = re.compile(sinfo[0])
    val = storageRegex.sub(sinfo[1], path)
    return val


def cmd_exist(cmd):
    """ Check if linux command exist
    """
    try:
        p = None
        with open("/dev/null", "w") as null:
            p = subprocess.Popen("/usr/bin/which %s" % cmd, stdout=null, stderr=null, shell=True)
            p.communicate()
        if p and p.returncode == 0:
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
        logger.info('Error: Failed to create file %s locally.' % filename)
        raise


def removeDummyFile(filename, logger):
    """ Remove created dummy file
    """
    abspath = os.path.abspath(filename)
    try:
        os.remove(abspath)
    except (OSError, FileNotFoundError):
        logger.info('Warning: Failed to delete file %s' % filename)


def executeCommand(command):
    """ Execute passed bash command. There is no check for command success or failure. Who`s calling
        this command, has to check exitcode, out and err
    """
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    exitcode = process.returncode
    return out, err, exitcode


def execute_command(command=None, logger=None, timeout=None, redirect=True):
    """
    execute command with optional logging and timeout.
    Returns a 3-ple: stdout, stderr, rc
      rc=0 means success.
      rc=124 (SIGTERM) means that command timed out
    Redirection of std* can be turned off if the command will need to interact with caller
    writing messages and/or asking for input, like if needs to get a passphrase to access
    usercert/key for (my)proxy creation as in
    https://github.com/dmwm/WMCore/blob/75c5abd83738a6a3534027369cd6e109667de74e/src/python/WMCore/Credential/Proxy.py#L383-L385
    Should eventually replace executeCommand and be used everywhere.
    Imported here from WMCore/Credential/Proxy.py and then adapted to avoid deadlocks
    as per https://github.com/dmwm/CRABClient/issues/5026
    """

    stdout, stderr, rc = None, None, 99999
    if logger:
        logger.debug('Executing command :\n %s' % command)
    if timeout:
        if logger:
            logger.debug('add timeout at %s seconds', timeout)
        command = ('timeout %s ' % timeout) + command
    if redirect:
        proc = subprocess.Popen(
            command, shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
    else:
        proc = subprocess.Popen(command, shell=True)

    out, err = proc.communicate()
    rc = proc.returncode
    if rc == 124 and timeout:
        if logger:
            logger.error('ERROR: Timeout after %s seconds in executing:\n %s' % (timeout, command))
    # for Py3 compatibility
    stdout = out.decode(encoding='UTF-8') if out else ''
    stderr = err.decode(encoding='UTF-8') if err else ''
    if logger:
        logger.debug('output : %s\n error: %s\n retcode : %s' % (stdout, stderr, rc))

    return stdout, stderr, rc


def isFailurePermanent(reason, gridJob=False):
    """ Method that decides whether a failure reason should be considered as a
        permanent failure and submit task or not.
    """

    checkQuota = "Please check that you have write access to destination site\n and that your quota is not exceeded"
    checkQuota += "\n use crab checkwrite for more information."
    refuseToSubmit = " Can't submit task because write check at destination site fails.\n"
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
        SB: why do we have this ? the classAd object returned by classad.parse has
            the semantic of a dictionary ! Currently it is only used in cmscp.py
            and in job wrapper we are not sure to have HTCondor available.
            Note that we also have a parseAd() method inside CMSRunAnalysis.py which should
            do finely also in cmscp.py
    """
    jobAd = {}
    with open(filename, 'r', encoding='utf-8') as fd:
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
    return hashlib.sha224(lfn.encode('utf-8')).hexdigest()


def generateTaskName(username, requestname, timestamp=None):
    """ Generate a taskName which is saved in database
    """
    if not timestamp:
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
    taskname = "%s:%s_%s" % (timestamp, username, requestname)
    return taskname


def getUsernameFromTaskname(taskname):
    """
    extract username from a taskname constructed as in generateTaskName above
    :param taskname:
    :return: username
    """
    username = taskname.split(':')[1].split('_')[0]
    return username


def getTimeFromTaskname(taskname):
    """ Get the submission time from the taskname and return the seconds since epoch
        corresponding to it.
    """

    # validate taskname. In principle not necessary, but..
    if not isinstance(taskname, str):
        raise TypeError('In ServerUtilities.getTimeFromTaskname: "taskname" parameter must be a string')
    stime = taskname.split(':')[0]  # s stands for string
    stimePattern = r'^\d{6}_\d{6}$'
    if not re.match(stimePattern, stime):
        raise ValueError('In ServerUtilities.getTimeFromTaskname: "taskname" parameter must match %s' % stimePattern)
    # convert the time
    dtime = time.strptime(stime, '%y%m%d_%H%M%S')  # d stands for data structured
    return calendar.timegm(dtime)


# Remove this from CRABClient ? This is kind of common for WMCore not only for CRAB. Maybe better place to have this in WMCore?
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
                encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join(map(quote, configreq[lparam]))
            del configreq[lparam]
    if isinstance(configreq, dict):
        # Make sure parameters we encode are streing. Otherwise u'IamAstring' may become 'uIamAstring' in the DB
        for k, v in configreq.items():
            if isinstance(v, basestring):
                configreq[k] = str(v)
            if isinstance(v, list):
                configreq[k] = [str(x) if isinstance(x, basestring) else x for x in v]
    encoded = urlencode(configreq) + encodedLists
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
            docOut[dockey[3:]] = docInfo[dockey]  # rm tm_ which is specific for database
        if key:
            if docOut[key] not in outputDict:
                outputDict[docOut[key]] = []
            outputDict[docOut[key]].append(docOut)
        else:
            outputDict.append(docOut)
    return outputDict


def checkTaskLifetime(submissionTime):
    """ Verify that at least 7 days are left before the task periodic remove expression
        evaluates to true. This is to let job finish and possibly not remove a task with
        running jobs.

        submissionTime are the seconds since epoch of the task submission time in the DB
    """

    msg = "ok"
    # resubmitLifeTime is 23 days expressed in seconds
    resubmitLifeTime = TASKLIFETIME - NUM_DAYS_FOR_RESUBMITDRAIN * 24 * 60 * 60
    if time.time() > (submissionTime + resubmitLifeTime):
        msg = "Resubmission of the task is not possible since less than {0} days".format(NUM_DAYS_FOR_RESUBMITDRAIN)
        msg += "are left before the task is removed from the""schedulers.\n"
        msg += "A task expires {0} days after its submission\n".format(TASKLIFETIME / (24 * 60 * 60))
        msg += "You can submit a 'recovery task' if you need to execute again the failed jobs\n"
        msg += "See https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ for more information about recovery tasks"
    return msg


def getEpochFromDBTime(startTime):
    """
    args:
    startTime: a time tuple such as returned by the gmtime() function in the time module
    returns:
    seconds from Epoch
    """
    return calendar.timegm(startTime.utctimetuple())


def getColumn(dictresult, columnName):
    """ Given a dict returned by REST calls, return the value of columnName
    """
    columnIndex = dictresult['desc']['columns'].index(columnName)
    value = dictresult['result'][columnIndex]
    if value == 'None':
        return None
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
        if self.oldProxy:
            del os.environ['X509_USER_PROXY']
        if self.oldCert:
            del os.environ['X509_USER_CERT']
        if self.oldKey:
            del os.environ['X509_USER_KEY']
        # set environment to whatever user wants
        if self.newProxy:
            os.environ['X509_USER_PROXY'] = self.newProxy
        if self.newCert:
            os.environ['X509_USER_CERT'] = self.newCert
        if self.newKey:
            os.environ['X509_USER_KEY'] = self.newKey

    def __exit__(self, a, b, c):
        # restore X509 environment
        if self.oldProxy:
            os.environ['X509_USER_PROXY'] = self.oldProxy
        else:
            if os.getenv('X509_USER_PROXY'):
                del os.environ['X509_USER_PROXY']
        if self.oldCert:
            os.environ['X509_USER_CERT'] = self.oldCert
        else:
            if os.getenv('X509_USER_CERT'):
                del os.environ['X509_USER_CERT']
        if self.oldKey:
            os.environ['X509_USER_KEY'] = self.oldKey
        else:
            if os.getenv('X509_USER_KEY'):
                del os.environ['X509_USER_KEY']


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

    def __init__(self, logger=None, level=None):
        self.previousLogLevel = None
        self.newLogLevel = level
        self.logger = logger

    def __enter__(self):
        self.previousLogLevel = self.logger.getEffectiveLevel()
        self.logger.setLevel(self.newLogLevel)

    def __exit__(self, a, b, c):
        self.logger.setLevel(self.previousLogLevel)


####################################################################
# Below a few convenience functions to access the S3 implementation of crabcache
# Clients are expected to use the following functions which all have the same signature:
#  downloadFromS3        to retrieve an object into a file
#  retrieveFromS3        to retrieve an object as JSON to use in the code
#  uploadToS3            to upload a file into an S3 object
#  getDownloadUrlFromS3  to obtain a PreSigned URL to access an existing object in S3
#    this can be used e.g. to share access to logs
# Common signature is: (crabserver, filepath, objecttype, taskname, username, tarballname, logger)
#  see the individual functions for more details
#
# Other functions are uploadToS3viaPSU and downloadFromS3viaPSU are for internal use.
####################################################################
def downloadFromS3(crabserver=None, filepath=None, objecttype=None, taskname=None,
                   username=None, tarballname=None, logger=None):
    """
    one call to make a 2-step operation:
    obtains a preSignedUrl from crabserver RESTCache and use it to download a file
    :param crabserver: a RESTInteraction/CRABRest object : points to CRAB Server to use
    :param filepath: string : the full path of the file to create with the downloaded content
        if file exists already, it is silently overwritten
    :param objecttype: string : the kind of object to dowbload: clientlog|twlog|sandbox|debugfiles|runtimefiles
    :param taskname: string : the task this object belongs to, if applicable
    :param username: string : the username this sandbox belongs to, in case objecttype=sandbox
    :param tarballname: string : for sandbox, taskname is not used but tarballname is needed
    :return: nothing. Raises an exception in case of error
    """
    preSignedUrl = getDownloadUrlFromS3(crabserver=crabserver, objecttype=objecttype,
                                        taskname=taskname, username=username,
                                        tarballname=tarballname, logger=logger)
    downloadFromS3ViaPSU(filepath=filepath, preSignedUrl=preSignedUrl, logger=logger)


def retrieveFromS3(crabserver=None, objecttype=None, taskname=None,
                   username=None, tarballname=None, logger=None):
    """
    obtains a preSignedUrl from crabserver RESTCache and use it to retrieve a file content as JSON
    :param crabserver: a RESTInteraction/CRABRest object : points to CRAB Server to use
    :param objecttype: string : the kind of object to retrieve: clientlog|twlog|sandbox|debugfiles|runtimefiles
    :param taskname: string : the task this object belongs to, if applicable
    :param username: string : the username this sandbox belongs to, in case objecttype=sandbox
    :param tarballname: string : when retrieving sandbox taskname is not used but tarballname is needed
    :return: the content of the S3 object as a string containing JSON data (to be used with json.loads)
    """
    api = 'cache'
    dataDict = {'subresource': 'retrieve', 'objecttype': objecttype}
    if taskname:
        dataDict['taskname'] = taskname
    if username:
        dataDict['username'] = username
    if tarballname:
        dataDict['tarballname'] = tarballname
    data = encodeRequest(dataDict)
    try:
        # calls to restServer alway return a 3-ple ({'result':a-list}, HTTPcode, HTTPreason)
        res = crabserver.get(api, data)
        result = res[0]['result'][0]
    except Exception as e:
        raise Exception('Failed to retrieve S3 file content via CRABServer:\n%s' % str(e))
    logger.debug("%s retrieved OK", objecttype)
    return result


def uploadToS3(crabserver=None, filepath=None, objecttype=None, taskname=None,
               username=None, tarballname=None, logger=None):
    """
    one call to make a 2-step operation:
    obtains a preSignedUrl from crabserver RESTCache and use it to upload a file
    :param crabserver: a RESTInteraction/CRABRest object : points to CRAB Server to use
    :param filepath: string : the full path of the file to upload, if applicable
    :param objecttype: string : the kind of object to upload: clientlog|twlog|sandbox|debugfiles|runtimefiles
    :param username: string : the username this sandbox belongs to, in case objecttype=sandbox
      username is not needed for upload, it is here to have same signature as other methods
    :param tarballname: string : when uploading sandbox, taskname is not used but tarballname is needed
    :return: nothing. Raises an exception in case of error
    """
    api = 'cache'
    dataDict = {'subresource': 'upload', 'objecttype': objecttype}
    if taskname:
        dataDict['taskname'] = taskname
    if username:
        dataDict['username'] = username
    if tarballname:
        dataDict['tarballname'] = tarballname
    data = encodeRequest(dataDict)
    try:
        # calls to restServer alway return a 3-ple ({'result':'string'}, HTTPcode, HTTPreason)
        # this returns in result.value a 2-element list (url, dictionay)
        res = crabserver.get(api, data)
        result = res[0]['result']
    except Exception as e:
        raise Exception('Failed to get PreSignedURL from CRAB Server:\n%s' % str(e))
    # prepare a single dictionary with curl arguments for the actual upload
    url = result[0]
    if not url and objecttype == 'sandbox':
        # in this case a null string as url indicates that sandbox with this name is there already
        logger.debug("%s %s is already in the S3 store, will not upload again", objecttype, tarballname)
        return
    fields = result[1]
    preSignedUrl = {'url': url}
    preSignedUrl.update(fields)
    try:
        uploadToS3ViaPSU(filepath=filepath, preSignedUrlFields=preSignedUrl, logger=logger)
    except Exception as e:
        raise Exception('Upload to S3 failed\n%s' % str(e))
    logger.debug('%s %s successfully uploaded to S3', objecttype, filepath)


def getDownloadUrlFromS3(crabserver=None, objecttype=None, taskname=None,
                         username=None, tarballname=None, logger=None):
    """
    obtains a PreSigned URL to access an existing object in S3
    :param crabserver: a RESTInteraction/CRABRest object : points to CRAB Server to use
    :param objecttype: string : the kind of object to retrieve: clientlog|twlog|sandbox|debugfiles|runtimefiles
    :param taskname: string : the task this object belongs to, if applicable
    :param username: string : the username this sandbox belongs to, in case objecttype=sandbox
    :param tarballname: string : for sandbox, taskname is not used but tarballname is needed
    :return: a (short lived) pre-signed URL to use e.g. in a wget
    """
    api = 'cache'
    dataDict = {'subresource': 'download', 'objecttype': objecttype}
    if taskname:
        dataDict['taskname'] = taskname
    if username:
        dataDict['username'] = username
    if tarballname:
        dataDict['tarballname'] = tarballname
    data = encodeRequest(dataDict)
    try:
        # calls to restServer alway return a 3-ple ({'result':a-list}, HTTPcode, HTTPreason)
        res = crabserver.get(api, data)
        result = res[0]['result'][0]
    except Exception as e:
        raise Exception('Failed to retrieve S3 preSignedUrl via CRABServer:\n%s' % str(e))
    logger.debug("PreSignedUrl to download %s received OK", objecttype)
    return result


def uploadToS3ViaPSU(filepath=None, preSignedUrlFields=None, logger=None):
    """
    uploads a file to s3.cern.ch usign PreSigned URLs obtained e.g. from a call to
    crabserver RESTCache API /crabserver/prod/cache?subresource=upload
    based on env.variable 'CRAB_useGoCurl' presence, implementation can use plain curl
    or gocurl to execute upload to S3 command
    :param filepath: string : the full path to a file to upload
    :param preSignedUrlFields: dictionary: a dictionary with the needed fields as
             returned from boto3 client when generating preSignedUrls:
             dictionary keys are: AWSAccessKeyId, policy, signature, key, url
             see e.g. https://boto3.amazonaws.com/v1/documentation/api/1.9.185/guide/s3-presigned-urls.html
             and https://gist.github.com/henriquemenezes/61ab0a0e5b54d309194c
    :return: nothing. Raises an exception in case of error
    """

    # validate args
    if not filepath:
        raise Exception("mandatory filepath argument missing")
    if not os.path.exists(filepath):
        raise Exception("filepath argument points to non existing file")
    if not preSignedUrlFields:
        raise Exception("mandatory preSignedUrlFields argument missing")

    # parse field disctionary into a list of arguments for curl
    AWSAccessKeyId = preSignedUrlFields['AWSAccessKeyId']
    signature = preSignedUrlFields['signature']
    policy = preSignedUrlFields['policy']
    key = preSignedUrlFields['key']
    url = preSignedUrlFields['url']   # N.B. this contains the bucket name e.g. https://s3.cern.ch/bucket1

    userAgent = 'CRAB'
    uploadCommand = ''

    # CRAB_useGoCurl env. variable is used to define how upload to S3 command should be executed.
    # If variable is set, then goCurl is used for command execution: https://github.com/vkuznet/gocurl
    # The same variable is also used inside CRABClient, we should keep name changes (if any) synchronized
    if os.getenv('CRAB_useGoCurl'):
        uploadCommand += '/cvmfs/cms.cern.ch/cmsmon/gocurl -verbose 2 -method POST'
        uploadCommand += ' -header "User-Agent:%s"' % userAgent
        uploadCommand += ' -form "key=%s"' % key
        uploadCommand += ' -form "AWSAccessKeyId=%s"' % AWSAccessKeyId
        uploadCommand += ' -form "policy=%s"' % policy
        uploadCommand += ' -form "signature=%s"' % signature
        uploadCommand += ' -form "file=@%s"' % filepath
        uploadCommand += ' -url "%s"' % url
    else:
        # am I running in the CMSSW SCRAM environment (e.g. CRABClient) ?
        arch = os.getenv('SCRAM_ARCH')
        if arch and arch.startswith('slc6'):
            # make sure to use latest curl version
            uploadCommand += 'source /cvmfs/cms.cern.ch/slc6_amd64_gcc700/external/curl/7.59.0/etc/profile.d/init.sh; '

        # curl: -f flag makes it fail if server return HTTP error
        uploadCommand += 'curl -f -v -X POST '
        uploadCommand += ' -H "User-Agent: %s"' % userAgent
        uploadCommand += ' -F "key=%s"' % key
        uploadCommand += ' -F "AWSAccessKeyId=%s"' % AWSAccessKeyId
        uploadCommand += ' -F "policy=%s"' % policy
        uploadCommand += ' -F "signature=%s"' % signature
        uploadCommand += ' -F "file=@%s"' % filepath
        uploadCommand += ' "%s"' % url

    logger.debug('Will execute:\n%s', uploadCommand)
    uploadProcess = subprocess.Popen(uploadCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = uploadProcess.communicate()
    exitcode = uploadProcess.returncode
    logger.debug('exitcode: %s\nstdout: %s', exitcode, stdout)

    if exitcode != 0:
        raise Exception('Failed to upload file with %s. stderr is:\n%s' % (uploadCommand, stderr))


def downloadFromS3ViaPSU(filepath=None, preSignedUrl=None, logger=None):
    """
    More generic than the name implies:
    downloads a file usign PreSigned URLs obtained e.g. from a call to
    crabserver RESTCache API /crabserver/prod/cache?subresource=download
    based on env.variable 'CRAB_useGoCurl' presence, implementation can use wget
    or gocurl to execute download from S3 command
    :param filepath: string : the full path to the file to be created
        if the file exists already, it will be silently overwritten
    :param preSignedUrl: string : the URL to use (includes the host name)
    :return: nothing. Raises an exception in case of error
    """

    # validate args
    if not filepath:
        raise Exception("mandatory filepath argument missing")
    if not preSignedUrl:
        raise Exception("mandatory preSignedUrl argument missing")

    downloadCommand = ''

    # CRAB_useGoCurl env. variable is used to define how download from S3 command should be executed.
    # If variable is set, then goCurl is used for command execution: https://github.com/vkuznet/gocurl
    # The same variable is also used inside CRABClient, we should keep name changes (if any) synchronized
    if os.getenv('CRAB_useGoCurl'):
        downloadCommand += '/cvmfs/cms.cern.ch/cmsmon/gocurl -verbose 2 -method GET'
        downloadCommand += ' -out "%s"' % filepath
        downloadCommand += ' -url "%s"' % preSignedUrl
    else:
        downloadCommand += 'wget -Sq -O %s ' % filepath
        downloadCommand += ' "%s"' % preSignedUrl

    downloadProcess = subprocess.Popen(downloadCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    logger.debug("Will execute:\n%s", downloadCommand)
    stdout, stderr = downloadProcess.communicate()
    exitcode = downloadProcess.returncode
    logger.debug('exitcode: %s\nstdout: %s', exitcode, stdout)

    if exitcode != 0:
        raise Exception('Download command %s failed. stderr is:\n%s' % (downloadCommand, stderr))
    if not os.path.exists(filepath):
        raise Exception("Download failure with %s. File %s was not created" % (downloadCommand, filepath))


class MeasureTime:
    """
    Context manager to measure how long a portion of code takes.
    It is intended to be used as:

    logger = logging.getLogger()
    with MeasureTime(logger, modulename=__name__, label="myfuncname") as _:
        myfuncname()

    You can access contextmanager's variable outside "with" statement to
    get the elapsed time and use in your own code like this:

    with MeasureTime() as mt:
        ret = c.executemany(None, *binds, **kwbinds)
    cherrypy.log("%s executemany time: %6f" % (trace, mt.perf_counter,))

    """

    def __init__(self, logger=None, modulename="", label=""):
        self.logger = logger
        self.modulename = modulename
        self.label = label
        self.perf_counter = None
        self.process_time = None
        self.thread_time = None
        self.readout = None

    def __enter__(self):
        self.perf_counter = time.perf_counter()
        self.process_time = time.process_time()
        self.thread_time = time.thread_time()
        return self

    def __exit__(self, t, v, tb):
        self.thread_time = time.thread_time() - self.thread_time
        self.process_time = time.process_time() - self.process_time
        self.perf_counter = time.perf_counter() - self.perf_counter
        self.readout = 'tot={:.4f} proc={:.4f} thread={:.4f}'.format(
            self.perf_counter, self.process_time, self.thread_time)
        if self.logger:
            self.logger.info("MeasureTime:seconds - modulename=%s label='%s' %s",
                             self.modulename, self.label, self.readout)


def measure_size(obj, logger=None, modulename="", label=""):
    with MeasureTime() as size_time:
        obj_size = get_size(obj)
    if logger:
        logger.info("MeasureSize:bytes - modulename=%s label='%s' obj_size=%d get_size_time=%.6f", modulename, label, obj_size, size_time.perf_counter)
    return obj_size


def get_size(obj, seen=None):
    """Recursively finds size of objects
    Copy from https://gist.github.com/bosswissam/a369b7a31d9dcab46b4a034be7d263b2#file-pysize-py"""

    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


def parseDBSInstance(dbsurl):
    """Parse `tm_dbs_url` to get dbsInstance in `<dbsenv>/<instance>` format.

    >>> url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    >>> parseDBSInstance(url)
    'prod/global'
    """
    return "{}/{}".format(dbsurl.split("//")[1].split("/")[2], dbsurl.split("//")[1].split("/")[3])


def isDatasetUserDataset(inputDataset, dbsInstance):
    """Return `True` if it is USER dataset.

    >>> dbs = 'prod/phys03'
    >>> dataset = '/JPsiToMuMuGun/JPsiToMuMuGun_Pt0To30withTail/USER'
    >>> isDatasetUserDataset(dataset, dbs)
    True
    """
    return (dbsInstance.split('/')[1] != 'global') and \
        (inputDataset.split('/')[-1] == 'USER')


def isEnoughRucioQuota(rucioClient, site, account=''):
    """
    Check quota with Rucio server.

    Return dict of result to construct message on caller, where
    - `hasQuota`: `True` if user has quota on that site
    - `isEnough`: `True` if user has remain quota more than `RUCIO_QUOTA_MINIMUM_GB`
    - `isQuotaWarning`: `True` if user has remain quota more than
      `RUCIO_QUOTA_MINIMUM_GB`, but less than `RUCIO_QUOTA_WARNING_GB`
    - `total`: (float) total quota in GiB
    - `used`: (float) used quota in GiB
    - `free`: (float) remain quota in GiB

    :param rucioClient: Rucio's client object
    :type rucioClient: rucio.client.client.Client
    :param site: RSE name, e.g. T2_CH_CERN
    :type site: str
    :param account: (optional) Rucio account name (for server)
    :type account: str

    :return: dict of result of quota checking (see details above)
    :rtype: dict
    """
    ret = {
        'hasQuota': False,
        'isEnough': False,
        'isQuotaWarning': False,
        'total': 0,
        'used': 0,
        'free': 0,
    }
    if not account:
        account = rucioClient.account
    quotas = list(rucioClient.get_local_account_usage(account, site))
    if quotas:
        ret['hasQuota'] = True
        quota = quotas[0]
        ret['total'] = quota['bytes_limit'] / 2 ** (10 * 3)  # GiB
        ret['used'] = quota['bytes'] / 2 ** (10 * 3)  # GiB
        ret['free'] = quota['bytes_remaining'] / 2 ** (10 * 3)  # GiB
        if ret['free'] > RUCIO_QUOTA_MINIMUM_GB:
            ret['isEnough'] = True
            if ret['free'] <= RUCIO_QUOTA_WARNING_GB:
                ret['isQuotaWarning'] = True
    return ret


def getRucioAccountFromLFN(lfn):
    """
    Extract Rucio account from LFN.
    For Rucio's group account, account always has `_group` suffix, but path and
    scope do not contains `_group` suffix.

    >>> getRucioAccountFromLFN(lfn='/store/user/rucio/cmsbot/path/to/dir')
    'cmsbot'
    >>> getRucioAccountFromLFN(lfn='/store/group/rucio/crab_test/path/to/dir')
    'crab_test_group'

    :param lfn: LFN
    :type lfn: str

    :return: Rucio's account
    :rtype: str
    """
    if lfn.startswith('/store/user/rucio') or lfn.startswith('/store/group/rucio'):
        account = lfn.split('/')[4]
        if lfn.startswith('/store/group/rucio/'):
            return "{0}_group".format(account)
        return account
    raise Exception("Expected /store/(user,group)/rucio/<account>, got {0}".format(lfn))
