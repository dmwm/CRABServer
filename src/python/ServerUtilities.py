"""
This contains some utility methods to share between the server and the client, or by the server components themself
"""

from __future__ import print_function

import os
import re
import traceback
import subprocess
from httplib import HTTPException

from RESTInteractions import HTTPRequests

FEEDBACKMAIL = 'hn-cms-computing-tools@cern.ch'

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
        subprocess.Popen("/usr/bin/which %s" % cmd, stdout=null, stderr=null)
        null.close()
        return True
    except OSError:
        return False
