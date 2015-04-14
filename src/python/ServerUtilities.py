"""
This contains some utility methods to share between the server and the client, or by the server components themself
"""

import os
import re


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


#setDashboardLogs function is shared between the postjob and the job wrapper. Sharing it here
def setDashboardLogs(params, webdir, jobid, retry):
    log_files = [("job_out", "txt"), ("job_fjr", "json"), ("postjob", "txt")]
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
