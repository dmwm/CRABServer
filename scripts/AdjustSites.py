import os
import re
import sys
import time
import shutil
import traceback
import glob
import classad
import htcondor


if '_CONDOR_JOB_AD' not in os.environ or not os.path.exists(os.environ["_CONDOR_JOB_AD"]):
    sys.exit(0)


new_stdout = "adjust_out.txt"
fd = os.open(new_stdout, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0644)
if not os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
    os.dup2(fd, 1)
    os.dup2(fd, 2)
os.close(fd)

terminator_re = re.compile(r"^\.\.\.$")
event_re = re.compile(r"016 \(-?\d+\.\d+\.\d+\) \d+/\d+ \d+:\d+:\d+ POST Script terminated.")
term_re = re.compile(r"Normal termination \(return value 2\)")
node_re = re.compile(r"DAG Node: Job(\d+)")
def adjustPostScriptExitStatus(resubmit):
    """
    ...
    016 (146493.000.000) 11/11 17:45:46 POST Script terminated.
        (1) Normal termination (return value 1)
        DAG Node: Job105
    ...
    """
    if not resubmit:
        return
    resubmit_all = (resubmit == True)
    ra_buffer = []
    alt = None
    output = ''
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
                for l in ra_buffer: output += l
                output += line
                ra_buffer = []
        elif len(ra_buffer) == 2:
            m = term_re.search(line)
            if m:
                ra_buffer.append("        (1) Normal termination (return value 1)\n")
                alt = line
            else:
                for l in ra_buffer: output += l
                output += line
                ra_buffer = []
        elif len(ra_buffer) == 3:
            m = node_re.search(line)
            print line, m, m.groups(), resubmit
            if m and (resubmit_all or (m.groups()[0] in resubmit)):
                print m.groups()[0], resubmit
                for l in ra_buffer: output += l
            else:
                for l in ra_buffer[:-1]: output += l
                output += alt
            output += line
            ra_buffer = []
        else:
            output += line
    if ra_buffer:
        for l in ra_buffer: output += l
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


def adjustMaxRetries(resubmitJobIds, ad):
    """
    Edit the DAG file adjusting the maximum allowed number of retries to the
    current retry + CRAB_NumAutomJobRetries for the job ids passed in the
    resubmitJobIds argument.
    """
    if not resubmitJobIds:
        return
    if not os.path.exists("RunJobs.dag"):
        return
    ## Search for the RETRY directives in the DAG file for the job ids passed in the
    ## resubmitJobIds argument and change the maximum retries to the current retry
    ## count + CRAB_NumAutomJobRetries.
    retry_re = re.compile(r'RETRY Job([0-9]+) ([0-9]+) ')
    output = ""
    resubmitAll = (resubmitJobIds == True)
    numAutomJobRetries = int(ad.get('CRAB_NumAutomJobRetries', 2))
    with open("RunJobs.dag", 'r') as fd:
        for line in fd.readlines():
            match_retry_re = retry_re.search(line)
            if match_retry_re:
                jobId = match_retry_re.groups()[0]
                if resubmitAll or (jobId in resubmitJobIds):
                    try:
                        maxRetries = int(match_retry_re.groups()[1]) + (1 + numAutomJobRetries)
                    except ValueError:
                        maxRetries = numAutomJobRetries
                    line = retry_re.sub(r'RETRY Job%s %d ' % (jobId, maxRetries), line)
            output += line
    with open("RunJobs.dag", 'w') as fd:
        fd.write(output)


def make_webdir(ad):
    path = os.path.expanduser("~/%s" % ad['CRAB_ReqName'])
    try:
        try:
            os.makedirs(path)
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", "job_log")), os.path.join(path, "jobs_log.txt"))
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", "node_state")), os.path.join(path, "node_state.txt"))
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", "site.ad")), os.path.join(path, "site_ad.txt"))
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", "aso_status.json")), os.path.join(path, "aso_status.json"))
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", ".job.ad")), os.path.join(path, "job_ad.txt"))
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", "error_summary.json")), os.path.join(path, "error_summary.json"))
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", "RunJobs.dag")), os.path.join(path, "RunJobs.dag"))
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", "RunJobs.dag.dagman.out")), os.path.join(path, "RunJobs.dag.dagman.out"))
        except:
            pass
        try:
            os.symlink(os.path.abspath(os.path.join(".", "RunJobs.dag.nodes.log")), os.path.join(path, "RunJobs.dag.nodes.log"))
        except:
            pass
        try:
            shutil.copy2(os.path.join(".", "sandbox.tar.gz"), os.path.join(path, "sandbox.tar.gz"))
        except:
            pass
    except OSError:
        pass
    try:
        storage_rules = htcondor.param['CRAB_StorageRules']
    except:
        storage_rules = "^/home/remoteGlidein,http://submit-5.t2.ucsd.edu/CSstoragePath"
    sinfo = storage_rules.split(",")
    storage_re = re.compile(sinfo[0])
    val = storage_re.sub(sinfo[1], path)
    ad['CRAB_UserWebDir'] = val
    id = '%d.%d' % (ad['ClusterId'], ad['ProcId'])
    try:
        htcondor.Schedd().edit([id], 'CRAB_UserWebDir', ad.lookup('CRAB_UserWebDir'))
    except RuntimeError, reerror:
        print str(reerror)

def make_job_submit(ad):
    count = ad['CRAB_JobCount']
    for i in range(1, count+1):
        shutil.copy("Job.submit", "Job.%d.submit" % i)

def clear_automatic_blacklist(ad):
    for file in glob.glob("task_statistics.*"):
        try:
            os.unlink(file)
        except Exception, e:
            print "ERROR when clearing statistics: %s" % str(e)

def updatewebdir(ad):
    data = {'subresource' : 'addwebdir'}
    host = ad['CRAB_RestHost']
    uri = ad['CRAB_RestURInoAPI'] + '/task'
    data['workflow'] = ad['CRAB_ReqName']
    data['webdirurl'] = ad['CRAB_UserWebDir']
    cert = ad['X509UserProxy']
    try:
        from RESTInteractions import HTTPRequests
        from httplib import HTTPException
        import urllib
        server = HTTPRequests(host, cert, cert)
        server.post(uri, data = urllib.urlencode(data))
        return 0
    except:
        print traceback.format_exc()
        return 1

def main():
    ad = classad.parseOld(open(os.environ['_CONDOR_JOB_AD']))
    make_webdir(ad)
    make_job_submit(ad)

    retries = 0
    exit_code = 1
    while retries < 3 and exit_code != 0:
        exit_code = updatewebdir(ad)
        if exit_code != 0:
            time.sleep(retries*20)
        retries += 1

    clear_automatic_blacklist(ad)

    resubmitJobIds = []
    if 'CRAB_ResubmitList' in ad:
        resubmitJobIds = set(ad['CRAB_ResubmitList'])
        id = '%d.%d' % (ad['ClusterId'], ad['ProcId'])
        ad['foo'] = []
        try:
            htcondor.Schedd().edit([id], 'CRAB_ResubmitList', ad['foo'])
        except RuntimeError, reerror:
            print "ERROR: %s" % str(reerror)
    if resubmitJobIds != True:
        resubmitJobIds = [str(i) for i in resubmitJobIds]
    if resubmitJobIds:
        if hasattr(htcondor, 'lock'):
            # While dagman is not running at this point, the schedd may be writing events to this
            # file; hence, we only edit the file while holding an appropriate lock.
            # Note this lock method didn't exist until 8.1.6; prior to this, we simply
            # run dangerously.
            with htcondor.lock(open("RunJobs.dag.nodes.log", "a"), htcondor.LockType.WriteLock) as lock:
                adjustPostScriptExitStatus(resubmitJobIds)
        else:
            adjustPostScriptExitStatus(resubmitJobIds)
        adjustMaxRetries(resubmitJobIds, ad)

    if 'CRAB_SiteAdUpdate' in ad:
        new_site_ad = ad['CRAB_SiteAdUpdate']
        with open("site.ad") as fd:
            site_ad = classad.parse(fd)
        site_ad.update(new_site_ad)
        with open("site.ad", "w") as fd:
            fd.write(str(site_ad))
        id = '%d.%d' % (ad['ClusterId'], ad['ProcId'])
        ad['foo'] = []
        try:
            ## Is CRAB_ResubmitList the attribute we want to edit ?
            ## Or is it CRAB_SiteAdUpdate ?
            htcondor.Schedd().edit([id], 'CRAB_ResubmitList', ad['foo'])
        except RuntimeError, reerror:
            print "ERROR: %s" % str(reerror)


if __name__ == '__main__':
    main()

