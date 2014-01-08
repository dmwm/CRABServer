
import os
import re
import sys
import shutil
import traceback

import classad
import htcondor

if '_CONDOR_JOB_AD' not in os.environ or not os.path.exists(os.environ["_CONDOR_JOB_AD"]):
    sys.exit(0)

terminator_re = re.compile(r"^\.\.\.$")
event_re = re.compile(r"016 \(\d+\.\d+\.\d+\) \d+/\d+ \d+:\d+:\d+ POST Script terminated.")
term_re = re.compile(r"Normal termination \(return value 2\)")
node_re = re.compile(r"DAG Node: Job(\d+)")
def adjustPost(resubmit):
    """
    ...
    016 (146493.000.000) 11/11 17:45:46 POST Script terminated.
        (1) Normal termination (return value 1)
        DAG Node: Job105
    ...
    """
    if not resubmit:
        return
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
            if m and (m.groups()[0] in resubmit):
                print m.groups()[0], resubmit
                for l in ra_buffer: output += l
            else:
                for l in ra_buffer[:-1]: output += l
                output += alt
            output += line
            ra_buffer = []
        else:
            output += line
    output_fd = open("RunJobs.dag.nodes.log", "w")
    output_fd.write(output)
    output_fd.close()

def resubmitDag(filename, resubmit):
    if not os.path.exists(filename):
        return
    retry_re = re.compile(r'RETRY Job([0-9]+) ([0-9]+) ')
    output = ""
    for line in open(filename).readlines():
        m = retry_re.search(line)
        if m:
            job_id = m.groups()[0]
            if job_id in resubmit:
                try:
                    retry_count = int(m.groups()[1]) + 10
                except ValueError:
                    retry_count = 10
                line = retry_re.sub(r'RETRY Job%s %d ' % (job_id, retry_count), line)
        output += line
    output_fd = open(filename, 'w')
    output_fd.write(output)
    output_fd.close()

def make_webdir(ad):
    path = os.path.expanduser("~/%s" % ad['CRAB_ReqName'])
    try:
        os.makedirs(path)
        os.symlink(os.path.abspath(os.path.join(".", "job_log")), os.path.join(path, "jobs_log.txt"))
        os.symlink(os.path.abspath(os.path.join(".", "node_state")), os.path.join(path, "node_state.txt"))
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
    try:
        fd = open(os.environ['_CONDOR_JOB_AD'], 'a')
        fd.write('CRAB_UserWebDir = %s\n' % ad.lookup('CRAB_UserWebDir'))
    except:
        print traceback.format_exc()

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

def main():
    ad = classad.parseOld(open(os.environ['_CONDOR_JOB_AD']))
    make_webdir(ad)
    make_job_submit(ad)

    clear_automatic_blacklist(ad)

    blacklist = set()
    if 'CRAB_SiteBlacklist' in ad:
        blacklist = set(ad['CRAB_SiteBlacklist'])

    whitelist = set()
    if 'CRAB_SiteWhitelist' in ad:
        whitelist = set(ad['CRAB_SiteWhitelist'])

    resubmit = []
    if 'CRAB_ResubmitList' in ad:
        resubmit = set(ad['CRAB_ResubmitList'])
        id = '%d.%d' % (ad['ClusterId'], ad['ProcId'])
        ad['foo'] = []
        try:
            htcondor.Schedd().edit([id], 'CRAB_ResubmitList', ad['foo'])
        except RuntimeError, reerror:
            print "ERROR: %s" % str(reerror)
        # To do this right, we ought to look up how many existing retries were done
        # and adjust the retry account according to that.
    resubmit = [str(i) for i in resubmit]

    if resubmit:
        adjustPost(resubmit)
        resubmitDag("RunJobs.dag", resubmit)

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
            htcondor.Schedd().edit([id], 'CRAB_ResubmitList', ad['foo'])
        except RuntimeError, reerror:
            print "ERROR: %s" % str(reerror)

if __name__ == '__main__':
    main()

