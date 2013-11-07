
import os
import re
import sys

import classad
import htcondor

if '_CONDOR_JOB_AD' not in os.environ or not os.path.exists(os.environ["_CONDOR_JOB_AD"]):
    sys.exit(0)

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

def main():
    ad = classad.parseOld(open(os.environ['_CONDOR_JOB_AD']))

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
            print str(reerror)
        # To do this right, we ought to look up how many existing retries were done
        # and adjust the retry account according to that.
    resubmit = [str(i) for i in resubmit]

    if resubmit:
        resubmitDag("RunJobs.dag.orig", resubmit)
        resubmitDag("RunJobs.dag", resubmit)

    desired_re = re.compile(r'DESIRED_Sites="\\"(.*?)\\""')
    split_re = re.compile(",\s*")

    if not os.path.exists('RunJobs.dag.orig'):
        os.rename('RunJobs.dag', 'RunJobs.dag.orig')
    output_fd = open('RunJobs.dag', 'w')
    for line in open('RunJobs.dag.orig').readlines():
        if line.startswith('VARS '):
            m = desired_re.search(line)
            if m:
                orig_sites = m.groups()[0]
                orig_sites = set(split_re.split(orig_sites))
                if whitelist:
                    orig_sites = orig_sites & whitelist
                orig_sites = orig_sites - blacklist
                line = desired_re.sub(r'DESIRED_Sites="\"%s\""' % (", ".join(orig_sites)), line)
        output_fd.write(line)

if __name__ == '__main__':
    main()

