
import os
import re
import sys
import classad

if '_CONDOR_JOB_AD' not in os.environ or not os.path.exists(os.environ["_CONDOR_JOB_AD"]):
    sys.exit(0)

ad = classad.parseOld(open(os.environ['_CONDOR_JOB_AD']))

blacklist = set()
if 'CRAB_SiteBlacklist' in ad:
    blacklist = set(ad['CRAB_SiteBlacklist'])

whitelist = set()
if 'CRAB_SiteWhitelist' in ad:
    whitelist = set(ad['CRAB_SiteWhitelist'])

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
    else:
        output_fd.write(line)

