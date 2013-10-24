
#!/usr/bin/python

import os
import re
import sys
import time
import errno
import signal
import commands

import classad
import htcondor

import TaskWorker.Actions.ASO as ASO

import WMCore.Services.PhEDEx.PhEDEx as PhEDEx

import AdderCmsPlugin

from WMCore.Configuration import Configuration
adder_config = Configuration()
adder_config.section_("Adder")
adder_config.Adder.logfile = "./aso.log"
# TODO: Figure out how we will bootstrap auth
adder_config.Adder.authfile = os.path.expanduser("~/aso_auth_file.txt")
adder_config.Adder.skipcache = True
# Insert our config file into the Adder plugin
AdderCmsPlugin.config = adder_config

fake_results = False

# The next three classes are used to mock-up the interaction necessary for AdderCmsPlugin
class FakeFile:

    def __init__(self, lfn, fsize, checksum, type, dest_site):
        self.lfn = lfn
        self.fsize = fsize
        self.checksum = checksum
        self.type = type
        self.destinationSE = dest_site


class FakeJob:

    def __init__(self, id, fjr_json, job_args, input_dataset, publish_dataset, computingSite, dn):
        self.jobStatus = "success"
        self.metadata = fjr_json
        self.PandaID = id
        self.Files = []
        self.jobParameters = str(job_args)
        self.destinationDBlock = publish_dataset
        self.computingSite = computingSite
        self.prodUserID = dn
        self.prodDBlock = input_dataset

    def addFile(self, file):
        self.Files.append(file)


class FakeResult:

    def __init__(self):
        self.transferringFiles = []
        self.statusCode = 0

    def setSucceeded(self):
        self.statusCode = 0


def fix_perms(count):
    """
    HTCondor will default to a umask of 0077 for stdout/err.  When the DAG finishes,
    the schedd will chown the sandbox to the user which runs HTCondor.

    This prevents the user who owns the DAGs from retrieving the job output as they
    are no longer world-readable.
    """
    for base_file in ["job_err", "job_out"]:
        try:
            os.chmod("%s.%s" % (base_file, count), 0644)
        except OSError, oe:
            if oe.errno != errno.ENOENT and oe.errno != errno.EPERM:
                raise

def async_stageout(dest_site, source_dir, dest_dir, count, job_id, *filenames, **kwargs):

    print "LFNs for async stageout: %s" % ", ".join(filenames)

    # Fix permissions of output files.
    fix_perms(count)

    # Here's the magic.  Pull out the site the job ran at from its user log
    if 'source_site' not in kwargs:
        cmd = "condor_q -userlog job_log.%s -af MATCH_EXP_JOBGLIDEIN_CMSSite -af JOBGLIDEIN_CMSSite" % count
        status, output = commands.getstatusoutput(cmd)
        if status:
            print "Failed to query condor user log:\n%s" % output
            return 1
        match_site, source_site = output.split('\n')[0].split(" ", 1)
        # TODO: Testing mode.  If CMS site is not known, assume Nebraska
        if match_site == 'Unknown' or source_site == 'Unknown':
            source_site = 'T2_US_Nebraska'
    else:
        source_site = kwargs['source_site']

    with open(os.environ['_CONDOR_JOB_AD'], 'r') as fd:
        my_ad = classad.parseOld(fd)
    # Query for the parent job
    if fake_results:
        with open(os.path.join(os.environ['CRAB3_TEST_BASE'], "test/data/Actions/dag_job_completed")) as fd:
            job_ad = classad.parseOld(fd)
    else:
        schedd = htcondor.Schedd()
        job_ad = schedd.query('(CRAB_Id =?= %s) && (CRAB_ReqName =?= %s) && (CRAB_JobType =?= "Analysis")' % (my_ad['CRAB_Id'], my_ad.lookup('CRAB_ReqName')))

    fjr_json = ""
    json_filename = "jobReport.json.%(id)s"
    if fake_results:
        json_filename = os.path.join(os.environ['CRAB3_TEST_BASE'], "test/data/Actions/dag_job_completed_json")
    with open(json_filename % {'id': job_ad['CRAB_Id']}) as fd:
        fjr_json = fd.read()

    job = FakeJob("%s.%s" % (job_ad['ClusterID'], job_ad['ProcID']),
                  fjr_json,
                  job_ad['Arguments'],
                  job_ad['CRAB_InputData'],
                  job_ad['CRAB_OutputData'],
                  source_site,
                  job_ad['CRAB_UserDN'])

    transfer_list = ASO.resolvePFNs(source_site, dest_site, source_dir, dest_dir, filenames)
    for source, dest in transfer_list:
        print "Copying %s to %s" % (source, dest)

    source_list = [i[0] for i in transfer_list]
    sizes = ASO.determineSizes(source_list)

    for idx in range(len(filenames)):
        outfile = filenames[idx]
        size = sizes[idx]
        fakefile = FakeFile(os.path.join(source_dir, outfile),
                            size, "", "output", dest_site)
        job.addFile(fakefile)

    plugin = AdderCmsPlugin.AdderCmsPlugin(job)
    plugin.job = job
    plugin.result = FakeResult()

    plugin.execute()

    return plugin.result.statusCode

if __name__ == '__main__':
    sys.exit(async_stageout("T2_US_Nebraska", '/store/user/bbockelm/crab_bbockelm_crab3_1', '/store/user/bbockelm', '1', 'dumper_16.root', 'dumper_17.root', source_site='T2_US_Nebraska'))

