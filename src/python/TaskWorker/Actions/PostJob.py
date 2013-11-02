
#!/usr/bin/python

import os
import re
import sys
import time
import json
import errno
import shutil
import signal
import urllib
import commands

import classad

import WMCore.Services.PhEDEx.PhEDEx as PhEDEx

from RESTInteractions import HTTPRequests
from httplib import HTTPException

import RetryJob

fts_server = 'https://fts3-pilot.cern.ch:8443'

g_Job = None

def sighandler(*args):
    if g_Job:
        g_Job.cancel()

signal.signal(signal.SIGHUP,  sighandler)
signal.signal(signal.SIGINT,  sighandler)
signal.signal(signal.SIGTERM, sighandler)

REGEX_ID = re.compile("([a-f0-9]{8,8})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{12,12})")

class FTSJob(object):

    def __init__(self, transfer_list, count):
        self._id = None
        self._cancel = False
        self._sleep = 20
        self._transfer_list = transfer_list
        self._count = count
        with open("copyjobfile_%s" % count, "w") as fd:
            for source, dest in transfer_list:
                fd.write("%s %s\n" % (source, dest))

    def cancel(self):
        if self._id:
            cmd = "glite-transfer-cancel -s %s %s" % (fts_server, self._id)
            print "+", cmd
            os.system(cmd)

    def submit(self):
        cmd = "glite-transfer-submit -o -s %s -f copyjobfile_%s" % (fts_server, self._count)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-submit exited with status %s.\n%s" % (status, output))
        output = output.strip()
        print "Resulting transfer ID: %s" % output
        return output

    def status(self, long_status=False):
        if long_status:
            cmd = "glite-transfer-status -l -s %s %s" % (fts_server, self._id)
        else:
            cmd = "glite-transfer-status -s %s %s" % (fts_server, self._id)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-status exited with status %s.\n%s" % (status, output))
        return output.strip()

    def run(self):
        self._id = self.submit()
        if not REGEX_ID.match(self._id):
            raise Exception("Invalid ID returned from FTS transfer submit")
        while True:
            time.sleep(self._sleep)
            status = self.status()
            print status

            if status in ['Submitted', 'Pending', 'Ready', 'Active', 'Canceling', 'Hold']:
                continue

            #if status in ['Done', 'Finished', 'FinishedDirty', 'Failed', 'Canceled']:
            #TODO: Do parsing of "-l"
            if status in ['Done', 'Finished']:
                return 0

            if status in ['FinishedDirty', 'Failed', 'Canceled']:
                print self.status(True)
                return 1


def determineSizes(transfer_list):
    sizes = []
    for pfn in transfer_list:
        cmd = "lcg-ls -D srmv2 -b -l %s" % pfn
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            sizes.append(-1)
            continue
        info = output.split("\n")[0].split()
        if len(info) < 5:
            print "Invalid lcg-ls output:\n%s" % output
            sizes.append(-1)
            continue
        try:
            sizes.append(info[4])
        except ValueError:
            print "Invalid lcg-ls output:\n%s" % output
            sizes.append(-1)
            continue
    return sizes


def reportResults(job_id, dest_list, sizes):
    filtered_dest = [dest_list[i] for i in range(len(dest_list)) if sizes[i] >= 0]
    filtered_sizes = [i for i in sizes if i >= 0]
    retval = 0

    cmd = 'condor_qedit %s OutputSizes "\\"%s\\""' % (job_id, ",".join(filtered_sizes))
    print "+", cmd
    status, output = commands.getstatusoutput(cmd)
    if status:
        retval = status
        print output

    cmd = 'condor_qedit %s OutputPFNs "\\"%s\\""' % (job_id, ",".join(filtered_dest))
    print "+", cmd
    status, output = commands.getstatusoutput(cmd)
    if status:
        retval = status
        print output

    return retval


def resolvePFNs(source_site, dest_site, source_dir, dest_dir, filenames):

    p = PhEDEx.PhEDEx()
    lfns = [os.path.join(source_dir, filename) for filename in filenames]
    lfns += [os.path.join(dest_dir, filename) for filename in filenames]
    dest_info = p.getPFN(nodes=[source_site, dest_site], lfns=lfns)

    results = []
    for filename in filenames:
        slfn = os.path.join(source_dir, filename)
        dlfn = os.path.join(dest_dir, filename)
        if (source_site, slfn) not in dest_info:
            print "Unable to map LFN %s at site %s" % (slfn, source_site)
        if (dest_site, dlfn) not in dest_info:
            print "Unable to map LFN %s at site %s" % (dlfn, dest_site)
        results.append((dest_info[source_site, slfn], dest_info[dest_site, dlfn]))
    return results


REQUIRED_ATTRS = ['CRAB_ReqName', 'CRAB_Id', 'CRAB_OutputData', 'CRAB_JobSW', 'CRAB_AsyncDest']

class PostJob():


    def __init__(self):
        self.ad = None
        self.crab_id = -1
        self.report = None
        self.output = None
        self.input = None
        self.outputFiles = []


    def makeAd(self, reqname, id, outputdata, sw, async_dest):
        self.ad = classad.ClassAd()
        self.ad['CRAB_ReqName'] = reqname
        self.ad['CRAB_Id'] = id
        self.ad['CRAB_OutputData'] = outputdata
        self.ad['CRAB_JobSW'] = sw
        self.ad['CRAB_AsyncDest'] = async_dest
        self.crab_id = int(self.ad['CRAB_Id'])


    def parseJson(self):
        with open("jobReport.json.%d" % self.crab_id) as fd:
            self.full_report = json.load(fd)

        if 'steps' not in self.full_report or 'cmsRun' not in self.full_report['steps']:
            raise ValueError("Invalid jobReport.json: missing cmsRun")
        self.report = self.full_report['steps']['cmsRun']

        if 'input' not in self.report or 'output' not in self.report:
            raise ValueError("Invalid jobReport.json: missing input/output")
        self.output = self.report['output']
        self.input = self.report['input']

        for outputModule in self.output.values():
            for outputFile in outputModule:
                if outputFile.get("output_module_class") != 'PoolOutputModule':
                    continue
                fileInfo = {}
                self.outputFiles.append(fileInfo)

                fileInfo['inparentlfns'] = outputFile.get("input", [])

                fileInfo['events'] = outputFile.get("events", -1)
                fileInfo['checksums'] = outputFile.get("checksums", {"cksum": "0", "adler32": "0"})

                if 'runs' not in outputFile:
                    continue
                fileInfo['outfileruns'] = []
                fileInfo['outfilelumis'] = []
                for run, lumis in outputFile['runs']:
                    for lumi in lumis:
                        fileInfo['outfileruns'].append(run)
                        fileInfo['outfilelumis'].append(lumi)


    def fixPerms(self):
        """
        HTCondor will default to a umask of 0077 for stdout/err.  When the DAG finishes,
        the schedd will chown the sandbox to the user which runs HTCondor.

        This prevents the user who owns the DAGs from retrieving the job output as they
        are no longer world-readable.
        """
        for base_file in ["job_err", "job_out"]:
            try:
                os.chmod("%s.%d" % (base_file, self.crab_id), 0644)
            except OSError, oe:
                if oe.errno != errno.ENOENT and oe.errno != errno.EPERM:
                    raise


    def upload(self):
        for fileInfo in self.outputFiles:
            configreq = {"taskname":        self.ad['CRAB_ReqName'],
                         "outfilelumis":    fileInfo['outfilelumis'],
                         "inparentlfns":    fileInfo['inparentlfns'],
                         "globalTag":       fileInfo['globalTag'],
                         "outfileruns":     fileInfo['outfileruns'],
                         "pandajobid":      self.crab_id,
                         "outsize":         fileInfo['outsize'],
                         "publishdataname": self.ad['CRAB_OutputData'],
                         "appver":          self.ad['CRAB_JobSW'],
                         "outtype":         "EDM", # Not implemented
                         "checksummd5":     "asda", # Not implemented; garbage value taken from ASO
                         "checksumcksum":   fileInfo['checksums']['cksum'],
                         "checksumadler32": fileInfo['checksums']['adler32'],
                         "outlocation":     fileInfo['outlocation'], 
                         "outtmplocation":  fileInfo['outdatasetname'],
                         "acquisitionera":  "null", # Not implemented
                         "outlfn":          fileInfo['outlfn'],
                         "events":          fileInfo['events'],
                    }
            print self.resturl, urllib.urlencode(configreq)
            self.server.put(self.resturl, data = urllib.urlencode(configreq))


    def uploadFakeLog(self, state="TRANSFERRING"):
        # Upload a fake log status
        configreq = {"taskname":        self.ad['CRAB_ReqName'],
                     "outfileruns":     fileInfo['outfileruns'],
                     "pandajobid":      self.crab_id,
                     "outsize":         "0",
                     "publishdataname": self.ad['CRAB_OutputData'],
                     "appver":          self.ad['CRAB_JobSW'],
                     "outtype":         "FAKE", # Not implemented
                     "checksummd5":     "asda", # Not implemented
                     "checksumcksum":   "3701783610", # Not implemented
                     "checksumadler32": "6d1096fe", # Not implemented
                     "outlocation":     "T2_US_Nowhere",
                     "outtmplocation":  "T2_US_Nowhere",
                     "acquisitionera":  "null", # Not implemented
                     "events":          "0",
                     "outlfn":          "/store/user/fakefile/FakeDataset/FakePublish/5b6a581e4ddd41b130711a045d5fecb9/1/log/fake.log.%s" % str(self.crab_id),
                     "outdatasetname":  "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER",
                     "filestate":       state,
                    }
        print self.resturl, urllib.urlencode(configreq)
        try:
            self.server.put(self.resturl, data = urllib.urlencode(configreq))
        except HTTPException, hte:
            if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
               not hte.headers.get('X-Error-Http', -1) == '400':
                   raise 
            self.uploadState(state)

    def uploadState(self, state):
        # Record this job as a permanent failure
        configreq = {"taskname":  self.ad['CRAB_ReqName'],
                     "filestate": state,
                     "outlfn":    "/store/user/fakefile/FakeDataset/FakePublish/5b6a581e4ddd41b130711a045d5fecb9/1/log/fake.log.%s" % str(self.crab_id),
                    }
        self.server.post(self.resturl, data = urllib.urlencode(configreq))
        return 2


    def getSourceSite(self):
        cmd = "condor_q -const true -userlog job_log.%d -af JOBGLIDEIN_CMSSite" % self.crab_id
        status, output = commands.getstatusoutput(cmd)
        if status:
            print "Failed to query condor user log:\n%s" % output
            return 1
        source_site = output.split('\n')[-1].strip()
        print "Determined a source site of %s from the user log" % source_site
        if source_site == 'Unknown' or source_site == "undefined":
            # Didn't find it the first time, try looking in the jobReport.json
            if self.full_report.get('executed_site', None):
                print "Getting source_site from jobReport"
                source_site = self.full_report['executed_site']
            else:
                # TODO: Testing mode. If nothing turns up, just say it wwas
                #       Nebraska
                print "Site was unknown, so we just guessed Nebraska ..."
                source_site = 'T2_US_Nebraska'
        return source_site


    def stageout(self, source_dir, dest_dir, *filenames):
        self.dest_site = self.ad['CRAB_AsyncDest']
        source_site = self.getSourceSite()
        self.source_site = source_site

        transfer_list = resolvePFNs(source_site, self.dest_site, source_dir, dest_dir, filenames)
        for source, dest in transfer_list:
            print "Copying %s to %s" % (source, dest)

        # Skip the first file - it's a tarball of the stdout/err
        for outfile in zip(filenames[1:], self.outputFiles, transfer_list):
            outlfn = os.path.join(dest_dir, outfile[0])
            outfile[1]['outlfn'] = outlfn
            outfile[1]['outtmplocation'] = outfile[2][0]
            outfile[1]['outlocation'] = outfile[2][1]

        global g_Job
        g_Job = FTSJob(transfer_list, self.crab_id)
        fts_job_result = g_Job.run()

        source_list = [i[0] for i in transfer_list]
        print "Source list", source_list
        dest_list = [i[1] for i in transfer_list]
        print "Dest list", dest_list
        source_sizes = determineSizes(source_list)
        print "Source sizes", source_sizes
        dest_sizes = determineSizes(dest_list)
        print "Dest sizes", dest_sizes
        sizes = zip(source_sizes, dest_sizes)

        failures = len([i for i in sizes if (i[1]<0 or (i[0] != i[1]))])
        if failures:
            raise RuntimeError("There were %d failed stageout attempts" % failures)

        return fts_job_result


    def execute(self, status, retry_count, max_retries, restinstance, resturl, reqname, id, outputdata, sw, async_dest, source_dir, dest_dir, *filenames):

        fd = os.open("postjob.%s" % id, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0755)
        os.dup2(fd, 1)
        os.dup2(fd, 2)

        stdout = "job_out.%s" % id
        stderr = "job_err.%s" % id
        if os.path.exists(stdout):
            shutil.copy(stdout, stdout+"."+retry_count)
        if os.path.exists(stderr):
            shutil.copy(stderr, stderr+"."+retry_count)

        if 'X509_USER_PROXY' not in os.environ:
            return 10

        self.server = HTTPRequests(restinstance, os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY'])
        self.resturl = resturl

        self.makeAd(reqname, id, outputdata, sw, async_dest)

        status = int(status)

        self.uploadFakeLog(state="TRANSFERRING")

        print "Retry count %s; max retry %s" % (retry_count, max_retries)
        if status and (retry_count == max_retries):
            # This was our last retry and it failed.
            return self.uploadState("FAILED")

        retry = RetryJob.RetryJob()
        retval = retry.execute(status, retry_count, max_retries, self.crab_id)
        if retval:
           if retval == RetryJob.FATAL_ERROR:
               return self.uploadState("FAILED")
           return retval

        self.parseJson()

        self.fixPerms()
        self.stageout(source_dir, dest_dir, *filenames)
        self.upload()
        self.uploadFakeLog(state="FINISHED")

        return 0

if __name__ == '__main__':
    pj = PostJob()
    sys.exit(pj.execute(*sys.argv[2:]))

