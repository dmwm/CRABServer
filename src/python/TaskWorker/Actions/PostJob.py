
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
        cmd = "lcg-ls --srm-timeout 300 --connect-timeout 300 --sendreceive-timeout 60 -D srmv2 -b -l %s" % pfn
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


def resolvePFNs(dest_site, source_dir, dest_dir, source_sites, filenames):

    p = PhEDEx.PhEDEx()
    lfns = [os.path.join(source_dir, filename) for filename in filenames]
    lfns += [os.path.join(dest_dir, filename) for filename in filenames]
    dest_info = p.getPFN(nodes=(source_sites + [dest_site]), lfns=lfns)

    results = []
    found_log = False
    for source_site, filename in zip(source_sites, filenames):
        if not found_log and filename.startswith("cmsRun") and (filename[-7:] == ".tar.gz"):
            slfn = os.path.join(source_dir, "log", filename)
            dlfn = os.path.join(dest_dir, "log", filename)
        found_log = True
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
                print outputFile
                # Note incorrect spelling of 'output module' in current WMCore
                fileInfo = {}
                if outputFile.get(u"output_module_class") == u'PoolOutputModule' or \
                        outputFile.get(u"ouput_module_class") == u'PoolOutputModule':
                    fileInfo['filetype'] = "EDM"
                elif outputFile.get(u"Source"):
                    fileInfo['filetype'] = "TFILE"
                else:
                    continue
                self.outputFiles.append(fileInfo)

                fileInfo['inparentlfns'] = [str(i) for i in outputFile.get(u"input", [])]

                fileInfo['events'] = outputFile.get(u"events", -1)
                fileInfo['checksums'] = outputFile.get(u"checksums", {"cksum": "0", "adler32": "0"})
                fileInfo['outsize'] = outputFile.get(u"size", 0)

                if u'SEName' in outputFile and self.node_map.get(str(outputFile['SEName'])):
                    fileInfo['outtmplocation'] = self.node_map[outputFile['SEName']]

                if u'runs' not in outputFile:
                    continue
                fileInfo['outfileruns'] = []
                fileInfo['outfilelumis'] = []
                for run, lumis in outputFile[u'runs'].items():
                    for lumi in lumis:
                        fileInfo['outfileruns'].append(str(run))
                        fileInfo['outfilelumis'].append(str(lumi))


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
                         "globalTag":       "None",
                         "pandajobid":      self.crab_id,
                         "outsize":         fileInfo['outsize'],
                         "publishdataname": self.ad['CRAB_OutputData'],
                         "appver":          self.ad['CRAB_JobSW'],
                         "outtype":         fileInfo['filetype'],
                         "checksummd5":     "asda", # Not implemented; garbage value taken from ASO
                         "checksumcksum":   fileInfo['checksums']['cksum'],
                         "checksumadler32": fileInfo['checksums']['adler32'],
                         "outlocation":     fileInfo['outlocation'], 
                         "outtmplocation":  fileInfo.get('outtmplocation', self.source_site),
                         "acquisitionera":  "null", # Not implemented
                         "outlfn":          fileInfo['outlfn'],
                         "events":          fileInfo['events'],
                         "outdatasetname":  "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER",
                    }
            configreq = configreq.items()
            for run in fileInfo['outfileruns']:
                configreq.append(("outfileruns", run))
            for lumi in fileInfo['outfilelumis']:
                configreq.append(("outfilelumis", lumi))
            for lfn in fileInfo['inparentlfns']:
                configreq.append(("inparentlfns", lfn))
            print self.resturl, urllib.urlencode(configreq)
            try:
                self.server.put(self.resturl, data = urllib.urlencode(configreq))
            except HTTPException, hte:
                print hte.headers
                raise


    def uploadLog(self, outlfn):
        source_site = self.source_site
        if 'SEName' in self.full_report:
            source_site = self.node_map.get(self.full_report['SEName'], source_site)
        log_size = self.full_report.get(u'log_size', 0)
        configreq = {"taskname":        self.ad['CRAB_ReqName'],
                     "pandajobid":      self.crab_id,
                     "outsize":         log_size, # Not implemented
                     "publishdataname": self.ad['CRAB_OutputData'],
                     "appver":          self.ad['CRAB_JobSW'],
                     "outtype":         "LOG",
                     "checksummd5":     "asda", # Not implemented
                     "checksumcksum":   "3701783610", # Not implemented
                     "checksumadler32": "6d1096fe", # Not implemented
                     "outlocation":     self.ad['CRAB_AsyncDest'],
                     "outtmplocation":  source_site,
                     "acquisitionera":  "null", # Not implemented
                     "events":          "0",
                     "outlfn":          outlfn,
                     "outdatasetname":  "/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER"
                    }
        print self.resturl, urllib.urlencode(configreq)
        try:
            self.server.put(self.resturl, data = urllib.urlencode(configreq))
        except HTTPException, hte:
            print hte.headers
            if not hte.headers.get('X-Error-Detail', '') == 'Object already exists' or \
               not hte.headers.get('X-Error-Http', -1) == '400':
                   raise


    def uploadFakeLog(self, state="TRANSFERRING"):
        # Upload a fake log status.  This is to be replaced by a proper job state table.
        configreq = {"taskname":        self.ad['CRAB_ReqName'],
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
        if self.full_report.get('executed_site', None):
            print "Getting source_site from jobReport"
            return self.full_report['executed_site']

        cmd = "condor_q -const true -userlog job_log.%d -af JOBGLIDEIN_CMSSite" % self.crab_id
        status, output = commands.getstatusoutput(cmd)
        if status:
            print "Failed to query condor user log:\n%s" % output
            raise ValueError("Failed to query condor user log:\n%s" % output)
        source_site = output.split('\n')[-1].strip()
        print "Determined a source site of %s from the user log" % source_site
        if source_site == 'Unknown' or source_site == "undefined":
            raise ValueError("Unable to determine source side")
        return source_site


    def getFileSourceSite(self, filename):
        filename = os.path.split(filename)[-1]
        for outfile in self.outputFiles:
            if (u'pfn' not in outfile) or (u'SEName' not in outfile):
                continue
            json_pfn = outfile[u'pfn']
            pfn = os.path.split(filename)[-1]
            left_piece, fileid = pfn.rsplit("_", 1)
            right_piece = fileid.split(".", 1)[-1]
            pfn = left_piece + "." + right_piece
            if pfn == json_pfn:
                return self.node_map.get(outfile[u'SEName'], self.source_site)
        return self.node_map.get(self.full_report.get(u"SEName"), self.source_site)


    def stageout(self, source_dir, dest_dir, *filenames):
        self.dest_site = self.ad['CRAB_AsyncDest']

        source_sites = []
        for filename in filenames:
            source_sites.append(self.getFileSourceSite(filename))

        transfer_list = resolvePFNs(self.dest_site, source_dir, dest_dir, source_sites, filenames)
        for source, dest in transfer_list:
            print "Copying %s to %s" % (source, dest)

        # Skip the first file - it's a tarball of the stdout/err
        for outfile in zip(filenames[1:], self.outputFiles, source_sites[1:]):
            outlfn = os.path.join(dest_dir, outfile[0])
            outfile[1]['outlfn'] = outlfn
            if 'outtmplocation' not in outfile[1]:
                outfile[1]['outtmplocation'] = outfile[2]
            outfile[1]['outlocation'] = self.dest_site

        global g_Job
        g_Job = FTSJob(transfer_list, self.crab_id)
        fts_job_result = g_Job.run()
        # If no files failed, return success immediately.  Otherwise, see how many files failed.
        if not fts_job_result:
            return fts_job_result

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


    def makeNodeMap(self):
        p = PhEDEx.PhEDEx()
        nodes = p.getNodeMap()['phedex']['node']
        self.node_map = {}
        for node in nodes:
            self.node_map[str(node[u'se'])] = str(node[u'name']).replace("_Disk", "").replace("_Buffer", "").replace("_MSS", "").replace("_Export", "")

    def execute(self, *args, **kw):
        retry_count = args[1]
        id = args[6]
        reqname = args[5]
        logpath = os.path.expanduser("~/%s" % reqname)
        postjob = os.path.join(logpath, "postjob.%s.%s.txt" % (id, retry_count))
        try:
            retval = self.execute_internal(*args, **kw)
        except:
            sys.stdout.flush()
            sys.stderr.flush()
            shutil.copy("postjob.%s" % id, postjob)
            os.chmod(postjob, 0644)
            raise
        if retval:
            sys.stdout.flush()
            sys.stderr.flush()
            shutil.copy("postjob.%s" % id, postjob)
            os.chmod(postjob, 0644)
        return retval

    def execute_internal(self, status, retry_count, max_retries, restinstance, resturl, reqname, id, outputdata, sw, async_dest, source_dir, dest_dir, *filenames):

        stdout = "job_out.%s" % id
        stderr = "job_err.%s" % id
        jobreport = "jobReport.json.%s" % id

        fd = os.open("postjob.%s" % id, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0755)
        os.dup2(fd, 1)
        os.dup2(fd, 2)

        logpath = os.path.expanduser("~/%s" % reqname)
        try:
            os.makedirs(logpath)
        except OSError, oe:
            if oe.errno != errno.EEXIST:
                raise

        if os.path.exists(stdout):
            fname = os.path.join(logpath, "job_out."+id+"."+retry_count+".txt")
            shutil.copy(stdout, fname)
            os.chmod(fname, 0644)
        if os.path.exists(stderr):
            fname = os.path.join(logpath, "job_err."+id+"."+retry_count+".txt")
            shutil.copy(stderr, fname)
            os.chmod(fname, 0644)
        if os.path.exists(jobreport):
            fname = os.path.join(logpath, "job_fjr."+id+"."+retry_count+".json")
            shutil.copy(jobreport, fname)
            os.chmod(fname, 0644)

        if 'X509_USER_PROXY' not in os.environ:
            return 10

        self.server = HTTPRequests(restinstance, os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY'])
        self.resturl = resturl

        self.makeAd(reqname, id, outputdata, sw, async_dest)

        self.makeNodeMap()

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
           else:
               self.uploadState("COOLOFF")
               return retval

        self.parseJson()
        self.source_site = self.getSourceSite()

        self.fixPerms()
        try:
            self.uploadLog(os.path.join(dest_dir, filenames[0]))
            self.stageout(source_dir, dest_dir, *filenames)
            self.upload()
        except:
            self.uploadState("COOLOFF")
            raise
        self.uploadFakeLog(state="FINISHED")

        return 0

if __name__ == '__main__':
    pj = PostJob()
    sys.exit(pj.execute(*sys.argv[2:]))

