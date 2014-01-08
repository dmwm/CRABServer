#!/usr/bin/python

import re
import sys
import time
import signal
import commands

import WMCore.Services.PhEDEx.PhEDEx as PhEDEx

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

    def __init__(self, source, dest):
        self._id = None
        self._cancel = False
        self._sleep = 20
        self._source = source
        self._dest = dest

    def cancel(self):
        if self._id:
            cmd = "glite-transfer-cancel -s %s %s" % (fts_server, self._id)
            print "+", cmd
            os.system(cmd)

    def submit(self):
        cmd = "glite-transfer-submit -s %s %s %s" % (fts_server, self._source, self._dest)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-submit exited with status %s.\n%s" % (status, output))
        output = output.strip()
        print "Resulting transfer ID: %s" % output
        return output

    def status(self):
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
        idx = 0
        while True:
            idx += 1
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
                return 1

def resolvePFNs(url1, url2):

    ssite, slfn = url1.split(":", 1)
    dsite, dlfn = url2.split(":", 1)
    p = PhEDEx.PhEDEx()
    dest_info = p.getPFN(nodes=[dsite, ssite], lfns=[dlfn, slfn])

    return dest_info[ssite, slfn], dest_info[dsite, dlfn]

def main():
    if len(sys.argv) != 3:
        print "Usage: ftscp <source> <dest>"
        print "\nSource and destination should be of the format $SITE:$LFN"
        sys.exit(1)

    source, dest = resolvePFNs(sys.argv[1], sys.argv[2])
    print "Copying %s to %s" % (source, dest)

    global g_Job
    g_Job = FTSJob(source, dest)
    return g_Job.run()

if __name__ == '__main__':
    sys.exit(main())

