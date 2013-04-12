#!/usr/bin/env python

import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=DeprecationWarning)
    import popen2

import os
import sys
import signal
import tarfile

if 'CMS_PATH' not in os.environ:
    if 'VO_CMS_SW_DIR' in os.environ:
        os.environ['CMS_PATH'] = os.environ['VO_CMS_SW_DIR']
    elif 'OSG_APP' in os.environ:
        os.environ['CMS_PATH'] = os.path.join(os.environ['OSG_APP'], 'cmssoft', 'cms')
    elif os.path.exists('/cvmfs/cms.cern.ch'):
        os.environ['CMS_PATH'] = '/cvmfs/cms.cern.ch'

if os.path.exists("WMCore.zip") and "WMCore.zip" not in sys.path:
    sys.path.append("WMCore.zip")

try:
    import WMCore
except ImportError, ie:
    import urllib, tarfile, tempfile
    fd = urllib.urlopen("http://hcc-briantest.unl.edu/TaskManagerRun.tar.gz")
    with tempfile.TemporaryFile() as fd2:
       fd2.write(fd.read()); fd2.seek(0)
       tf = tarfile.open(mode='r:gz', fileobj=fd2)
       fd3 = tf.extractfile("WMCore.zip")
       try:
           name = "WMCore.zip"
           fd4 = os.open(name, os.O_EXCL | os.O_RDWR | os.O_CREAT, 0644)
       except:
           fd4, name = tempfile.mkstemp()
       os.fdopen(fd4, 'w').write(fd3.read())
    sys.path.insert(0, name)
    import WMCore

import WMCore.Storage.StageOutMgr as StageOutMgr

from WMCore.Lexicon                  import lfn     as lfnRegEx
from WMCore.Lexicon                  import userLfn as userLfnRegEx

from WMCore.Algorithms.Alarm import Alarm, alarmHandler

classad_output = """\
PluginVersion = "0.1"
PluginType = "FileTransfer"
SupportedMethods = "cms"\
"""

waitTime = 60*60

def parseArgs():
    # ARRRRRGH!  HTCondor's file transfer plugin does completely non-standard
    # argument passing for Linux.  Great.
    if '-classad' in sys.argv[1:]:
        print classad_output
        sys.exit(0)

    if len(sys.argv) != 3:
        print "Usage: cmscp.py <source> <dest>"
        sys.exit(1)

    source, dest = sys.argv[1:]
    return source, dest, source.startswith('/')

def compress(source, tarball, count):
    output = tarball + ".tar"
    tf = tarfile.open(output, "a")
    names = tf.getnames()
    curCount = len(names)
    print "Adding %s to tarball %s" % (source, output)
    if source not in names:
        tf.add(source, arcname=os.path.split(source)[-1])
        curCount += 1
    tf.close()
    if curCount >= count:
        os.system("gzip %s" % output)
        output += '.gz'
    return output, curCount

def main():
    source, dest, stageout = parseArgs()

    # See WMCore.WMSpec.Steps.Executors.StageOut for more details on extending.
    stageOutCall = {}
    manager = StageOutMgr.StageOutMgr(
        retryPauseTime  = 60,
        numberOfRetries = 2,
        **stageOutCall)
    manager.retryPauseTime = 60
    manager.numberOfRetries = 2

    if stageout:

        if dest.startswith("cms:/"): dest = dest[5:]

        info = dest.split("?")
        source = source.split("?")[0]
        if not os.path.exists(source):
            return 1
        if len(info) == 2:
            dest = info[0]
            info = info[1].split("&")
            info = dict([(i.split("=")[0], i.split("=")[1]) for i in info])
            dest_dir, dest_file = os.path.split(dest)
            dest_file = info['remoteName']
            if 'compressCount' in info:
                try:
                    count = int(info['compressCount'])
                except:
                    count = 1
                tarball = os.path.join(os.path.split(source)[0], dest_file)
                source, curCount = compress(source, tarball, count)
                dest_file += '.tar.gz'
                if curCount < count:
                    return 0
            dest = os.path.join(dest_dir, dest_file)
        else:
            return 0

        fileForTransfer = {'LFN': dest, 'PFN': source}
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            manager(fileForTransfer)
        except Alarm:
            print "Indefinite hang during stageOut of %s" % dest
            manager.cleanSuccessfulStageOuts()
            return 60403
        except Exception, ex:
            print "Error during stageout: %s" % ex
            manager.cleanSuccessfulStageOuts()
            raise
            return 60307
        signal.alarm(0)
    return 0

if __name__ == '__main__':
    sys.exit(main())

