#!/usr/bin/env python2.6

import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)

import os
import sys
import json
import signal
import logging
import tarfile
import traceback

if 'CMS_PATH' not in os.environ:
    if 'VO_CMS_SW_DIR' in os.environ:
        os.environ['CMS_PATH'] = os.environ['VO_CMS_SW_DIR']
    elif 'OSG_APP' in os.environ:
        os.environ['CMS_PATH'] = os.path.join(os.environ['OSG_APP'], 'cmssoft', 'cms')
    elif os.path.exists('/cvmfs/cms.cern.ch'):
        os.environ['CMS_PATH'] = '/cvmfs/cms.cern.ch'

if os.path.exists("CRAB3.zip") and "CRAB3.zip" not in sys.path:
    sys.path.append("CRAB3.zip")

if 'http_proxy' in os.environ and not os.environ['http_proxy'].startswith("http://"):
    os.environ['http_proxy'] = "http://%s" % os.environ['http_proxy']

try:
    import WMCore
except ImportError, ie:
    import urllib, tempfile
    fd = urllib.urlopen("http://hcc-briantest.unl.edu/TaskManagerRun.tar.gz")
    with tempfile.TemporaryFile() as fd2:
        fd2.write(fd.read()); fd2.seek(0)
        tf = tarfile.open(mode='r:gz', fileobj=fd2)
        fd3 = tf.extractfile("CRAB3.zip")
        try:
            name = "CRAB3.zip"
            fd4 = os.open(name, os.O_EXCL | os.O_RDWR | os.O_CREAT, 0644)
        except:
            fd4, name = tempfile.mkstemp()
        os.fdopen(fd4, 'w').write(fd3.read())
    sys.path.insert(0, name)
    import WMCore

import WMCore.Storage.StageOutMgr as StageOutMgr

from WMCore.Algorithms.Alarm import Alarm, alarmHandler
import WMCore.WMException as WMException

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

def get_job_id(source):
    filename = os.path.split(source)[-1]
    left_piece, fileid = filename.rsplit("_", 1)
    fileid, right_piece = fileid.split(".", 1)
    try:
        fileid = int(fileid)
    except ValueError:
        fileid = -1
    
    return left_piece + "." + right_piece, fileid

def set_se_name(dest_file, se_name):
    """
    Alter the job report to record where the given file was
    staged out to.  If we cannot determine the matching file,
    then record it in the top-level of the JSON (hopefully it
    means that it is the log file).
    """
    filename, id = get_job_id(dest_file)

    with open("jobReport.json.%d" % id) as fd:
        full_report = json.load(fd)

    if 'steps' not in full_report or 'cmsRun' not in full_report['steps']:
        #raise ValueError("Invalid jobReport.json: missing cmsRun")
        return
    report = full_report['steps']['cmsRun']

    if 'output' not in report:
        #raise ValueError("Invalid jobReport.json: missing output")
        return
    output = report['output']

    found_output = False
    for outputModule in output.values():
        for outputFile in outputModule:

            if outputFile.get(u"output_module_class") != u'PoolOutputModule' and \
                    outputFile.get(u"ouput_module_class") != u'PoolOutputModule':
                continue

            if str(outputFile.get(u"pfn")) != filename:
                continue

            outputFile['SEName'] = se_name
            found_output = True
            break

    if not found_output:
        full_report['SEName'] = se_name
        try:
            full_report['log_size'] = os.stat(dest_file).st_size
        except:
            pass

    with open("jobReport.json.%d" % id, "w") as fd:
        json.dump(full_report, fd)


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
            print "Output file %s does not exist." % source
            return 60302

        if len(info) == 2:
            dest = info[0]
            info = info[1].split("&")
            info = dict([(i.split("=")[0], i.split("=")[1]) for i in info])
            dest_dir, dest_file = os.path.split(dest)
            dest_file = info['remoteName']
            try:
                fileid = get_job_id(dest_file)[1]
            except ValueError:
                fileid = 0
            counter = "%04d" % (fileid / 1000)
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
                dest = os.path.join(dest_dir, counter, "log", dest_file)
            else:
                dest = os.path.join(dest_dir, counter, dest_file)
        else:
            return 0

        fileForTransfer = {'LFN': dest, 'PFN': source}
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            result = manager(fileForTransfer)
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
        set_se_name(dest_file, result['SEName'])
    return 0

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        retval = main()
    except:
        retval = 60307
        traceback.print_exc()
    sys.exit(retval)

