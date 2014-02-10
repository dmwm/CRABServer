#!/usr/bin/env python2.6

import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)

import os
import re
import sys
import json
import time
import signal
import logging
import tarfile
import traceback

# Bootstrap the CMS_PATH variable; the StageOutMgr will need it
if 'CMS_PATH' not in os.environ:
    if 'VO_CMS_SW_DIR' in os.environ:
        os.environ['CMS_PATH'] = os.environ['VO_CMS_SW_DIR']
    elif 'OSG_APP' in os.environ:
        os.environ['CMS_PATH'] = os.path.join(os.environ['OSG_APP'], 'cmssoft', 'cms')
    elif os.path.exists('/cvmfs/cms.cern.ch'):
        os.environ['CMS_PATH'] = '/cvmfs/cms.cern.ch'

if os.path.exists("WMCore.zip") and "WMCore.zip" not in sys.path:
    sys.path.append("WMCore.zip")

if 'http_proxy' in os.environ and not os.environ['http_proxy'].startswith("http://"):
    os.environ['http_proxy'] = "http://%s" % os.environ['http_proxy']

import WMCore.Storage.StageOutMgr as StageOutMgr

from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Algorithms.Alarm import Alarm, alarmHandler

waitTime = 60*60
numberOfRetries = 2
retryPauseTime = 60


def compress(id):
    retval = 0
    output = "cmsRun_%d.log.tar.gz" % id
    tf = tarfile.open(output, "w:gz")
    if os.path.exists("cmsRun-stdout.log"):
        print "Adding cmsRun-stdout.log to tarball %s" % output
        tf.add("cmsRun-stdout.log", arcname="cmsRun-stdout-%d.log" % id)
    else:
        print "== ERROR: cmsRun-stdout.log is missing.  Will fail stageout."
        retval = 60307
    if os.path.exists("cmsRun-stderr.log"):
        print "Adding cmsRun-stderr.log to tarball %s" % output
        tf.add("cmsRun-stderr.log", arcname="cmsRun-stderr-%d.log" % id)
    else:
        print "== ERROR: cmsRun-stderr.log is missing.  Will fail stageout."
        retval = 60307
    if os.path.exists("FrameworkJobReport.xml"):
        print "Adding FrameworkJobReport.xml to tarball %s" % output
        tf.add("FrameworkJobReport.xml", arcname="FrameworkJobReport-%d.xml" % id)
    else:
        print "== ERROR: FrameworkJobReport.xml is missing.  Will fail stageout."
        retval = 60307
    tf.close()
    return retval


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
    print "== Attempting to set SE name of %s for file %s ==" % (se_name, dest_file)

    filename, id = get_job_id(dest_file)

    with open("jobReport.json.%d" % id) as fd:
        full_report = json.load(fd)

    if 'steps' not in full_report or 'cmsRun' not in full_report['steps']:
        return
    report = full_report['steps']['cmsRun']

    if 'output' not in report:
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


def performTransfer(manager, source, dest, direct_pfn, direct_se):
    fileForTransfer = {'LFN': dest, 'PFN': source}
    signal.signal(signal.SIGALRM, alarmHandler)
    signal.alarm(waitTime)
    result = 0
    try:
        # Throws on any failure
        manager(fileForTransfer)
    except Alarm:
        print "Indefinite hang during stageOut of %s" % dest
        manager.cleanSuccessfulStageOuts()
        result = 60403
    except Exception, ex:
        print "== Error during stageout: %s" % ex
        manager.cleanSuccessfulStageOuts()
        result = 60307
    finally:
        signal.alarm(0)
    if result:
        print "== Local stageout resulted in status %d; attempting direct stageout at %s. ==" % (result, time.ctime())
        try:
            result = performDirectTransfer(source, direct_pfn, direct_se)
        except WMException.WMException, ex:
            print "Error during direct stageout: %s" % str(ex)
            return ex.data.get("ErrorCode", 60307)
        finally:
            signal.alarm(0)
    else:
        set_se_name(os.path.split(dest)[-1], result['SEName'])


def performDirectTransfer(source, direct_pfn, direct_se):
    command = "srmv2-lcg"
    
    try:
        impl = retrieveStageOutImpl(command)
    except Exception, ex:
        msg = "Unable to retrieve impl for local stage out:\n"
        msg += "Error retrieving StageOutImpl for command named: %s\n" % (
            command,)
        raise StageOutFailure(msg, Command = command,
                              LFN = direct_pfn, ExceptionDetail = str(ex))

    impl.numRetries = numberOfRetries
    impl.retryPause = retryPauseTime

    signal.alarm(waitTime)
    result = 0
    try:
        impl("srmv2", source, direct_pfn, None, None)
    except Alarm:
        print "== Indefinite hang during stageOut of %s; setting return code to 60403." % dest
        result = 60403
    except Exception, ex:
        msg = "== Failure for local stage out:\n"
        msg += str(ex)
        try:
            msg += traceback.format_exc()
        except AttributeError, ex:
            msg += "Traceback unavailable\n"
        raise StageOutFailure(msg, Command = command, Protocol = protocol,
                              LFN = lfn, InputPFN = localPfn,
                              TargetPFN = pfn)
    finally:
        signal.alarm(0)

    if not result:
        set_se_name(os.path.split(direct_pfn)[-1], direct_se)

    return result


def main():

    # Parse the job ad to get the stageout information.
    crab_id = -1
    output_files = None
    dest_se = None
    dest_dir = None
    dest_files = None
    if '_CONDOR_JOB_AD' not in os.environ:
        print "== ERROR: _CONDOR_JOB_AD not in environment =="
        print "No stageout will be performed."
    elif not os.path.exists(os.environ['_CONDOR_JOB_AD']):
        print "== ERROR: _CONDOR_JOB_AD (%s) does not exist =="
        print "No stageout will be performed."
        return 60307
    else:
        attr_re = re.compile("([A-Z_a-z0-9]+?) = (.*)")
        split_re = re.compile(",\s*")
        with open(os.environ['_CONDOR_JOB_AD']) as fd:
            for line in fd.readlines():
                m = attr_re.match(line)
                if not m:
                    continue
                name, val = m.groups()
                if name == 'CRAB_Id':
                    crab_id = int(val)
                elif name == "CRAB_localOutputFiles":
                    output_files = split_re.split(val.replace('"', ''))
                elif name == "CRAB_AsyncDestSE":
                    dest_se = val.replace('"', '')
                elif name == "CRAB_Dest":
                    dest_dir = val.replace('"', '')
                elif name == "CRAB_Destination":
                    dest_files = split_re.split(val.replace('"', ''))
        if crab_id == -1:
            print "== ERROR: Unable to determine CRAB Job ID."
            print "No stageout will be performed."
            return 60307
        if output_files == None:
            print "== ERROR: Unable to determine output files."
            print "No stageout will be performed."
            return 60307
        if dest_se == None:
            print "== ERROR: Unable to determine destination SE."
            print "No stageout will be performed."
            return 60307
        if dest_dir == None:
            print "== ERROR: Unable to determine local destination directory."
            print "No stageout will be performed."
            return 60307
        if dest_files == None:
            print "== ERROR: Unable to determine remote destinations."
            print "No stageout will be performed."
            return 60307

    # The stageout manager will be used by all attempts
    stageOutCall = {}
    manager = StageOutMgr.StageOutMgr(
        retryPauseTime  = 60,
        numberOfRetries = 2,
        **stageOutCall)
    manager.retryPauseTime = retryPauseTime
    manager.numberOfRetries = numberOfRetries

    counter = "%04d" % (crab_id / 1000)
    dest_dir = os.path.join(dest_dir, counter)

    log_file = "cmsRun_%d.log.tar.gz" % crab_id
    dest = os.path.join(dest_dir, "log", log_file)
    try:
        print "==== Starting compression of user logs at %s ====" % time.ctime()
        std_retval = compress(crab_id)
        print "==== Finished compression of user logs at %s (status %d) ====" % (time.ctime(), std_retval)
        if not std_retval:
            print "==== Starting stageout of user logs at %s ====" % time.ctime()
            std_rtval = performTransfer(manager, log_file, dest, dest_files[0], dest_se)
    except Exception, ex:
        print "== ERROR: Unhandled exception when performing stageout of user logs."
        traceback.print_exc()
        if not std_retval:
            std_retval = 60307
    finally:
        print "==== Stageout of user logs ended at %s (status %d) ====" % (time.ctime(), std_retval)

    out_retval = 0
    for dest, remote_dest in zip(output_files, dest_files[1:]):

        info = dest.split("=")
        if len(info) != 2:
            print "== ERROR: Invalid output format (%s)." % dest
            out_retval = 60307
            continue
        source_file, dest_file = info

        if not os.path.exists(source_file):
            print "== ERROR: Output file %s does not exist." % source
            out_retval = 60307
            continue

        dest = os.path.join(dest_dir, dest_file)
        try:
            print "==== Starting stageout of %s at %s ====" % (source_file, time.ctime())
            cur_out_retval = performTransfer(manager, source_file, dest, remote_dest, dest_se)
        except Exception, ex:
            print "== ERROR: Unhandled exception when performing stageout."
            traceback.print_exc()
            cur_out_retval = 60307
        finally:
            print "====  Finished stageout of %s at %s (status %d) ====" % (source_file, time.ctime(), out_retval)
        if cur_out_retval and not out_retval:
            out_retval = cur_out_retval

    if std_retval:
        return std_retval
    return out_retval


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        retval = main()
    except:
        retval = 60307
        traceback.print_exc()
    sys.exit(retval)

