#!/usr/bin/env python2.6

import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)

import os
import re
import sys
import json
import time
import pprint
import signal
import hashlib
import logging
import tarfile
import datetime
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
import WMCore.WMException as WMException
import WMCore.Storage.StageOutError as StageOutError
import WMCore.Database.CMSCouch as CMSCouch
import WMCore.Services.PhEDEx.PhEDEx as PhEDEx

import DashboardAPI

waitTime = 60*60
numberOfRetries = 2
retryPauseTime = 60
g_now = None
g_now_epoch = None

def parseAd():
    fd = open(os.environ['_CONDOR_JOB_AD'])
    jobad = {}
    for adline in fd.readlines():
        info = adline.split(" = ", 1)
        if len(info) != 2:
            continue
        if info[1].startswith('undefined'):
            val = info[1].strip()
        elif info[1].startswith('"'):
            val = info[1].strip()[1:-1]
        else:
            try:
                val = int(info[1].strip())
            except ValueError:
                continue
        jobad[info[0]] = val
    return jobad


def reportFailureToDashboard(exitCode):
    try:
        ad = parseAd()
    except:
        print "==== ERROR: Unable to parse job's HTCondor ClassAd ===="
        print "Will NOT report stageout failure to Dashboard"
        print traceback.format_exc()
        return
    for attr in ['CRAB_ReqName', 'CRAB_Id', 'CRAB_Retry']:
        if attr not in ad:
            print "==== ERROR: HTCondor ClassAd is missing attribute %s. ====" % attr
            print "Will not report stageout failure to Dashboard"
    params = {
        'MonitorID': ad['CRAB_ReqName'],
        'MonitorJobID': '%d_https://glidein.cern.ch/%d/%s_%d' % (ad['CRAB_Id'], ad['CRAB_Id'], ad['CRAB_ReqName'].replace("_", ":"), ad['CRAB_Retry']),
        'JobExitCode': exitCode
    }
    print "Dashboard stageout failure parameters: %s" % str(params)
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    DashboardAPI.apmonFree()


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


def set_se_name(dest_file, se_name, direct=False):
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
            outputFile['direct_stageout'] = direct
            found_output = True
            break

    if not found_output:
        full_report['SEName'] = se_name
        full_report['direct_stageout'] = direct
        try:
            full_report['log_size'] = os.stat(dest_file).st_size
        except:
            pass

    with open("jobReport.json.%d" % id, "w") as fd:
        json.dump(full_report, fd)


def performTransfer(manager, stageout_policy, source, dest, direct_pfn, direct_se):
    result = -1
    for policy in stageout_policy:
        if policy == "local":
            print "== Attempting local stageout at %s. ==" % time.ctime()
            result = performLocalTransfer(manager, source, dest)
            if result:
                print "== ERROR: Local stageout resulted in status %d at %s. ==" % (result, time.ctime())
            else:
                print "== Local stageout succeeded at %s. ==" % time.ctime()
                break
        elif policy == "remote":
            print "== Attempting remote stageout at %s. ==" % time.ctime()
            result = performDirectTransfer(source, direct_pfn, direct_se)
            if result:
                print "== ERROR: Remote stageout resulted in status %d at %s. ==" % (result, time.ctime())
            else:
                print "== Remote stageout succeeded at %s. ==" % time.ctime()
                break
        else:
            print "== ERROR: Skipping unknown policy named '%s'. ==" % policy
    if result == -1:
        print "== FATAL ERROR: No stageout policy was attempted. =="
        result = 60307
    return result


def performLocalTransfer(manager, source, dest):
    fileForTransfer = {'LFN': dest, 'PFN': source}
    signal.signal(signal.SIGALRM, alarmHandler)
    signal.alarm(waitTime)
    result = 0
    try:
        # Throws on any failure
        stageout_info = manager(fileForTransfer)
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
    if not result:
        set_se_name(os.path.split(dest)[-1], stageout_info['SEName'])
        injectToASO(dest, stageout_info['SEName'])
    return result


def injectToASO(dest_lfn, se_name):
    ad = parseAd()
    for attr in ["CRAB_ASOURL", "CRAB_AsyncDest", "CRAB_InputData", "CRAB_UserGroup", "CRAB_UserRole", "CRAB_DBSURL",\
             "CRAB_PublishDBSURL", "CRAB_ReqName", "CRAB_UserHN", "CRAB_SaveLogsFlag"]:
        if attr not in ad:
            print "==== ERROR: Unable to inject into ASO because %s is missing from job ad" % attr
            return False
    if 'X509_USER_PROXY' not in os.environ:
        print "==== ERROR: X509_USER_PROXY missing from user environment. Unable to inject into ASO. ===="
        return False
    if not os.path.exists(os.environ['X509_USER_PROXY']):
        print "==== ERROR: User proxy %s missing from disk. ====" % os.environ['X509_USER_PROXY']
        return False
    source_dir, fname = os.path.split(dest_lfn)
    local_fname, id = get_job_id(fname)
    found_logfile = False
    file_type = 'output'
    dest_lfn = os.path.join(source_dir, fname)
    if re.match("cmsRun_[0-9]+.log.tar.gz", fname):
        found_logfile = True
        file_type = 'log'

    with open("jobReport.json.%d" % id) as fd:
        full_report = json.load(fd)

    if not found_logfile:
        if 'steps' not in full_report or 'cmsRun' not in full_report['steps']:
            return
        report = full_report['steps']['cmsRun']

        if 'output' not in report:
            return
        output = report['output']

        found_output = False
        for outputModule in output.values():
            for outputFile in outputModule:
                if str(outputFile.get(u"pfn")) != local_fname:
                    continue
                checksums = outputFile.get(u"checksums", {"cksum": "0", "adler32": "0"})
                size = outputFile.get(u"size", 0)
                found_output = True
        if not found_output:
            print "==== ERROR: Unable to find output file in FrameworkJobReport.  Cannot inject to ASO."
            return False
    else:
        size = full_report.get(u'log_size', 0)
        # Copied from PostJob.py, but not sure if it does anything. BB
        checksums = {'adler32': 'abc'}

    p = PhEDEx.PhEDEx()
    nodes = p.getNodeMap()['phedex']['node']
    node_name = None
    for node in nodes:
        if str(node[u'se']) == str(se_name):
            node_name = str(node[u'name'])
            break
    if not node_name:
        print "==== ERROR: Unable to determine local node name.  Cannot inject to ASO. ===="
        return False

    role = str(ad['CRAB_UserRole'])
    if str(ad['CRAB_UserRole']).lower() == 'undefined':
        role = ''
    group = str(ad['CRAB_UserGroup'])
    if str(ad['CRAB_UserGroup']).lower() == 'undefined':
        group = ''
    dbs_url = str(ad['CRAB_DBSURL'])
    publish_dbs_url = str(ad['CRAB_PublishDBSURL'])
    if publish_dbs_url.lower() == 'undefined':
        publish_dbs_url = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/"

    last_update = int(time.time())
    global g_now
    if g_now == None:
        g_now = str(datetime.datetime.now())
        g_now_epoch = last_update

    # NOTE: it's almost certainly a mistake to include dest_lfn in the hash here as it
    # includes /store/temp/user/foo.$HASH.  We should normalize based on the final LFN (/store/user/...)
    doc_id = hashlib.sha224(dest_lfn).hexdigest()
    info = {"state": "new",
            "source": node_name,
            "destination": ad['CRAB_AsyncDest'],
            "checksums": checksums,
            "size": size,
            "last_update": last_update, # The following four times - and how they're calculated - makes no sense to me.  BB
            "start_time": g_now,
            "end_time": '',
            "job_end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
            "retry_count": [],
            "failure_reason": [],
            "job_retry_count": ad.get("CRAB_Retry", -1)
           }
    print "ASO info so far:"
    print info

    couchServer = CMSCouch.CouchServer(dburl=ad['CRAB_ASOURL'], ckey=os.environ['X509_USER_PROXY'], cert=os.environ['X509_USER_PROXY'])
    couchDatabase = couchServer.connectDatabase("asynctransfer", create = False)
    print "Stageout job description: %s" % pprint.pformat(info)

    try:
        doc = couchDatabase.document(doc_id)
        doc.update(info)
        print ("Will retry LFN %s (id %s)" % (dest_lfn, doc_id))
    except CMSCouch.CouchNotFoundError:
        print "LFN %s (id %s) is not yet known to ASO; uploading new stageout job." % (dest_lfn, doc_id)
        doc = {"_id": doc_id,
               "inputdataset": ad["CRAB_InputData"],
               "group": group,
               "lfn": dest_lfn.replace('/store/user', '/store/temp/user', 1),
               "checksums": checksums,
               "user": ad['CRAB_UserHN'],
               "role": role,
               "dbSource_url": "gWMS",
               "publish_dbs_url": publish_dbs_url,
               "dbs_url": dbs_url,
               "workflow": ad['CRAB_ReqName'],
               "jobid": id,
               "publication_state": 'not_published',
               "publication_retry_count": [],
               "type" : file_type,
               "publish" : 1,
              }
        doc.update(info)
    except Exception, ex:
        msg = "Error loading document from couch. Transfer submission failed."
        msg += str(ex)
        msg += str(traceback.format_exc())
        print (msg)
        return False
    commit_result = couchDatabase.commitOne(doc)[0]
    if 'error' in commit_result:
        print("Couldn't add to ASO database; error follows")
        print(commit_result)
        return False
    print "Final stageout job description: %s" % pprint.pformat(doc)
    full_report['aso_start_time'] = g_now
    full_report['aso_start_timestamp'] = g_now_epoch
    with open("jobReport.json.%d" % id, "w") as fd:
        json.dump(full_report, fd)

    return True


def performDirectTransfer(source, direct_pfn, direct_se):
    try:
        return performDirectTransferImpl(source, direct_pfn, direct_se)
    except WMException.WMException, ex:
        print "="*79
        print "==== START DUMP OF TRANSFER ERROR INFO ===="
        print "="*79
        print "Error during direct stageout:\n%s" % str(ex)
        print "="*79
        print "==== FINISH DUMP OF TRANSFER ERROR INFO ===="
        print "="*79
        return ex.data.get("ErrorCode", 60307)


def performDirectTransferImpl(source, direct_pfn, direct_se):
    command = "srmv2-lcg"
    protocol = "srmv2"
    
    try:
        impl = retrieveStageOutImpl(command)
    except Exception, ex:
        msg = "Unable to retrieve impl for local stage out:\n"
        msg += "Error retrieving StageOutImpl for command named: %s\n" % (
            command,)
        raise StageOutError.StageOutFailure(msg, Command = command,
                              LFN = direct_pfn, ExceptionDetail = str(ex))

    impl.numRetries = numberOfRetries
    impl.retryPause = retryPauseTime

    signal.alarm(waitTime)
    result = 0
    try:
        impl(protocol, source, direct_pfn, None, None)
    except Alarm:
        print "== Indefinite hang during stageOut of %s; setting return code to 60403." % source
        result = 60403
    except Exception, ex:
        msg = "== Failure for local stage out:\n"
        msg += str(ex)
        try:
            msg += traceback.format_exc()
        except AttributeError, ex:
            msg += "Traceback unavailable\n"
        raise StageOutError.StageOutFailure(msg, Command = command, Protocol = protocol,
                              LFN = direct_pfn, InputPFN = source,
                              TargetPFN = direct_pfn)
    finally:
        signal.alarm(0)

    if not result:
        set_se_name(os.path.split(direct_pfn)[-1], direct_se, direct=True)

    return result


def main():

    # Parse the job ad to get the stageout information.
    crab_id = -1
    output_files = None
    dest_se = None
    dest_dir = None
    dest_files = None
    stageout_policy = None
    save_logs = None
    if '_CONDOR_JOB_AD' not in os.environ:
        print "== ERROR: _CONDOR_JOB_AD not in environment =="
        print "No stageout will be performed."
    elif not os.path.exists(os.environ['_CONDOR_JOB_AD']):
        print "== ERROR: _CONDOR_JOB_AD (%s) does not exist =="
        print "No stageout will be performed."
        return 60307
    else:
        #XXX why don't we use parseAd() ?
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
                elif name == "CRAB_StageoutPolicy":
                    stageout_policy = split_re.split(val.replace('"', ''))
                elif name == "CRAB_SaveLogsFlag":
                    save_logs = int(val)
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
        if stageout_policy == None:
            print "== ERROR: Unable to determine stageout policy."
            print "No stageout will be performed."
            return 60307
        else:
            print "Stageout policy: %s" % ", ".join(stageout_policy)
        if save_logs == None: #well, actually we might assume saveLogs=False and delete these lines..
            print "== ERROR: Unable to determine save_logs parameter."
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
            if not save_logs:
                print "Performing only local stageout for log files as the user did not specify saveLogs = True"
            std_retval = performTransfer(manager, stageout_policy if save_logs else ["local"], log_file, dest, dest_files[0], dest_se)
    except Exception, ex:
        print "== ERROR: Unhandled exception when performing stageout of user logs."
        traceback.print_exc()
        if not std_retval:
            std_retval = 60307
    finally:
        print "==== Stageout of user logs ended at %s (status %d) ====" % (time.ctime(), std_retval)
    if not save_logs and std_retval:
        print "Ignoring log stageout failure because user did not request that they be staged out."
        std_retval = 0

    out_retval = 0
    for dest, remote_dest in zip(output_files, dest_files[1:]):

        info = dest.split("=")
        if len(info) != 2:
            print "== ERROR: Invalid output format (%s)." % dest
            out_retval = 60307
            continue
        source_file, dest_file = info

        if not os.path.exists(source_file):
            print "== ERROR: Output file %s does not exist." % source_file
            out_retval = 60307
            continue

        dest = os.path.join(dest_dir, dest_file)
        try:
            print "==== Starting stageout of %s at %s ====" % (source_file, time.ctime())
            cur_out_retval = performTransfer(manager, stageout_policy, source_file, dest, remote_dest, dest_se)
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
    if retval:
        try:
            reportFailureToDashboard(retval)
        except:
            print "==== ERROR: Unhandled exception when reporting failure to Dashboard. ===="
            traceback.print_exc()
    sys.exit(retval)

