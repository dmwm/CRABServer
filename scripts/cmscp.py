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

# See the explanation of this sentry file in CMSRunAnalysis.py
with open("wmcore_initialized", "w") as fd:
    fd.write("wmcore initialized.\n")

waitTime = 60*60
numberOfRetries = 2
retryPauseTime = 60
g_now = None
g_now_epoch = None
g_cmsRun_exit_code = 0
g_job_exit_code = 0
g_job_report_name = None
g_job_id = None
g_aso_start_time_set_in_JR = False


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
        retval = 80000
    if os.path.exists("cmsRun-stderr.log"):
        print "Adding cmsRun-stderr.log to tarball %s" % output
        tf.add("cmsRun-stderr.log", arcname="cmsRun-stderr-%d.log" % id)
    else:
        print "== ERROR: cmsRun-stderr.log is missing.  Will fail stageout."
        retval = 80000
    if os.path.exists("FrameworkJobReport.xml"):
        print "Adding FrameworkJobReport.xml to tarball %s" % output
        tf.add("FrameworkJobReport.xml", arcname="FrameworkJobReport-%d.xml" % id)
    else:
        print "== ERROR: FrameworkJobReport.xml is missing.  Will fail stageout."
        retval = 80000
    tf.close()
    return retval


def getJobId(source):
    file_basename = os.path.split(source)[-1]
    left_piece, right_piece = file_basename.rsplit("_", 1)
    job_id, file_extension = right_piece.split(".", 1)
    try:
        job_id = int(job_id)
    except ValueError:
        job_id = -1
    orig_file_name = left_piece + "." + file_extension
    return orig_file_name, job_id


def getFromJR(key, default = None, location = []):
    """
    ------------------------------------------------------------------------------------------
    Extract and return from the JR section specified by the keys given in the 'location' list
    (which is expected to be a dictionary) the value corresponding to the given key ('key').
    If not found, return 'default'.
    ------------------------------------------------------------------------------------------
    """
    if g_job_report_name is None:
        return default
    with open(g_job_report_name) as fd:
        job_report = json.load(fd)
    subreport = job_report
    subreport_name = ''
    for loc in location:
        if loc in subreport:
            subreport = subreport[loc]
            subreport_name += "['%s']" % loc
        else:
            print "WARNING: Job report doesn't contain section %s['%s']." % (subreport_name, loc)
            return default
    if type(subreport) != dict:
        if subreport_name:
            print "WARNING: Job report section %s is not a dict." % subreport_name
        else:
            print "WARNING: Job report is not a dict."
        return default
    return subreport.get(key, default)


def getOutputFileFromJR(file_name, job_report = None):
    """
    ------------------------------------------------------------------------------------------
    Extract and return from the JR ('job_report') section ['steps']['cmsRun']['output'] the 
    part corresponding to the given output file ('file_name'). If not found, return None.
    ------------------------------------------------------------------------------------------
    """
    if job_report is None:
        if g_job_report_name is None:
            return None
        with open(g_job_report_name) as fd:
            job_report = json.load(fd)
    job_report_output = job_report['steps']['cmsRun']['output']
    for output_module in job_report_output.values():
        for output_file_info in output_module:
            if os.path.split(str(output_file_info.get(u'pfn')))[-1] == file_name:
                return output_file_info
    return None


def addToJR(key_value_pairs, location = [], mode = 'overwrite'):
    """
    ------------------------------------------------------------------------------------------
    Add pairs of (key, value) given in the 'key_value_pairs' list (a list of 2-tuples) to the
    JR in the section specified by the keys given in the 'location' list. This JR section
    is expected to be a dictionary. For example, if location = ['steps', 'cmsRun', 'output'],
    add each (key, value) pair in 'key_value_pairs' to JR['steps']['cmsRun']['output'] (for
    short JR[location]). There are three different modes ('mode') of adding the information
    to the JR: 'overwrite' (does a direct assignment: JR[location][key] = value), 'new'
    (same as 'overwrite', but the given key must not exist in JR[location]; if it exists,
    don't modify the JR and return False) and 'update' (JR[location][key] is a list and so
    append the value into that list; if the key doesn't exist in JR[location], add it). In 
    case of an identified problem, don't modify the JR, print a warning message and return
    False. Return True otherwise.
    ------------------------------------------------------------------------------------------
    """
    if g_job_report_name is None:
        return False
    with open(g_job_report_name) as fd:
        job_report = json.load(fd)
    subreport = job_report
    subreport_name = ''
    for loc in location:
        if loc in subreport:
            subreport = subreport[loc]
            subreport_name += "['%s']" % loc
        else:
            print "WARNING: Job report doesn't contain section %s['%s']." % (subreport_name, loc)
            return False
    if type(subreport) != dict:
        if subreport_name:
            print "WARNING: Job report section %s is not a dict." % subreport_name
        else:
            print "WARNING: Job report is not a dict."
        return False
    if mode in ['new', 'overwrite']:
        for key, value in key_value_pairs:
            if mode == 'new' and key in subreport:
                print "WARNING: Key '%s' already exists in job report section %s." % (key, subreport_name)
                return False
            subreport[key] = value
    elif mode == 'update':
        for key, value in key_value_pairs:
            subreport.setdefault(key, []).append(value)
    else:
        print "WARNING: Unknown mode '%s'." % mode
        return False
    with open(g_job_report_name, "w") as fd:
        json.dump(job_report, fd)
    return True


def addOutputFileToJR(file_name, key = 'addoutput'):
    """
    ------------------------------------------------------------------------------------------
    Add the given output file ('file_name') to the JR section ['steps']['cmsRun']['output']
    under the given key ('key'). The value to add is a dictionary {'pfn': file_name}.
    ------------------------------------------------------------------------------------------
    """
    print "==== Attempting to add file %s to job report. ====" % file_name
    output_file_info = {}
    output_file_info['pfn'] = file_name
    try:
        file_size = os.stat(file_name).st_size
    except:
        pass
    else:
        output_file_info['size'] = file_size
    is_ok = addToJR([(key, output_file_info)], location = ['steps', 'cmsRun', 'output'], mode = 'update')
    if not is_ok:
        print "==== Failed to add file %s to job report. ====" % file_name
    else:
        print "==== Successfully added file %s to job report. ====" % file_name
    return is_ok


def addSEToJR(file_name, is_log, se_name, direct_stageout):
    """
    ------------------------------------------------------------------------------------------
    Alter the JR to record where the given file ('file_name') was staged out to ('se_name')
    and whether it was a direct stageout or not ('direct_stageout').
    ------------------------------------------------------------------------------------------
    """
    print "== Attempting to set SE name to %s for file %s in job report. ==" % (se_name, file_name)
    is_ok = True
    key_value_pairs = [('SEName', se_name), ('direct_stageout', direct_stageout)]
    is_ok = addToFileInJR(file_name, is_log, key_value_pairs)
    if not is_ok:
        print "== Failed to set SE name for file %s in job report. ==" % file_name
    else:
        print "== Successfully set SE name for file %s in job report. ==" % file_name
    return is_ok


def addToFileInJR(file_name, is_log, key_value_pairs):
    """
    ------------------------------------------------------------------------------------------
    Alter the JR for the given file ('file_name') with the given key and value pairs
    ('key_value_pairs'). If the given file is the log, record in the top-level of the JR.
    ------------------------------------------------------------------------------------------
    """
    if is_log:
        is_ok = addToJR(key_value_pairs)
        return is_ok
    if g_job_report_name is None:
        return False
    orig_file_name, _ = getJobId(file_name)
    with open(g_job_report_name) as fd:
        job_report = json.load(fd)
    output_file_info = getOutputFileFromJR(orig_file_name, job_report)
    if output_file_info is None:
        print "WARNING: Metadata for file %s not found in job report." % file_name
        return False
    for key, value in key_value_pairs:
        output_file_info[key] = value
    with open(g_job_report_name, "w") as fd:
        json.dump(job_report, fd)
    return True


def performTransfer(manager, stageout_policy, source_file, dest_temp_lfn, dest_pfn, dest_se, is_log, inject = True):

    result = -1
    for policy in stageout_policy:
        if policy == "local":
            print "== Attempting local stageout at %s. ==" % time.ctime()
            result = performLocalTransfer(manager, source_file, dest_temp_lfn, is_log, inject)
            if result:
                print "== ERROR: Local stageout resulted in status %d at %s. ==" % (result, time.ctime())
            else:
                print "== Local stageout succeeded at %s. ==" % time.ctime()
                break
        elif policy == "remote":
            print "== Attempting remote stageout at %s. ==" % time.ctime()
            result = performDirectTransfer(source_file, dest_pfn, dest_se, is_log)
            if result:
                print "== ERROR: Remote stageout resulted in status %d at %s. ==" % (result, time.ctime())
            else:
                print "== Remote stageout succeeded at %s. ==" % time.ctime()
                break
        else:
            print "== ERROR: Skipping unknown policy named '%s'. ==" % policy

    if result == -1:
        print "== FATAL ERROR: No stageout policy was attempted. =="
        result = 80000

    return result


def performLocalTransfer(manager, source_file, dest_temp_lfn, is_log, inject = True):

    fileForTransfer = {'LFN': dest_temp_lfn, 'PFN': source_file}
    signal.signal(signal.SIGALRM, alarmHandler)
    signal.alarm(waitTime)
    result = 0

    try:
        # Throws on any failure
        stageout_info = manager(fileForTransfer)
    except Alarm:
        print "== Timeout reached during stageOut of %s; setting return code to 60403. ==" % dest_temp_lfn
        try:
            manager.cleanSuccessfulStageOuts()
        except StageOutError:
            pass
        result = 60403
    except Exception, ex:
        print "== Error during stageout: %s" % ex
        try:
            manager.cleanSuccessfulStageOuts()
        except StageOutError:
            pass
        result = 60307
    finally:
        signal.alarm(0)

    if not result:
        dest_temp_file_name = os.path.split(dest_temp_lfn)[-1]
        se_name = stageout_info['SEName']
        addSEToJR(dest_temp_file_name, is_log, se_name, False)
        if inject:
            injectToASO(dest_temp_lfn, se_name, is_log)

    return result


def injectToASO(source_lfn, se_name, is_log):
    ad = parseAd()
    for attr in ["CRAB_ASOURL", "CRAB_AsyncDest", "CRAB_InputData", "CRAB_UserGroup", "CRAB_UserRole", "CRAB_DBSURL",\
                 "CRAB_PublishDBSURL", "CRAB_ReqName", "CRAB_UserHN", "CRAB_Publish"]:
        if attr not in ad:
            print "==== ERROR: Unable to inject into ASO because %s is missing from job ad" % attr
            return False
    if 'X509_USER_PROXY' not in os.environ:
        print "==== ERROR: X509_USER_PROXY missing from user environment. Unable to inject into ASO. ===="
        return False
    if not os.path.exists(os.environ['X509_USER_PROXY']):
        print "==== ERROR: User proxy %s missing from disk. ====" % os.environ['X509_USER_PROXY']
        return False
    source_dir, file_name = os.path.split(source_lfn)
    file_type = 'log' if is_log else 'output'
    source_lfn = os.path.join(source_dir, file_name)

    orig_file_name, id = getJobId(file_name)

    if is_log:
        size = getFromJR(u'log_size', 0)
        # Copied from PostJob.py, but not sure if it does anything. BB
        checksums = {'adler32': 'abc'}
    else:
        output_file_info = getOutputFileFromJR(orig_file_name)
        if output_file_info:
            checksums = output_file_info.get(u'checksums', {'cksum': '0', 'adler32': '0'})
            size = output_file_info.get(u'size', 0)
            isEDM = (output_file_info.get(u'output_module_class', '') == u'PoolOutputModule' or \
                     output_file_info.get(u'ouput_module_class' , '') == u'PoolOutputModule')
        else:
            checksums = {'cksum': '0', 'adler32': '0'}
            size = 0
            isEDM = False

    p = PhEDEx.PhEDEx()
    nodes = p.getNodeMap()['phedex']['node']
    node_name = None
    for node in nodes:
        if str(node[u'se']) == str(se_name):
            node_name = str(node[u'name'])
            break
    if not node_name:
        print "==== ERROR: Unable to determine local node name. Cannot inject to ASO. ===="
        return False

    role = str(ad['CRAB_UserRole'])
    if str(ad['CRAB_UserRole']).lower() == 'undefined':
        role = ''
    group = str(ad['CRAB_UserGroup'])
    if str(ad['CRAB_UserGroup']).lower() == 'undefined':
        group = ''
    dbs_url = str(ad['CRAB_DBSURL'])
    task_publish = int(ad['CRAB_Publish'])
    publish = int(task_publish and file_type == 'output' and isEDM)
    if task_publish and file_type == 'output' and not isEDM:
        print "Disabling publication of output file %s, because it is not of EDM type." % file_name
    publish = int(publish and not g_cmsRun_exit_code)
    publish_dbs_url = str(ad['CRAB_PublishDBSURL'])
    if publish_dbs_url.lower() == 'undefined':
        publish_dbs_url = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/"

    last_update = int(time.time())
    global g_now
    global g_now_epoch
    if g_now == None:
        g_now = str(datetime.datetime.now())
        g_now_epoch = last_update

    ## NOTE: it's almost certainly a mistake to include source_lfn in the hash here as it
    ## includes /store/temp/user/foo.$HASH. We should normalize based on the final LFN (/store/user/...)
    doc_id = hashlib.sha224(source_lfn).hexdigest()
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

    couchServer = CMSCouch.CouchServer(dburl = ad['CRAB_ASOURL'], ckey = os.environ['X509_USER_PROXY'], cert = os.environ['X509_USER_PROXY'])
    couchDatabase = couchServer.connectDatabase("asynctransfer", create = False)
    print "Stageout job description: %s" % pprint.pformat(info)

    needs_commit = True
    try:
        doc = couchDatabase.document(doc_id)
        ## The document is already in ASO database. This means we are retrying the job and the
        ## document was injected by a previous job retry. The transfer status must be terminal
        ## ('done', 'failed' or 'killed'), since the PostJob doesn't exit until all transfers
        ## are finished.
        transfer_status = doc.get('state')
        msg = "LFN %s (id %s) is already in ASO database (file transfer status is '%s')." % (source_lfn, doc_id, transfer_status)
        if transfer_status in ['new', 'acquired', 'retry']:
            msg += "\nWARNING: File transfer status expected to be terminal ('done', 'failed' or 'killed')."
            msg += " Will not upload a new stageout request for the current job."
            needs_commit = False
        else:
            msg += " Uploading new stageout request for the current job."
        print msg
    except CMSCouch.CouchNotFoundError:
        ## The document is not yet in ASO database. We commit a new document.
        print "LFN %s (id %s) is not yet in ASO database. Uploading new stageout request." % (source_lfn, doc_id)
        doc = {"_id": doc_id,
               "inputdataset": ad["CRAB_InputData"],
               "group": group,
               "lfn": source_lfn,
               "user": ad['CRAB_UserHN'],
               "role": role,
               "dbSource_url": "gWMS",
               "publish_dbs_url": publish_dbs_url,
               "dbs_url": dbs_url,
               "workflow": ad['CRAB_ReqName'],
               "jobid": id,
               "publication_state": 'not_published',
               "publication_retry_count": [],
               "type": file_type,
               "publish": publish,
              }
    except Exception, ex:
        msg = "Error loading document from ASO database. Transfer submission failed."
        msg += str(ex)
        msg += str(traceback.format_exc())
        print (msg)
        return False
    if needs_commit:
        doc.update(info)
        commit_result = couchDatabase.commitOne(doc)[0]
        if 'error' in commit_result:
            print("Couldn't add to ASO database; error follows:")
            print(commit_result)
            return False
        print "Final stageout job description: %s" % pprint.pformat(doc)
        if not g_aso_start_time_set_in_JR:
            print "==== Setting ASO start time to %s (%s) in job report. ====" % (g_now, g_now_epoch)
            if not addToJR([('aso_start_time', g_now), ('aso_start_timestamp', g_now_epoch)]):
                print "WARNING: Failed to set ASO start time in job report."
            else:
                g_aso_start_time_set_in_JR = True

    return True


def performDirectTransfer(source_file, dest_pfn, dest_se, is_log):
    try:
        return performDirectTransferImpl(source_file, dest_pfn, dest_se, is_log)
    except WMException.WMException, ex:
        print "="*79
        print "==== START DUMP OF TRANSFER ERROR INFO ===="
        print "="*79
        print "Error during direct stageout:\n%s" % str(ex)
        print "="*79
        print "==== FINISH DUMP OF TRANSFER ERROR INFO ===="
        print "="*79
        return ex.data.get("ErrorCode", 60307)


def performDirectTransferImpl(source_file, dest_pfn, dest_se, is_log):
    command = "srmv2-lcg"
    protocol = "srmv2"

    try:
        impl = retrieveStageOutImpl(command)
    except Exception, ex:
        msg  = "Unable to retrieve impl for local stage out:\n"
        msg += "Error retrieving StageOutImpl for command named: %s\n" % (command,)
        raise StageOutError.StageOutFailure(msg, Command = command, LFN = dest_pfn, ExceptionDetail = str(ex))

    impl.numRetries = numberOfRetries
    impl.retryPause = retryPauseTime

    signal.alarm(waitTime)
    result = 0
    try:
        impl(protocol, source_file, dest_pfn, None, None)
    except Alarm:
        print "== Timeout reached during stageOut of %s; setting return code to 60403. ==" % source_file
        result = 60403
    except Exception, ex:
        msg = "== Failure for local stage out:\n"
        msg += str(ex)
        try:
            msg += traceback.format_exc()
        except AttributeError, ex:
            msg += "Traceback unavailable\n"
        raise StageOutError.StageOutFailure(msg, Command = command, Protocol = protocol, LFN = dest_pfn, InputPFN = source_file, TargetPFN = dest_pfn)
    finally:
        signal.alarm(0)

    if not result:
        dest_file_name = os.path.split(dest_pfn)[-1]
        addSEToJR(dest_file_name, is_log, dest_se, True)
    return result


def main():

    # Parse the job ad to get the stageout information.
    output_files = None
    dest_se = None
    dest_temp_dir = None
    dest_files = None
    stageout_policy = None
    transfer_logs = None
    transfer_outputs = None
    if '_CONDOR_JOB_AD' not in os.environ:
        print "== ERROR: _CONDOR_JOB_AD not in environment =="
        print "No stageout will be performed."
        return 80000
    elif not os.path.exists(os.environ['_CONDOR_JOB_AD']):
        print "== ERROR: _CONDOR_JOB_AD (%s) does not exist =="
        print "No stageout will be performed."
        return 80000
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
                    global g_job_id
                    g_job_id = int(val)
                elif name == "CRAB_localOutputFiles":
                    output_files = split_re.split(val.replace('"', ''))
                elif name == "CRAB_AsyncDestSE":
                    dest_se = val.replace('"', '')
                elif name == "CRAB_Dest":
                    dest_temp_dir = val.replace('"', '')
                elif name == "CRAB_Destination":
                    dest_files = split_re.split(val.replace('"', ''))
                elif name == "CRAB_StageoutPolicy":
                    stageout_policy = split_re.split(val.replace('"', ''))
                elif name == "CRAB_SaveLogsFlag":
                    transfer_logs = int(val)
                elif name == "CRAB_TransferOutputs":
                    transfer_outputs = int(val)
                elif name == "CRAB_NoWNStageout":
                    no_stageout = int(val)
        if g_job_id == None:
            print "== ERROR: Unable to determine CRAB Job ID."
            print "No stageout will be performed."
            return 80000
        if output_files == None:
            print "== ERROR: Unable to determine output files."
            print "No stageout will be performed."
            return 80000
        if dest_se == None:
            print "== ERROR: Unable to determine destination SE."
            print "No stageout will be performed."
            return 80000
        if dest_temp_dir == None:
            print "== ERROR: Unable to determine local destination directory."
            print "No stageout will be performed."
            return 80000
        if dest_files == None:
            print "== ERROR: Unable to determine remote destinations."
            print "No stageout will be performed."
            return 80000
        if stageout_policy == None:
            print "== ERROR: Unable to determine stageout policy."
            print "No stageout will be performed."
            return 80000
        else:
            print "Stageout policy: %s" % ", ".join(stageout_policy)
        if transfer_logs == None: #well, actually we might assume saveLogs=False and delete these lines..
            print "== ERROR: Unable to determine transfer_logs parameter."
            print "No stageout will be performed."
            return 80000
        if transfer_outputs == None:
            print "== ERROR: Unable to determine transfer_outputs parameter."
            print "No stageout will be performed."
            return 80000

    ## If CRAB_NoStageout has been set to an integer value >0 (maybe with extraJDL from the client) then we don't do the stageout
    if no_stageout:
        print "==== NOT PERFORMING STAGEOUT AS CRAB_NoWNStageout is 1 ===="
        return 0

    ## Set the JR name.
    global g_job_report_name
    g_job_report_name = 'jobReport.json.%d' % g_job_id

    ## Retrive the JR.
    try:
        with open(g_job_report_name) as fd:
            job_report = json.load(fd)
    except Exception, ex:
        print "== ERROR: Unable to retrieve %s." % g_job_report_name
        traceback.print_exc()
        return 80000

    ## Sanity check of the JR.
    if 'steps'  not in job_report:
        print "== ERROR: Invalid job report: missing 'steps'."
        return 80000
    if 'cmsRun' not in job_report['steps']:
        print "== ERROR: Invalid job report: missing 'cmsRun'."
        return 80000
    if 'output' not in job_report['steps']['cmsRun']:
        print "== ERROR: Invalid job report: missing 'output'."
        return 80000

    # The stageout manager will be used by all attempts
    stageOutCall = {}
    manager = StageOutMgr.StageOutMgr(
        retryPauseTime  = 60,
        numberOfRetries = 2,
        **stageOutCall)
    manager.retryPauseTime = retryPauseTime
    manager.numberOfRetries = numberOfRetries

    counter = "%04d" % (g_job_id / 1000)
    dest_temp_dir = os.path.join(dest_temp_dir, counter)

    ## Try to determine whether the payload actually succeeded.
    ## If the payload didn't succeed, we put it in a different directory. This prevents us from
    ## putting failed output files in the same directory as successful output files; we worry
    ## that users may simply 'ls' the directory and run on all listed files.
    global g_cmsRun_exit_code
    try:
        g_cmsRun_exit_code = job_report['jobExitCode']
    except Exception, ex:
        print "== WARNING: Unable to retrieve cmsRun exit code from job report."
        traceback.print_exc()
    if g_cmsRun_exit_code:
        dest_temp_dir = os.path.join(dest_temp_dir, "failed")

    ## Transfer of log tarball.
    logfile_name = 'cmsRun_%d.log.tar.gz' % g_job_id
    try:
        log_size = os.stat(logfile_name).st_size
    except:
        pass
    else:
        addToJR([('log_size', log_size)])
    dest_temp_lfn = os.path.join(dest_temp_dir, "log", logfile_name)
    try:
        print "==== Starting compression of user logs at %s ====" % time.ctime()
        std_retval = compress(g_job_id)
        print "==== Finished compression of user logs at %s (status %d) ====" % (time.ctime(), std_retval)
        if not std_retval:
            print "==== Starting stageout of user logs at %s ====" % time.ctime()
            if not transfer_logs:
                print "Performing only local stageout of user logs since the user did not specify General.saveLogs = True"
            dest_pfn = dest_files.pop(0)
            std_retval = performTransfer(manager, stageout_policy if transfer_logs else ["local"], \
                                         logfile_name, dest_temp_lfn, dest_pfn, dest_se, is_log = True, inject = transfer_logs)
    except Exception, ex:
        print "== ERROR: Unhandled exception when performing stageout of user logs."
        traceback.print_exc()
        if not std_retval:
            std_retval = 60318
    finally:
        print "==== Finished stageout of user logs at %s (status %d) ====" % (time.ctime(), std_retval)
    if not transfer_logs and std_retval:
        print "Ignoring stageout failure of user logs, because the user did not request the logs to be staged out."
        std_retval = 0

    ## Transfer of output files.
    out_retval = 0
    for outfile_name_info, dest_pfn in zip(output_files, dest_files):
        if len(outfile_name_info.split("=")) != 2:
            print "== ERROR: Invalid output format (%s)." % outfile_name_info
            out_retval = 80000
            continue
        outfile_name, dest_outfile_name = outfile_name_info.split("=")
        if not os.path.exists(outfile_name):
            print "== ERROR: Output file %s does not exist." % outfile_name
            out_retval = 60302
            continue
        ## Check if the file is in the JR. If it is not, add it.
        is_file_in_job_report = bool(getOutputFileFromJR(outfile_name))
        if not is_file_in_job_report:
            addOutputFileToJR(outfile_name)
        ## Try to do the transfer.
        dest_temp_lfn = os.path.join(dest_temp_dir, dest_outfile_name)
        try:
            print "==== Starting stageout of %s at %s ====" % (outfile_name, time.ctime())
            if not transfer_outputs:
                print "Performing only local stageout of output files since the user specified General.transferOutput = False"
            cur_out_retval = performTransfer(manager, stageout_policy if transfer_outputs else ["local"], \
                                             outfile_name, dest_temp_lfn, dest_pfn, dest_se, is_log = False, inject = transfer_outputs)
        except Exception, ex:
            print "== ERROR: Unhandled exception when performing stageout of user outputs."
            traceback.print_exc()
            cur_out_retval = 60318
        finally:
            print "==== Finished stageout of %s at %s (status %d) ====" % (outfile_name, time.ctime(), out_retval)
        if cur_out_retval and not out_retval:
            out_retval = cur_out_retval

    if std_retval:
        return std_retval

    return out_retval


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        for arg in sys.argv:
            if 'JOB_EXIT_CODE=' in arg and len(arg.split("=")) == 2:
                g_job_exit_code = int(arg.split("=")[1])
    except:
        pass
    try:
        retval = main()
    except:
        print "==== ERROR: Unhandled exception."
        traceback.print_exc()
        retval = 60307
    if g_job_exit_code:
        retval = g_job_exit_code
    if retval:
        try:
            reportFailureToDashboard(retval)
        except:
            print "==== ERROR: Unhandled exception when reporting failure to Dashboard. ===="
            traceback.print_exc()
    sys.exit(retval)

