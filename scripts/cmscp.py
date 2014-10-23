#!/usr/bin/env python2.6
import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category = DeprecationWarning)

import os
import sys
import re
import json
import time
import pprint
import signal
import logging
import tarfile
import datetime
import traceback
import hashlib

## Bootstrap the CMS_PATH variable; the StageOutMgr will need it.
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
    os.environ['http_proxy'] = "http://%s" % (os.environ['http_proxy'])

import WMCore.Storage.StageOutMgr as StageOutMgr
import WMCore.Storage.StageOutError as StageOutError
from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Algorithms.Alarm import Alarm, alarmHandler
import WMCore.WMException as WMException
import WMCore.Database.CMSCouch as CMSCouch
import WMCore.Services.PhEDEx.PhEDEx as PhEDEx
import DashboardAPI
from WMCore.Services.Requests import Requests
from httplib import HTTPException

## See the explanation of this sentry file in CMSRunAnalysis.py.
with open('wmcore_initialized', 'w') as fd_wmcore:
    fd_wmcore.write('wmcore initialized.\n')

##==============================================================================
## GLOBAL VARIABLES USED BY THE CODE.
##------------------------------------------------------------------------------

## This variable defines a timeout for local and direct transfers. We use it
## with the python signal module to define an alarm to signal a timeout.
G_TRANSFERS_TIMEOUT = 60*60 # = 60 minutes

## Stageout settings used by the local stageout manager.
G_NUMBER_OF_RETRIES = 2
G_RETRY_PAUSE_TIME = 60

## Variables used to set the time at which documents are injected to the ASO
## database (the so called aso start time).
G_NOW = None
G_NOW_EPOCH = None

## Name of the JSON job report.
G_JOB_REPORT_NAME = None

## The exit code of the job wrapper is put here after reading it from the job
## report. The exit code is used only to know if the job has failed, in which
## case we change the stageout directory (adding a 'failed' subdirectory) and
## to turn off the publication.
G_JOB_EXIT_CODE = None

## List to collect the files that have been staged out directly. The list is
## filed by the perform_direct_stageout() function. For each file, append a
## dictionary with relevant information used then in the clean_stageout_area() 
## function. If a file is removed from the remote storage, we still keep the
## file in this list, but set the 'removed' flag to True.
G_DIRECT_STAGEOUTS = []

## List to collect the transfer requests to ASO for files that were
## successfully transferred to the local storage. This list is filled in by the
## perform_local_stageout() function. For each file, append a dictionary with
## the information needed by the inject_to_aso() function.
G_ASO_TRANSFER_REQUESTS = []

## Dictionary with the job's HTCondor ClassAd. Filled in by parse_job_ad().
G_JOB_AD = {}

## Dictionary with the mapping of node storage element name to site name.
## Will be filled in by the make_node_map() function using PhEDEx.
G_NODE_MAP = {}

##==============================================================================
## FUNCTIONS USED BY THE CODE.
##------------------------------------------------------------------------------

def parse_job_ad():
    """
    Parse the job's HTCondor ClassAd.
    """
    ## TODO: Why don't we use the same method as in the PostJob?
    global G_JOB_AD
    with open(os.environ['_CONDOR_JOB_AD']) as fd_job_ad:
        for adline in fd_job_ad.readlines():
            info = adline.split(' = ', 1)
            if len(info) != 2:
                continue
            if info[1].startswith('undefined'):
                val = info[1].strip()
            elif info[1].startswith('"'):
                val = info[1].strip().replace('"', '')
            else:
                try:
                    val = int(info[1].strip())
                except ValueError:
                    continue
            G_JOB_AD[info[0]] = val

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def report_failure_to_dashboard(exit_code):
    """
    Report failure to Dashboard.
    """
    if os.environ.get('TEST_CMSCP_NO_STATUS_UPDATE', False):
        msg  = "Environment flag TEST_CMSCP_NO_STATUS_UPDATE is set."
        msg += " Will NOT send report to dashbaord."
        print msg
        return
    if not G_JOB_AD:
        try:
            parse_job_ad()
        except Exception:
            msg  = "ERROR: Unable to parse job's HTCondor ClassAd."
            msg += "\nWill NOT report stageout failure to Dashboard."
            msg += "\n%s" % (traceback.format_exc())
            print msg
            return
    for attr in ['CRAB_Id', 'CRAB_ReqName', 'CRAB_Retry']:
        if attr not in G_JOB_AD:
            msg  = "ERROR: Job's HTCondor ClassAd is missing attribute %s."
            msg += "\nWill not report stageout failure to Dashboard."
            msg  = msg % (attr)
            print msg
            return
    params = {
        'MonitorID': G_JOB_AD['CRAB_ReqName'],
        'MonitorJobID': '%d_https://glidein.cern.ch/%d/%s_%d' % \
                        (G_JOB_AD['CRAB_Id'],
                         G_JOB_AD['CRAB_Id'],
                         G_JOB_AD['CRAB_ReqName'].replace("_", ":"),
                         G_JOB_AD['CRAB_Retry']),
        'JobExitCode': exit_code
    }
    print "Dashboard stageout failure parameters: %s" % (str(params))
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    DashboardAPI.apmonFree()

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def make_logs_archive(arch_file_name):
    """
    Make a zipped tar archive file of the user logs plus the framework job
    report xml file.
    """
    retval = 0
    arch_file = tarfile.open(arch_file_name, 'w:gz')
    file_names = ['cmsRun-stdout.log', \
                  'cmsRun-stderr.log', \
                  'FrameworkJobReport.xml']
    for file_name in file_names:
        if os.path.exists(file_name):
            print "Adding %s to archive file %s" % (file_name, arch_file_name)
            file_name_no_ext, ext = file_name.rsplit('.', 1)
            job_id_str = '-%s' % (G_JOB_AD['CRAB_Id'])
            file_name_in_arch = file_name_no_ext + job_id_str + '.' + ext
            arch_file.add(file_name, arcname = file_name_in_arch)
        else:
            ## Will not fail stageout is logs are missing, because for example
            ## when runinng scriptExe there are no cmsRun-stdout.log and
            ## cmsRun-stderr.log files.
            print "== WARNING: %s is missing." % (file_name)
    arch_file.close()
    return retval

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def get_job_id(source):
    """
    Extract the job id from the file name.
    """
    file_basename = os.path.split(source)[-1]
    left_piece, right_piece = file_basename.rsplit('_', 1)
    if len(right_piece.split('.', 1)) == 2:
        job_id, ext = right_piece.split('.', 1)
    else:
        job_id, ext = right_piece, None
    try:
        job_id = int(job_id)
    except ValueError:
        job_id = -1
    if ext:
        orig_file_name = left_piece + '.' + ext
    else:
        orig_file_name = left_piece
    return orig_file_name, job_id

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def get_from_job_report(key, default = None, location = None):
    """
    Extract and return from the json job report section specified by the keys
    given in the location list (which is expected to be a dictionary) the value
    corresponding to the given key. If not found, return the default.
    """
    if G_JOB_REPORT_NAME is None:
        return default
    with open(G_JOB_REPORT_NAME) as fd_job_report:
        job_report = json.load(fd_job_report)
    subreport = job_report
    subreport_name = ''
    if location is None:
        location = []
    for loc in location:
        if loc in subreport:
            subreport = subreport[loc]
            subreport_name += "['%s']" % (loc)
        else:
            msg = "WARNING: Job report doesn't contain section %s['%s']."
            msg = msg % (subreport_name, loc)
            print msg
            return default
    if type(subreport) != dict:
        if subreport_name:
            msg = "WARNING: Job report section %s is not a dict."
            msg = msg % (subreport_name)
            print msg
        else:
            print "WARNING: Job report is not a dict."
        return default
    return subreport.get(key, default)

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def get_output_file_from_job_report(file_name, job_report = None):
    """
    Extract and return from the json job report, section ['steps']['cmsRun']
    ['output'] the part corresponding to the given output file name. If not
    found, return None.
    """
    if job_report is None:
        if G_JOB_REPORT_NAME is None:
            return None
        with open(G_JOB_REPORT_NAME) as fd_job_report:
            job_report = json.load(fd_job_report)
    job_report_output = job_report['steps']['cmsRun']['output']
    for output_module in job_report_output.values():
        for output_file_info in output_module:
            if os.path.split(str(output_file_info.get(u'pfn')))[-1] == file_name:
                return output_file_info
    return None

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def add_to_job_report(key_value_pairs, location = None, mode = 'overwrite'):
    """
    Add pairs of (key, value) given in the key_value_pairs list of 2-tuples to
    the json job report in the section specified by the keys given in the
    location list. This job report section is expected to be a dictionary. For
    example, if location is ['steps', 'cmsRun', 'output'], add each (key, value)
    pair in key_value_pairs to jobreport['steps']['cmsRun']['output'] (for short
    jobreport[location]). There are three different modes of adding the
    information to the job report: overwrite (does a direct assignment:
    jobreport[location][key] = value), new (same as overwrite, but the given key
    must not exist in jobreport[location]; if it exists don't modify the job
    report and return False) and update (jobreport[location][key] is a list and
    so append the value into that list; if the key doesn't exist in
    jobreport[location], add it). In case of an identified problem, don't modify
    the job report, print a warning message and return False. Return True
    otherwise.
    """
    if G_JOB_REPORT_NAME is None:
        return False
    with open(G_JOB_REPORT_NAME) as fd_job_report:
        job_report = json.load(fd_job_report)
    subreport = job_report
    subreport_name = ''
    if location is None:
        location = []
    for loc in location:
        if loc in subreport:
            subreport = subreport[loc]
            subreport_name += "['%s']" % loc
        else:
            msg = "WARNING: Job report doesn't contain section %s['%s']."
            msg = msg % (subreport_name, loc)
            print msg
            return False
    if type(subreport) != dict:
        if subreport_name:
            msg = "WARNING: Job report section %s is not a dict."
            msg = msg % (subreport_name) 
            print msg
        else:
            print "WARNING: Job report is not a dict."
        return False
    if mode in ['new', 'overwrite']:
        for key, value in key_value_pairs:
            if mode == 'new' and key in subreport:
                msg = "WARNING: Key '%s' already exists in job report section %s."
                msg = msg % (key, subreport_name)
                print msg
                return False
            subreport[key] = value
    elif mode == 'update':
        for key, value in key_value_pairs:
            subreport.setdefault(key, []).append(value)
    else:
        print "WARNING: Unknown mode '%s'." % mode
        return False
    with open(G_JOB_REPORT_NAME, 'w') as fd_job_report:
        json.dump(job_report, fd_job_report)
    return True

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def add_output_file_to_job_report(file_name, key = 'addoutput'):
    """
    Add the given output file to the json job report section ['steps']['cmsRun']
    ['output'] under the given key. The value to add is a dictionary
    {'pfn': file_name}.
    """
    print "Adding file %s to job report." % (file_name)
    output_file_info = {}
    output_file_info['pfn'] = file_name
    try:
        file_size = os.stat(file_name).st_size
    except:
        print "WARNING: Unable to add output file size to job report."
    else:
        output_file_info['size'] = file_size
    is_ok = add_to_job_report([(key, output_file_info)], \
                              ['steps', 'cmsRun', 'output'], 'update')
    if not is_ok:
        print "ERROR: Failed to add file to job report."
    return is_ok

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def add_sites_to_job_report(file_name, is_log, \
                            temp_storage_site, storage_site, \
                            local_stageout, direct_stageout):
    """
    Alter the json job report to record the source and destination sites where
    this file was (or will be) staged out and whether it was a direct stageout
    or not.
    """
    orig_file_name, _ = get_job_id(file_name)
    msg  = "Setting temp_storage_site = '%s', storage_site = '%s',"
    msg += " local_stageout = %s and direct_stageout = %s"
    msg += " for file %s in job report."
    msg  = msg % (temp_storage_site, storage_site, str(bool(local_stageout)), \
                  str(bool(direct_stageout)), orig_file_name)
    print msg
    is_ok = True
    key_value_pairs = [('temp_storage_site', temp_storage_site), \
                       ('storage_site', storage_site), \
                       ('local_stageout', bool(local_stageout)), \
                       ('direct_stageout', bool(direct_stageout))]
    is_ok = add_to_file_in_job_report(file_name, is_log, key_value_pairs)
    if not is_ok:
        msg  = "ERROR: Failed to set the above keys and values"
        msg += " for file %s in job report." % (orig_file_name)
        print msg
    return is_ok

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def add_to_file_in_job_report(file_name, is_log, key_value_pairs):
    """
    Alter the json job report file for the given file ('file_name') with the
    given key and value pairs ('key_value_pairs'). If the given file is the log,
    record in the top-level of.
    """
    if is_log:
        is_ok = add_to_job_report(key_value_pairs)
        return is_ok
    if G_JOB_REPORT_NAME is None:
        return False
    orig_file_name, _ = get_job_id(file_name)
    with open(G_JOB_REPORT_NAME) as fd_job_report:
        job_report = json.load(fd_job_report)
    output_file_info = get_output_file_from_job_report(orig_file_name, job_report)
    if output_file_info is None:
        msg = "WARNING: Metadata for file %s not found in job report."
        msg = msg % (orig_file_name)
        print msg
        return False
    for key, value in key_value_pairs:
        output_file_info[key] = value
    with open(G_JOB_REPORT_NAME, 'w') as fd_job_report:
        json.dump(job_report, fd_job_report)
    return True

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def make_node_map():
    """
    Fill in the G_NODE_MAP dictionary with the mapping of node storage element
    name to site name.
    """
    phedex = PhEDEx.PhEDEx()
    nodes = phedex.getNodeMap()['phedex']['node']
    global G_NODE_MAP
    for node in nodes:
        G_NODE_MAP[str(node[u'se'])] = str(node[u'name'])

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def perform_stageout(local_stageout_mgr, direct_stageout_impl, \
                     direct_stageout_command, direct_stageout_protocol, \
                     policy, \
                     source_file, dest_temp_lfn, dest_pfn, dest_lfn, dest_site, \
                     is_log, inject):
    """
    Wrapper for local and direct stageouts.
    """
    result = -1
    if policy == 'local':
        result = perform_local_stageout(local_stageout_mgr, \
                                        source_file, dest_temp_lfn, \
                                        dest_lfn, dest_site, \
                                        is_log, inject)
    elif policy == 'remote':
        ## Can return 60311, 60307 or 60403.
        result = perform_direct_stageout(direct_stageout_impl, \
                                         direct_stageout_command, \
                                         direct_stageout_protocol, \
                                         source_file, dest_pfn, dest_site, \
                                         is_log)
    else:
        print "ERROR: Skipping unknown stageout policy named '%s'." % (policy)
    if result == -1:
        print "FATAL ERROR: No stageout policy was attempted."
        result = 80000
    return result

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def perform_local_stageout(local_stageout_mgr, \
                           source_file, dest_temp_lfn, dest_lfn, dest_site, \
                           is_log, inject):
    """
    Wrapper for local stageouts.
    """
    file_for_transfer = {'LFN': dest_temp_lfn, 'PFN': source_file}
    ## Start the clock for timeout counting.
    signal.signal(signal.SIGALRM, alarmHandler)
    signal.alarm(G_TRANSFERS_TIMEOUT)
    ## Do the local stageout.
    result = 0
    try:
        ## Throws on any failure.
        print "       -----> Stageout manager log start"
        stageout_info = local_stageout_mgr(file_for_transfer)
        print "       <----- Stageout manager log finish"
    except Alarm:
        ## Alarm was raised, because the timeout (G_TRANSFERS_TIMEOUT) was
        ## reached.
        print "       <----- Stageout manager log finish"
        msg  = "Timeout reached during stageout of %s;" % (source_file)
        msg += " setting return code to 60403."
        print msg
        result = 60403
    except Exception, ex:
        print "Error during stageout: %s" % (ex)
        print "       <----- Stageout manager log finish"
        result = 60307
    finally:
        signal.alarm(0)
    if result == 0:
        dest_temp_file_name = os.path.split(dest_temp_lfn)[-1]
        dest_temp_se = stageout_info['SEName']
        dest_temp_site = G_NODE_MAP.get(dest_temp_se, 'unknown')
        sites_added_ok = add_sites_to_job_report(dest_temp_file_name, \
                                                 is_log, dest_temp_site, \
                                                 dest_site if inject else 'unknown', \
                                                 True, False)
        if not sites_added_ok:
            msg  = "WARNING: Ignoring failure in adding the above information"
            msg += " to the job report."
            print msg
        if inject:
            file_transfer_info = {'source'             : {'lfn': dest_temp_lfn, 'site': dest_temp_site},
                                  'destination'        : {'lfn': dest_lfn,      'site': dest_site     },
                                  'is_log'             : is_log,
                                  'local_stageout_mgr' : local_stageout_mgr,
                                  'inject'             : True
                                 }
            G_ASO_TRANSFER_REQUESTS.append(file_transfer_info)
    return result

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def inject_to_aso(file_transfer_info):
    """
    Inject a document to the ASO database.
    """
    parse_job_ad()
    for attr in ['CRAB_ASOURL', 'CRAB_AsyncDest', 'CRAB_InputData', \
                 'CRAB_UserGroup', 'CRAB_UserRole', 'CRAB_DBSURL', \
                 'CRAB_ReqName', 'CRAB_UserHN', 'CRAB_Publish', \
                 'CRAB_RestHost', 'CRAB_RestURInoAPI']:
        if attr not in G_JOB_AD:
            msg  = "ERROR: Job's HTCondor ClassAd is missing attribute %s."
            msg += " Cannot inject to ASO."
            msg  = msg % (attr)
            print msg
            return 80000
    if 'X509_USER_PROXY' not in os.environ:
        msg  = "ERROR: X509_USER_PROXY missing in user environment."
        msg += " Cannot inject to ASO."
        print msg
        return 80000
    if not os.path.exists(os.environ['X509_USER_PROXY']):
        msg  = "ERROR: User proxy %s missing on disk."
        msg += " Cannot inject to ASO."
        msg  = msg % (os.environ['X509_USER_PROXY'])
        print msg
        return 80000

    file_name = os.path.split(file_transfer_info['source']['lfn'])[-1]
    file_type = 'log' if file_transfer_info['is_log'] else 'output'

    orig_file_name, _ = get_job_id(file_name)
    if file_transfer_info['is_log']:
        size = get_from_job_report('log_size', 0)
        # Copied from PostJob.py, but not sure if it does anything. BB
        checksums = {'adler32': 'abc'}
    else:
        output_file_info = get_output_file_from_job_report(orig_file_name)
        if output_file_info:
            checksums = output_file_info.get(u'checksums', {'cksum': '0', 'adler32': '0'})
            size = output_file_info.get(u'size', 0)
            is_edm = (output_file_info.get(u'output_module_class', '') == u'PoolOutputModule' or \
                      output_file_info.get(u'ouput_module_class',  '') == u'PoolOutputModule')
        else:
            checksums = {'cksum': '0', 'adler32': '0'}
            size = 0
            is_edm = False

    source_site = file_transfer_info['source']['site']
    if source_site is None:
        msg  = "ERROR: Unable to determine local node name."
        msg += " Cannot inject to ASO." 
        print msg
        return 80000

    role = str(G_JOB_AD['CRAB_UserRole'])
    if str(G_JOB_AD['CRAB_UserRole']).lower() == 'undefined':
        role = ''
    group = str(G_JOB_AD['CRAB_UserGroup'])
    if str(G_JOB_AD['CRAB_UserGroup']).lower() == 'undefined':
        group = ''
    task_publish = int(G_JOB_AD['CRAB_Publish'])
    publish = int(task_publish and file_type == 'output' and is_edm)
    if task_publish and file_type == 'output' and not is_edm:
        msg  = "Disabling publication of output file %s,"
        msg += " it is not of EDM type (not produced by PoolOutputModule)."
        msg  = msg % (file_name)
        print msg
    publish = int(publish and G_JOB_EXIT_CODE == 0)

    last_update = int(time.time())
    global G_NOW
    global G_NOW_EPOCH
    if G_NOW == None:
        G_NOW = str(datetime.datetime.now())
        G_NOW_EPOCH = last_update

    ## NOTE: it's almost certainly a mistake to include the source LFN in the
    ## hash here as it includes /store/temp/user/foo.$HASH. We should normalize
    ## based on the final LFN (/store/user/foo/).
    doc_id = hashlib.sha224(file_transfer_info['source']['lfn']).hexdigest()
    doc_new_info = {'state'           : 'new',
                    'source'          : source_site,
                    'destination'     : G_JOB_AD['CRAB_AsyncDest'],
                    'checksums'       : checksums,
                    'size'            : size,
                    # The following four times - and how they're calculated - makes no sense to me. BB
                    'last_update'     : last_update,
                    'start_time'      : G_NOW,
                    'end_time'        : '',
                    'job_end_time'    : time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                    'retry_count'     : [],
                    'failure_reason'  : [],
                    'job_retry_count' : G_JOB_AD.get('CRAB_Retry', -1),
                   }

    couch_server = CMSCouch.CouchServer(dburl = G_JOB_AD['CRAB_ASOURL'], \
                                        ckey = os.environ['X509_USER_PROXY'], \
                                        cert = os.environ['X509_USER_PROXY'])
    couch_database = couch_server.connectDatabase("asynctransfer", create = False)
    print "Stageout request document so far:\n%s" % (pprint.pformat(doc_new_info))

    needs_commit = True
    try:
        doc = couch_database.document(doc_id)
        ## The document is already in ASO database. This means we are retrying
        ## the job and the document was injected by a previous job retry. The
        ## transfer status must be terminal ('done', 'failed' or 'killed'),
        ## since the post-job doesn't exit until all transfers are finished.
        transfer_status = doc.get('state')
        msg = "LFN %s (id %s) is already in ASO database (file transfer status is '%s')."
        msg = msg % (file_transfer_info['source']['lfn'], doc_id, transfer_status)
        if transfer_status in ['new', 'acquired', 'retry']:
            msg += "\nFile transfer status is not terminal ('done', 'failed' or 'killed')."
            msg += " Will not upload a new stageout request for the current job retry."
            needs_commit = False
        else:
            msg += " Uploading new stageout request for the current job retry."
        print msg
    except CMSCouch.CouchNotFoundError:
        ## The document is not yet in ASO database. We commit a new document.
        msg  = "LFN %s (id %s) is not yet in ASO database."
        msg += " Uploading new stageout request."
        msg  = msg % (file_transfer_info['source']['lfn'], doc_id)
        print msg
        doc = {'_id'                     : doc_id,
               'workflow'                : G_JOB_AD['CRAB_ReqName'],
               'jobid'                   : G_JOB_AD['CRAB_Id'],
               'rest_host'               : G_JOB_AD['CRAB_RestHost'],
               'rest_uri'                : G_JOB_AD['CRAB_RestURInoAPI'],
               'inputdataset'            : G_JOB_AD['CRAB_InputData'],
               'dbs_url'                 : str(G_JOB_AD['CRAB_DBSURL']),
               'lfn'                     : file_transfer_info['source']['lfn'],
               'source_lfn'              : file_transfer_info['source']['lfn'],
               'destination_lfn'         : file_transfer_info['destination']['lfn'],
               'type'                    : file_type,
               'publish'                 : publish,
               'publication_state'       : 'not_published',
               'publication_retry_count' : [],
               'user'                    : G_JOB_AD['CRAB_UserHN'],
               'role'                    : role,
               'group'                   : group,
              }
    except Exception:
        msg  = "Error loading document from ASO database."
        msg += " Transfer submission failed."
        msg += "\n%s" % (traceback.format_exc())
        print msg
        return 60320
    if needs_commit:
        doc.update(doc_new_info)
        commit_result = couch_database.commitOne(doc)[0]
        if 'error' in commit_result:
            print "Couldn't add to ASO database; error follows:"
            print commit_result
            return 60320
        print "Final stageout job description:\n%s" % (pprint.pformat(doc))
        if get_from_job_report('aso_start_time') is None or \
           get_from_job_report('aso_start_timestamp') is None:
            msg  = "Setting aso_start_time = %s and aso_start_time_stamp = %s"
            msg += " in job report."
            msg  = msg % (G_NOW, G_NOW_EPOCH)
            print msg 
            is_ok = add_to_job_report([('aso_start_time', G_NOW), \
                                       ('aso_start_timestamp', G_NOW_EPOCH)])
            if not is_ok:
                print "WARNING: Failed to set aso_start_time in job report."

    return 0

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def perform_direct_stageout(direct_stageout_impl, \
                            direct_stageout_command, direct_stageout_protocol, \
                            source_file, dest_pfn, dest_site, \
                            is_log):
    """
    Wrapper for direct stageouts.
    """
    ## Keep track of the directly staged out files. First use case is to remove
    ## them in case of stageout failure.
    global G_DIRECT_STAGEOUTS
    direct_stageout_info = {'dest_pfn'  : dest_pfn,
                            'dest_site' : dest_site,
                            'is_log'    : is_log,
                            'removed'   : False
                           }
    G_DIRECT_STAGEOUTS.append(direct_stageout_info)
    result = 0
    try:
        ## Start the clock for timeout counting.
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(G_TRANSFERS_TIMEOUT)
        ## Do the direct stageout.
        try:
            print "       -----> Stageout implementation log start"
            direct_stageout_impl(direct_stageout_protocol, \
                                 source_file, dest_pfn, None, None)
            print "       <----- Stageout implementation log finish"
        except Alarm:
            print "       <----- Stageout implementation log finish"
            ## Alarm was raised, because the timeout (G_TRANSFERS_TIMEOUT) was
            ## reached.
            msg  = "Timeout reached during stage out of %s;"
            msg += " setting return code to 60403."
            msg  = msg % (source_file)
            print msg
            result = 60403
        except Exception, ex:
            msg = "Failure in direct stage out:\n"
            msg += str(ex)
            try:
                msg += "\n%s" % (traceback.format_exc())
            except AttributeError, ex:
                msg += "\nTraceback unavailable\n"
            ## StageOutError.StageOutFailure has error code 60311.
            raise StageOutError.StageOutFailure(msg, Command = direct_stageout_command, Protocol = direct_stageout_protocol, \
                                                LFN = dest_pfn, InputPFN = source_file, TargetPFN = dest_pfn)
        finally:
            signal.alarm(0)
    except WMException.WMException, ex:
        print "Error during direct stageout:\n%s" % str(ex)
        print "       <----- Stageout implementation log finish"
        result = ex.data.get("ErrorCode", 60307)
    if result == 0:
        dest_file_name = os.path.split(dest_pfn)[-1]
        sites_added_ok = add_sites_to_job_report(dest_file_name, is_log, \
                                                 'unknown', dest_site, \
                                                 False, True)
        if not sites_added_ok:
            msg  = "WARNING: Ignoring failure in adding the above information"
            msg += " to the job report."
            print msg

    return result

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def clean_stageout_area(local_stageout_mgr, direct_stageout_impl, policy, \
                        logs_arch_dest_temp_lfn, keep_log):
    """
    Wrapper for cleaning the local or remote storage areas.
    """
    if policy == 'local':
        add_back_logs_arch = False
        if keep_log:
            ## Temporarily removing the logs archive file from the list of
            ## successfully completed local stageouts (if corresponds to this
            ## manager), because we don't want to remove the logs archive from
            ## the local temporary storage when calling
            ## local_stageout_mgr.cleanSuccessfulStageOuts(), because we want
            ## the user to be able to retrieve the logs archive via
            ## 'crab getlog'.
            if logs_arch_dest_temp_lfn in local_stageout_mgr.completedFiles:
                logs_arch_info = local_stageout_mgr.completedFiles[logs_arch_dest_temp_lfn]
                del local_stageout_mgr.completedFiles[logs_arch_dest_temp_lfn]
                add_back_logs_arch = True
                msg  = "Will not remove logs archive file from local temporary"
                msg += " storage (but will consider its local stageout"
                msg += " as failed for transfer purposes)."
                print msg
        for dest_temp_lfn in local_stageout_mgr.completedFiles.keys():
            file_name = os.path.basename(dest_temp_lfn)
            orig_file_name, _ = get_job_id(file_name)
            is_log = (dest_temp_lfn == logs_arch_dest_temp_lfn)
            msg = "Setting local_stageout = False for file %s in job report."
            msg = msg % (orig_file_name)
            print msg
            add_to_file_in_job_report(file_name, is_log, [('local_stageout', False)])
        num_files_to_remove = len(local_stageout_mgr.completedFiles)
        if num_files_to_remove > 0:
            msg = "Will remove %d %sfile%s from local temporary storage."
            msg = msg % (num_files_to_remove, \
                         'other ' if add_back_logs_arch else '', \
                         's' if num_files_to_remove > 1 else '')
            print msg
            ## Remove from the local temporary storage the files that were
            ## successfully transferred to that storage by the local stageout
            ## manager given as input.
            try:
                print "       -----> Stageout manager log start"
                local_stageout_mgr.cleanSuccessfulStageOuts()
                print "       <----- Stageout manager log finish"
            except StageOutError:
                print "       <----- Stageout manager log finish"
                pass
        else:
            msg = "There are no %sfiles to remove in local temporary storage."
            msg = msg % ('other ' if add_back_logs_arch else '')
            print msg
        if add_back_logs_arch:
            ## Now add back the logs archive file to the list of successfully
            ## completed local stageouts (if corresponds to this manager).
            local_stageout_mgr.completedFiles[logs_arch_dest_temp_lfn] = logs_arch_info
        ## Remove these same files from the list of files that need injection to ASO
        ## database. Notice that the logs archive file is removed from the list even
        ## if not removed from the storage.
        global G_ASO_TRANSFER_REQUESTS
        for file_transfer_info in G_ASO_TRANSFER_REQUESTS:
            if file_transfer_info['local_stageout_mgr'] == local_stageout_mgr:
                file_transfer_info['inject'] = False
    elif policy == 'remote':
        num_files_to_remove = 0
        found_log = False
        for direct_stageout_info in G_DIRECT_STAGEOUTS:
            if direct_stageout_info['removed']:
                continue
            dest_site = direct_stageout_info['dest_site']
            if direct_stageout_info['is_log']:
                found_log = True
                if keep_log:
                    continue
            num_files_to_remove += 1
        if num_files_to_remove > 0:
            msg = "Will remove %d %sfile%s from permanent storage at %s."
            msg = msg % (num_files_to_remove, \
                         'other ' if found_log and keep_log else '', \
                         's' if num_files_to_remove > 1 else '', dest_site)
            print msg
            for direct_stageout_info in G_DIRECT_STAGEOUTS:
                if direct_stageout_info['removed']:
                    continue
                if direct_stageout_info['is_log'] and keep_log:
                    msg  = "Will not remove logs archive file from"
                    msg += " permanent storage at %s" % (dest_site)
                    msg += " (but will consider its direct stageout as failed)."
                    print msg
                    continue
                dest_site = direct_stageout_info['dest_site']
                dest_pfn  = direct_stageout_info['dest_pfn']
                file_name = os.path.basename(dest_pfn)
                orig_file_name, _ = get_job_id(file_name)
                msg = "Setting direct_stageout = False for file %s in job report."
                msg = msg % (orig_file_name)
                print msg
                add_to_file_in_job_report(file_name, \
                                          direct_stageout_info['is_log'], \
                                          [('direct_stageout', False)])
                direct_stageout_info['removed'] = True
                msg = "Attempting to remove PFN %s from permanent storage at %s."
                msg = msg % (dest_pfn, dest_site)
                print msg
                try:
                    print "       -----> Stageout implementation log start"
                    direct_stageout_impl.removeFile(dest_pfn)
                    print "       <----- Stageout implementation log finish"
                    print "File successfully removed."
                except:
                    print "       <----- Stageout implementation log finish"
                    print "WARNING: Failed to remove file (maybe the file was not transferred)."
                    pass
        else:
            msg = "There are no %sfiles to remove in the permanent storage at %s."
            msg = msg % ('other ' if found_log and keep_log else '', dest_site)
            print msg
    else:
        msg  = "WARNING: Unknown stageout policy '%s'." % (policy)
        msg += " Skipping cleanup of stageout area."
        print msg

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def upload_log_file_metadata(dest_temp_lfn, dest_lfn):
    """
    Upload the logs archive file metadata.
    """
    if 'X509_USER_PROXY' not in os.environ:
        msg  = "ERROR: X509_USER_PROXY missing in user environment."
        msg += " Unable to upload file metadata."
        print msg
        return 80000
    if not os.path.exists(os.environ['X509_USER_PROXY']):
        msg  = "ERROR: User proxy %s missing on disk." % os.environ['X509_USER_PROXY']
        msg += " Unable to upload file metadata."
        print msg
        return 80000
    for attr in ['CRAB_ReqName', 'CRAB_Id', 'CRAB_PublishName', 'CRAB_JobSW', \
                 'CRAB_RestHost', 'CRAB_RestURInoAPI']:
        if attr not in G_JOB_AD:
            msg  = "ERROR: Job's HTCondor ClassAd is missing attribute %s." % attr
            msg += " Unable to upload file metadata."
            print msg
            return 80000
    temp_storage_site = str(get_from_job_report('temp_storage_site', 'unknown'))
    if temp_storage_site == 'unknown':
        msg  = "WARNING: Temporary storage site for logs archive file"
        msg += " not defined in job report."
        msg += " Will use the executed site as the temporary storage site."
        print msg
        temp_storage_site = str(get_from_job_report('executed_site', 'unknown'))
        if temp_storage_site == 'unknown':
            msg  = "WARNING: Unable to determine executed site from job report."
            msg += " Aborting logs archive file metadata upload."
            print msg
            return 80000
    configreq = {'taskname'        : G_JOB_AD['CRAB_ReqName'],
                 'pandajobid'      : G_JOB_AD['CRAB_Id'],
                 'outsize'         : int(get_from_job_report('log_size', 0)),
                 'publishdataname' : G_JOB_AD['CRAB_PublishName'],
                 'appver'          : G_JOB_AD['CRAB_JobSW'],
                 'outtype'         : 'LOG',
                 'checksummd5'     : 'asda', # Not implemented
                 'checksumcksum'   : '3701783610', # Not implemented
                 'checksumadler32' : '6d1096fe', # Not implemented
                 'acquisitionera'  : 'null', # Not implemented
                 'events'          : 0,
                 'outlocation'     : G_JOB_AD['CRAB_AsyncDest'],
                 'outlfn'          : dest_lfn,
                 'outtmplocation'  : temp_storage_site,
                 #'outtmplfn'       : dest_temp_lfn,
                 'outdatasetname'  : '/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER',
                 #'localstageout'   : int(get_from_job_report('local_stageout', 0)),
                 'directstageout'  : int(get_from_job_report('direct_stageout', 0))
                }
    rest_host = G_JOB_AD['CRAB_RestHost']
    if not rest_host.startswith('http'):
        rest_host = 'https://' + rest_host
    rest_uri_no_api = G_JOB_AD['CRAB_RestURInoAPI']
    rest_api = 'filemetadata'
    rest_uri = rest_uri_no_api + '/' + rest_api
    rest_url = rest_host + rest_uri
    msg = "Uploading file metadata for %s to %s: %s"
    msg = msg % (os.path.basename(dest_temp_lfn), rest_url, configreq)
    print msg
    server = Requests(rest_host, {'key' : os.environ['X509_USER_PROXY'], \
                                  'cert': os.environ['X509_USER_PROXY']})
    headers = {'Accept': '*/*'} #'User-agent': 'CRABClient/3.3.10'}
    try:
        server.put(rest_uri, configreq, headers)
    except HTTPException, hte:
        msg  = "Got HTTP exception when uploading logs archive file metadata:"
        msg += "%s \n%s" % (str(hte.headers), traceback.format_exc())
        print msg
        raise
    except Exception:
        msg  = "Got exception when uploading logs archive file metadata."
        msg += "\n%s" % (traceback.format_exc())
        print msg
        raise
    return 0

##==============================================================================
## THE MAIN FUNCTION THAT RUNS CMSCP.
##------------------------------------------------------------------------------

def main():
    """
    cmscp main.
    """
    ## Initialize the cmscp return code.
    cmscp_return_code = 0
    def update_cmscp_return_code(cmscp_return_code, return_code, force = False):
        """
        Function used to update the cmscp return code.
        """
        if (cmscp_return_code == 0 and not return_code in [None, 0]) or \
           (force                  and not return_code in [None]   ):
            msg = "Setting cmscp.py return status code to %d." % (return_code)
            print msg
            cmscp_return_code = return_code

    transfer_logs    = None
    transfer_outputs = None
    output_files     = None
    stageout_policy  = None
    dest_temp_dir    = None
    dest_site        = None
    dest_files       = None

    ## Auxiliary variable that can be used to make the code more readable.
    no_condition = True

    ## These flags can be used to force skipping any of the steps coded below.
    ## May be useful for debugging.
    skip_job_report_validation = False
    skip_outputs_exist = False
    skip_outputs_in_job_report = False
    skip_logs_arch = False
    skip_init_local_stageout_mgr = False
    skip_init_direct_stageout_impl = False
    skip_logs_stageout = {}
    skip_logs_stageout['local']  = False
    skip_logs_stageout['remote'] = False
    skip_outputs_stageout = {}
    skip_outputs_stageout['local']  = False
    skip_outputs_stageout['remote'] = False
    skip_aso_injection = False
    skip_logs_metadata_upload = False

    ## Fill in the G_NODE_MAP dictionary with the mapping of node storage
    ## storage name to site name. Currently only used to translate the SE name
    ## returned by the local stageout manager into a site name. 
    make_node_map()

    ##--------------------------------------------------------------------------
    ## Start PARSE JOB AD
    ##--------------------------------------------------------------------------
    ## Parse the job's HTCondor ClassAd.
    if '_CONDOR_JOB_AD' not in os.environ:
        msg  = "ERROR: _CONDOR_JOB_AD not in environment."
        msg += "\nNo stageout will be performed."
        print msg
        update_cmscp_return_code(cmscp_return_code, 80000, True)
        return cmscp_return_code
    if not os.path.exists(os.environ['_CONDOR_JOB_AD']):
        msg  = "ERROR: _CONDOR_JOB_AD (%s) does not exist."
        msg += "\nNo stageout will be performed."
        msg  = msg % (os.environ['_CONDOR_JOB_AD'])
        print msg
        update_cmscp_return_code(cmscp_return_code, 80000, True)
        return cmscp_return_code
    try:
        parse_job_ad()
    except Exception:
        global G_JOB_AD
        G_JOB_AD = {}
        msg  = "WARNING: Unable to parse job's HTCondor ClassAd."
        msg += "\n%s" % (traceback.format_exc())   
        print msg
    ## If CRAB_NoWNStageout has been set to an integer value > 0 (maybe with
    ## extraJDL from the client) then we don't do the stageout.
    if G_JOB_AD.get('CRAB_NoWNStageout', 0):
        print "==== NOT PERFORMING STAGEOUT AS CRAB_NoWNStageout is 1 ===="
        update_cmscp_return_code(cmscp_return_code, 0, True)
        return cmscp_return_code
    ## If we couldn't read CRAB_SaveLogsFlag from the job ad, we assume False.
    if 'CRAB_SaveLogsFlag' not in G_JOB_AD:
        msg  = "WARNING: Job's HTCondor ClassAd is missing attribute"
        msg += "CRAB_SaveLogsFlag. Will assume CRAB_SaveLogsFlag = False."
        print msg
        transfer_logs = False
    else:
        transfer_logs = G_JOB_AD['CRAB_SaveLogsFlag']
    ## If we couldn't read CRAB_TransferOutputs from the job ad, we assume True.
    if 'CRAB_TransferOutputs' not in G_JOB_AD:
        msg  = "WARNING: Job's HTCondor ClassAd is missing attribute"
        msg += " CRAB_TransferOutputs. Will assume CRAB_TransferOutputs = True."
        print msg
        transfer_outputs = True
    else:
        transfer_outputs = G_JOB_AD['CRAB_TransferOutputs']
    ## If we couldn't read CRAB_Id or CRAB_localOutputFiles from the job ad, we
    ## can not continue. If we couldn't read CRAB_StageoutPolicy,
    ## CRAB_Destination or CRAB_Dest, we don't continue.
    for attr in ['CRAB_Id', 'CRAB_localOutputFiles', \
                 'CRAB_StageoutPolicy', 'CRAB_Destination', 'CRAB_Dest', \
                 'CRAB_AsyncDest']:
        if attr not in G_JOB_AD:
            msg  = "ERROR: Job's HTCondor ClassAd is missing attribute %s."
            msg += "\nNo stageout will be performed."
            msg  = msg % (attr)
            print msg
            update_cmscp_return_code(cmscp_return_code, 80000, True)
            return cmscp_return_code
    split_re = re.compile(",\s*")
    output_files = split_re.split(G_JOB_AD['CRAB_localOutputFiles'])
    stageout_policy = split_re.split(G_JOB_AD['CRAB_StageoutPolicy'])
    print "Stageout policy: %s" % ", ".join(stageout_policy)
    dest_temp_dir = G_JOB_AD['CRAB_Dest']
    dest_files = split_re.split(G_JOB_AD['CRAB_Destination'])
    dest_site = G_JOB_AD['CRAB_AsyncDest']
    ##--------------------------------------------------------------------------
    ## Finish PARSE JOB AD
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start JOB REPORT VALIDATION
    ##--------------------------------------------------------------------------
    ## Set the json job report name.
    global G_JOB_REPORT_NAME
    G_JOB_REPORT_NAME = 'jobReport.json.%d' % G_JOB_AD['CRAB_Id']
    ## Load the json job report and make sure it has the expected structure.
    retval_job_report = None
    condition = no_condition
    if skip_job_report_validation:
        msg  = "WARNING: Internal wrapper flag 'skip_job_report_validation' is"
        msg += " True. Skipping to validate the json job report."
        print msg
    elif condition:
        msg = "====== %s: Starting job report validation."
        msg = msg % (time.asctime(time.gmtime()))
        print msg
        try:
            with open(G_JOB_REPORT_NAME) as fd_job_report:
                job_report = json.load(fd_job_report)
            retval_job_report = 0
        except Exception:
            msg  = "ERROR: Unable to load %s." % (G_JOB_REPORT_NAME)
            msg += "\n%s" % (traceback.format_exc())
            print msg
            retval_job_report = 80000
        ## Sanity check of the json job report.
        if 'steps' not in job_report:
            print "ERROR: Invalid job report: missing 'steps'."
            retval_job_report = 80000
        elif 'cmsRun' not in job_report['steps']:
            print "ERROR: Invalid job report: missing 'cmsRun'."
            retval_job_report = 80000
        elif 'output' not in job_report['steps']['cmsRun']:
            print "ERROR: Invalid job report: missing 'output'."
            retval_job_report = 80000
        print "Job report is ok (it has the expected structure)."
        ## Try to determine whether the payload actually succeeded.
        ## If the payload didn't succeed, we put it in a different directory.
        ## This prevents us from putting failed output files in the same
        ## directory as successful output files; we worry that users may simply
        ## 'ls' the directory and run on all listed files.
        global G_JOB_EXIT_CODE
        try:
            G_JOB_EXIT_CODE = job_report['jobExitCode']
            msg = "Retrieved jobExitCode = %s from job report." % (G_JOB_EXIT_CODE)
            print msg
        except Exception:
            msg  = "WARNING: Unable to retrieve jobExitCode from job report."
            msg += "\nWill assume job executable failed, with following implications:"
            msg += "\n- if stageout is still possible, it will be done into a subdirectory named 'failed';"
            msg += "\n- if stageout is still possible, publication will be disabled."
            print msg
        msg = "====== %s: Finished job report validation (status %d)."
        msg = msg % (time.asctime(time.gmtime()), retval_job_report)
        print msg
    update_cmscp_return_code(cmscp_return_code, retval_job_report)
    ##--------------------------------------------------------------------------
    ## Finish JOB REPORT VALIDATION
    ##--------------------------------------------------------------------------

    ## Modify the stageout temporary directory by:
    ## a) adding a four-digit counter;
    counter = "%04d" % (G_JOB_AD['CRAB_Id'] / 1000)
    dest_temp_dir = os.path.join(dest_temp_dir, counter)
    ## b) adding a 'failed' subdirectory in case cmsRun failed.
    if G_JOB_EXIT_CODE != 0:
        dest_temp_dir = os.path.join(dest_temp_dir, 'failed')

    ## Definitions needed for the logs archive creation, stageout and metadata
    ## upload.
    logs_arch_file_name = 'cmsRun.log.tar.gz'
    logs_arch_dest_file_name = os.path.basename(dest_files[0])
    logs_arch_dest_pfn = dest_files[0]
    logs_arch_dest_pfn_path = os.path.dirname(dest_files[0])
    if G_JOB_EXIT_CODE != 0:
        if logs_arch_dest_pfn_path.endswith('/log'):
            logs_arch_dest_pfn_path = logs_arch_dest_pfn_path.rstrip('/log')
        logs_arch_dest_pfn_path = os.path.join(logs_arch_dest_pfn_path, 'failed', 'log')
        logs_arch_dest_pfn = os.path.join(logs_arch_dest_pfn_path, logs_arch_dest_file_name)
    logs_arch_dest_temp_lfn = os.path.join(dest_temp_dir, 'log', logs_arch_dest_file_name)
    logs_arch_dest_lfn = None
    ## TODO: This is a hack; the logs destination LFN should be in the job ad.
    if len(logs_arch_dest_pfn_path.split('/store/')) == 2:
        logs_arch_dest_lfn = os.path.join('/store', \
                                          logs_arch_dest_pfn_path.split('/store/')[1], \
                                          logs_arch_dest_file_name)

    ##--------------------------------------------------------------------------
    ## Start CHECK OUTPUT FILES EXIST
    ##--------------------------------------------------------------------------
    ## Check that the output files are well defined in the job ad and that they
    ## exist in the worker node.
    retval_outputs_exist = None
    condition = no_condition
    if skip_outputs_exist:
        msg  = "WARNING: Internal wrapper flag 'skip_outputs_exist' is True."
        msg += " Skipping to check if user output files exist."
        print msg
    elif condition:
        msg = "====== %s: Starting to check if user output files exist."
        msg = msg % (time.asctime(time.gmtime()))
        print msg
        for output_file_name_info in output_files:
            cur_retval = 0
            ## The output_file_name_info is something like this:
            ## my_output_file.root=my_output_file_<job-id>.root
            if len(output_file_name_info.split('=')) != 2:
                print "ERROR: Invalid output format (%s)." % (output_file_name_info)
                cur_retval = 80000
            else:
                output_file_name = output_file_name_info.split('=')[0]
                if not os.path.exists(output_file_name):
                    print "ERROR: Output file %s does not exist." % (output_file_name)
                    cur_retval = 60302
                else:
                    print "Output file %s exists." % (output_file_name)
            if retval_outputs_exist in [None, 0]:
                retval_outputs_exist = cur_retval
        msg = "====== %s: Finished to check if user output files exist (status %d)."
        msg = msg % (time.asctime(time.gmtime()), retval_outputs_exist)
        print msg
    update_cmscp_return_code(cmscp_return_code, retval_outputs_exist)
    ##--------------------------------------------------------------------------
    ## Finish CHECK OUTPUT FILES EXIST
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start CHECK OUTPUT FILES IN JOB REPORT
    ##--------------------------------------------------------------------------
    ## Check if the output file is in the json job report. If it is not, add it.
    retval_outputs_in_job_report = None
    condition = (retval_job_report == 0 and retval_outputs_exist == 0)
    if skip_outputs_in_job_report:
        msg  = "WARNING: Internal wrapper flag 'skip_outputs_in_job_report' is"
        msg += " True. Skipping to check if user output files are in the job"
        msg += " report."
        print msg
    elif condition:
        msg  = "====== %s: Starting to check if user output files are in job"
        msg += " report."
        msg  = msg % (time.asctime(time.gmtime()))
        print msg
        for output_file_name_info in output_files:
            cur_retval = 0
            ## The output_file_name_info is something like this:
            ## my_output_file.root=my_output_file_<job-id>.root
            if len(output_file_name_info.split('=')) != 2:
                print "ERROR: Invalid output format (%s)." % (output_file_name_info)
                cur_retval = 80000
            else:
                output_file_name = output_file_name_info.split('=')[0]
                is_file_in_job_report = bool(get_output_file_from_job_report(output_file_name))
                if not is_file_in_job_report:
                    print "Output file %s not found in job report." % (output_file_name)
                    file_added_ok = add_output_file_to_job_report(output_file_name)
                    if not file_added_ok:
                        cur_retval = 60318
                else:
                    print "Output file %s found in job report." % (output_file_name)
            if retval_outputs_in_job_report in [None, 0]:
                retval_outputs_in_job_report = cur_retval
        msg  = "====== %s: Finished to check if user output files are in job"
        msg += " report (status %d)."
        msg = msg % (time.asctime(time.gmtime()), retval_outputs_in_job_report)
        print msg
    update_cmscp_return_code(cmscp_return_code, retval_outputs_in_job_report)
    ##--------------------------------------------------------------------------
    ## Finish CHECK OUTPUT FILES IN JOB REPORT
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start LOGS TARBALL CREATION
    ##--------------------------------------------------------------------------
    ## Create a zipped tar archive file of the user logs.
    retval_logs_arch = None
    condition = no_condition
    if skip_logs_arch:
        msg  = "WARNING: Internal wrapper flag 'skip_logs_arch' is True."
        msg += " Skipping creation of user logs archive file."
        print msg
    elif condition:
        msg = "====== %s: Starting creation of user logs archive file."
        msg = msg % (time.asctime(time.gmtime()))
        print msg
        try:
            retval_logs_arch = make_logs_archive(logs_arch_file_name)
        except tarfile.TarError:
            msg  = "ERROR creating user logs archive file."
            msg += "\n%s" % (traceback.format_exc())
            print msg
            if retval_logs_arch in [None, 0]:
                retval_logs_arch = 80000
        msg = "====== %s: Finished creation of user logs archive file (status %d)."
        msg = msg % (time.asctime(time.gmtime()), retval_logs_arch)
        print msg
        ## Determine the logs archive file size and write it in the job report.
        try:
            log_size = os.stat(logs_arch_file_name).st_size
            add_to_file_in_job_report(logs_arch_dest_file_name, True, [('log_size', log_size)])
        except Exception:
            msg = "WARNING: Unable to add logs archive file size to job report."
            print msg
    if transfer_logs:
        update_cmscp_return_code(cmscp_return_code, retval_logs_arch)
    elif not retval_logs_arch in [None, 0]:
        msg  = "Ignoring compression failure of user logs,"
        msg += " because the user did not request the logs to be staged out."
        print msg
    ##--------------------------------------------------------------------------
    ## Finish LOGS TARBALL CREATION
    ##--------------------------------------------------------------------------

    ## Define what are so far the conditions for doing the stageouts.
    condition_logs_stageout = (retval_logs_arch == 0)
    condition_outputs_stageout = (retval_outputs_exist == 0 and \
                                  retval_outputs_in_job_report == 0)
    condition_stageout = (condition_logs_stageout or condition_outputs_stageout)

    ##--------------------------------------------------------------------------
    ## Start LOCAL STAGEOUT MANAGER INITIALIZATION
    ##--------------------------------------------------------------------------
    ## This stageout manager will be used for all local stageout attempts (for
    ## user logs archive and user outputs).
    retval_local_stageout_mgr = None
    condition = ('local' in stageout_policy and condition_stageout)
    if skip_init_local_stageout_mgr:
        msg  = "WARNING: Internal wrapper flag 'skip_init_local_stageout_mgr'"
        msg += " is True. Skipping initialization of stageout manager for"
        msg += " local stageouts."
        print msg
    elif condition:
        msg  = "====== %s: Starting initialization of stageout manager for"
        msg += " local stageouts."
        msg  = msg % (time.asctime(time.gmtime()))
        print msg
        try:
            print "       -----> Stageout manager log start"
            local_stageout_mgr = StageOutMgr.StageOutMgr()
            local_stageout_mgr.numberOfRetries = G_NUMBER_OF_RETRIES
            local_stageout_mgr.retryPauseTime = G_RETRY_PAUSE_TIME
            print "       <----- Stageout manager log finish"
            retval_local_stageout_mgr = 0
            print "Initialization was ok."
        except Exception:
            print "       <----- Stageout manager log finish"
            msg  = "WARNING: Error initializing StageOutMgr."
            msg += " Will not be able to do local stageouts."
            print msg
            retval_local_stageout_mgr = 60311
        msg  = "====== %s: Finished initialization of stageout manager for"
        msg += " local stageouts (status %d)."
        msg  = msg % (time.asctime(time.gmtime()), retval_local_stageout_mgr)
        print msg
    if not transfer_outputs:
        update_cmscp_return_code(cmscp_return_code, retval_local_stageout_mgr)
    ##--------------------------------------------------------------------------
    ## Finish LOCAL STAGEOUT MANAGER INITIALIZATION
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start DIRECT STAGEOUT IMPLEMENTATION INITIALIZATION
    ##--------------------------------------------------------------------------
    ## This stageout implementation will be used for all direct stageout
    ## attempts (for user logs archive and user outputs).
    retval_direct_stageout_impl = None
    condition = ('remote' in stageout_policy and condition_stageout)
    if skip_init_direct_stageout_impl:
        msg  = "WARNING: Internal wrapper flag 'skip_init_direct_stageout_impl'"
        msg += " is True. Skipping initialization of stageout implementation"
        msg += " for direct stageouts."
        print msg
    elif condition:
        msg  = "====== %s: Starting initialization of stageout implementation"
        msg += " for direct stageouts."
        msg  = msg % (time.asctime(time.gmtime()))
        print msg
        direct_stageout_command = "srmv2-lcg"
        direct_stageout_protocol = "srmv2"
        try:
            direct_stageout_impl = retrieveStageOutImpl(direct_stageout_command)
            direct_stageout_impl.numRetries = G_NUMBER_OF_RETRIES
            direct_stageout_impl.retryPause = G_RETRY_PAUSE_TIME
            retval_direct_stageout_impl = 0
            print "Initialization was ok."
        except Exception:
            msg  = "WARNING: Error retrieving StageOutImpl for command '%s'."
            msg += " Will not be able to do direct stageouts."
            msg  = msg % (direct_stageout_command) 
            print msg
            retval_direct_stageout_impl = 60311
        msg  = "====== %s: Finished initialization of stageout implementation"
        msg += " for direct stageouts (status %d)."
        msg  = msg % (time.asctime(time.gmtime()), retval_direct_stageout_impl)
        print msg
    if (transfer_outputs or transfer_logs) and retval_local_stageout_mgr != 0:
        update_cmscp_return_code(cmscp_return_code, retval_direct_stageout_impl) 
    ##--------------------------------------------------------------------------
    ## Finish DIRECT STAGEOUT IMPLEMENTATION INITIALIZATION
    ##--------------------------------------------------------------------------

    ## Current transfer paradigm: If a user didn't request the transfer of
    ## files, should we still do a local stageout? Eric said that we shouldn't;
    ## that if a user disables the transfers this should be interpreted as "I
    ## don't care about my files". But we know that there might be users that
    ## don't have a storage area where to stage out the files. Therefore I
    ## prefer to still do a local stageout. Now, if the local stageout fails,
    ## should we fail the job? So far we fail the job if the outputs were not
    ## requested to be transferred.

    ##--------------------------------------------------------------------------
    ## Start STAGEOUT OF USER LOGS TARBALL AND USER OUTPUTS
    ##--------------------------------------------------------------------------
    ## Stage out the logs archive file and the output files. Do local or direct
    ## stageout according to the (configurable) stageout policy. But don't
    ## inject to ASO. Injection to ASO is done after all the local stageouts are
    ## done successfully.
    retval_logs_stageout    = {'local': None, 'remote': None}
    retval_outputs_stageout = {'local': None, 'remote': None}
    first_stageout_failure_code = None
    is_log_in_storage = {'local': False, 'remote': False}
    for policy in stageout_policy:
        clean = False
        ##---------------
        ## Logs stageout.
        ##---------------
        condition = condition_logs_stageout
        if policy == 'local':
            condition = (condition and retval_local_stageout_mgr == 0 and \
                         retval_logs_stageout['remote'] != 0)
        elif policy == 'remote':
            condition = (condition and retval_direct_stageout_impl == 0 and \
                         retval_logs_stageout['local'] != 0)
        ## There are some cases where we don't have to stage out the logs.
        if condition:
            skip = False
            if skip_logs_stageout[policy]:
                msg  = "WARNING: Internal wrapper flag"
                msg += " 'skip_logs_stageout['%s']' is True."
                msg += " Skipping %s stageout of user logs archive file."
                msg  = msg % (policy, policy)
                print msg
                skip = True
            elif policy == 'remote' and not transfer_logs:
                msg  = "Will not do remote stageout of user logs archive file,"
                msg += " since the user did not specify to transfer the logs."
                print msg
                skip = True
            condition = not skip
        ## If we have to, stage out the logs.
        if condition:
            msg = "====== %s: Starting %s stageout of user logs archive file."
            msg = msg % (time.asctime(time.gmtime()), policy)
            print msg
            try:
                retval_logs_stageout[policy] = perform_stageout(local_stageout_mgr, \
                                                                direct_stageout_impl, \
                                                                direct_stageout_command, \
                                                                direct_stageout_protocol, \
                                                                policy, \
                                                                logs_arch_file_name, \
                                                                logs_arch_dest_temp_lfn, \
                                                                logs_arch_dest_pfn, \
                                                                logs_arch_dest_lfn, \
                                                                dest_site, is_log = True, \
                                                                inject = transfer_logs)
            except Exception:
                msg  = "ERROR: Unhandled exception when performing stageout"
                msg += " of user logs archive file.\n%s"
                msg  = msg % (traceback.format_exc())
                print msg
                if retval_logs_stageout[policy] in [None, 0]:
                    retval_logs_stageout[policy] = 60318
            msg  = "====== %s: Finished %s stageout of user logs archive file"
            msg += " (status %d)."
            msg  = msg % (time.asctime(time.gmtime()), policy, \
                          retval_logs_stageout[policy])
            print msg
            if retval_logs_stageout[policy] == 0:
                is_log_in_storage[policy] = True
            ## If the stageout failed, clean the stageout area. But don't remove
            ## the log from the local stageout area (we want to keep it there in
            ## case the next stageout policy also fails).
            if not retval_logs_stageout[policy] in [None, 0]:
                clean = True
                if transfer_logs and first_stageout_failure_code is None:
                    first_stageout_failure_code = retval_logs_stageout[policy]
        ##------------------
        ## Outputs stageout.
        ##------------------
        condition = condition_outputs_stageout
        if policy == 'local':
            condition = (condition and retval_local_stageout_mgr == 0 and \
                         retval_outputs_stageout['remote'] != 0)
        elif policy == 'remote':
            condition = (condition and retval_direct_stageout_impl == 0 and \
                         retval_outputs_stageout['local'] != 0)
        ## There are some cases where we don't have to stage out the outputs.
        if condition:
            skip = False
            if skip_outputs_stageout[policy]:
                msg  = "WARNING: Internal wrapper flag"
                msg += " 'skip_outputs_stageout['%s']' is True."
                msg += " Skipping %s stageout of user outputs."
                msg  = msg % (policy, policy)
                print msg
                skip = True
            if policy == 'remote' and not transfer_outputs:
                msg  = "Will not do remote stageout of output files,"
                msg += " since the user specified to not transfer the outputs."
                print msg
                skip = True
            if not retval_logs_stageout[policy] in [None, 0]:
                msg  = "Will not do %s stageout of output files, because"
                msg += " %s stageout already failed for the logs archive file."
                msg  = msg % (policy, policy)
                print msg
                skip = True
                retval_outputs_stageout[policy] = 60318
            condition = not skip
        ## If we have to, stage out the outputs.
        if condition:
            msg = "====== %s: Starting %s stageout of user outputs."
            msg = msg % (time.asctime(time.gmtime()), policy)
            print msg
            for output_file_name_info, output_dest_pfn in zip(output_files, dest_files[1:]):
                ## The output_file_name_info is something like this:
                ## my_output_file.root=my_output_file_<job-id>.root
                if len(output_file_name_info.split('=')) != 2:
                    print "ERROR: Invalid output format (%s)." % (output_file_name_info)
                    cur_retval = 80000
                else:
                    cur_retval = None
                    output_file_name, output_dest_file_name = output_file_name_info.split('=')
                    msg = "-----> %s: Starting %s stageout of %s."
                    msg = msg % (time.asctime(time.gmtime()), policy, output_file_name)
                    print msg
                    output_dest_temp_lfn = os.path.join(dest_temp_dir, output_dest_file_name)
                    output_dest_pfn_path = os.path.dirname(output_dest_pfn)
                    if G_JOB_EXIT_CODE != 0:
                        output_dest_pfn_path = os.path.join(output_dest_pfn_path, 'failed')
                    output_dest_pfn = os.path.join(output_dest_pfn_path, output_dest_file_name)
                    output_dest_lfn = None
                    ## TODO: This is a hack; the output destination LFN should
                    ## be in the job ad.
                    if len(output_dest_pfn_path.split('/store/')) == 2:
                        output_dest_lfn = os.path.join('/store', output_dest_pfn_path.split('/store/')[1], output_dest_file_name)
                    try:
                        cur_retval = perform_stageout(local_stageout_mgr, \
                                                      direct_stageout_impl, \
                                                      direct_stageout_command, \
                                                      direct_stageout_protocol, \
                                                      policy, \
                                                      output_file_name, \
                                                      output_dest_temp_lfn, \
                                                      output_dest_pfn, \
                                                      output_dest_lfn, \
                                                      dest_site, is_log = False, \
                                                      inject = transfer_outputs)
                    except Exception, ex:
                        msg  = "ERROR: Unhandled exception when performing stageout."
                        msg += "\n%s" % (traceback.format_exc())
                        print msg
                        if cur_retval in [None, 0]:
                            cur_retval = 60318
                    msg = "<----- %s: Finished %s stageout of %s (status %d)."
                    msg = msg % (time.asctime(time.gmtime()), policy, \
                                 output_file_name, cur_retval)
                    print msg
                if retval_outputs_stageout[policy] in [None, 0]:
                    retval_outputs_stageout[policy] = cur_retval
                ## If the stageout failed for one of the outputs, don't even try
                ## to stage out the rest of the outputs.
                if not retval_outputs_stageout[policy] in [None, 0]:
                    msg  = "%s stageout of %s failed. Will not attempt"
                    msg += " %s stageout for any other output files (if any)."
                    msg  = msg % (policy.title(), output_file_name, policy)
                    print msg
                    break
            msg = "====== %s: Finished %s stageout of user outputs (status %d)."
            msg = msg % (time.asctime(time.gmtime()), policy, retval_outputs_stageout[policy])
            print msg
            if not retval_outputs_stageout[policy] in [None, 0]:
                clean = True
                if first_stageout_failure_code is None:
                    first_stageout_failure_code = retval_outputs_stageout[policy]
        if clean:
            ## If the stageout failed, clean the stageout area. But don't remove
            ## the logs archive file from the local stageout area (we want to
            ## keep it there in case the direct stageout also fails). Not
            ## cleaning the log doesn't mean that we will request ASO to
            ## transfer it; we will not.
            msg = "====== %s: Starting to clean %s stageout area."
            msg = msg % (time.asctime(time.gmtime()), policy)
            print msg
            clean_log = (policy == 'remote')
            if clean_log:
                is_log_in_storage[policy] = False
            clean_stageout_area(local_stageout_mgr, direct_stageout_impl, policy, \
                                logs_arch_dest_temp_lfn, keep_log = not clean_log)
            msg = "====== %s: Finished to clean %s stageout area."
            msg = msg % (time.asctime(time.gmtime()), policy)
            print msg
            ## Since we cleaned the storage area, we have to set the return
            ## status of this policy stageout to a general failure code (if
            ## originally 0).
            if retval_logs_stageout[policy] == 0:
                retval_logs_stageout[policy] = 60318
            if retval_outputs_stageout[policy] == 0:
                retval_outputs_stageout[policy] = 60318
        ## If the local (remote) stageout succeeded for the both logs archive
        ## file and output files, then we don't need to try remote (local)
        ## stageout.
        if retval_logs_stageout[policy] == 0 and retval_outputs_stageout[policy] == 0:
            break
    ## If stageout failed, update the cmscp return code with the stageout failure
    ## that happened first (except that if it was the local stageout of the logs
    ## archive file what failed first and the user didn't request the logs to be
    ## transferred, we ignore that failure).
    if transfer_logs:
        if not (retval_logs_stageout['local'] == 0 or retval_logs_stageout['remote'] == 0):
            update_cmscp_return_code(cmscp_return_code, first_stageout_failure_code)
    elif not retval_logs_stageout['local'] in [None, 0]:
        msg  = "Ignoring stageout failure of user logs,"
        msg += " because the user did not request the logs to be staged out."
        print msg
    if not (retval_outputs_stageout['local'] == 0 or retval_outputs_stageout['remote'] == 0):
        update_cmscp_return_code(cmscp_return_code, first_stageout_failure_code)
    ##--------------------------------------------------------------------------
    ## Finish STAGEOUT OF USER LOGS TARBALL AND USER OUTPUTS
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start INJECTION TO ASO
    ##--------------------------------------------------------------------------
    ## Do the injection of the transfer request documents to the ASO database
    ## only if all the local or direct stageouts have succeeded.
    retval_aso_injection = None
    condition_inject_outputs = False
    not_inject_msg_outputs = ''
    if transfer_outputs:
        condition_inject_outputs = (retval_outputs_stageout['local'] == 0 and \
                                    retval_outputs_stageout['remote'] != 0)
        if not condition_inject_outputs:
            not_inject_msg_outputs = "Will not inject transfer requests to ASO for the user outputs,"
            if retval_outputs_stageout['remote'] == 0:
                not_inject_msg_outputs += " because they were staged out directly to the permanent storage."
            else:
                not_inject_msg_outputs += " because their local stageouts were not successful"
                not_inject_msg_outputs += " (or files were removed from local temporary storage"
                not_inject_msg_outputs += " or local stageout was not even performed)."
    else:
        not_inject_msg_outputs = "Will not inject transfer requests to ASO for the user outputs"
        if retval_outputs_stageout['local'] == 0:
            not_inject_msg_outputs += " (even if their local stageouts were successful)"
        not_inject_msg_outputs += ", because the user didn't request the outputs to be transferred."
        if retval_outputs_stageout['local'] != 0:
            not_inject_msg_outputs += " (And in any case, their local stageouts were not successful -or not even performed-.)"
    condition_inject_logs = False
    not_inject_msg_logs = ''
    if transfer_logs:
        condition_inject_logs = (retval_logs_stageout['local'] == 0 and \
                                 retval_logs_stageout['remote'] != 0)
        if not condition_inject_logs:
            not_inject_msg_logs = "Will not inject transfer request to ASO the for user logs archive file,"
            if retval_logs_stageout['remote'] == 0:
                not_inject_msg_logs += " because it was staged out directly to the permanent storage."
            else:
                not_inject_msg_logs += " because its local stageout was not successful"
                not_inject_msg_logs += " (or file was removed from local temporary storage"
                not_inject_msg_logs += " or local stageout was not even performed)."
    else:
        not_inject_msg_logs = "Will not inject transfer request to ASO for the user logs archive file"
        if retval_logs_stageout['local'] == 0:
            not_inject_msg_logs += " (even if its local stageout was successful)"
        not_inject_msg_logs += ", because the user didn't request the logs to be transferred."
        if retval_logs_stageout['local'] != 0:
            not_inject_msg_logs += " (And in any case, its local stageout was not successful -or not even performed-.)"
    condition = condition_inject_outputs or condition_inject_logs
    if skip_aso_injection:
        msg  = "WARNING: Internal wrapper flag 'skip_aso_injection' is True."
        msg += " Skipping injection of transfer requests to ASO."
        print msg
    elif condition:
        msg = "====== %s: Starting injection of transfer requests to ASO."
        msg = msg % (time.asctime(time.gmtime()))
        print msg
        if not_inject_msg_logs:
            print not_inject_msg_logs
        if not_inject_msg_outputs:
            print not_inject_msg_outputs
        num_docs_to_inject = 0
        for file_transfer_info in G_ASO_TRANSFER_REQUESTS:
            if file_transfer_info['inject']:
                num_docs_to_inject += 1
        if num_docs_to_inject > 0:
            msg = "Will inject %d %sdocument%s."
            msg = msg % (num_docs_to_inject, \
                         'other ' if not_inject_msg_outputs else '', \
                         's' if num_docs_to_inject > 1 else '')
            print msg
            if 'CRAB_ASOURL' in G_JOB_AD and G_JOB_AD['CRAB_ASOURL']:
                print "Will use ASO server at %s." % G_JOB_AD['CRAB_ASOURL']
            for file_transfer_info in G_ASO_TRANSFER_REQUESTS:
                if not file_transfer_info['inject']:
                    continue
                file_name = os.path.basename(file_transfer_info['source']['lfn'])
                msg = "-----> %s: Starting injection for %s."
                msg = msg % (time.asctime(time.gmtime()), file_name)
                print msg
                try:
                    cur_retval = inject_to_aso(file_transfer_info)
                except Exception:
                    msg  = "ERROR: Unhandled exception when injecting document to ASO."
                    msg += "\n%s" % (traceback.format_exc())
                    print msg
                    if cur_retval in [None, 0]:
                        cur_retval = 60318
                msg = "<----- %s: Finished injection for %s (status %d)."
                msg = msg % (time.asctime(time.gmtime()), file_name, cur_retval)
                print msg
                if retval_aso_injection in [None, 0]:
                    retval_aso_injection = cur_retval
        else:
            msg = "There are no %sdocuments to inject."
            msg = msg % ('other ' if not_inject_msg_outputs or not_inject_msg_logs else '')
            print msg
        msg = "====== %s: Finished injection of transfer requests to ASO (status %d)."
        msg = msg % (time.asctime(time.gmtime()), retval_aso_injection)
        print msg
    else:
        if not_inject_msg_logs:
            print not_inject_msg_logs
        if not_inject_msg_outputs:
            print not_inject_msg_outputs
    ## We don't care to update the cmscp return code with the injection return
    ## code, because the injection of the transfer request documents to the ASO
    ## database will be retried by the post-job for those injections that failed
    ## in cmscp.
    ##--------------------------------------------------------------------------
    ## Finish INJECTION TO ASO
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start LOG FILE METADATA UPLOAD
    ##--------------------------------------------------------------------------
    ## Upload of the log file metadata to the crab cache. Ignore any failure
    ## since the post-job can always retry the upload.
    retval_logs_metadata = None
    condition = (is_log_in_storage['local'] or is_log_in_storage['remote'])
    if skip_logs_metadata_upload:
        msg  = "WARNING: Internal wrapper flag 'skip_logs_metadata_upload' is"
        msg += " True. Skipping upload of logs archive file metadata."
        print msg
    elif condition:
        msg = "====== %s: Starting upload of logs archive file metadata."
        msg = msg % (time.asctime(time.gmtime()))
        print msg
        try:
            retval_logs_metadata = upload_log_file_metadata(logs_arch_dest_temp_lfn, \
                                                            logs_arch_dest_lfn)
        except Exception:
            msg  = "WARNING: Failed to upload logs archive file metadata."
            msg += " Will ignore the failure, since the post-job can retry"
            msg += " the upload."
            print msg
            if retval_logs_metadata in [None, 0]:
                retval_logs_metadata = 80000 ## TODO: Need a new code here.
        add_to_file_in_job_report(logs_arch_dest_file_name, True, \
                                  [('file_metadata_upload', not bool(retval_logs_metadata))])
        msg  = "====== %s: Finished upload of logs archive file metadata"
        msg += " (status %d)."
        msg  = msg % (time.asctime(time.gmtime()), retval_logs_metadata)
        print msg
    else:
        msg  = "Will NOT upload logs archive file metadata,"
        msg += " since the logs archive file is not in a storage area."
        print msg
    ##--------------------------------------------------------------------------
    ## Finish LOG FILE METADATA UPLOAD
    ##--------------------------------------------------------------------------

    return cmscp_return_code

##==============================================================================
## CMSCP RUNS HERE WHEN SOURCED FROM gWMS-CMSRunAnalysis.sh.
##------------------------------------------------------------------------------

if __name__ == '__main__':
    MSG = "====== %s: cmscp.py STARTING."
    MSG = MSG % (time.asctime(time.gmtime()))
    print MSG
    logging.basicConfig(level = logging.INFO)
    JOB_WRAPPER_EXIT_CODE = 0
    try:
        for arg in sys.argv:
            if 'JOB_WRAPPER_EXIT_CODE=' in arg and len(arg.split('=')) == 2:
                JOB_WRAPPER_EXIT_CODE = int(arg.split('=')[1])
    except:
        pass
    try:
        JOB_STGOUT_WRAPPER_EXIT_CODE = main()
    except:
        MSG  = "ERROR: Unhandled exception."
        MSG += "\n%s" % (traceback.format_exc())
        print MSG
        JOB_STGOUT_WRAPPER_EXIT_CODE = 60307
    if JOB_WRAPPER_EXIT_CODE:
        JOB_STGOUT_WRAPPER_EXIT_CODE = JOB_WRAPPER_EXIT_CODE
    elif JOB_STGOUT_WRAPPER_EXIT_CODE:
        add_to_job_report([('exitCode', JOB_STGOUT_WRAPPER_EXIT_CODE)])
    if JOB_STGOUT_WRAPPER_EXIT_CODE:
        if G_JOB_AD:
            try:
                MSG  = "Stageout wrapper finished with exit code %s."
                MSG += " Will report stageout failure to Dashboard."
                MSG  = MSG % (JOB_STGOUT_WRAPPER_EXIT_CODE)
                print MSG
                report_failure_to_dashboard(JOB_STGOUT_WRAPPER_EXIT_CODE)
            except:
                MSG  = "ERROR: Unhandled exception when reporting failure"
                MSG += " to Dashboard.\n%s"
                MSG  = MSG % (traceback.format_exc())
                print MSG
        else:
            MSG  = "ERROR: Job's HTCondor ClassAd was not read."
            MSG += " Will not report stageout failure to Dashboard."
            print MSG
    MSG = "====== %s: cmscp.py FINISHING (status %d)."
    MSG = MSG % (time.asctime(time.gmtime()), JOB_STGOUT_WRAPPER_EXIT_CODE)
    print MSG
    sys.exit(JOB_STGOUT_WRAPPER_EXIT_CODE)

##==============================================================================
