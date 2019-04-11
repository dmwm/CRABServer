from __future__ import print_function
import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category = DeprecationWarning)

import os
import sys
import re
import json
import time
import signal
import logging
import tarfile
import traceback

if os.path.exists("WMCore.zip") and "WMCore.zip" not in sys.path:
    sys.path.append("WMCore.zip")

from ServerUtilities import cmd_exist, parseJobAd

if 'http_proxy' in os.environ and not os.environ['http_proxy'].startswith("http://"):
    os.environ['http_proxy'] = "http://%s" % (os.environ['http_proxy'])

import WMCore.Storage.StageOutMgr as StageOutMgr
import WMCore.Storage.StageOutError as StageOutError
from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Algorithms.Alarm import Alarm, alarmHandler
import WMCore.WMException as WMException
import DashboardAPI

## See the explanation of this sentry file in CMSRunAnalysis.py.
with open('wmcore_initialized', 'w') as fd_wmcore:
    fd_wmcore.write('wmcore initialized.\n')

##==============================================================================
## DESCRIPTION OF THE STAGEOUT LOGIC USED IN CMSCP.
##------------------------------------------------------------------------------

## The code assumes there are only two possible stageout policies, namely local
## and remote, but doesn't assume any particular order. However, in the next
## explanation of the logic the assumption is "first local, then remote".
## The code is written such that one can clearly identify the different actions
## that are involved in this stageout wrapper. Each action can be skipped by
## turning on the corresponding flag in the "skip" dictionary, which can be
## useful when debugging or running a test.
## The actions follow a general order, but since the logic is not such that one
## action should only be executed if the previous action succeeded, the code
## doesn't exit if an action fails and instead keeps track of the exit status
## of each action, updating the cmscp exit code after each action finishes.
## Each action has a condition that needs to be satisfied in order for the
## action to be executed. The conditions are in general written using the exit
## codes of the previous actions. So for example, the condition for executing
## the stageout of the output files is that the outputs exist and that they
## are documented in the job report, and this is written in term of the exit
## codes of these two actions (their exit codes have to be equal to 0).
## Ok, now to the logic.
## One thing to keep in mind: Even if the user didn't request the outputs to be
## transferred, we still do local stageout and fail the job if the local
## stageout fails. For the logs we also do local stageout, but don't fail the
## job if the stageout fails.
##  1) Parse the job ad. Don't exit if fails.
##  2) Do a basic validation of the job report. Don't exit if fails.
##  3) Check that the output files listed in the job ad exist in the WN.
##  4) If 2) and 3) were ok, check that the output files are documented in the
##     job report (if they are not, add them).
##  5) Create the logs archive file.
##  6) Initialize the local stageout manager (if local in stageout policy).
##  7) Initialize the direct stageout implementation (if remote in stageout
##     policy).
##  8) Do the stageout:
##     8.1) try local stageout for the logs archive (if 5) and 6) were ok);
##     8.2) if 8.1) was ok, try local stageout for the outputs;
##     8.3) if 8.2) was ok, continue to 9);
##     8.3) if 8.1) or 8.2) failed, clean the local temp storage (actually,
##          don't clean the log so that it is available in case the remote
##          stageout also fails);
##     8.4) if 8.1) or 8.2) failed, try remote stageout for the files that were
##          requested to be transferred;
##     8.5) if 8.4) failed, clean the remote permanent storage.
##  9) Inject to ASO for the files that were requested to the transferred, if
##     and only if their local stageout was successful and their remote stageout
##     was not (or was not performed).
## 10) Upload the logs file metadata if and only if the logs archive file was
##     successfully staged out either to the local temporary storage or to the
##     remote permanent storage.

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
## report. This exit code is used to determine whether the output/log files
## should be put in the "failed" subdirectory and whether publication has to be
## turned off. If this exit code is not in [0, None], cmscp will inherit it.
G_JOB_WRAPPER_EXIT_CODE = None

## The payload exit code is put here after reading it from the job report.
## This exit code is basically not used here as we consider CMSRunAnalysis
## as one single atomic thing and we only use G_JOB_WRAPPER_EXIT_CODE.
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

## Dictionary with the job's HTCondor ClassAd.
G_JOB_AD = {}

##==============================================================================
## FUNCTIONS USED BY THE CODE.
##------------------------------------------------------------------------------

def make_logs_archive(arch_file_name):
    """
    Make a zipped tar archive file of the user log files plus the framework job
    report xml file.
    """
    retval, retmsg = 0, None
    arch_file = tarfile.open(arch_file_name, 'w:gz')
    file_names = ['cmsRun-stdout.log', \
                  'cmsRun-stderr.log', \
                  'FrameworkJobReport.xml']
    for file_name in file_names:
        if os.path.exists(file_name):
            msg = "Adding %s to archive file %s" % (file_name, arch_file_name)
            print(msg)
            file_name_no_ext, ext = file_name.rsplit('.', 1)
            job_id_str = '-%s' % (G_JOB_AD['CRAB_Id'])
            file_name_in_arch = file_name_no_ext + job_id_str + '.' + ext
            arch_file.add(file_name, arcname = file_name_in_arch)
        else:
            ## Will not fail stageout if log files are missing.
            msg = "WARNING: %s is missing." % (file_name)
            print(msg)
    arch_file.close()
    return retval, retmsg

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
    with open(G_JOB_REPORT_NAME) as fd:
        job_report = json.load(fd)
    subreport = job_report
    subreport_name = ''
    if location is None:
        location = []
    for loc in location:
        if loc in subreport:
            subreport = subreport[loc]
            subreport_name += "['%s']" % (loc)
        else:
            msg = "WARNING: Job report doesn't contain section %s['%s']." % (subreport_name, loc)
            print(msg)
            return default
    if not isinstance(subreport, dict):
        if subreport_name:
            msg = "WARNING: Job report section %s is not a dict." % (subreport_name)
        else:
            msg = "WARNING: Job report is not a dict."
        print(msg)
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
        with open(G_JOB_REPORT_NAME) as fd:
            job_report = json.load(fd)
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
    with open(G_JOB_REPORT_NAME) as fd:
        job_report = json.load(fd)
    subreport = job_report
    subreport_name = ''
    if location is None:
        location = []
    for loc in location:
        if loc in subreport:
            subreport = subreport[loc]
            subreport_name += "['%s']" % loc
        else:
            msg = "WARNING: Job report doesn't contain section %s['%s']." % (subreport_name, loc)
            print(msg)
            return False
    if not isinstance(subreport, dict):
        if subreport_name:
            msg = "WARNING: Job report section %s is not a dict." % (subreport_name)
        else:
            msg = "WARNING: Job report is not a dict."
        print(msg)
        return False
    if mode in ['new', 'overwrite']:
        for key, value in key_value_pairs:
            if mode == 'new' and key in subreport:
                msg = "WARNING: Key '%s' already exists in job report section %s." % (key, subreport_name)
                print(msg)
                return False
            subreport[key] = value
    elif mode == 'update':
        for key, value in key_value_pairs:
            subreport.setdefault(key, []).append(value)
    else:
        msg = "WARNING: Unknown mode '%s'." % (mode)
        print(msg)
        return False
    with open(G_JOB_REPORT_NAME, 'w') as fd:
        json.dump(job_report, fd)
    return True

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def add_output_file_to_job_report(file_name, key = 'addoutput'):
    """
    Add the given output file to the json job report section ['steps']['cmsRun']
    ['output'] under the given key. The value to add is a dictionary
    {'pfn': file_name}.
    """
    msg = "Adding file %s to job report." % (file_name)
    print(msg)
    output_file_info = {}
    output_file_info['pfn'] = file_name
    try:
        file_size = os.stat(file_name).st_size
    except:
        msg = "WARNING: Unable to add output file size to job report."
        print(msg)
    else:
        output_file_info['size'] = file_size
    is_ok = add_to_job_report([(key, output_file_info)], \
                              ['steps', 'cmsRun', 'output'], 'update')
    if not is_ok:
        msg = "ERROR: Failed to add file to job report."
        print(msg)
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
    msg  = "Setting"
    key_value_pairs = []
    if temp_storage_site is not None:
        key_value_pairs.append(('temp_storage_site', temp_storage_site))
        msg += " temp_storage_site = '%s'," % (temp_storage_site)
    if storage_site is not None:
        key_value_pairs.append(('storage_site', storage_site))
        msg += " storage_site = '%s'," % (storage_site)
    if local_stageout is not None:
        key_value_pairs.append(('local_stageout', bool(local_stageout)))
        msg += " local_stageout = %s" % (str(bool(local_stageout)))
    if direct_stageout is not None:
        key_value_pairs.append(('direct_stageout', bool(direct_stageout)))
        msg += " direct_stageout = %s" % (str(bool(direct_stageout)))
    msg += " for file %s in job report." % (orig_file_name)
    if not key_value_pairs:
        return True
    print(msg)
    is_ok = add_to_file_in_job_report(file_name, is_log, key_value_pairs)
    if not is_ok:
        msg  = "ERROR: Failed to set the above keys and values"
        msg += " for file %s in job report." % (orig_file_name)
        print(msg)
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
    with open(G_JOB_REPORT_NAME) as fd:
        job_report = json.load(fd)
    output_file_info = get_output_file_from_job_report(orig_file_name, job_report)
    if output_file_info is None:
        msg = "WARNING: Metadata for file %s not found in job report." % (orig_file_name)
        print(msg)
        return False
    for key, value in key_value_pairs:
        output_file_info[key] = value
    with open(G_JOB_REPORT_NAME, 'w') as fd:
        json.dump(job_report, fd)
    return True

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def perform_stageout(local_stageout_mgr, direct_stageout_impl, \
                     direct_stageout_command, direct_stageout_protocol, \
                     policy, \
                     source_file, dest_temp_lfn, dest_pfn, dest_lfn, dest_site, \
                     source_site, is_log, inject):
    """
    Wrapper for local and direct stageouts.
    """
    if policy == 'local':
        retval, retmsg = perform_local_stageout(local_stageout_mgr, \
                                                source_file, dest_temp_lfn, \
                                                dest_lfn, dest_site, \
                                                source_site, is_log, inject)
    elif policy == 'remote':
        ## Can return 60311, 60307 or 60403.
        retval, retmsg = perform_direct_stageout(direct_stageout_impl, \
                                                 direct_stageout_command, \
                                                 direct_stageout_protocol, \
                                                 source_file, dest_pfn, dest_site, source_site, \
                                                 is_log)
    else:
        msg = "ERROR: Skipping unknown stageout policy named '%s'." % (policy)
        print(msg)
        retval, retmsg = 80000, msg
    return retval, retmsg

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def perform_local_stageout(local_stageout_mgr, \
                           source_file, dest_temp_lfn, dest_lfn, dest_site, \
                           source_site, is_log, inject):
    """
    Wrapper for local stageouts.
    """
    file_for_transfer = {'LFN': dest_temp_lfn, 'PFN': source_file}
    ## Start the clock for timeout counting.
    signal.signal(signal.SIGALRM, alarmHandler)
    signal.alarm(G_TRANSFERS_TIMEOUT)
    ## Do the local stageout.
    retval, retmsg = 0, None
    stageout_info = {}
    try:
        ## Throws on any failure.
        print("       -----> Stageout manager log start")
        stageout_info = local_stageout_mgr(file_for_transfer)
        print("       <----- Stageout manager log finish")
    except Alarm:
        ## Alarm was raised, because the timeout (G_TRANSFERS_TIMEOUT) was
        ## reached.
        print("       <----- Stageout manager log finish")
        msg  = "Timeout reached during stageout of %s;" % (source_file)
        msg += " setting return code to 60403."
        print(msg)
        retval, retmsg = 60403, msg
    except Exception as ex:
        msg = "Error during stageout: %s" % (ex)
        print(msg)
        print("       <----- Stageout manager log finish")
        retval, retmsg = 60307, msg
    finally:
        signal.alarm(0)
    if retval == 0:
        dest_temp_file_name = os.path.split(dest_temp_lfn)[-1]
        
        # If fallback stageout happens, PNN can be different as source
        if 'PNN' in stageout_info:
            print("INFO: PNN is defined in site-local-config. %s changed to %s" % (source_site, stageout_info['PNN']))
            source_site = stageout_info['PNN']
        else:
            print("WARNING: PNN is not defined in site-local-config.")

        sites_added_ok = add_sites_to_job_report(dest_temp_file_name, \
                                                 is_log, source_site, \
                                                 dest_site if inject else 'unknown', \
                                                 True, None)
        if not sites_added_ok:
            msg = "WARNING: Ignoring failure in adding the above information to the job report."
            print(msg)
        if inject:
            file_transfer_info = {'source'             : {'lfn': dest_temp_lfn, 'site': source_site},
                                  'destination'        : {'lfn': dest_lfn,      'site': dest_site     },
                                  'is_log'             : is_log,
                                  'local_stageout_mgr' : local_stageout_mgr,
                                  'inject'             : True
                                 }
            G_ASO_TRANSFER_REQUESTS.append(file_transfer_info)
    if stageout_info.get('StageOutReport'):
        DashboardAPI.reportFailureToDashboard(G_JOB_WRAPPER_EXIT_CODE, G_JOB_AD, stageout_info['StageOutReport'])
    return retval, retmsg

## = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

def setASOStartTime():
    if get_from_job_report('aso_start_time') is None or \
       get_from_job_report('aso_start_timestamp') is None:
        msg  = "Setting"
        msg += " aso_start_time = %s" % (G_NOW)
        msg += " and"
        msg += " aso_start_time_stamp = %s" % (G_NOW_EPOCH)
        msg += " in job report."
        print(msg)
        is_ok = add_to_job_report([('aso_start_time', G_NOW), \
                                   ('aso_start_timestamp', G_NOW_EPOCH)])
        if not is_ok:
            msg = "WARNING: Failed to set aso_start_time in job report."
            print(msg)



def perform_direct_stageout(direct_stageout_impl, \
                            direct_stageout_command, direct_stageout_protocol, \
                            source_file, dest_pfn, dest_site, source_site, \
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
    stageoutDashboardReport = {'LFN': dest_pfn,
                               'PNN': dest_site,
                               'PSN': source_site,
                               'StageOutCommand': direct_stageout_command,
                               'StageOutType': 'DIRECT',
                              }
    G_DIRECT_STAGEOUTS.append(direct_stageout_info)
    retval, retmsg = 0, None
    try:
        ## Start the clock for timeout counting.
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(G_TRANSFERS_TIMEOUT)
        ## Do the direct stageout.
        try:
            print("       -----> Stageout implementation log start")
            options = "--nochecksum" if direct_stageout_command == "gfal2" else None
            direct_stageout_impl(direct_stageout_protocol, \
                                 source_file, dest_pfn, options, None)
            print("       <----- Stageout implementation log finish")
        except Alarm:
            print("       <----- Stageout implementation log finish")
            ## Alarm was raised, because the timeout (G_TRANSFERS_TIMEOUT) was
            ## reached.
            msg  = "Timeout reached during stage out of %s;" % (source_file)
            msg += " setting return code to 60403."
            print(msg)
            retval, retmsg = 60403, msg
        except Exception as ex:
            msg  = "Failure in direct stage out:"
            msg += "\n%s" % (str(ex))
            try:
                msg += "\n%s" % (traceback.format_exc())
            except AttributeError as ex:
                msg += "\nTraceback unavailable\n"
            ## StageOutError.StageOutFailure has error code 60311.
            stageoutDashboardReport['StageOutExit'] = 60311
            DashboardAPI.reportFailureToDashboard(60311, G_JOB_AD, [stageoutDashboardReport])
            raise StageOutError.StageOutFailure(msg, Command = direct_stageout_command, Protocol = direct_stageout_protocol, \
                                                LFN = dest_pfn, InputPFN = source_file, TargetPFN = dest_pfn)
        finally:
            signal.alarm(0)
    except WMException.WMException as ex:
        msg  = "Error during direct stageout:"
        msg += "\n%s" % (str(ex))
        print(msg)
        print("       <----- Stageout implementation log finish")
        retval, retmsg = ex.data.get("ErrorCode", 60307), msg
    if retval == 0:
        dest_file_name = os.path.split(dest_pfn)[-1]
        sites_added_ok = add_sites_to_job_report(dest_file_name, is_log, \
                                                 None, dest_site, \
                                                 None, True)
        if not sites_added_ok:
            msg = "WARNING: Ignoring failure in adding the above information to the job report."
            print(msg)
    stageoutDashboardReport['StageOutExit'] = retval
    DashboardAPI.reportFailureToDashboard(retval, G_JOB_AD, [stageoutDashboardReport])
    return retval, retmsg

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
                msg  = "Will not remove logs archive file from local temporary storage"
                msg += " (but will consider its local stageout as failed for transfer purposes)."
                print(msg)
        for dest_temp_lfn in local_stageout_mgr.completedFiles.keys():
            file_name = os.path.basename(dest_temp_lfn)
            orig_file_name, _ = get_job_id(file_name)
            is_log = (dest_temp_lfn == logs_arch_dest_temp_lfn)
            msg = "Setting local_stageout = False for file %s in job report." % (orig_file_name)
            print(msg)
            add_to_file_in_job_report(file_name, is_log, \
                                      [('local_stageout', False)])
        num_files_to_remove = len(local_stageout_mgr.completedFiles)
        if num_files_to_remove > 0:
            msg = "Will remove %d %sfile%s from local temporary storage."
            msg = msg % (num_files_to_remove, \
                         'other ' if add_back_logs_arch else '', \
                         's' if num_files_to_remove > 1 else '')
            print(msg)
            ## Remove from the local temporary storage the files that were
            ## successfully transferred to that storage by the local stageout
            ## manager given as input.
            try:
                print("       -----> Stageout manager log start")
                local_stageout_mgr.cleanSuccessfulStageOuts()
                print("       <----- Stageout manager log finish")
            except StageOutError:
                print("       <----- Stageout manager log finish")
        else:
            msg = "There are no %sfiles to remove in local temporary storage."
            msg = msg % ('other ' if add_back_logs_arch else '')
            print(msg)
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
            print(msg)
            for direct_stageout_info in G_DIRECT_STAGEOUTS:
                if direct_stageout_info['removed']:
                    continue
                if direct_stageout_info['is_log'] and keep_log:
                    msg  = "Will not remove logs archive file"
                    msg += " from permanent storage at %s" % (dest_site)
                    msg += " (but will consider its direct stageout as failed)."
                    print(msg)
                    continue
                dest_site = direct_stageout_info['dest_site']
                dest_pfn  = direct_stageout_info['dest_pfn']
                file_name = os.path.basename(dest_pfn)
                orig_file_name, _ = get_job_id(file_name)
                msg  = "Setting"
                msg += " direct_stageout = False"
                msg += " for file %s in job report." % (orig_file_name)
                print(msg)
                add_to_file_in_job_report(file_name, \
                                          direct_stageout_info['is_log'], \
                                          [('direct_stageout', False)])
                direct_stageout_info['removed'] = True
                msg  = "Attempting to remove PFN %s" % (dest_pfn)
                msg += " from permanent storage at %s." % (dest_site)
                print(msg)
                try:
                    print("       -----> Stageout implementation log start")
                    direct_stageout_impl.removeFile(dest_pfn)
                    print("       <----- Stageout implementation log finish")
                    msg = "File successfully removed."
                    print(msg)
                except:
                    print("       <----- Stageout implementation log finish")
                    msg  = "WARNING: Failed to remove file"
                    msg += " (maybe the file was not transferred)."
                    print(msg)
                    pass
        else:
            msg = "There are no %sfiles to remove in the permanent storage at %s."
            msg = msg % ('other ' if found_log and keep_log else '', dest_site)
            print(msg)
    else:
        msg  = "WARNING: Unknown stageout policy '%s'." % (policy)
        msg += " Skipping cleanup of stageout area."
        print(msg)

##==============================================================================
## THE MAIN FUNCTION THAT RUNS CMSCP.
##------------------------------------------------------------------------------

def main():
    """
    cmscp main.
    """
    ## Initialize the cmscp exit status. This is what will be written into the
    ## report.
    exit_info = {'exit_code': 0,
                 'exit_acronym': 'OK',
                 'exit_msg': 'OK',
                }
    def update_exit_info(exit_info, exit_code, exit_msg = None, force = False):
        """
        Function used to update the cmscp exit_info dictionary.
        """
        if (exit_info['exit_code'] == 0 and exit_code not in [None, 0]) or \
           (force                       and exit_code not in [None]   ):
            exit_info['exit_code'] = exit_code
            exit_info['exit_acronym'] = 'FAILED' if exit_code else 'OK'
            if exit_code:
                if exit_msg:
                    header_msg  = "CmsCpFailure"
                    header_msg += "\ncmscp error message follows."
                    exit_msg = header_msg + "\n%s" % (exit_msg)
                else:
                    header_msg  = "CmsCpFailure"
                    header_msg += "\ncmscp error message not propagated to job report"
                    header_msg += " (and therefore not available in crab status error summary)."
                    header_msg += "\nPlease check the corresponding job log files (e.g. in the monitoring pages) to find the error reason."
                    exit_msg = header_msg
            if exit_msg is None:
                exit_msg = ""
            exit_info['exit_msg'] = exit_msg
            msg = "Setting stageout wrapper exit info to %s." % (exit_info)
            print(msg)

    transfer_logs    = None
    transfer_outputs = None
    output_files     = None
    stageout_policy  = None
    dest_temp_dir    = None
    dest_final_dir   = None
    dest_site        = None
    dest_files       = None

    ## Auxiliary variable that can be used to make the code more readable.
    no_condition = True

    ## These flags can be used to force skipping any of the steps coded below.
    ## May be useful for debugging.
    skip = {'job_report_validation'     : False,
            'outputs_exist'             : False,
            'outputs_in_job_report'     : False,
            'logs_archive'              : False,
            'init_local_stageout_mgr'   : False,
            'init_direct_stageout_impl' : False,
            'logs_stageout'             : {'local'  : False,
                                           'remote' : False},
            'outputs_stageout'          : {'local'  : False,
                                           'remote' : False},
            'aso_injection'             : False,
            'logs_metadata_upload'      : False
           }

    ## A dictionary with a RetTuple for each of the steps coded below.
    ## Return status code = None means the step was not executed.
    cmscp_status = {'job_report_validation'     : {'return_code': None, 'return_msg': None},
                    'outputs_exist'             : {'return_code': None, 'return_msg': None},
                    'outputs_in_job_report'     : {'return_code': None, 'return_msg': None},
                    'logs_archive'              : {'return_code': None, 'return_msg': None},
                    'init_local_stageout_mgr'   : {'return_code': None, 'return_msg': None},
                    'init_direct_stageout_impl' : {'return_code': None, 'return_msg': None},
                    'logs_stageout'             : {'local'  : {'return_code': None, 'return_msg': None},
                                                   'remote' : {'return_code': None, 'return_msg': None}},
                    'outputs_stageout'          : {'local'  : {'return_code': None, 'return_msg': None},
                                                   'remote' : {'return_code': None, 'return_msg': None}},
                    'aso_injection'             : {'return_code': None, 'return_msg': None},
                    'logs_metadata_upload'      : {'return_code': None, 'return_msg': None}
                   }

    ##--------------------------------------------------------------------------
    ## Check python version
    ##--------------------------------------------------------------------------
    if not sys.version_info >= (2,6):
        msg = "cmscp is exiting because it cannot find a proper python version"
        print(msg)
        #TODO: 10043 is "Unable to bootstrap WMCore libraries", maybe we want a better error
        #(let's wait that errors are in WMCore)
        update_exit_info(exit_info, 10043, msg, True)
        return exit_info

    ##--------------------------------------------------------------------------
    ## Start PARSE JOB AD
    ##--------------------------------------------------------------------------
    ## Parse the job's HTCondor ClassAd.
    if '_CONDOR_JOB_AD' not in os.environ:
        msg  = "ERROR: _CONDOR_JOB_AD not in environment."
        msg += "\nNo stageout will be performed."
        print(msg)
        update_exit_info(exit_info, 80000, msg, True)
        return exit_info
    if not os.path.exists(os.environ['_CONDOR_JOB_AD']):
        msg  = "ERROR: _CONDOR_JOB_AD (%s) does not exist."
        msg += "\nNo stageout will be performed."
        msg  = msg % (os.environ['_CONDOR_JOB_AD'])
        print(msg)
        update_exit_info(exit_info, 80000, msg, True)
        return exit_info
    global G_JOB_AD
    try:
        G_JOB_AD = parseJobAd(os.environ['_CONDOR_JOB_AD'])
    except Exception:
        msg  = "WARNING: Unable to parse job's HTCondor ClassAd."
        msg += "\n%s" % (traceback.format_exc())
        print(msg)
    ## If CRAB_NoWNStageout has been set to an integer value > 0 (maybe with
    ## extraJDL from the client) then we don't do the stageout.
    if G_JOB_AD.get('CRAB_NoWNStageout', 0):
        print("==== NOT PERFORMING STAGEOUT AS CRAB_NoWNStageout is 1 ====")
        update_exit_info(exit_info, 0, 'OK', True)
        return exit_info
    ## If we couldn't read CRAB_SaveLogsFlag from the job ad, we assume False.
    if 'CRAB_SaveLogsFlag' not in G_JOB_AD:
        msg  = "WARNING: Job's HTCondor ClassAd is missing attribute CRAB_SaveLogsFlag."
        msg += " Will assume CRAB_SaveLogsFlag = False."
        print(msg)
        transfer_logs = False
    else:
        transfer_logs = G_JOB_AD['CRAB_SaveLogsFlag']
    if not transfer_logs:
        msg  = "The user has not specified to transfer the log files."
        msg += " No log files stageout (nor log files metadata upload) will be performed."
        print(msg)
    ## If we couldn't read CRAB_TransferOutputs from the job ad, we assume True.
    if 'CRAB_TransferOutputs' not in G_JOB_AD:
        msg  = "WARNING: Job's HTCondor ClassAd is missing attribute CRAB_TransferOutputs."
        msg += " Will assume CRAB_TransferOutputs = True."
        print(msg)
        transfer_outputs = True
    else:
        transfer_outputs = G_JOB_AD['CRAB_TransferOutputs']
    ## If we couldn't read CRAB_localOutputFiles from the job ad, we abort.
    if 'CRAB_localOutputFiles' not in G_JOB_AD:
        msg  = "ERROR: Job's HTCondor ClassAd is missing attribute CRAB_localOutputFiles."
        msg += "\nNo stageout will be performed."
        print(msg)
        update_exit_info(exit_info, 80000, msg, True)
        return exit_info
    split_re = re.compile(",\s*")
    ## Get the list of output files produced by the job.
    output_files = []
    if G_JOB_AD['CRAB_localOutputFiles'].replace(' ', ''):
        output_files = split_re.split(G_JOB_AD['CRAB_localOutputFiles'].replace(' ', ''))
    ## If there is no list of output files, turn off their transfer.
    if len(output_files) == 0:
        if not transfer_outputs:
            msg  = "Job's HTCondor ClassAd attribute CRAB_localOutputFiles is empty,"
            msg += " indicating that the job doesn't produce any output file."
            msg += " In any case, the user has specified to not transfer output files."
            print(msg)
        else:
            msg  = "The transfer of output files flag in on,"
            msg += " but the job's HTCondor ClassAd attribute CRAB_localOutputFiles is empty,"
            msg += " indicating that the job doesn't produce any output file."
            msg += " Turning off the transfer of output files flag."
            print(msg)
            transfer_outputs = False
    else:
        if not transfer_outputs:
            msg  = "The user has specified to not transfer the output files."
            msg += " No output files stageout (nor output files metadata upload) will be performed."
            print(msg)
    ## If we don't have to transfer the log files or the output files, there is
    ## nothing to do in cmscp. So exit right here.
    if not (transfer_logs or transfer_outputs):
        msg = "Stageout wrapper has no work to do. Finishing here."
        print(msg)
        update_exit_info(exit_info, 0, 'OK', True)
        return exit_info
    ## At this point we are sure that one of the transfer flags (transfer_logs
    ## or transfer_outputs) is True.
    ## List of attributes that the code must be able to get from the job ad.
    ## CRAB_Id: job number in the task,
    ## CRAB_StageoutPolicy: local or remote stage-out,
    ## CRAB_Destination: PFNs of user's files in the home SE,
    ## CRAB_OutTempLFNDir: LFN dir. of the outputs in the close SE,
    ## CRAB_OutLFNDir: LFN dir. of the outputs in the user's home SE,
    ## CRAB_AsyncDest: user's home site.
    job_ad_required_attrs = ['CRAB_Id', \
                             'CRAB_StageoutPolicy', \
                             'CRAB_Destination', \
                             'CRAB_OutTempLFNDir', \
                             'CRAB_OutLFNDir', \
                             'CRAB_AsyncDest']
    ## Check that the above attributes are defined in the job ad.
    for attr in job_ad_required_attrs:
        if attr not in G_JOB_AD:
            msg  = "ERROR: Job's HTCondor ClassAd is missing attribute %s." % (attr)
            msg += "\nNo stageout will be performed."
            print(msg)
            update_exit_info(exit_info, 80000, msg, True)
            return exit_info
    ## Retrieve the above attributes from the job ad.
    stageout_policy = split_re.split(G_JOB_AD['CRAB_StageoutPolicy'])
    print("Stageout policy: %s" % (", ".join(stageout_policy)))
    dest_temp_dir = G_JOB_AD['CRAB_OutTempLFNDir']
    dest_final_dir = G_JOB_AD['CRAB_OutLFNDir']
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
    G_JOB_REPORT_NAME = 'jobReport.json.%s' % G_JOB_AD['CRAB_Id']
    ## Load the json job report and make sure it has the expected structure.
    condition = no_condition
    if skip['job_report_validation']:
        msg  = "WARNING: Internal wrapper flag skip['job_report_validation'] is True."
        msg += " Skipping to validate the json job report."
        print(msg)
    elif condition:
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Starting job report validation."
        print(msg)
        try:
            job_report = {}
            with open(G_JOB_REPORT_NAME) as fd:
                job_report = json.load(fd)
            cmscp_status['job_report_validation']['return_code'] = 0
        except Exception:
            msg  = "ERROR: Unable to load %s." % (G_JOB_REPORT_NAME)
            msg += "\n%s" % (traceback.format_exc())
            print(msg)
            cmscp_status['job_report_validation']['return_code'] = 80000
            cmscp_status['job_report_validation']['return_msg'] = msg
        ## Sanity check of the json job report.
        if 'steps' not in job_report:
            msg = "ERROR: Invalid job report: missing 'steps'."
            print(msg)
            cmscp_status['job_report_validation']['return_code'] = 80000
            cmscp_status['job_report_validation']['return_msg'] = msg
        elif 'cmsRun' not in job_report['steps']:
            msg = "ERROR: Invalid job report: missing 'cmsRun'."
            print(msg)
            cmscp_status['job_report_validation']['return_code'] = 80000
            cmscp_status['job_report_validation']['return_msg'] = msg
        elif 'output' not in job_report['steps']['cmsRun']:
            msg = "ERROR: Invalid job report: missing 'output'."
            print(msg)
            cmscp_status['job_report_validation']['return_code'] = 80000
            cmscp_status['job_report_validation']['return_msg'] = msg
        else:
            print("Job report seems ok (it has the expected structure).")
        ## Try to determine whether the payload actually succeeded.
        ## If the payload didn't succeed, we put it in a different directory.
        ## This prevents us from putting failed output files in the same
        ## directory as successful output files; we worry that users may simply
        ## 'ls' the directory and run on all listed files.
        global G_JOB_EXIT_CODE
        global G_JOB_WRAPPER_EXIT_CODE
        try:
            G_JOB_EXIT_CODE = job_report['jobExitCode']
            msg = "Retrieved payload exit code ('jobExitCode') = %s from job report." % (G_JOB_EXIT_CODE)
            print(msg)
        except Exception:
            msg  = "WARNING: Unable to retrieve payload exit code ('jobExitCode') from job report."
            msg += "\nCurrently this exit code is not used for anything, so this error can be ignored."
            print(msg)
        try:
            G_JOB_WRAPPER_EXIT_CODE = job_report['exitCode']
            msg = "Retrieved job wrapper exit code ('exitCode') = %s from job report." % (G_JOB_WRAPPER_EXIT_CODE)
            print(msg)
        except Exception:
            msg  = "WARNING: Unable to retrieve job wrapper exit code ('exitCode') from job report."
            msg += "\nWill assume job executable failed, with following implications:"
            msg += "\n- if stageout is still possible, it will be done into a subdirectory named 'failed';"
            msg += "\n- if stageout is still possible, publication will be disabled."
            print(msg)
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Finished job report validation"
        msg += " (status %d)." % (cmscp_status['job_report_validation']['return_code'])
        print(msg)
    update_exit_info(exit_info, \
                     cmscp_status['job_report_validation']['return_code'], \
                     cmscp_status['job_report_validation']['return_msg'])
    ##--------------------------------------------------------------------------
    ## Finish JOB REPORT VALIDATION
    ##--------------------------------------------------------------------------

    source_site = str(get_from_job_report('phedex_node', 'unknown'))
    ## Check if Exe Site == Dest Site, if true - stageout directly
    if source_site == dest_site:
        # Make sure two policies are used (local, remote) and we can change it
        # It can be that only one policy is specified. See TW configuration file.
        if stageout_policy == ["local", "remote"]:
            print('Job execution site is the same as destination site. Changing stageout policy.')
            stageout_policy = ["remote", "local"]
            print('New stageout policy: %s' % (", ".join(stageout_policy)))
        else:
            print('Not rewriting stageout policy. Continue with %s stageout policy.' % (", ".join(stageout_policy)))


    ## Modify the stageout temporary and final directory by:
    ## a) adding a four-digit counter;
    logs_arch_dest_pfn_path = os.path.dirname(dest_files[0])
    if logs_arch_dest_pfn_path.endswith('/log'):
        logs_arch_dest_pfn_path = re.sub(r'/log$', '', logs_arch_dest_pfn_path)
    counter = os.path.basename(logs_arch_dest_pfn_path)
    dest_temp_dir = os.path.join(dest_temp_dir, counter)
    dest_final_dir = os.path.join(dest_final_dir, counter)
    ## b) adding a 'failed' subdirectory in case cmsRun failed.
    if G_JOB_WRAPPER_EXIT_CODE != 0:
        dest_temp_dir = os.path.join(dest_temp_dir, 'failed')
        dest_final_dir = os.path.join(dest_final_dir, 'failed')
    ## Definitions needed for the logs archive creation, stageout and metadata
    ## upload.
    logs_arch_file_name = 'cmsRun.log.tar.gz'
    logs_arch_dest_file_name = os.path.basename(dest_files[0])
    logs_arch_dest_pfn = dest_files[0]
    if G_JOB_WRAPPER_EXIT_CODE != 0:
        logs_arch_dest_pfn_path = os.path.join(logs_arch_dest_pfn_path, 'failed', 'log')
        logs_arch_dest_pfn = os.path.join(logs_arch_dest_pfn_path, logs_arch_dest_file_name)
    logs_arch_dest_temp_lfn = os.path.join(dest_temp_dir, 'log', logs_arch_dest_file_name)
    logs_arch_dest_lfn = os.path.join(dest_final_dir, 'log', logs_arch_dest_file_name)

    ##--------------------------------------------------------------------------
    ## Start CHECK OUTPUT FILES EXIST
    ##--------------------------------------------------------------------------
    ## Check that the output files are well defined in the job ad and that they
    ## exist in the worker node.
    condition = transfer_outputs
    if skip['outputs_exist']:
        msg  = "WARNING: Internal wrapper flag skip['outputs_exist'] is True."
        msg += " Skipping to check if user output files exist."
        print(msg)
    elif condition:
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Starting to check if user output files exist."
        print(msg)
        for output_file_name_info in output_files:
            cur_retval = 0
            ## The output_file_name_info is something like this:
            ## my_output_file.root=my_output_file_<job-id>.root
            if len(output_file_name_info.split('=')) != 2:
                msg = "ERROR: Invalid output format (%s)." % (output_file_name_info)
                print(msg)
                cur_retval = 80000
            else:
                output_file_name = output_file_name_info.split('=')[0]
                if not os.path.exists(output_file_name):
                    msg = "ERROR: Output file %s does not exist." % (output_file_name)
                    print(msg)
                    cur_retval = 60302
                else:
                    msg = "Output file %s exists." % (output_file_name)
                    print(msg)
            if cmscp_status['outputs_exist']['return_code'] in [None, 0]:
                cmscp_status['outputs_exist']['return_code'] = cur_retval
                if cur_retval:
                    cmscp_status['outputs_exist']['return_msg'] = msg
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Finished to check if user output files exist"
        msg += " (status %d)." % (cmscp_status['outputs_exist']['return_code'])
        print(msg)
    update_exit_info(exit_info, \
                     cmscp_status['outputs_exist']['return_code'], \
                     cmscp_status['outputs_exist']['return_msg'])
    ##--------------------------------------------------------------------------
    ## Finish CHECK OUTPUT FILES EXIST
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start CHECK OUTPUT FILES IN JOB REPORT
    ##--------------------------------------------------------------------------
    ## Check if the output file is in the json job report. If it is not, add it.
    condition = (cmscp_status['job_report_validation']['return_code'] == 0 and \
                 cmscp_status['outputs_exist']['return_code'] == 0)
    if skip['outputs_in_job_report']:
        msg  = "WARNING: Internal wrapper flag skip['outputs_in_job_report'] is True."
        msg += " Skipping to check if user output files are in the job report."
        print(msg)
    elif condition:
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Starting to check if user output files are in job report."
        print(msg)
        for output_file_name_info in output_files:
            cur_retval = 0
            ## The output_file_name_info is something like this:
            ## my_output_file.root=my_output_file_<job-id>.root
            if len(output_file_name_info.split('=')) != 2:
                msg = "ERROR: Invalid output format (%s)." % (output_file_name_info)
                print(msg)
                cur_retval = 80000
            else:
                output_file_name = output_file_name_info.split('=')[0]
                is_file_in_job_report = bool(get_output_file_from_job_report(output_file_name))
                if not is_file_in_job_report:
                    msg = "Output file %s not found in job report." % (output_file_name)
                    print(msg)
                    file_added_ok = add_output_file_to_job_report(output_file_name)
                    if not file_added_ok:
                        cur_retval = 60318
                else:
                    msg = "Output file %s found in job report." % (output_file_name)
                    print(msg)
            if cmscp_status['outputs_in_job_report']['return_code'] in [None, 0]:
                cmscp_status['outputs_in_job_report']['return_code'] = cur_retval
                if cur_retval:
                    cmscp_status['outputs_in_job_report']['return_msg'] = msg
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Finished to check if user output files are in job report"
        msg += " (status %d)." % (cmscp_status['outputs_in_job_report']['return_code'])
        print(msg)
    update_exit_info(exit_info, \
                     cmscp_status['outputs_in_job_report']['return_code'], \
                     cmscp_status['outputs_in_job_report']['return_msg'])
    ##--------------------------------------------------------------------------
    ## Finish CHECK OUTPUT FILES IN JOB REPORT
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start LOGS TARBALL CREATION
    ##--------------------------------------------------------------------------
    ## Create a zipped tar archive file of the user logs.
    condition = transfer_logs
    if skip['logs_archive']:
        msg  = "WARNING: Internal wrapper flag skip['logs_archive'] is True."
        msg += " Skipping creation of user logs archive file."
        print(msg)
    elif condition:
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Starting creation of user logs archive file."
        print(msg)
        try:
            cmscp_status['logs_archive']['return_code'], \
            cmscp_status['logs_archive']['return_msg'] = \
                                        make_logs_archive(logs_arch_file_name)
        except tarfile.TarError:
            msg  = "ERROR creating user logs archive file."
            msg += "\n%s" % (traceback.format_exc())
            print(msg)
            if cmscp_status['logs_archive']['return_code'] in [None, 0]:
                cmscp_status['logs_archive']['return_code'] = 80000
                cmscp_status['logs_archive']['return_msg'] = msg
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Finished creation of user logs archive file"
        msg += " (status %d)." % (cmscp_status['logs_archive']['return_code'])
        print(msg)
        ## Determine the logs archive file size and write it in the job report.
        try:
            log_size = os.stat(logs_arch_file_name).st_size
            add_to_file_in_job_report(logs_arch_dest_file_name, True, \
                                      [('log_size', log_size)])
        except Exception:
            msg = "WARNING: Unable to add logs archive file size to job report."
            print(msg)
    update_exit_info(exit_info, \
                     cmscp_status['logs_archive']['return_code'], \
                     cmscp_status['logs_archive']['return_msg'])
    ##--------------------------------------------------------------------------
    ## Finish LOGS TARBALL CREATION
    ##--------------------------------------------------------------------------

    ## Define what are so far the conditions for doing the stageouts.
    condition_logs_stageout = (cmscp_status['logs_archive']['return_code'] == 0)
    condition_outputs_stageout = (cmscp_status['outputs_exist']['return_code'] == 0 and \
                                  cmscp_status['outputs_in_job_report']['return_code'] == 0)
    condition_stageout = (condition_logs_stageout or condition_outputs_stageout)

    ##--------------------------------------------------------------------------
    ## Start LOCAL STAGEOUT MANAGER INITIALIZATION
    ##--------------------------------------------------------------------------
    ## This stageout manager will be used for all local stageout attempts (for
    ## the user logs archive file and the user output files).
    local_stageout_mgr = None
    condition = ('local' in stageout_policy and condition_stageout)
    if skip['init_local_stageout_mgr']:
        msg  = "WARNING: Internal wrapper flag skip['init_local_stageout_mgr'] is True."
        msg += " Skipping initialization of stageout manager for local stageouts."
        print(msg)
    elif condition:
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Starting initialization of stageout manager for local stageouts."
        print(msg)
        try:
            print("       -----> Stageout manager log start")
            local_stageout_mgr = StageOutMgr.StageOutMgr()
            local_stageout_mgr.numberOfRetries = G_NUMBER_OF_RETRIES
            local_stageout_mgr.retryPauseTime = G_RETRY_PAUSE_TIME
            print("       <----- Stageout manager log finish")
            msg = "Initialization was ok."
            print(msg)
            cmscp_status['init_local_stageout_mgr']['return_code'] = 0
        except Exception:
            print("       <----- Stageout manager log finish")
            msg  = "WARNING: Error initializing StageOutMgr."
            msg += " Will not be able to do local stageouts.\n"
            msg += traceback.format_exc()
            print(msg)
            cmscp_status['init_local_stageout_mgr']['return_code'] = 60311
            cmscp_status['init_local_stageout_mgr']['return_msg'] = msg
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Finished initialization of stageout manager for local stageouts"
        msg += " (status %d)." % (cmscp_status['init_local_stageout_mgr']['return_code'])
        print(msg)
    update_exit_info(exit_info, \
                     cmscp_status['init_local_stageout_mgr']['return_code'], \
                     cmscp_status['init_local_stageout_mgr']['return_msg'])
    ##--------------------------------------------------------------------------
    ## Finish LOCAL STAGEOUT MANAGER INITIALIZATION
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start DIRECT STAGEOUT IMPLEMENTATION INITIALIZATION
    ##--------------------------------------------------------------------------
    ## This stageout implementation will be used for all direct stageout
    ## attempts (for the user logs archive file and the user output files).
    direct_stageout_impl = None
    direct_stageout_command = "srmv2-lcg"
    direct_stageout_protocol = "srmv2"
    if cmd_exist("gfal-copy"):
        print('Will use gfal2 commands for direct stageout.')
        direct_stageout_command = "gfal2"
    condition = ('remote' in stageout_policy and condition_stageout)
    if skip['init_direct_stageout_impl']:
        msg  = "WARNING: Internal wrapper flag skip['init_direct_stageout_impl'] is True."
        msg += " Skipping initialization of stageout implementation for direct stageouts."
        print(msg)
    elif condition:
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Starting initialization of stageout implementation for direct stageouts."
        print(msg)
        try:
            direct_stageout_impl = retrieveStageOutImpl(direct_stageout_command)
            direct_stageout_impl.numRetries = G_NUMBER_OF_RETRIES
            direct_stageout_impl.retryPause = G_RETRY_PAUSE_TIME
            cmscp_status['init_direct_stageout_impl']['return_code'] = 0
            msg = "Initialization was ok."
            print(msg)
        except Exception:
            msg  = "WARNING: Error retrieving StageOutImpl for command '%s'." % (direct_stageout_command)
            msg += " Will not be able to do direct stageouts."
            print(msg)
            cmscp_status['init_direct_stageout_impl']['return_code'] = 60311
            cmscp_status['init_direct_stageout_impl']['return_msg'] = msg
        msg  = "====== %s: " % (time.asctime(time.gmtime()))
        msg += "Finished initialization of stageout implementation for direct stageouts"
        msg += " (status %d)." % (cmscp_status['init_direct_stageout_impl']['return_code'])
        print(msg)
    if cmscp_status['init_local_stageout_mgr']['return_code'] != 0:
        update_exit_info(exit_info, \
                         cmscp_status['init_direct_stageout_impl']['return_code'], \
                         cmscp_status['init_direct_stageout_impl']['return_msg'], True)
    ##--------------------------------------------------------------------------
    ## Finish DIRECT STAGEOUT IMPLEMENTATION INITIALIZATION
    ##--------------------------------------------------------------------------

    ##--------------------------------------------------------------------------
    ## Start STAGEOUT OF USER LOGS TARBALL AND USER OUTPUTS
    ##--------------------------------------------------------------------------
    ## Stage out the logs archive file and the output files. Do local or direct
    ## stageout according to the (configurable) stageout policy. But don't
    ## inject to ASO. Injection to ASO is done after all the local stageouts are
    ## done successfully.
    first_stageout_failure_code = None
    first_stageout_failure_msg  = None
    is_log_in_storage = {'local': False, 'remote': False}
    for policy in stageout_policy:
        clean = False
        ##---------------
        ## Logs stageout.
        ##---------------
        condition = condition_logs_stageout
        if policy == 'local':
            condition = (condition and \
                         cmscp_status['init_local_stageout_mgr']['return_code'] == 0 and \
                         cmscp_status['logs_stageout']['remote']['return_code'] != 0)
        elif policy == 'remote':
            condition = (condition and \
                         cmscp_status['init_direct_stageout_impl']['return_code'] == 0 and \
                         cmscp_status['logs_stageout']['local']['return_code'] != 0)
        ## There are some cases where we don't have to stage out the log files.
        if condition:
            if skip['logs_stageout'][policy]:
                msg  = "WARNING: Internal wrapper flag skip['logs_stageout']['%s'] is True." % (policy)
                msg += " Skipping %s stageout of user logs archive file." % (policy)
                print(msg)
                condition = False
        ## If we have to, stage out the logs archive file.
        if condition:
            msg  = "====== %s: " % (time.asctime(time.gmtime()))
            msg += "Starting %s stageout of user logs archive file." % (policy)
            print(msg)
            try:
                cmscp_status['logs_stageout'][policy]['return_code'], \
                cmscp_status['logs_stageout'][policy]['return_msg'] = \
                                                     perform_stageout(local_stageout_mgr, \
                                                                      direct_stageout_impl, \
                                                                      direct_stageout_command, \
                                                                      direct_stageout_protocol, \
                                                                      policy, \
                                                                      logs_arch_file_name, \
                                                                      logs_arch_dest_temp_lfn, \
                                                                      logs_arch_dest_pfn, \
                                                                      logs_arch_dest_lfn, \
                                                                      dest_site, source_site, \
                                                                      is_log = True, inject = transfer_logs)
            except Exception:
                msg  = "ERROR: Unhandled exception when performing stageout of user logs archive file."
                msg += "\n%s" % (traceback.format_exc())
                print(msg)
                if cmscp_status['logs_stageout'][policy]['return_code'] in [None, 0]:
                    cmscp_status['logs_stageout'][policy]['return_code'] = 60318
                    cmscp_status['logs_stageout'][policy]['return_msg'] = msg
            msg  = "====== %s: " % (time.asctime(time.gmtime()))
            msg += "Finished %s stageout of user logs archive file" % (policy)
            msg += " (status %d)." % (cmscp_status['logs_stageout'][policy]['return_code'])
            print(msg)
            if cmscp_status['logs_stageout'][policy]['return_code'] == 0:
                is_log_in_storage[policy] = True
            ## If the stageout failed, clean the stageout area. But don't remove
            ## the log from the local stageout area (we want to keep it there in
            ## case the next stageout policy also fails).
            if cmscp_status['logs_stageout'][policy]['return_code'] not in [None, 0]:
                clean = True
                if transfer_logs and first_stageout_failure_code is None:
                    first_stageout_failure_code = cmscp_status['logs_stageout'][policy]['return_code']
                    first_stageout_failure_msg  = cmscp_status['logs_stageout'][policy]['return_msg']
        ##------------------
        ## Outputs stageout.
        ##------------------
        condition = condition_outputs_stageout
        if policy == 'local':
            condition = (condition and \
                         cmscp_status['init_local_stageout_mgr']['return_code'] == 0 and \
                         cmscp_status['outputs_stageout']['remote']['return_code'] != 0)
        elif policy == 'remote':
            condition = (condition and \
                         cmscp_status['init_direct_stageout_impl']['return_code'] == 0 and \
                         cmscp_status['outputs_stageout']['local']['return_code'] != 0)
        ## There are some cases where we don't have to stage out the output files.
        if condition:
            if skip['outputs_stageout'][policy]:
                msg  = "WARNING: Internal wrapper flag skip['outputs_stageout']['%s'] is True." % (policy)
                msg += " Skipping %s stageout of user output files." % (policy)
                print(msg)
                condition = False
            elif cmscp_status['logs_stageout'][policy]['return_code'] not in [None, 0]:
                msg  = "Will not do %s stageout of output files," % (policy)
                msg += " because %s stageout already failed for the logs archive file." % (policy)
                print(msg)
                cmscp_status['outputs_stageout'][policy]['return_code'] = 60318
                cmscp_status['outputs_stageout'][policy]['return_msg'] = msg
                condition = False
        ## If we have to, stage out the output files.
        if condition:
            msg  = "====== %s: " % (time.asctime(time.gmtime()))
            msg += "Starting %s stageout of user output files." % (policy)
            print(msg)
            for output_file_name_info, output_dest_pfn in zip(output_files, dest_files[1:]):
                ## The output_file_name_info is something like this:
                ## my_output_file.root=my_output_file_<job-id>.root
                if len(output_file_name_info.split('=')) != 2:
                    msg = "ERROR: Invalid output format (%s)." % (output_file_name_info)
                    print(msg)
                    cur_retval, cur_retmsg = 80000, msg
                else:
                    cur_retval, cur_retmsg = None, None
                    output_file_name, output_dest_file_name = output_file_name_info.split('=')
                    msg  = "-----> %s: " % (time.asctime(time.gmtime()))
                    msg += "Starting %s stageout of %s." % (policy, output_file_name)
                    print(msg)
                    output_dest_temp_lfn = os.path.join(dest_temp_dir, output_dest_file_name)
                    output_dest_pfn_path = os.path.dirname(output_dest_pfn)
                    if G_JOB_WRAPPER_EXIT_CODE != 0:
                        output_dest_pfn_path = os.path.join(output_dest_pfn_path, 'failed')
                    output_dest_pfn = os.path.join(output_dest_pfn_path, output_dest_file_name)
                    output_dest_lfn = os.path.join(dest_final_dir, output_dest_file_name)
                    try:
                        cur_retval, \
                        cur_retmsg = perform_stageout(local_stageout_mgr, \
                                                      direct_stageout_impl, \
                                                      direct_stageout_command, \
                                                      direct_stageout_protocol, \
                                                      policy, \
                                                      output_file_name, \
                                                      output_dest_temp_lfn, \
                                                      output_dest_pfn, \
                                                      output_dest_lfn, \
                                                      dest_site, source_site, \
                                                      is_log = False, inject = transfer_outputs)
                    except Exception:
                        msg  = "ERROR: Unhandled exception when performing stageout."
                        msg += "\n%s" % (traceback.format_exc())
                        print(msg)
                        if cur_retval in [None, 0]:
                            cur_retval, cur_retmsg = 60318, msg
                    msg  = "<----- %s: " % (time.asctime(time.gmtime()))
                    msg += "Finished %s stageout of %s" % (policy, output_file_name)
                    msg += " (status %d)." % (cur_retval)
                    print(msg)
                if cmscp_status['outputs_stageout'][policy]['return_code'] in [None, 0]:
                    cmscp_status['outputs_stageout'][policy]['return_code'] = cur_retval
                    cmscp_status['outputs_stageout'][policy]['return_msg'] = cur_retmsg
                ## If the stageout failed for one of the output files, don't even
                ## try to stage out the rest of the output files.
                if cmscp_status['outputs_stageout'][policy]['return_code'] not in [None, 0]:
                    msg  = "%s stageout of %s failed." % (policy.title(), output_file_name)
                    msg += " Will not attempt %s stageout for any other output files (if any)." % (policy)
                    print(msg)
                    break
            msg  = "====== %s: " % (time.asctime(time.gmtime()))
            msg += "Finished %s stageout of user output files" % (policy)
            msg += " (status %d)." % (cmscp_status['outputs_stageout'][policy]['return_code'])
            print(msg)
            if cmscp_status['outputs_stageout'][policy]['return_code'] not in [None, 0]:
                clean = True
                if first_stageout_failure_code is None:
                    first_stageout_failure_code = cmscp_status['outputs_stageout'][policy]['return_code']
                    first_stageout_failure_msg  = cmscp_status['outputs_stageout'][policy]['return_msg']
        if clean:
            ## If the stageout failed, clean the stageout area. But don't remove
            ## the logs archive file from the local stageout area (we want to
            ## keep it there in case the direct stageout also fails). Not
            ## cleaning the log doesn't mean that we will request ASO to
            ## transfer it; we will not.
            msg  = "====== %s: " % (time.asctime(time.gmtime()))
            msg += "Starting to clean %s stageout area." % (policy)
            print(msg)
            clean_log = (policy == 'remote')
            if clean_log:
                is_log_in_storage[policy] = False
            clean_stageout_area(local_stageout_mgr, direct_stageout_impl, \
                                policy, logs_arch_dest_temp_lfn, \
                                keep_log = not clean_log)
            msg  = "====== %s: " % (time.asctime(time.gmtime()))
            msg += "Finished to clean %s stageout area." % (policy)
            print(msg)
            ## Since we cleaned the storage area, we have to set the return
            ## status of this policy stageout to a general failure code (if
            ## originally 0).
            if cmscp_status['logs_stageout'][policy]['return_code'] == 0:
                cmscp_status['logs_stageout'][policy]['return_code'] = 60318
                cmscp_status['logs_stageout'][policy]['return_msg'] = \
                "%s stageout area has been cleaned after stageout failure." % (policy.title())
            if cmscp_status['outputs_stageout'][policy]['return_code'] == 0:
                cmscp_status['outputs_stageout'][policy]['return_code'] = 60318
                cmscp_status['outputs_stageout'][policy]['return_msg'] = \
                "%s stageout area has been cleaned after stageout failure." % (policy.title())
        ## If this stageout policy succeeded for the both logs archive file
        ## and output files, then we don't need to try any other stageout
        ## policy.
        if cmscp_status['logs_stageout'][policy]['return_code'] == 0 and \
           cmscp_status['outputs_stageout'][policy]['return_code'] == 0:
            break
    ## If stageout failed, update the cmscp return code with the stageout
    ## failure that happened first.
    if transfer_logs:
        if not (cmscp_status['logs_stageout']['local']['return_code'] == 0 or \
                cmscp_status['logs_stageout']['remote']['return_code'] == 0):
            update_exit_info(exit_info, \
                             first_stageout_failure_code, \
                             first_stageout_failure_msg)
    if transfer_outputs:
        if not (cmscp_status['outputs_stageout']['local']['return_code'] == 0 or \
                cmscp_status['outputs_stageout']['remote']['return_code'] == 0):
            update_exit_info(exit_info, \
                             first_stageout_failure_code, \
                             first_stageout_failure_msg)
    ##--------------------------------------------------------------------------
    ## Finish STAGEOUT OF USER LOGS TARBALL AND USER OUTPUTS
    ##--------------------------------------------------------------------------

    return exit_info

##==============================================================================
## CMSCP RUNS HERE WHEN SOURCED FROM gWMS-CMSRunAnalysis.sh.
##------------------------------------------------------------------------------

if __name__ == '__main__':
    MSG  = "====== %s: " % (time.asctime(time.gmtime()))
    MSG += "cmscp.py STARTING."
    print(MSG)
    logging.basicConfig(level = logging.INFO)
    ## Run the stageout wrapper.
    JOB_STGOUT_WRAPPER_EXIT_INFO = {}
    try:
        JOB_STGOUT_WRAPPER_EXIT_INFO = main()
    except:
        MSG  = "ERROR: Unhandled exception."
        MSG += "\n%s" % (traceback.format_exc())
        print(MSG)
        EXIT_MSG = "cmscp.py" + MSG
        JOB_STGOUT_WRAPPER_EXIT_INFO['exit_code'] = 80000
        JOB_STGOUT_WRAPPER_EXIT_INFO['exit_acronym'] = 'FAILED'
        JOB_STGOUT_WRAPPER_EXIT_INFO['exit_msg'] = EXIT_MSG
        MSG = "Setting stageout wrapper exit info to %s." % (JOB_STGOUT_WRAPPER_EXIT_INFO)
        print(MSG)
    ## If the job wrapper finished successfully, but the stageout wrapper
    ## didn't, record the failure in the job report.
    if G_JOB_WRAPPER_EXIT_CODE == 0 and JOB_STGOUT_WRAPPER_EXIT_INFO['exit_code'] != 0:
        add_to_job_report([('exitCode',    JOB_STGOUT_WRAPPER_EXIT_INFO['exit_code']), \
                           ('exitAcronym', JOB_STGOUT_WRAPPER_EXIT_INFO['exit_acronym']), \
                           ('exitMsg',     JOB_STGOUT_WRAPPER_EXIT_INFO['exit_msg'])])
    ## Now we have to exit with the appropriate exit code, and report failures
    ## to dashboard.
    if G_JOB_WRAPPER_EXIT_CODE == None:
        MSG = "Cannot retrieve the job exit code from the job report (does %s exist?)." % (G_JOB_REPORT_NAME)
        print(MSG)
    if G_JOB_WRAPPER_EXIT_CODE not in [0, None]:
        MSG  = "Job wrapper did not finish successfully (exit code %d)." % (G_JOB_WRAPPER_EXIT_CODE)
        MSG += " Setting that same exit code for the stageout wrapper."
        print(MSG)
        CMSCP_EXIT_CODE = G_JOB_WRAPPER_EXIT_CODE
    else:
        CMSCP_EXIT_CODE = JOB_STGOUT_WRAPPER_EXIT_INFO['exit_code']
    if CMSCP_EXIT_CODE != 0:
        if os.environ.get('TEST_CMSCP_NO_STATUS_UPDATE', False):
            MSG  = "Environment flag TEST_CMSCP_NO_STATUS_UPDATE is set."
            MSG += " Will not send report to dashbaord."
            print(MSG)
        elif G_JOB_AD:
            try:
                MSG  = "Stageout wrapper finished with exit code %s." % (CMSCP_EXIT_CODE)
                MSG += " Will report failure to Dashboard."
                print(MSG)
                DashboardAPI.reportFailureToDashboard(CMSCP_EXIT_CODE, G_JOB_AD)
            except:
                MSG  = "ERROR: Unhandled exception when reporting failure to dashboard."
                MSG += "\n%s" % (traceback.format_exc())
                print(MSG)
        else:
            MSG  = "ERROR: Job's HTCondor ClassAd was not read."
            MSG += " Will not report failure to Dashboard."
            print(MSG)
    MSG  = "====== %s: " % (time.asctime(time.gmtime()))
    MSG += "cmscp.py FINISHING"
    MSG += " (status %d)." % (CMSCP_EXIT_CODE)
    print(MSG)
    sys.exit(CMSCP_EXIT_CODE)

##==============================================================================
