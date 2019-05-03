
"""
CMSRunAnalysis.py - the runtime python portions to launch a CRAB3 / cmsRun job.
"""
from __future__ import print_function

import os
import re
import sys
import stat
import time
import json
import shutil
import socket
import pickle
import signal
import os.path
import logging
import commands
import traceback
from ast import literal_eval
from optparse import OptionParser, BadOptionError, AmbiguousOptionError

import DashboardAPI
import WMCore.Storage.SiteLocalConfig as SiteLocalConfig

# replicate here code from ServerUtilities.py to avoid importing CRABServer in jobs
# see there for more documentation. Ideally could move this to WMCore
class tempSetLogLevel():
    """
        a simple context manager to temporarely change logging level
        USAGE:
            with tempSetLogLevel(logger=myLogger,level=logging.ERROR):
               do stuff
    """
    import logging
    def __init__(self, logger=None, level=None):
        self.previousLogLevel = None
        self.newLogLevel = level
        self.logger = logger
    def __enter__(self):
        self.previousLogLevel = self.logger.getEffectiveLevel()
        self.logger.setLevel(self.newLogLevel)
    def __exit__(self,a,b,c):
        self.logger.setLevel(self.previousLogLevel)


logCMSSWSaved = False

def sighandler(signum):
    print('Job was killed with signal: %s' % signum)
    if not logCMSSWSaved:
        logCMSSW()
    sys.exit(50669)

signal.signal(signal.SIGTERM, sighandler)

EC_MissingArg  =        50113 #10 for ATLAS trf
EC_CMSMissingSoftware = 10034
EC_CMSRunWrapper =      10040
EC_MoveOutErr =         99999 #TODO define an error code
EC_ReportHandlingErr =  50115
EC_WGET =               99998 #TODO define an error code
EC_PsetHash           = 80453

def mintime():
    # enforce a minimum running time for failing jobs
    mymin = 20*60 # 20 minutes was used in the past
    mymin = 0 # atm we do not sleep on failing jobs. Keep the code just in case
    tottime = time.time()-starttime
    remaining = mymin - tottime
    if remaining > 0 and not "CRAB3_RUNTIME_DEBUG" in os.environ:
        print("==== Failure sleep STARTING at %s ====" % time.asctime(time.gmtime()))
        print("Sleeping for %d seconds due to failure." % remaining)
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(remaining)
        print("==== Failure sleep FINISHED at %s ====" % time.asctime(time.gmtime()))


class PassThroughOptionParser(OptionParser):
    """
    An unknown option pass-through implementation of OptionParser.

    When unknown arguments are encountered, bundle with largs and try again,
    until rargs is depleted.

    sys.exit(status) will still be called if a known argument is passed
    incorrectly (e.g. missing arguments or bad argument types, etc.)
    """
    def _process_args(self, largs, rargs, values):
        while rargs:
            try:
                OptionParser._process_args(self, largs, rargs, values)
            except (BadOptionError, AmbiguousOptionError) as e:
                largs.append(e.opt_str)


def parseAd():
    fd = open(os.environ['_CONDOR_JOB_AD'])
    jobad = {}
    for adline in fd.readlines():
        info = adline.split(" = ", 1)
        if len(info) != 2:
            continue
        if info[1].startswith('"'):
            val = info[1].strip()[1:-1]
        else:
            try:
                val = int(info[1].strip())
            except ValueError:
                continue
        jobad[info[0]] = val
    return jobad


def populateDashboardMonitorInfo(myad, params):
    params['MonitorID'] = myad['CRAB_ReqName']
    params['MonitorJobID'] = '{0}_https://glidein.cern.ch/{0}/{1}_{2}'.format(myad['CRAB_Id'], myad['CRAB_ReqName'].replace("_", ":"), myad['CRAB_Retry'])


def calcOverflowFlag(myad):
    overflowflag = 0
    if 'DESIRED_Sites' in myad and 'JOB_CMSSite' in myad:
        overflowflag = 1
        job_exec_site = myad['JOB_CMSSite']
        desired_sites = myad['DESIRED_Sites'].split(',')
        if job_exec_site in desired_sites:
            overflowflag = 0
    return overflowflag


def earlyDashboardMonitoring(myad):
    params = {
        'WNHostName': socket.getfqdn(),
        'SyncGridJobId': 'https://glidein.cern.ch/{0}/{1}'.format(myad['CRAB_Id'], myad['CRAB_ReqName'].replace("_", ":")),
        'SyncSite': myad['JOB_CMSSite'],
        'SyncCE': myad['Used_Gatekeeper'].split(":")[0].split(" ")[0],
        'OverflowFlag': calcOverflowFlag(myad),
    }
    populateDashboardMonitorInfo(myad, params)
    print("Dashboard early startup params: %s" % str(params))
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    return params['MonitorJobID']


def earlyDashboardFailure(myad):
    params = {
        'JobExitCode': 10043,
    }
    populateDashboardMonitorInfo(myad, params)
    print("Dashboard early startup params: %s" % str(params))
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)


def startDashboardMonitoring(myad):
    params = {
        'WNHostName': socket.getfqdn(),
        'ExeStart': 'cmsRun',
    }
    populateDashboardMonitorInfo(myad, params)
    print("Dashboard startup parameters: %s" % str(params))
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)


def addReportInfo(params, fjr):
    if 'exitCode' in fjr:
        params['JobExitCode'] = fjr['exitCode']
    if 'jobExitCode' in fjr:
        params['ExeExitCode'] = fjr['jobExitCode']
    if 'steps' not in fjr or 'cmsRun' not in fjr['steps']:
        return
    fjr = fjr['steps']['cmsRun']
    if 'performance' in fjr:
        # CPU Statistics
        if 'cpu' in fjr['performance']:
            if fjr['performance']['cpu'].get("TotalJobTime"):
                params['ExeTime'] = int(float(fjr['performance']['cpu']['TotalJobTime']))
            if fjr['performance']['cpu'].get("TotalJobCPU"):
                params['CrabUserCpuTime'] = float(fjr['performance']['cpu']['TotalJobCPU'])
            if params.get('ExeTime') and params.get("CrabUserCpuTime"):
                params['CrabCpuPercentage'] = params['CrabUserCpuTime'] / float(params['ExeTime'])
        # Storage Statistics
        if 'storage' in fjr['performance']:
            if fjr['performance']['storage'].get("readTotalMB"):
                params['CRABUserReadMB'] = float(fjr['performance']['storage']['readTotalMB'])
            if fjr['performance']['storage'].get("writeTotalMB"):
                params['CRABUserWriteMB'] = float(fjr['performance']['storage']['writeTotalMB'])
        # Memory Statistics
        if 'memory' in fjr['performance']:
            if fjr['performance']['memory'].get("PeakValueRss"):
                params['CRABUserPeakRss'] = float(fjr['performance']['memory']['PeakValueRss'])
    # Num Events Statistics
    params['NEventsProcessed'] = 0
    if 'input' in fjr and 'source' in fjr['input']:
        for info in fjr['input']['source']:
            if 'events' in info:
                params['NEventsProcessed'] += info['events']


def reportPopularity(monitorId, monitorJobId, myad, fjr):
    """
    Using the FJR and job ad, report popularity information to the dashboard.

    Implementation is based on https://github.com/dmwm/CRAB2/blob/63a7c046e8411ab26d4960ef2ae5bf48642024de/python/parseCrabFjr.py

    Note: Unlike CRAB2, we do not report the runs/lumis processed.
    """

    if 'CRAB_DataBlock' not in myad:
        print("Not sending data to popularity service because 'CRAB_DataBlock' is not in job ad.")
    sources = fjr.get('steps', {}).get('cmsRun', {}).get('input', {}).get('source', [])
    #skippedFiles = fjr.get('skippedFiles', [])
    fallbackFiles = fjr.get('fallbackFiles', [])
    if not sources:
        print("Not sending data to popularity service because no input sources found.")

    # Now, compute the strings about the input files.  Each input file has the following format in the report:
    #
    # %(name)s::%(report_type)d::%(file_type)s::%(access_type)s::%(counter)d
    #
    # Where each variable has the following values:
    #  - name: the portion of the LFN after the common prefix
    #  - report_type: 1 for a file successfully processed, 0 for a failed file with an error message, 2 otherwise
    #  - file_type: EDM or Unknown
    #  - access_type: Remote or Local
    #  - counter: monotonically increasing counter

    # First, pull the LFNs from the FJR.  This will be used to compute the basename.
    inputInfo = {}
    parentInputInfo = {}
    inputCtr = 0
    parentCtr = 0
    for source in sources:
        if 'lfn' not in source:
            print("Excluding source data from popularity report because no LFN is present. %s" % str(source))
        if source.get('input_source_class') == 'PoolSource':
            file_type = 'EDM'
        else:
            file_type = 'Unknown'
        if source['lfn'] in fallbackFiles:
            access_type = "Remote"
        else:
            access_type = "Local"

        if source.get("input_type", "primaryFiles") == "primaryFiles":
            inputCtr += 1
            # Note we hardcode 'Local' here.  In CRAB2, it was assumed any PFN with the string 'xrootd'
            # in it was a remote access; we feel this is no longer a safe assumption.  CMSSW actually
            # differentiates the fallback accesses.  See https://github.com/dmwm/WMCore/issues/5087
            inputInfo[source['lfn']] = [source['lfn'], file_type, access_type, inputCtr]
        else:
            parentCtr += 1
            parentInputInfo[source['lfn']] = [source['lfn'], file_type, access_type, parentCtr]
    baseName = os.path.dirname(os.path.commonprefix(inputInfo.keys()))
    parentBaseName = os.path.dirname(os.path.commonprefix(parentInputInfo.keys()))

    # Next, go through the sources again to properly record the unique portion of the filename
    # The dictionary is keyed on the unique portion of the filename after the common prefix.
    if baseName:
        for info in inputInfo.values():
            lfn = info[0].split(baseName, 1)[-1]
            info[0] = lfn
    if parentBaseName:
        for info in parentInputInfo.values():
            lfn = info[0].split(parentBaseName, 1)[-1]
            info[0] = lfn

    inputString = ';'.join(["%s::1::%s::%s::%d" % tuple(value) for value in inputInfo.values()])
    parentInputString = ';'.join(["%s::1::%s::%s::%d" % tuple(value) for value in parentInputInfo.values()])

    # Currently, CRAB3 drops the FJR information for failed files.  Hence, we don't currently do any special
    # reporting for these like CRAB2 did.  TODO: Revisit once https://github.com/dmwm/CRABServer/issues/4272
    # is fixed.

    report = {
        'inputBlocks': myad['CRAB_DataBlock'],
        'Basename': str(baseName),
        'BasenameParent': str(parentBaseName),
        'inputFiles': str(inputString),
        'parentFiles': str(parentInputString),
    }
    print("Dashboard popularity report: %s" % str(report))
    DashboardAPI.apmonSend(monitorId, monitorJobId, report)


def stopDashboardMonitoring(myad):
    params = {
        'ExeEnd': 'cmsRun',
        'NCores': myad['RequestCpus'],
    }
    populateDashboardMonitorInfo(myad, params)
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    del params['ExeEnd']
    fjr = {}
    try:
        fjr = json.load(open("jobReport.json"))
    except:
        print("WARNING: Unable to parse jobReport.json; Dashboard reporting will not be useful.\n", traceback.format_exc())
    try:
        addReportInfo(params, fjr)
    except:
        if 'ExeExitCode' not in params:
            params['ExeExitCode'] = 50115
        if 'JobExitCode' not in params:
            params['JobExitCode'] = params['ExeExitCode']
        print("ERROR: Unable to parse job info from FJR.\n", traceback.format_exc())
    print("Dashboard end parameters: %s" % str(params))
    try:
        reportPopularity(params['MonitorID'], params['MonitorJobID'], myad, fjr)
    except:
        print("ERROR: Failed to report popularity information to Dashboard.\n", traceback.format_exc())
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    DashboardAPI.apmonFree()


def logCMSSW():
    """ First it checks if cmsRun-stdout.log is available and if it
    is available, it will check if file is not too big and also will
    limit each line to maxLineLen. These logs will be returned back to
    schedd and we don`t want to take a lot of space on it. Full log files
    will be returned back to user SE, if he set saveLogs flag in crab config."""
    global logCMSSWSaved
    if logCMSSWSaved:
        return
    if not os.path.exists("cmsRun-stdout.log"):
        print("ERROR: Cannot dump CMSSW stdout; perhaps CMSSW never executed (e.g.: scriptExe was set)?")
        logCMSSWSaved = True
        open('logCMSSWSaved.txt', 'a').close()
        os.utime('logCMSSWSaved.txt', None)
        return

    outfile = "cmsRun-stdout.log"

    # check size of outfile
    keepAtStart = 1000
    keepAtEnd   = 3000
    maxLineLen = 3000
    maxLines    = keepAtStart + keepAtEnd
    numLines = sum(1 for line in open(outfile))

    print("======== CMSSW OUTPUT STARTING ========")
    print("NOTICE: lines longer than %s characters will be truncated" % maxLineLen)

    tooBig = numLines > maxLines
    if tooBig :
        print("WARNING: CMSSW output more then %d lines; truncating to first %d and last %d" % (maxLines, keepAtStart, keepAtEnd))
        print("Use 'crab getlog' to retrieve full output of this job from storage.")
        print("=======================================")
        with open(outfile) as fp:
            for nl, line in enumerate(fp):
                if nl < keepAtStart:
                    printCMSSWLine("== CMSSW: %s " % line, maxLineLen)
                if nl == keepAtStart+1:
                    print("== CMSSW: ")
                    print("== CMSSW: [...BIG SNIP...]")
                    print("== CMSSW: ")
                if numLines-nl <= keepAtEnd:
                    printCMSSWLine("== CMSSW: %s " % line, maxLineLen)
    else:
        for line in open(outfile):
            printCMSSWLine("== CMSSW: %s " % line, maxLineLen)

    print("======== CMSSW OUTPUT FINSHING ========")
    logCMSSWSaved = True
    open('logCMSSWSaved.txt', 'a').close()
    os.utime('logCMSSWSaved.txt', None)

def printCMSSWLine(line, lineLenLimit):
    """ Simple print auxiliary function that truncates lines"""
    print(line[:lineLenLimit].rstrip())

def handleException(exitAcronym, exitCode, exitMsg):
    #first save the traceback before it gets overwritten by other tracebacks (e.g.: wrong jobReport)
    formatted_tb = traceback.format_exc()

    report = {}
    try:
        if os.path.exists("jobReport.json"):
            report = json.load(open("jobReport.json"))
        else:
            print("WARNING: WMCore did not produce a jobReport.json; FJR will not be useful.")
    except:
        print("WARNING: Unable to parse WMCore's jobReport.json; FJR will not be useful.\n", traceback.format_exc())

    if report.get('steps', {}).get('cmsRun', {}).get('errors'):
        exitMsg += '\nCMSSW error message follows.\n'
        for err in report['steps']['cmsRun']['errors']:
            if 'exitCode' in err:
                try:
                    exitCode = int(exitCode)
                    fjrExitCode = int(err['exitCode'])
                    if (fjrExitCode % 256 == exitCode) and (fjrExitCode != exitCode):
                        print("NOTE: FJR has exit code %d and WMCore reports %d; preferring the FJR one." % (fjrExitCode, exitCode))
                        exitCode = fjrExitCode
                except ValueError:
                    pass
            exitMsg += err['type'] + '\n'
            exitMsg += err['details'] + '\n'

    report['exitAcronym'] = exitAcronym
    report['exitCode'] = exitCode
    report['exitMsg'] = exitMsg
    print("ERROR: Exceptional exit at %s (%s): %s" % (time.asctime(time.gmtime()), str(exitCode), str(exitMsg)))
    if not formatted_tb.startswith("None"):
        print("ERROR: Traceback follows:\n", formatted_tb)

    try:
        slCfg = SiteLocalConfig.loadSiteLocalConfig()
        report['executed_site'] = slCfg.siteName
        print("== Execution site for failed job from site-local-config.xml: %s" % slCfg.siteName)
    except:
        print("ERROR: Failed to record execution site name in the FJR from the site-local-config.xml")
        print(traceback.format_exc())

    with open('jobReport.json', 'w') as rf:
        json.dump(report, rf)
    if ad and not "CRAB3_RUNTIME_DEBUG" in os.environ:
        stopDashboardMonitoring(ad)


def parseArgs():
    parser = PassThroughOptionParser()
    parser.add_option('-a',
                      dest='archiveJob',
                      type='string')
    parser.add_option('-o',
                      dest='outFiles',
                      type='string')
    parser.add_option('-r',
                      dest='runDir',
                      type='string')
    parser.add_option('--inputFile',
                      dest='inputFile',
                      type='string')
    parser.add_option('--sourceURL',
                      dest='sourceURL',
                      type='string')
    parser.add_option('--jobNumber',
                      dest='jobNumber',
                      type='string')
    parser.add_option('--cmsswVersion',
                      dest='cmsswVersion',
                      type='string')
    parser.add_option('--scramArch',
                      dest='scramArch',
                      type='string')
    parser.add_option('--runAndLumis',
                      dest='runAndLumis',
                      type='string',
                      default=None)
    parser.add_option('--lheInputFiles',
                      dest='lheInputFiles',
                      type='string',
                      default=False)
    parser.add_option('--firstEvent',
                      dest='firstEvent',
                      type='string',
                      default=0)
    parser.add_option('--firstLumi',
                      dest='firstLumi',
                      type='string',
                      default=None)
    parser.add_option('--lastEvent',
                      dest='lastEvent',
                      type='string',
                      default=-1)
    parser.add_option('--firstRun',
                      dest='firstRun',
                      type='string',
                      default=None)
    parser.add_option('--seeding',
                      dest='seeding',
                      type='string',
                      default=None)
    parser.add_option('--userFiles',
                      dest='userFiles',
                      type='string')
    parser.add_option('--oneEventMode',
                      dest='oneEventMode',
                      default=0)
    parser.add_option('--scriptExe',
                      dest='scriptExe',
                      type='string')
    parser.add_option('--scriptArgs',
                      dest='scriptArgs',
                      type='string')
    parser.add_option('--eventsPerLumi',
                      dest='eventsPerLumi',
                      type='string',
                      default=None)
    parser.add_option('--maxRuntime',
                      dest='maxRuntime',
                      type='string',
                      default=None)

    (opts, args) = parser.parse_args(sys.argv[1:])

    try:
        print("==== Parameters Dump at %s ===" % time.asctime(time.gmtime()))
        print("archiveJob:    ", opts.archiveJob)
        print("runDir:        ", opts.runDir)
        print("sourceURL:     ", opts.sourceURL)
        print("jobNumber:     ", opts.jobNumber)
        print("cmsswVersion:  ", opts.cmsswVersion)
        print("scramArch:     ", opts.scramArch)
        print("inputFile      ", opts.inputFile)
        print("outFiles:      ", opts.outFiles)
        print("runAndLumis:   ", opts.runAndLumis)
        print("lheInputFiles: ", opts.lheInputFiles)
        print("firstEvent:    ", opts.firstEvent)
        print("firstLumi:     ", opts.firstLumi)
        print("eventsPerLumi: ", opts.eventsPerLumi)
        print("lastEvent:     ", opts.lastEvent)
        print("firstRun:      ", opts.firstRun)
        print("seeding:       ", opts.seeding)
        print("userFiles:     ", opts.userFiles)
        print("oneEventMode:  ", opts.oneEventMode)
        print("scriptExe:     ", opts.scriptExe)
        print("scriptArgs:    ", opts.scriptArgs)
        print("maxRuntime:    ", opts.maxRuntime)
        print("===================")
    except:
        name, value, traceBack = sys.exc_info()
        print('ERROR: missing parameters: %s - %s' % (name, value))
        handleException("FAILED", EC_MissingArg, 'CMSRunAnalysisERROR: missing parameters: %s - %s' % (name, value))
        mintime()
        sys.exit(EC_MissingArg)

    return opts


#TODO: MM I do not believe this is necessary at all
def prepSandbox(opts):
    print("==== Sandbox preparation STARTING at %s ====" % time.asctime(time.gmtime()))
    os.environ['WMAGENTJOBDIR'] = os.getcwd()
    if opts.archiveJob and not "CRAB3_RUNTIME_DEBUG" in os.environ:
        if os.path.exists(opts.archiveJob):
            print("Sandbox %s already exists, skipping" % opts.archiveJob)
        elif opts.sourceURL == 'LOCAL' and not os.path.exists(opts.archiveJob):
            print("ERROR: Requested for condor to transfer the tarball, but it didn't show up")
            handleException("FAILED", EC_WGET, 'CMSRunAnalysisERROR: could not get jobO files from panda server')
            sys.exit(EC_WGET)
        else:
            print("--- wget for jobO ---")
            output = commands.getoutput('wget -h')
            wgetCommand = 'wget'
            for line in output.split('\n'):
                if re.search('--no-check-certificate', line) != None:
                    wgetCommand = 'wget --no-check-certificate'
                    break
            com = '%s %s/cache/%s' % (wgetCommand, opts.sourceURL, opts.archiveJob)
            nTry = 3
            for iTry in range(nTry):
                print('Try : %s' % iTry)
                status, output = commands.getstatusoutput(com)
                print(output)
                if status == 0:
                    break
                if iTry+1 == nTry:
                    print("ERROR : cound not get jobO files from panda server")
                    handleException("FAILED", EC_WGET, 'CMSRunAnalysisERROR: could not get jobO files from panda server')
                    sys.exit(EC_WGET)
                time.sleep(30)
    #The user sandbox.tar.gz has to be unpacked no matter what (even in DEBUG mode)
    print(commands.getoutput('tar xfm %s' % opts.archiveJob))
    print("==== Sandbox preparation FINISHED at %s ====" % time.asctime(time.gmtime()))

    #move the pset in the right place
    print("==== WMCore filesystem preparation STARTING at %s ====" % time.asctime(time.gmtime()))
    destDir = 'WMTaskSpace/cmsRun'
    if os.path.isdir(destDir):
        shutil.rmtree(destDir)
    os.makedirs(destDir)
    os.rename('PSet.py', destDir + '/PSet.py')
    open('WMTaskSpace/__init__.py', 'w').close()
    open(destDir + '/__init__.py', 'w').close()
    #move the additional user files in the right place
    if opts.userFiles:
        for myfile in opts.userFiles.split(','):
            os.rename(myfile, destDir + '/' + myfile)
    print("==== WMCore filesystem preparation FINISHED at %s ====" % time.asctime(time.gmtime()))


def extractUserSandbox(archiveJob, cmsswVersion):
    os.chdir(cmsswVersion)
    print(commands.getoutput('tar xfm %s ' % os.path.join('..', archiveJob)))
    os.chdir('..')

def getProv(filename, scram):
    #fh = logging.FileHandler('edmProvDumpOutput.log')
    #with LoggingContext(logging.getLogger(), level=logging.DEBUG, handler=fh, close=True):
    with tempSetLogLevel(logger=logging.getLogger(), level=logging.ERROR):
        ret = scram("edmProvDump %s" % filename, runtimeDir=os.getcwd())
    if ret > 0:
        scramMsg = scram.diagnostic()
        msg = "FAILED (%s)\n" % EC_CMSRunWrapper
        msg += "Error getting pset hash from file.\n\tCommand:edmProvDump %s\n\tScram Diagnostic %s" % (filename, scramMsg)
        print(msg)
        mintime()
        sys.exit(EC_CMSRunWrapper)
    #with open("edmProvDumpOutput.log", "r") as fd:
    #    output = fd.read()
    output = scram.getStdout()
    return output


def executeScriptExe(opts, scram):
    #make scriptexe executable
    st = os.stat(opts.scriptExe)
    os.chmod(opts.scriptExe, st.st_mode | stat.S_IEXEC)

    command_ = ('python %s/TweakPSet.py --location=%s '+
                                                          '--inputFile=\'%s\' '+
                                                          '--runAndLumis=\'%s\' '+
                                                          '--firstEvent=%s '+
                                                          '--lastEvent=%s '+
                                                          '--firstLumi=%s '+
                                                          '--firstRun=%s '+
                                                          '--seeding=%s '+
                                                          '--lheInputFiles=%s '+
                                                          '--oneEventMode=%s ' +
                                                          '--eventsPerLumi=%s ' +
                                                          '--maxRuntime=%s') %\
                                             (os.getcwd(), os.getcwd(),
                                                           opts.inputFile,
                                                           opts.runAndLumis,
                                                           opts.firstEvent,
                                                           opts.lastEvent,
                                                           opts.firstLumi,
                                                           opts.firstRun,
                                                           opts.seeding,
                                                           opts.lheInputFiles,
                                                           opts.oneEventMode,
                                                           opts.eventsPerLumi,
                                                           opts.maxRuntime)
    fh = logging.FileHandler('scramOutput.log')
    #with LoggingContext(logging.getLogger(), level=logging.DEBUG, handler=fh, close=True):
    with tempSetLogLevel(logger=logging.getLogger(), level=logging.ERROR):
        ret = scram(command_, runtimeDir = os.getcwd())
    if ret > 0:
        msg = scram.diagnostic()
        handleException("FAILED", EC_CMSRunWrapper, 'Error executing TweakPSet.\n\tScram Env %s\n\tCommand: %s' % (msg, command_))
        mintime()
        sys.exit(EC_CMSRunWrapper)

    command_ = os.getcwd() + "/%s %s %s" % (opts.scriptExe, opts.jobNumber, " ".join(json.loads(opts.scriptArgs)))

    #fh = logging.FileHandler('cmsRun-stdout.log')
    #with LoggingContext(logging.getLogger(), level=logging.DEBUG, handler=fh, close=True):
    with tempSetLogLevel(logger=logging.getLogger(), level=logging.DEBUG):
        ret = scram(command_, runtimeDir = os.getcwd(), cleanEnv = False)
    if ret > 0:
        msg = scram.diagnostic()
        handleException("FAILED", EC_CMSRunWrapper, 'Error executing scriptExe.\n\tScram Env %s\n\tCommand: %s' % (msg, command_))
        mintime()
        sys.exit(EC_CMSRunWrapper)
    with open('cmsRun-stdout.log','w') as fh:
        fh.write(scram.getStdout())
    return ret


def executeCMSSWStack(opts, scram):

    def getOutputModules():
        pythonScript = "from PSetTweaks.WMTweak import makeTweak;"+\
                       "config = __import__(\"WMTaskSpace.cmsRun.PSet\", globals(), locals(), [\"process\"], -1);"+\
                       "tweakJson = makeTweak(config.process).jsondictionary();"+\
                       "print tweakJson[\"process\"][\"outputModules_\"]"
        with tempSetLogLevel(logger=logging.getLogger(), level=logging.ERROR):
            ret = scram("python -c '%s'" % pythonScript, runtimeDir=os.getcwd())
        if ret > 0:
            msg = scram.diagnostic()
            handleException("FAILED", EC_CMSRunWrapper, 'Error getting output modules from the pset.\n\tScram Env %s\n\tCommand:%s' % (msg, pythonScript))
            mintime()
            sys.exit(EC_CMSRunWrapper)
        output = literal_eval(scram.getStdout())
        return output

    cmssw = CMSSW()
    cmssw.stepName = "cmsRun"
    cmssw.step = WMStep(cmssw.stepName)
    CMSSWTemplate().install(cmssw.step)
    cmssw.task = makeWMTask(cmssw.stepName)
    cmssw.workload = newWorkload(cmssw.stepName)
    cmssw.step.application.setup.softwareEnvironment = ''
    cmssw.step.application.setup.scramArch = opts.scramArch
    cmssw.step.application.setup.cmsswVersion = opts.cmsswVersion
    cmssw.step.application.configuration.section_("arguments")
    cmssw.step.application.configuration.arguments.globalTag = ""
    for output in getOutputModules():
        cmssw.step.output.modules.section_(output)
        getattr(cmssw.step.output.modules, output).primaryDataset   = ''
        getattr(cmssw.step.output.modules, output).processedDataset = ''
        getattr(cmssw.step.output.modules, output).dataTier         = ''
    #cmssw.step.application.command.arguments = '' #TODO
    cmssw.step.user.inputSandboxes = [opts.archiveJob]
    cmssw.step.user.userFiles = opts.userFiles or ''
    #Setting the following job attribute is required because in the CMSSW executor there is a call to analysisFileLFN to set up some attributes for TFiles.
    #Same for lfnbase. We actually don't use these information so I am setting these to dummy values. Next: fix and use this lfn or drop WMCore runtime..
    cmssw.job = {'counter' : 0, 'workflow' : 'unused'}
    cmssw.step.user.lfnBase = '/store/temp/user/'
    cmssw.step.section_("builder")
    cmssw.step.builder.workingDir = os.getcwd()
    cmssw.step.runtime.invokeCommand = 'python'
    cmssw.step.runtime.scramPreDir = os.getcwd()
    cmssw.step.runtime.preScripts = []


    cmssw.step.runtime.scramPreScripts = [('%s/TweakPSet.py --location=%s '+
                                                          '--inputFile=\'%s\' '+
                                                          '--runAndLumis=\'%s\' '+
                                                          '--firstEvent=%s '+
                                                          '--lastEvent=%s '+
                                                          '--firstLumi=%s '+
                                                          '--firstRun=%s '+
                                                          '--seeding=%s '+
                                                          '--lheInputFiles=%s '+
                                                          '--oneEventMode=%s ' +
                                                          '--eventsPerLumi=%s ' +
                                                          '--maxRuntime=%s') %
                                             (os.getcwd(), os.getcwd(),
                                                           opts.inputFile,
                                                           opts.runAndLumis,
                                                           opts.firstEvent,
                                                           opts.lastEvent,
                                                           opts.firstLumi,
                                                           opts.firstRun,
                                                           opts.seeding,
                                                           opts.lheInputFiles,
                                                           opts.oneEventMode,
                                                           opts.eventsPerLumi,
                                                           opts.maxRuntime)]
    cmssw.step.section_("execution") #exitStatus of cmsRun is set here
    cmssw.report = Report("cmsRun") #report is loaded and put here
    cmssw.execute()
    return cmssw


def AddChecksums(report):
    if 'steps' not in report:
        return
    if 'cmsRun' not in report['steps']:
        return
    if 'output' not in report['steps']['cmsRun']:
        return

    for outputMod in report['steps']['cmsRun']['output'].values():
        for fileInfo in outputMod:
            if 'checksums' in fileInfo:
                continue
            if 'pfn' not in fileInfo:
                if 'fileName' in fileInfo:
                    fileInfo['pfn'] = fileInfo['fileName']
                else:
                    continue           
            fileInfo['size'] = os.stat(fileInfo['pfn']).st_size
            print("==== Checksum computation STARTING at %s ====" % time.asctime(time.gmtime()))
            (adler32, cksum) = calculateChecksums(fileInfo['pfn'])
            print("==== Checksum FINISHED at %s ====" % time.asctime(time.gmtime()))
            print("== FileName: %s  -  FileAdler32: %s  - FileSize: %.3f MBytes" % \
                 (fileInfo['pfn'], adler32, float(fileInfo['size'])/(1024*1024)) ) 
            fileInfo['checksums'] = {'adler32': adler32, 'cksum': cksum}


def AddPsetHash(report, scram):
    """
    Example relevant output from edmProvDump:

    Processing History:
  LHC '' '"CMSSW_5_2_7_ONLINE"' [1]  (3d65e8b9ad872f46fe020c68d75d79ab)
    HLT '' '"CMSSW_5_2_7_ONLINE"' [1]  (5b994f2a1c1c3f9dcafe27bcfa8f085f)
      RECO '' '"CMSSW_5_3_7_patch6"' [1]  (c186b1d0b14a7353cd3d6e46639ccbbc)
        PAT '' '"CMSSW_5_3_11"' [1]  (8daee065cbf6da0cee1c034eb3f8af28)
    HLT '' '"CMSSW_5_2_7_ONLINE"' [2]  (d019fbf5930638478d250e8c8d5257cc)
      RECO '' '"CMSSW_5_3_7_patch6"' [1]  (c186b1d0b14a7353cd3d6e46639ccbbc)
  LHC '' '"CMSSW_5_2_7_onlpatch3_ONLINE"' [2]  (3d65e8b9ad872f46fe020c68d75d79ab)
    HLT '' '"CMSSW_5_2_7_onlpatch3_ONLINE"' [1]  (cd9d2672f701d636e7873de078595fcf)
      RECO '' '"CMSSW_5_3_7_patch6"' [1]  (c186b1d0b14a7353cd3d6e46639ccbbc)

    We want to take the line with the deepest prefix (PAT line above)
    """

    if 'steps' not in report:
        return
    if 'cmsRun' not in report['steps']:
        return
    if 'output' not in report['steps']['cmsRun']:
        return

    pset_re = re.compile("(\s+).*\(([a-f0-9]{32,32})\)$")
    processing_history_re = re.compile("^Processing History:$")
    for outputMod in report['steps']['cmsRun']['output'].values():
        for fileInfo in outputMod:
            if not (fileInfo.get('output_module_class', '') == 'PoolOutputModule' or \
                    fileInfo.get('ouput_module_class',  '') == 'PoolOutputModule'):
                continue
            if 'pfn' not in fileInfo:
                continue
            print("== Adding PSet Hash for filename: %s" % fileInfo['pfn'])
            if not os.path.exists(fileInfo['pfn']):
                print("== Output file missing!")
                continue
            m = re.match(r"^[A-Za-z0-9\-._]+$", fileInfo['pfn'])
            if not m:
                print("== EDM output filename (%s) must match RE ^[A-Za-z0-9\\-._]+$" % fileInfo['pfn'])
                continue
            print("==== PSet Hash computation STARTING at %s ====" % time.asctime(time.gmtime()))
            lines = getProv(fileInfo['pfn'], scram)
            found_history = False
            matches = {}
            for line in lines.splitlines():
                if not found_history:
                    if processing_history_re.match(line):
                        found_history = True
                    continue
                m = pset_re.match(line)
                if m:
                    # Note we want the deepest entry in the hierarchy
                    depth, pset_hash = m.groups()
                    depth = len(depth)
                    matches[depth] = pset_hash
                else:
                    break
            print("==== PSet Hash computation FINISHED at %s ====" % time.asctime(time.gmtime()))
            if matches:
                max_depth = max(matches.keys())
                pset_hash = matches[max_depth]
                print("== edmProvDump pset hash %s" % pset_hash)
                fileInfo['pset_hash'] = pset_hash
            else:
                print("ERROR: PSet Hash missing from edmProvDump output.  Full dump below.")
                print(lines)
                raise Exception("PSet hash missing from edmProvDump output.")


def StripReport(report):
    if 'steps' not in report:
        return
    if 'cmsRun' not in report['steps']:
        return
    if 'output' not in report['steps']['cmsRun']:
        return
    for outputMod in report['steps']['cmsRun']['output'].values():
        for fileInfo in outputMod:
            ## Stripping 'file:' from each output module pfn value is needed so that cmscp
            ## and PostJob are able to find the output file in the jobReport.json.
            if 'pfn' in fileInfo:
                fileInfo['pfn'] = re.sub(r'^file:', '', fileInfo['pfn'])
            ## Stripping 'file:' from each output module fileName value is needed because the
            ## function AddChecksums() uses fileName as the pfn if the last is not defined.
            if 'fileName' in fileInfo:
                fileInfo['fileName'] = re.sub(r'^file:', '', fileInfo['fileName'])


if __name__ == "__main__":
    print("==== CMSRunAnalysis.py STARTING at %s ====" % time.asctime(time.gmtime()))
    print("Local time : %s" % time.ctime())
    starttime = time.time()

    ad = {}
    try:
        ad = parseAd()
    except:
        print("==== FAILURE WHEN PARSING HTCONDOR CLASSAD AT %s ====" % time.asctime(time.gmtime()))
        print(traceback.format_exc())
        ad = {}
    if ad and not "CRAB3_RUNTIME_DEBUG" in os.environ:
        dashboardId = earlyDashboardMonitoring(ad)
        if dashboardId:
            os.environ['DashboardJobId'] = dashboardId

    #WMCore import here
    # Note that we may fail in the imports - hence we try to report to Dashboard first
    try:
        options = parseArgs()
        prepSandbox(options)
        from WMCore.WMRuntime.Bootstrap import setupLogging
        from WMCore.FwkJobReport.Report import Report
        from WMCore.FwkJobReport.Report import FwkJobReportException
        from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
        from Utils.FileTools import calculateChecksums
        from WMCore.WMSpec.Steps.Executors.CMSSW import CMSSW
        from WMCore.WMSpec.WMStep import WMStep
        from WMCore.WMSpec.WMTask import makeWMTask
        from WMCore.WMSpec.WMWorkload import newWorkload
        from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSW as CMSSWTemplate
        from WMCore.WMRuntime.Tools.Scram import Scram
    except:
        # We may not even be able to create a FJR at this point.  Record
        # error and exit.
        if ad and not "CRAB3_RUNTIME_DEBUG" in os.environ:
            earlyDashboardFailure(ad)
        print("==== FAILURE WHEN LOADING WMCORE AT %s ====" % time.asctime(time.gmtime()))
        print(traceback.format_exc())
        mintime()
        sys.exit(10043)

    # At this point, all our dependent libraries have been loaded; it's quite
    # unlikely python will see a segfault.  Drop a marker file in the working
    # directory; if we encounter a python segfault, the wrapper will look to see if
    # this file exists and report to Dashboard accordingly.
    with open("wmcore_initialized", "w") as mf:
        mf.write("wmcore initialized.\n")

    try:
        setupLogging('.')

        # Also add stdout to the logging
        logHandler = logging.StreamHandler(sys.stdout)
        logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
        logging.Formatter.converter = time.gmtime
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)

        if ad and not "CRAB3_RUNTIME_DEBUG" in os.environ:
            startDashboardMonitoring(ad)
        print("==== CMSSW Stack Execution STARTING at %s ====" % time.asctime(time.gmtime()))
        scr = Scram(
            version = options.cmsswVersion,
            directory = os.getcwd(),
            architecture = options.scramArch,
            )

        print("==== SCRAM Obj CREATED at %s ====" % time.asctime(time.gmtime()))
        if scr.project() or scr.runtime(): #if any of the two commands fail...
            dgn = scr.diagnostic()
            handleException("FAILED", EC_CMSMissingSoftware, 'Error setting CMSSW environment: %s' % dgn)
            mintime()
            sys.exit(EC_CMSMissingSoftware)

        extractUserSandbox(options.archiveJob, options.cmsswVersion)

        try:
            jobExitCode = None
            if options.scriptExe=='None':
                print("==== CMSSW JOB Execution started at %s ====" % time.asctime(time.gmtime()))
                cmsswSt = executeCMSSWStack(options, scr)
                jobExitCode = cmsswSt.step.execution.exitStatus
            else:
                print("==== ScriptEXE Execution started at %s ====" % time.asctime(time.gmtime()))
                jobExitCode = executeScriptExe(options, scr)
        except:
            print("==== CMSSW Stack Execution FAILED at %s ====" % time.asctime(time.gmtime()))
            logCMSSW()
            raise
        print("Job exit code: %s" % str(jobExitCode))
        print("==== CMSSW Stack Execution FINISHED at %s ====" % time.asctime(time.gmtime()))
        logCMSSW()
    except WMExecutionFailure as WMex:
        print("ERROR: Caught WMExecutionFailure - code = %s - name = %s - detail = %s" % (WMex.code, WMex.name, WMex.detail))
        exmsg = WMex.name

        # Try to recover what we can from the FJR.  handleException will use this if possible.
        if os.path.exists('FrameworkJobReport.xml'):
            try:
                rep = Report("cmsRun")
                rep.parse('FrameworkJobReport.xml', "cmsRun")
                try:
                    jobExitCode = rep.getExitCode()
                except:
                    jobExitCode = WMex.code
                rep = rep.__to_json__(None)
                #save the virgin WMArchive report
                with open('WMArchiveReport.json', 'w') as of:
                    json.dump(rep, of)
                StripReport(rep)
                rep['jobExitCode'] = jobExitCode
                with open('jobReport.json', 'w') as of:
                    json.dump(rep, of)
            except:
                print("WARNING: Failure when trying to parse FJR XML after job failure.")

        handleException("FAILED", WMex.code, exmsg)
        mintime()
        sys.exit(WMex.code)
    except Exception as ex:
        #print "jobExitCode = %s" % jobExitCode
        handleException("FAILED", EC_CMSRunWrapper, "failed to generate cmsRun cfg file at runtime")
        mintime()
        sys.exit(EC_CMSRunWrapper)

    #Create the report file
    try:
        print("==== Report file creation STARTING at %s ====" % time.asctime(time.gmtime()))
        rep = Report("cmsRun")
        rep.parse('FrameworkJobReport.xml', "cmsRun")
        jobExitCode = rep.getExitCode()
        rep = rep.__to_json__(None)
        with open('WMArchiveReport.json', 'w') as of:
            json.dump(rep, of)
        StripReport(rep)
        # Record the payload process's exit code separately; that way, we can distinguish
        # cmsRun failures from stageout failures.  The initial use case of this is to
        # allow us to use a different LFN on job failure.
        rep['jobExitCode'] = jobExitCode
        AddChecksums(rep)
        if not 'CRAB3_RUNTIME_DEBUG' in os.environ:
            try:
                AddPsetHash(rep, scr)
            except Exception as ex:
                exmsg = "Unable to compute pset hash for job output. Got exception:"
                exmsg += "\n" + str(ex) + "\n"
                handleException("FAILED", EC_PsetHash, exmsg)
                mintime()
                sys.exit(EC_PsetHash)
        if jobExitCode: #TODO check exitcode from fwjr
            rep['exitAcronym'] = "FAILED"
            rep['exitCode'] = jobExitCode
            rep['exitMsg'] = "Error while running CMSSW:\n"
            for error in rep['steps']['cmsRun']['errors']:
                rep['exitMsg'] += error['type'] + '\n'
                rep['exitMsg'] += error['details'] + '\n'
        else:
            rep['exitAcronym'] = "OK"
            rep['exitCode'] = 0
            rep['exitMsg'] = "OK"

        slc = SiteLocalConfig.loadSiteLocalConfig()
        rep['executed_site'] = slc.siteName
        if 'phedex-node' in slc.localStageOut:
            rep['phedex_node'] = slc.localStageOut['phedex-node']
        print("== Execution site from site-local-config.xml: %s" % slc.siteName)
        with open('jobReport.json', 'w') as of:
            json.dump(rep, of)
        with open('jobReportExtract.pickle', 'w') as of:
            pickle.dump(rep, of)
        if ad and not "CRAB3_RUNTIME_DEBUG" in os.environ:
            stopDashboardMonitoring(ad)
        print("==== Report file creation FINISHED at %s ====" % time.asctime(time.gmtime()))
    except FwkJobReportException as FJRex:
        extype = "BadFWJRXML"
        handleException("FAILED", EC_ReportHandlingErr, extype)
        mintime()
        sys.exit(EC_ReportHandlingErr)
    except Exception as ex:
        extype = "Exception while handling the job report."
        handleException("FAILED", EC_ReportHandlingErr, extype)
        mintime()
        sys.exit(EC_ReportHandlingErr)

    # rename output files. Doing this after checksums otherwise outfile is not found.
    if jobExitCode == 0:
        try:
            oldName = 'UNKNOWN'
            newName = 'UNKNOWN'
            for oldName, newName in literal_eval(options.outFiles).iteritems():
                os.rename(oldName, newName)
        except Exception as ex:
            handleException("FAILED", EC_MoveOutErr, "Exception while moving file %s to %s." %(oldName, newName))
            mintime()
            sys.exit(EC_MoveOutErr)
    else:
        mintime()

    print("==== CMSRunAnalysis.py FINISHED at %s ====" % time.asctime(time.gmtime()))
    print("Local time : %s" % time.ctime())
    sys.exit(jobExitCode)
