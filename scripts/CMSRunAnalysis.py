
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
import pickle
import signal
import os.path
import logging
import subprocess
import traceback
from ast import literal_eval
from optparse import OptionParser, BadOptionError, AmbiguousOptionError

import WMCore.Storage.SiteLocalConfig as SiteLocalConfig
from TweakPSet import prepareTweakingScript

# replicate here code from ServerUtilities.py to avoid importing CRABServer in jobs
# see there for more documentation. Ideally could move this to WMCore
class tempSetLogLevel():
    """
        a simple context manager to temporarely change logging level
        USAGE:
            with tempSetLogLevel(logger=myLogger,level=logging.ERROR):
               do stuff
    """
    #import logging  # already imported globally in this file
    def __init__(self, logger=None, level=None):
        self.previousLogLevel = None
        self.newLogLevel = level
        self.logger = logger
    def __enter__(self):
        self.previousLogLevel = self.logger.getEffectiveLevel()
        self.logger.setLevel(self.newLogLevel)
    def __exit__(self, a, b, c):
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

def calcOverflowFlag(myad):
    overflowflag = 0
    if 'DESIRED_Sites' in myad and 'JOB_CMSSite' in myad:
        overflowflag = 1
        job_exec_site = myad['JOB_CMSSite']
        desired_sites = myad['DESIRED_Sites'].split(',')
        if job_exec_site in desired_sites:
            overflowflag = 0
    return overflowflag

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

def logCMSSW():
    """ First it checks if cmsRun-stdout.log is available and if it
    is available, it will check if file is not too big and also will
    limit each line to maxLineLen. These logs will be returned back to
    schedd and we don`t want to take a lot of space on it. Full log files
    will be returned back to user SE, if he set saveLogs flag in crab config."""
    global logCMSSWSaved  # pylint: disable=global-statement
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
    maxLineLen  = 3000
    maxLines    = keepAtStart + keepAtEnd
    numLines = sum(1 for line in open(outfile))

    print("======== CMSSW OUTPUT STARTING ========")
    print("NOTICE: lines longer than %s characters will be truncated" % maxLineLen)

    tooBig = numLines > maxLines
    prefix = "== CMSSW: "
    if tooBig:
        print("WARNING: output more than %d lines; truncating to first %d and last %d" % (maxLines, keepAtStart, keepAtEnd))
        print("Use 'crab getlog' to retrieve full output of this job from storage.")
        print("=======================================")
        with open(outfile) as fp:
            for nl, line in enumerate(fp):
                if nl < keepAtStart:
                    printCMSSWLine("== CMSSW: %s " % line, maxLineLen)
                if nl == keepAtStart + 1:
                    print(prefix)
                    print(prefix + " [...BIG SNIP...]")
                    print(prefix)
                if numLines - nl <= keepAtEnd:
                    printCMSSWLine("%s" % prefix+line, maxLineLen)
    else:
        for line in open(outfile):
            printCMSSWLine("%s" % prefix+line, maxLineLen)

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
    except Exception:
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

    # check size of message string passed by caller
    maxChars = 10 * 1000
    if len(exitMsg) > maxChars:
        exitMsg = exitMsg[0:maxChars] + " + ... message truncated at 10k chars"
    report['exitMsg'] = exitMsg
    print("ERROR: Exceptional exit at %s (%s): %s" % (time.asctime(time.gmtime()), str(exitCode), str(exitMsg)))
    if not formatted_tb.startswith("None"):
        print("ERROR: Traceback follows:\n", formatted_tb)

    try:
        sLCfg = SiteLocalConfig.loadSiteLocalConfig()
        report['executed_site'] = sLCfg.siteName
        print("== Execution site for failed job from site-local-config.xml: %s" % sLCfg.siteName)
    except Exception:
        print("ERROR: Failed to record execution site name in the FJR from the site-local-config.xml")
        print(traceback.format_exc())

    with open('jobReport.json', 'w') as rf:
        json.dump(report, rf)
    with open('jobReport.exitCode.txt', 'w') as rf:
        rf.write(str(report['exitCode']))

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
                      default='False')
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

    (opts, _) = parser.parse_args(sys.argv[1:])

    # sanitize input options
    # Note about options: default value is None for all, but if, like it usually happens,
    # CRAB code calls this with inputs like --seeding=None
    # the option variable is set to the string 'None', not to the python type None
    # let's make life easier by replacing 'None' with None.
    for name,value in vars(opts).items():
        if value == 'None':
            setattr(opts, name, None)

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
    except Exception:
        name, value, _ = sys.exc_info()
        print('ERROR: missing parameters: %s - %s' % (name, value))
        handleException("FAILED", EC_MissingArg, 'CMSRunAnalysisERROR: missing parameters: %s - %s' % (name, value))
        mintime()
        sys.exit(EC_MissingArg)

    return opts

def prepSandbox(opts):
    print("==== Sandbox untarring in job's top dir STARTING at %s ====" % time.asctime(time.gmtime()))

    #The user sandbox.tar.gz has to be unpacked no matter what (even in DEBUG mode)
    print(f"expanding {opts.archiveJob} in {os.getcwd()}")
    print(subprocess.getoutput('tar xfm %s' % opts.archiveJob))
    # if the sandbox contains tar files, expand them
    files = subprocess.getoutput(f"tar tf {opts.archiveJob}").split('\n')
    for file in files:
        if ('.tar.' in file) or file.endswith('.tar') or\
                file.endswith('.tgz') or file.endswith('.tbz'):
            print(f"expanding {file} in {os.getcwd()}")
            print(subprocess.getoutput(f"tar xfm {file}"))

    print("==== Sandbox untarring FINISHED at %s ====" % time.asctime(time.gmtime()))

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
    # the user sandbox contains the user scram directory files and thus
    # is unpacked in the local CMSSW_X_Y_X dir, but the cmsRun command
    # will be executed from the job working directory, so we move "up"
    # the PSet which is also in the user sandbox
    os.chdir(cmsswVersion)
    print(subprocess.getoutput('tar xfm %s ' % os.path.join('..', archiveJob)))
    os.rename('PSet.py', '../PSet.py')
    os.rename('PSet.pkl', '../PSet.pkl')
    os.chdir('..')

def getProv(filename, scram):
    with tempSetLogLevel(logger=logging.getLogger(), level=logging.ERROR):
        ret = scram("edmProvDump %s" % filename, runtimeDir=os.getcwd())
    if ret > 0:
        scramMsg = scram.diagnostic()
        msg = "FAILED (%s)\n" % EC_CMSRunWrapper
        msg += "Error getting pset hash from file.\n\tCommand:edmProvDump %s\n\tScram Diagnostic %s" % (filename, scramMsg)
        print(msg)
        mintime()
        sys.exit(EC_CMSRunWrapper)
    output = scram.getStdout()
    return output

def executeUserApplication(command, scram, cleanEnv=True):
    """
    cmsRun failures will appear in FJR but do not raise exceptions
    exception can only be raised by unexpected failures of the Scram wrapper itself
    Scram() never raises and returns the exit code from executing 'command'
    """
    with tempSetLogLevel(logger=logging.getLogger(), level=logging.DEBUG):
        ret = scram(command, runtimeDir=os.getcwd(), cleanEnv=cleanEnv)
        dest = shutil.move('cmsRun-stdout.log.tmp', 'cmsRun-stdout.log')
        logging.debug("cmssw stdout moved to cmsRun-stdout.log: %s", dest)
    if ret > 0:
        with open('cmsRun-stdout.log', 'a') as fh:
            fh.write(scram.diagnostic())
        print("Error executing application in CMSSW environment.\n\tSee stdout log")
    return ret

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
                 (fileInfo['pfn'], adler32, float(fileInfo['size'])/(1024*1024)))
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
            if not (fileInfo.get('output_module_class', '') == 'PoolOutputModule'):
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
    except Exception:
        print("==== FAILURE WHEN PARSING HTCONDOR CLASSAD AT %s ====" % time.asctime(time.gmtime()))
        print(traceback.format_exc())
        ad = {}

    #WMCore import here
    # Note that we may fail in the imports
    try:
        options = parseArgs()
        prepSandbox(options)
        from WMCore.WMRuntime.Bootstrap import setupLogging
        from WMCore.FwkJobReport.Report import Report
        from WMCore.FwkJobReport.Report import FwkJobReportException
        from Utils.FileTools import calculateChecksums
        from WMCore.WMRuntime.Tools.Scram import Scram
    except Exception:
        # We may not even be able to create a FJR at this point.  Record
        # error and exit.
        print("==== FAILURE WHEN LOADING WMCORE AT %s ====" % time.asctime(time.gmtime()))
        print(traceback.format_exc())
        mintime()
        sys.exit(10043)

    # At this point, all our dependent libraries have been loaded; it's quite
    # unlikely python will see a segfault.  Drop a marker file in the working
    # directory; if we encounter a python segfault, the wrapper will look to see if
    # this file exists and report accordingly.
    with open("wmcore_initialized", "w") as mf:
        mf.write("wmcore initialized.\n")

    try:
        setupLogging('.')

        print("==== CMSSW Stack Execution STARTING at %s ====" % time.asctime(time.gmtime()))
        scram = Scram(
            version=options.cmsswVersion,
            directory=os.getcwd(),
            architecture=options.scramArch,
            )

        print("==== SCRAM Obj CREATED at %s ====" % time.asctime(time.gmtime()))
        if scram.project() or scram.runtime(): #if any of the two commands fail...
            dgn = scram.diagnostic()
            handleException("FAILED", EC_CMSMissingSoftware, 'Error setting CMSSW environment: %s' % dgn)
            mintime()
            sys.exit(EC_CMSMissingSoftware)
        print("==== SCRAM Obj INITIALIZED at %s ====" % time.asctime(time.gmtime()))

        print("==== Extract user sandbox in CMSSW directory ====")
        extractUserSandbox(options.archiveJob, options.cmsswVersion)

        # tweaking of the PSet is needed both for CMSSWStack and ScriptEXE
        print("==== Tweak PSet at %s ====" % time.asctime(time.gmtime()))
        tweakingScriptName = 'tweakThePset.sh'
        prepareTweakingScript(options, tweakingScriptName)
        command = 'sh %s' % tweakingScriptName
        print('Executing %s in Scram env' % command)
        with tempSetLogLevel(logger=logging.getLogger(), level=logging.ERROR):
            ret = scram(command, runtimeDir=os.getcwd())
        if ret > 0:
            msg = 'Error executing %s\n\tScram Diagnostic %s' % (tweakingScriptName, scram.diagnostic())
            handleException("FAILED", EC_CMSRunWrapper, msg)
            mintime()
            sys.exit(EC_CMSRunWrapper)
        # debugging help in initial development: print command output in any case
        print("%s output:\%s" % (tweakingScriptName, scram.diagnostic()))

        print("==== Tweak PSet Done at %s ====" % time.asctime(time.gmtime()))

        jobExitCode = None
        applicationName = 'CMSSW JOB' if not options.scriptExe else 'ScriptEXE'
        print("==== %s Execution started at %s ====" % (applicationName, time.asctime(time.gmtime())))
        cmd = "stdbuf -oL -eL "
        if not options.scriptExe :
            cmd += 'cmsRun -p PSet.py -j FrameworkJobReport.xml'
        else:
            # make sure scriptexe is executable
            st = os.stat(options.scriptExe)
            os.chmod(options.scriptExe, st.st_mode | stat.S_IEXEC)
            cmd += os.getcwd() + "/%s %s %s" %\
                  (options.scriptExe, options.jobNumber, " ".join(json.loads(options.scriptArgs)))
        cmd += " > cmsRun-stdout.log.tmp 2>&1"
        applicationExitCode = executeUserApplication(cmd, scram, cleanEnv=False)
        if applicationExitCode:
            print("==== Execution FAILED at %s ====" % time.asctime(time.gmtime()))
        print("==== %s Execution completed at %s ====" % (applicationName, time.asctime(time.gmtime())))
        print("Application exit code: %s" % str(applicationExitCode))
        print("==== Execution FINISHED at %s ====" % time.asctime(time.gmtime()))
        logCMSSW()
    except Exception as ex:
        print("ERROR: Caught Wrapper ExecutionFailure - detail =\n%s" % str(ex))
        jobExitCode = EC_CMSRunWrapper
        exmsg = str(ex)

        # Try to recover what we can from the FJR.  handleException will use this if possible.
        if os.path.exists('FrameworkJobReport.xml'):
            try:
                # sanitize FJR in case non-ascii chars have been captured in error messages
                # e.g. from xroot https://github.com/dmwm/CRABServer/issues/6640#issuecomment-909362639
                print("Sanitize FJR")
                cmd = 'cat -v FrameworkJobReport.xml > sane; mv sane FrameworkJobReport.xml'
                print(subprocess.getoutput(cmd))
                # parse FJR
                rep = Report("cmsRun")
                rep.parse('FrameworkJobReport.xml', "cmsRun")
                try:
                    jobExitCode = rep.getExitCode()
                except Exception:
                    jobExitCode = EC_CMSRunWrapper
                rep = rep.__to_json__(None)
                #save the virgin WMArchive report
                with open('WMArchiveReport.json', 'w') as of:
                    json.dump(rep, of)
                StripReport(rep)
                rep['jobExitCode'] = jobExitCode
                with open('jobReport.json', 'w') as of:
                    json.dump(rep, of)
                with open('jobReport.exitCode.txt', 'w') as rf:
                    rf.write(str(rep['jobExitCode']))
            except Exception:
                print("WARNING: Failure when trying to parse FJR XML after job failure.")

        handleException("FAILED", EC_CMSRunWrapper, exmsg)
        mintime()
        sys.exit(EC_CMSRunWrapper)

    #Create the report file
    try:
        print("==== Report file creation STARTING at %s ====" % time.asctime(time.gmtime()))
        # sanitize FJR in case non-ascii chars have been captured in error messages
        # e.g. from xroot https://github.com/dmwm/CRABServer/issues/6640#issuecomment-909362639
        print("Sanitize FJR")
        cmd = 'cat -v FrameworkJobReport.xml > sane; mv sane FrameworkJobReport.xml'
        print(subprocess.getoutput(cmd))
        # parse FJR
        rep = Report("cmsRun")
        rep.parse('FrameworkJobReport.xml', "cmsRun")
        jobExitCode = rep.getExitCode()
        print("Job Exit Code from FrameworkJobReport.xml: %s " % jobExitCode)
        rep = rep.__to_json__(None)
        with open('WMArchiveReport.json', 'w') as of:
            json.dump(rep, of)
        StripReport(rep)
        # Record the payload process's exit code separately; that way, we can distinguish
        # cmsRun failures from stageout failures.  The initial use case of this is to
        # allow us to use a different LFN on job failure.
        if jobExitCode == 0 and applicationExitCode > 0:
            # We need to consider applicationExitCode as well, not only the exitcode from FWJR.xml
            # ref: https://github.com/dmwm/CRABServer/issues/7571
            # A brief recap:
            # - the FWJR.xml does not contain an exit code if the application completed successfully
            # - getExitCode() will default to 0 if it can not find an exit code
            #   in a syntactically correct valid FWJR.
            # - For CMSSW versions earlier than 12_6, when the framework fails abnormally 
            #   it leaves behind an invalid FWJR.xml. 
            # - For any CMSSW version 12_6 and higher, cmsRun always creates a syntactically correct
            #   FWJR.xml (possibly with empty content) also when it exits with non-zero exit code.
            jobExitCode = applicationExitCode
            print("The application failed with exit code %s" % applicationExitCode)
            print("but WMCore.FwkJobReport.Report:getExitCode() returned 0 from FWJR.xml")
            print("This job will be marked as failed.")
            print("In order to help with debugging, we print the content of FrameworkJobReport.xml")
            print("== Start FrameworkJobReport.xml ==")
            with open('FrameworkJobReport.xml', 'r') as fwjr:
                for line in fwjr.readlines():
                    print("== FWJR: %s" % line, end="")
            print("== End FrameworkJobReport.xml ==")
            # we do not need the else statement:
            # if jobExitCode != 0: then the FWJR.xml has better info than the unix exit code,
            #                    : we can simply ignore applicationExitCode
            # if jobExitCode == 0 and applicationExitCode == 0: then just keep jobExitCode
        rep['jobExitCode'] = jobExitCode
        print("==== Job Exit Code from FrameworkJobReport.xml and Application exit code: %s ====" % jobExitCode)
        if not jobExitCode:
            # only if application succeeded compute output stats
            AddChecksums(rep)
            try:
                AddPsetHash(rep, scram)
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

        slCfg = SiteLocalConfig.loadSiteLocalConfig()
        rep['executed_site'] = slCfg.siteName
        if 'phedex-node' in slCfg.localStageOut:
            rep['phedex_node'] = slCfg.localStageOut['phedex-node']
        print("== Execution site from site-local-config.xml: %s" % slCfg.siteName)
        with open('jobReport.json', 'w') as of:
            json.dump(rep, of)
        with open('jobReport.exitCode.txt', 'w') as rf:
            rf.write(str(rep['exitCode']))
        with open('jobReportExtract.pickle', 'wb') as of:
            pickle.dump(rep, of)
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
            for oldName, newName in literal_eval(options.outFiles).items():
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
