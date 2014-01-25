
"""
CMSRunAnalysis.py - the runtime python portions to launch a CRAB3 / cmsRun job.
"""

import os
import shutil
import socket
import os.path
import time
import commands
import sys
import re
import json
import traceback
import pickle
import logging
import subprocess
from ast import literal_eval
from optparse import OptionParser, BadOptionError, AmbiguousOptionError

import DashboardAPI
import WMCore.Storage.SiteLocalConfig as SiteLocalConfig

EC_MissingArg  =        50113 #10 for ATLAS trf
EC_CMSMissingSoftware = 10034
EC_CMSRunWrapper =      10040
EC_MoveOutErr =         99999 #TODO define an error code
EC_ReportHandlingErr =  50115
EC_WGET =               99998 #TODO define an error code

ad = {}

print "==== CMSRunAnalysis.py STARTING at %s ====" % time.ctime()
starttime = time.time()

def mintime():
    mymin = 20*60
    tottime = time.time()-starttime
    remaining = mymin - tottime
    if remaining > 0:
        print "==== Failure sleep STARTING at %s ====" % time.ctime()
        print "Sleeping for %d seconds due to failure." % remaining
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(remaining)
        print "==== Failure sleep FINISHING at %s ====" % time.ctime()


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
            except (BadOptionError, AmbiguousOptionError), e:
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
    params['MonitorJobID'] = '%d_https://glidein.cern.ch/%d/%s_%d' % (myad['CRAB_Id'], myad['CRAB_Id'], myad['CRAB_ReqName'].replace("_", ":"), myad['CRAB_Retry'])


def earlyDashboardMonitoring(myad):
    params = {
        'WNHostName': socket.getfqdn(),
        'SyncGridJobId': 'https://glidein.cern.ch/%d/%s' % (myad['CRAB_Id'], myad['CRAB_ReqName'].replace("_", ":")),
        'SyncSite': myad['JOB_CMSSite'],
        'SyncCE': myad['Used_Gatekeeper'].split(":")[0],
    }
    populateDashboardMonitorInfo(myad, params)
    print "Dashboard early startup params: %s" % str(params)
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)

def earlyDashboardFailure(myad):
    params = {
        'JobExitCode': 10043,
    }
    populateDashboardMonitorInfo(myad, params)
    print "Dashboard early startup params: %s" % str(params)
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)


def startDashboardMonitoring(myad):
    params = {
        'WNHostName': socket.getfqdn(),
        'ExeStart': 'cmsRun',
    }
    populateDashboardMonitorInfo(myad, params)
    print "Dashboard startup parameters: %s" % str(params)
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)


def addReportInfo(params):
    fjr = json.load(open("jobReport.json"))
    if 'exitCode' in fjr:
        params['JobExitCode'] = fjr['exitCode']
        params['ExeExitCode'] = fjr['exitCode']
    if 'steps' not in fjr or 'cmsRun' not in fjr['steps']:
        return
    fjr = fjr['steps']['cmsRun']
    if 'performance' in fjr and 'cpu' in fjr['performance']:
        params['ExeTime'] = int(float(fjr['performance']['cpu']['TotalJobTime']))
        params['CrabUserCpuTime'] = float(fjr['performance']['cpu']['TotalJobCPU'])
        if params['ExeTime']:
            params['CrabCpuPercentage'] = params['CrabUserCpuTime'] / float(params['ExeTime'])
    inputEvents = 0
    if 'input' in fjr and 'source' in fjr['input']:
        for info in fjr['input']['source']:
            if 'events' in info:
                params['NoEventsPerRun'] = info['events']
                params['NbEvPerRun'] = info['events']
                params['NEventsProcessed'] = info['events']


def stopDashboardMonitoring(myad):
    params = {
        'ExeEnd': 'cmsRun',
    }
    populateDashboardMonitorInfo(myad, params)
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    del params['ExeEnd']
    try:
        addReportInfo(params)
    except:
        if 'ExeExitCode' not in params:
            params['ExeExitCode'] = 50115
        if 'JobExitCode' not in params:
            params['JobExitCode'] = params['ExeExitCode']
        print traceback.format_exc()
    print "Dashboard end parameters: %s" % str(params)
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    DashboardAPI.apmonFree()


def logCMSSW():
    if not os.path.exists("cmsRun-stdout.log"):
        print "ERROR: Cannot dump CMSSW stdout; perhaps CMSSW never executed?"
    print "======== CMSSW OUTPUT STARTING ========"
    fd = open("cmsRun-stdout.log")
    st = os.fstat(fd.fileno())
    if st.st_size > 10*1024*1024:
        print "WARNING: CMSSW output is over 10MB; truncating to last 10MB."
        print "Use 'crab getlog' to retrieve full output of this job from storage."
        fd.seek(-10*1024*1024, 2)
        it = fd.xreadlines()
        it.next()
    else:
        it = fd.xreadlines()
    for line in it:
        print "== CMSSW: ", line,
    print "======== CMSSW OUTPUT FINSHING ========"


def handleException(exitAcronymn, exitCode, exitMsg):
    report = {}
    report['exitAcronym'] = exitAcronymn
    report['exitCode'] = exitCode
    report['exitMsg'] = exitMsg
    print "ERROR: Exceptional exit at %s (%s): %s" % (time.ctime(), str(exitCode), str(exitMsg))
    print "ERROR: Traceback follows as applicable:\n", traceback.format_exc()
    with open('jobReport.json','w') as of:
        json.dump(report, of)
    with open('jobReportExtract.pickle','w') as of:
        pickle.dump(report, of)
    if ad:
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
                      default='{}')
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

    (opts, args) = parser.parse_args(sys.argv[1:])

    try:
        print "==== Parameters Dump at %s ===" % time.ctime()
        print "archiveJob:    ", opts.archiveJob
        print "runDir:        ", opts.runDir
        print "sourceURL:     ", opts.sourceURL
        print "jobNumber:     ", opts.jobNumber
        print "cmsswVersion:  ", opts.cmsswVersion
        print "scramArch:     ", opts.scramArch
        print "inputFile      ", opts.inputFile
        print "outFiles:      ", opts.outFiles
        print "runAndLumis:   ", opts.runAndLumis
        print "lheInputFiles: ", opts.lheInputFiles
        print "firstEvent:    ", opts.firstEvent
        print "firstLumi:     ", opts.firstLumi
        print "lastEvent:     ", opts.lastEvent
        print "firstRun:      ", opts.firstRun
        print "seeding:       ", opts.seeding
        print "userFiles:     ", opts.userFiles
        print "oneEventMode:  ", opts.oneEventMode
        print "==================="
    except:
        type, value, traceBack = sys.exc_info()
        print 'ERROR: missing parameters : %s - %s' % (type, value)
        handleException("FAILED", EC_MissingArg, 'CMSRunAnalysisERROR: missing parameters : %s - %s' % (type, value))
        mintime()
        sys.exit(EC_MissingArg)

    return opts


def prepSandbox(opts):
    print "==== Sandbox preparation STARTING at %s ====" % time.ctime()
    if opts.archiveJob:
        os.environ['WMAGENTJOBDIR'] = os.getcwd()
        if os.path.exists(opts.archiveJob):
            print "Sandbox %s already exists, skipping" % opts.archiveJob
        elif opts.sourceURL == 'LOCAL' and not os.path.exists(opts.archiveJob):
            print "ERROR: Requested for condor to transfer the tarball, but it didn't show up"
            handleException("FAILED", EC_WGET, 'CMSRunAnalysisERROR: cound not get jobO files from panda server')
            sys.exit(EC_WGET)
        else:
            print "--- wget for jobO ---"
            output = commands.getoutput('wget -h')
            wgetCommand = 'wget'
            for line in output.split('\n'):
                if re.search('--no-check-certificate', line) != None:
                    wgetCommand = 'wget --no-check-certificate'
                    break
            com = '%s %s/cache/%s' % (wgetCommand, opts.sourceURL, opts.archiveJob)
            nTry = 3
            for iTry in range(nTry):
                print 'Try : %s' % iTry
                status, output = commands.getstatusoutput(com)
                print output
                if status == 0:
                    break
                if iTry+1 == nTry:
                    print "ERROR : cound not get jobO files from panda server"
                    handleException("FAILED", EC_WGET, 'CMSRunAnalysisERROR: cound not get jobO files from panda server')
                    sys.exit(EC_WGET)
                time.sleep(30)
        print commands.getoutput('tar xvfzm %s' % opts.archiveJob)
    print "==== Sandbox preparation FINISHING at %s ====" % time.ctime()

    #move the pset in the right place
    print "==== WMCore filesystem preparation STARTING at %s ====" % time.ctime()
    destDir = 'WMTaskSpace/cmsRun'
    if os.path.isdir(destDir):
        shutil.rmtree(destDir)
    os.makedirs(destDir)
    os.rename('PSet.py', destDir + '/PSet.py')
    open('WMTaskSpace/__init__.py','w').close()
    open(destDir + '/__init__.py','w').close()
    #move the additional user files in the right place
    if opts.userFiles:
        for myfile in opts.userFiles.split(','):
            os.rename(myfile, destDir + '/' + myfile)
    print "==== WMCore filesystem preparation FINISHING at %s ====" % time.ctime()

#WMCore import here
# Note that we may fail in the imports - hence we try to report to Dashboard first

ad = {}
try:
    ad = parseAd()
except:
    print traceback.format_exc()
    ad = {}
if ad:
    earlyDashboardMonitoring(ad)

try:
    opts = parseArgs()
    prepSandbox(opts)
    from WMCore.WMRuntime.Bootstrap import setupLogging
    from WMCore.FwkJobReport.Report import Report
    from WMCore.FwkJobReport.Report import FwkJobReportException
    from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
    import WMCore.FwkJobReport.FileInfo as FileInfo
    from WMCore.WMSpec.Steps.Executors.CMSSW import CMSSW
    from WMCore.Configuration import Configuration
    from WMCore.WMSpec.WMStep import WMStep
    from WMCore.WMSpec.WMTask import makeWMTask
    from WMCore.WMSpec.WMWorkload import newWorkload
    from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSW as CMSSWTemplate
    from WMCore.WMRuntime.Tools.Scram import Scram
except:
    # We may not even be able to create a FJR at this point.  Record
    # error and exit.
    if ad:
        earlyDashboardFailure(ad)
    print "==== FAILURE WHEN LOADING WMCORE AT %s ====" % time.ctime()
    print traceback.format_exc()
    raise

def executeCMSSWStack(opts):

    def getOutputModules():
        scram = Scram(
            command = cmssw.step.application.setup.scramCommand,
            version = opts.cmsswVersion,
            initialise = cmssw.step.application.setup.softwareEnvironment,
            directory = os.getcwd(),
            architecture = opts.scramArch,
            )
        pythonScript = "from PSetTweaks.WMTweak import makeTweak;"+\
                       "config = __import__(\"WMTaskSpace.cmsRun.PSet\", globals(), locals(), [\"process\"], -1);"+\
                       "tweakJson = makeTweak(config.process).jsondictionary();"+\
                       "print tweakJson[\"process\"][\"outputModules_\"]"
        if scram.project() or scram.runtime(): #if any of the two commands fail...
            msg = scram.diagnostic()
            handleException("FAILED", EC_CMSMissingSoftware, 'Error setting CMSSW environment: %s' % msg)
            mintime()
            sys.exit(EC_CMSMissingSoftware)
        ret = scram("python -c '%s'" % pythonScript, logName=subprocess.PIPE, runtimeDir=os.getcwd())
        if ret > 0:
            msg = scram.diagnostic()
            handleException("FAILED", EC_CMSRunWrapper, 'Error getting output modules from the pset.\n\tScram Env %s\n\tCommand:%s' % (msg, pythonScript))
            mintime()
            sys.exit(EC_CMSRunWrapper)
        return literal_eval(scram.stdout)

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
                                                          '--oneEventMode=%s') %
                                             (os.getcwd(), os.getcwd(),
                                                           opts.inputFile,
                                                           opts.runAndLumis,
                                                           opts.firstEvent,
                                                           opts.lastEvent,
                                                           opts.firstLumi,
                                                           opts.firstRun,
                                                           opts.seeding,
                                                           opts.lheInputFiles,
                                                           opts.oneEventMode)]
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
            print "==== Checksum cksum STARTING at %s ====" % time.ctime()
            print "== Filename: %s" % fileInfo['pfn']
            cksum = FileInfo.readCksum(fileInfo['pfn'])
            print "==== Checksum finishing FINISHING at %s ====" % time.ctime()
            print "==== Checksum adler32 STARTING at %s ====" % time.ctime()
            adler32 = FileInfo.readAdler32(fileInfo['pfn'])
            print "==== Checksum adler32 FINISHING at %s ====" % time.ctime()
            fileInfo['checksums'] = {'adler32': adler32, 'cksum': cksum}
            fileInfo['size'] = os.stat(fileInfo['pfn']).st_size

def AddPsetHash(report):
    if 'steps' not in report:
        return
    if 'cmsRun' not in report['steps']:
        return
    if 'output' not in report['steps']['cmsRun']:
        return

    pset_re = re.compile("^ParameterSetID: ([a-f0-9]{32,32})$")
    for outputMod in report['steps']['cmsRun']['output'].values():
        for fileInfo in outputMod:
            if fileInfo.get('ouput_module_class') != 'PoolOutputModule':
                continue
            if 'pfn' not in fileInfo:
                continue
            print "== Filename: %s" % fileInfo['pfn']
            if not os.path.exists(fileInfo['pfn']):
                print "== Output file missing!"
                continue
            print "==== PSet Hash lookup STARTING at %s ====" % time.ctime()
            prov = subprocess.Popen(["edmProvDump", fileInfo['pfn']], stdout=subprocess.PIPE)
            pset_hash = None
            for line in prov.stdout.readlines():
                 m = pset_re.match(line)
                 if m:
                     pset_hash = m.groups()[0]
            rc = prov.wait()
            print "== edmProvDump return code %d" % rc
            print "==== PSet Hash lookup FINISHED at %s ====" % time.ctime()
            if pset_hash and not rc:
                print "== edmProvDump pset hash %s" % pset_hash
                fileInfo['pset_hash'] = pset_hash

try:
    setupLogging('.')

    # Also add stdout to the logging
    logHandler = logging.StreamHandler(sys.stdout)
    logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    logHandler.setFormatter(logFormatter)
    logging.getLogger().addHandler(logHandler)

    if ad:
        startDashboardMonitoring(ad)
    print "==== CMSSW Stack Execution STARTING at %s ====" % time.ctime()
    try:
        cmssw = executeCMSSWStack(opts)
    except:
        print "==== CMSSW Stack Execution FAILED at %s ====" % time.ctime()
        logCMSSW()
        raise
    jobExitCode = cmssw.step.execution.exitStatus
    print "Job exit code: %s" % str(jobExitCode)
    print "==== CMSSW Stack Execution FINISHING at %s ====" % time.ctime()
    logCMSSW()
except WMExecutionFailure, WMex:
    print "ERROR: Caught WMExecutionFailure - code = %s - name = %s - detail = %s" % (WMex.code, WMex.name, WMex.detail)
    exmsg = WMex.name
    #exmsg += WMex.detail
    #print "jobExitCode = %s" % jobExitCode
    handleException("FAILED", WMex.code, exmsg)
    mintime()
    sys.exit(WMex.code)
except Exception, ex:
    #print "jobExitCode = %s" % jobExitCode
    handleException("FAILED", EC_CMSRunWrapper, "failed to generate cmsRun cfg file at runtime")
    mintime()
    sys.exit(EC_CMSRunWrapper)

#Create the report file
try:
    print "==== Report file creation STARTING at %s ====" % time.ctime()
    report = Report("cmsRun")
    report.parse('FrameworkJobReport.xml', "cmsRun")
    jobExitCode = report.getExitCode()
    #import pdb;pdb.set_trace()
    report = report.__to_json__(None)
    AddChecksums(report)
    AddPsetHash(report)
    if jobExitCode: #TODO check exitcode from fwjr
        report['exitAcronym'] = "FAILED"
        report['exitCode'] = jobExitCode
        report['exitMsg'] = "Error while running CMSSW:\n"
        for error in report['steps']['cmsRun']['errors']:
            report['exitMsg'] += error['type'] + '\n'
            report['exitMsg'] += error['details'] + '\n'
    else:
        report['exitAcronym'] = "OK"
        report['exitCode'] = 0
        report['exitMsg'] = "OK"

    slc = SiteLocalConfig.loadSiteLocalConfig()
    report['executed_site'] = slc.siteName
    print "== Execution site from site-local-config.xml: %s" % slc.siteName
    with open('jobReport.json','w') as of:
        json.dump(report, of)
    with open('jobReportExtract.pickle','w') as of:
        pickle.dump(report, of)
    if ad:
        stopDashboardMonitoring(ad)
    print "==== Report file creation FINISHING at %s ====" % time.ctime()
except FwkJobReportException, FJRex:
    msg = "BadFWJRXML"
    handleException("FAILED", EC_ReportHandlingErr, msg)
    mintime()
    sys.exit(EC_ReportHandlingErr)
except Exception, ex:
    msg = "Exception while handling the job report."
    handleException("FAILED", EC_ReportHandlingErr, msg)
    mintime()
    sys.exit(EC_ReportHandlingErr)

# rename output files. Doing this after checksums otherwise outfile is not found.
if jobExitCode == 0:
    try:
        oldName = 'UNKNOWN'
        newName = 'UNKNOWN'
        for oldName, newName in literal_eval(opts.outFiles).iteritems():
            os.rename(oldName, newName)
    except Exception, ex:
        handleException("FAILED", EC_MoveOutErr, "Exception while moving file %s to %s." %(oldName, newName))
        mintime()
        sys.exit(EC_MoveOutErr)
else:
    mintime()
print "==== CMSRunAnalysis.py FINISHING at %s ====" % time.ctime()
sys.exit(jobExitCode)
