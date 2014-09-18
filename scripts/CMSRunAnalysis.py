
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
EC_PsetHash           = 60453

def mintime():
    mymin = 20*60
    tottime = time.time()-starttime
    remaining = mymin - tottime
    if remaining > 0:
        print "==== Failure sleep STARTING at %s ====" % time.asctime(time.gmtime())
        print "Sleeping for %d seconds due to failure." % remaining
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(remaining)
        print "==== Failure sleep FINISHING at %s ====" % time.asctime(time.gmtime())


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
    return params['MonitorJobID']


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


def addReportInfo(params, fjr):
    if 'exitCode' in fjr:
        params['JobExitCode'] = fjr['exitCode']
        params['ExeExitCode'] = fjr['exitCode']
    if 'steps' not in fjr or 'cmsRun' not in fjr['steps']:
        return
    fjr = fjr['steps']['cmsRun']
    if 'performance' in fjr and 'cpu' in fjr['performance']:
        if fjr['performance']['cpu'].get("TotalJobTime"):
            params['ExeTime'] = int(float(fjr['performance']['cpu']['TotalJobTime']))
        if fjr['performance']['cpu'].get("TotalJobCPU"):
            params['CrabUserCpuTime'] = float(fjr['performance']['cpu']['TotalJobCPU'])
        if params.get('ExeTime') and params.get("CrabUserCpuTime"):
            params['CrabCpuPercentage'] = params['CrabUserCpuTime'] / float(params['ExeTime'])
    inputEvents = 0
    if 'input' in fjr and 'source' in fjr['input']:
        for info in fjr['input']['source']:
            if 'events' in info:
                params['NoEventsPerRun'] = info['events']
                params['NbEvPerRun'] = info['events']
                params['NEventsProcessed'] = info['events']


def reportPopularity(monitorId, monitorJobId, myad, fjr):
    """
    Using the FJR and job ad, report popularity information to the dashboard.

    Implementation is based on https://github.com/dmwm/CRAB2/blob/63a7c046e8411ab26d4960ef2ae5bf48642024de/python/parseCrabFjr.py

    Note: Unlike CRAB2, we do not report the runs/lumis processed.
    """

    if 'CRAB_DataBlock' not in myad:
        print "Not sending data to popularity service because 'CRAB_DataBlock' is not in job ad."
    sources = fjr.get('steps', {}).get('cmsRun', {}).get('input', {}).get('source', [])
    skippedFiles = fjr.get('skippedFiles', [])
    fallbackFiles = fjr.get('fallbackFiles', [])
    if not sources:
        print "Not sending data to popularity service because no input sources found."

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
            print "Excluding source data from popularity report because no LFN is present. %s" % str(source)
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
    print "Dashboard popularity report: %s" % str(report)
    DashboardAPI.apmonSend(monitorId, monitorJobId, report)


def stopDashboardMonitoring(myad):
    params = {
        'ExeEnd': 'cmsRun',
    }
    populateDashboardMonitorInfo(myad, params)
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    del params['ExeEnd']
    fjr = {}
    try:
        fjr = json.load(open("jobReport.json"))
    except:
        print "WARNING: Unable to parse jobReport.json; Dashboard reporting will not be useful.  Traceback follows:\n", traceback.format_exc()
    try:
        addReportInfo(params, fjr)
    except:
        if 'ExeExitCode' not in params:
            params['ExeExitCode'] = 50115
        if 'JobExitCode' not in params:
            params['JobExitCode'] = params['ExeExitCode']
        print "ERROR: Unable to parse job info from FJR.  Traceback follows:\n", traceback.format_exc()
    print "Dashboard end parameters: %s" % str(params)
    try:
        reportPopularity(params['MonitorID'], params['MonitorJobID'], myad, fjr)
    except:
        print "ERROR: Failed to report popularity information to Dashboard.  Traceback follows:\n", traceback.format_exc()
    DashboardAPI.apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    DashboardAPI.apmonFree()


def logCMSSW():
    if not os.path.exists("cmsRun-stdout.log"):
        print "ERROR: Cannot dump CMSSW stdout; perhaps CMSSW never executed (e.g.: scriptExe was set)?"
        return
    print "======== CMSSW OUTPUT STARTING ========"
    fd = open("cmsRun-stdout.log")
    st = os.fstat(fd.fileno())
    if st.st_size > 1*1024*1024:
        print "WARNING: CMSSW output is over 1MB; truncating to last 1MB."
        print "Use 'crab getlog' to retrieve full output of this job from storage."
        fd.seek(-1*1024*1024, 2)
        it = fd.xreadlines()
        it.next()
    else:
        it = fd.xreadlines()
    for line in it:
        print "== CMSSW: ", line,
    print "======== CMSSW OUTPUT FINSHING ========"


def handleException(exitAcronymn, exitCode, exitMsg):
    report = {}
    try:
        if os.path.exists("jobReport.json"):
            report = json.load(open("jobReport.json"))
        else:
            print "WARNING: WMCore did not produce a jobReport.json; FJR will not be useful."
    except: 
        print "WARNING: Unable to parse WMCore's jobReport.json; FJR will not be useful.  Traceback follows:\n", traceback.format_exc()

    if report.get('steps', {}).get('cmsRun', {}).get('errors'):
        exitMsg += '\nCMSSW error message follows.\n'
        for error in report['steps']['cmsRun']['errors']:
            if 'exitCode' in error:
                try:
                    exitCode = int(exitCode)
                    fjrExitCode = int(error['exitCode'])
                    if (fjrExitCode % 256 == exitCode) and (fjrExitCode != exitCode):
                        print "NOTE: FJR has exit code %d and WMCore reports %d; preferring the FJR one." % (fjrExitCode, exitCode)
                        exitCode = fjrExitCode
                except ValueError:
                    pass
            exitMsg += error['type'] + '\n'
            exitMsg += error['details'] + '\n'

    report['exitAcronym'] = exitAcronymn
    report['exitCode'] = exitCode
    report['exitMsg'] = exitMsg
    print "ERROR: Exceptional exit at %s (%s): %s" % (time.asctime(time.gmtime()), str(exitCode), str(exitMsg))
    formatted_tb = traceback.format_exc()
    if not formatted_tb.startswith("None"):
        print "ERROR: Traceback follows:\n", formatted_tb

    try:
        slc = SiteLocalConfig.loadSiteLocalConfig()
        report['executed_site'] = slc.siteName
        print "== Execution site for failed job from site-local-config.xml: %s" % slc.siteName
    except:
        print "ERROR: Failed to record execution site name in the FJR from the site-local-config.xml"
        print traceback.format_exc()

    with open('jobReport.json','w') as of:
        json.dump(report, of)
    with open('jobReportExtract.pickle','w') as of:
        pickle.dump(report, of)
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
    parser.add_option('--scriptExe',
                      dest='scriptExe',
                      type='string')
    parser.add_option('--scriptArgs',
                      dest='scriptArgs',
                      type='string')

    (opts, args) = parser.parse_args(sys.argv[1:])

    try:
        print "==== Parameters Dump at %s ===" % time.asctime(time.gmtime())
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
        print "scriptExe:     ", opts.scriptExe
        print "scriptArgs:    ", opts.scriptArgs
        print "==================="
    except:
        type, value, traceBack = sys.exc_info()
        print 'ERROR: missing parameters : %s - %s' % (type, value)
        handleException("FAILED", EC_MissingArg, 'CMSRunAnalysisERROR: missing parameters : %s - %s' % (type, value))
        mintime()
        sys.exit(EC_MissingArg)

    return opts


def prepSandbox(opts):
    print "==== Sandbox preparation STARTING at %s ====" % time.asctime(time.gmtime())
    os.environ['WMAGENTJOBDIR'] = os.getcwd()
    if opts.archiveJob and not "CRAB3_RUNTIME_DEBUG" in os.environ:
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
    print "==== Sandbox preparation FINISHING at %s ====" % time.asctime(time.gmtime())

    #move the pset in the right place
    print "==== WMCore filesystem preparation STARTING at %s ====" % time.asctime(time.gmtime())
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
    #move also scriptExe in the working directory (if present)
    if opts.scriptExe:
        os.rename(opts.scriptExe, destDir + '/' + opts.scriptExe)
    print "==== WMCore filesystem preparation FINISHING at %s ====" % time.asctime(time.gmtime())


def getProv(filename, opts):
    scram = Scram(
        version = opts.cmsswVersion,
        directory = os.getcwd(),
        architecture = opts.scramArch,
    )
    if scram.project() or scram.runtime(): #if any of the two commands fail...
        msg = scram.diagnostic()
        handleException("FAILED", EC_CMSMissingSoftware, 'Error setting CMSSW environment: %s' % msg)
        mintime()
        sys.exit(EC_CMSMissingSoftware)
    ret = scram("edmProvDump %s" % filename, runtimeDir=os.getcwd(), logName="edmProvDumpOutput.log")
    if ret > 0:
        msg = scram.diagnostic()
        handleException("FAILED", EC_CMSRunWrapper, 'Error getting pset hash from file.\n\tScram Env %s\n\tCommand:edmProvDump %s' % (msg, filename))
        mintime()
        sys.exit(EC_CMSRunWrapper)
    output = ''
    with open("edmProvDumpOutput.log", "r") as fd:
        output = fd.read()
    return output


def executeScriptExe(opts):
    scram = Scram(
        version = opts.cmsswVersion,
        directory = "WMTaskSpace/cmsRun",
        architecture = opts.scramArch,
    )
    if scram.project() or scram.runtime(): #if any of the two commands fail...
        msg = scram.diagnostic()
        handleException("FAILED", EC_CMSMissingSoftware, 'Error setting CMSSW environment: %s' % msg)
        mintime()
        sys.exit(EC_CMSMissingSoftware)
    command_ = "%s %s %s" % (opts.scriptExe, opts.jobNumber, " ".join(opts.scriptArgs))
    ret = scram(command_, runtimeDir="WMTaskSpace/cmsRun", logName=subprocess.PIPE)
    if ret > 0:
        msg = scram.diagnostic()
        handleException("FAILED", EC_CMSRunWrapper, 'Error executing scriptExe.\n\tScram Env %s\n\tCommand: %s' % (msg, command_))
        mintime()
        sys.exit(EC_CMSRunWrapper)
    return ret


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
            print "==== Checksum cksum STARTING at %s ====" % time.asctime(time.gmtime())
            print "== Filename: %s" % fileInfo['pfn']
            cksum = FileInfo.readCksum(fileInfo['pfn'])
            print "==== Checksum finishing FINISHING at %s ====" % time.asctime(time.gmtime())
            print "==== Checksum adler32 STARTING at %s ====" % time.asctime(time.gmtime())
            adler32 = FileInfo.readAdler32(fileInfo['pfn'])
            print "==== Checksum adler32 FINISHING at %s ====" % time.asctime(time.gmtime())
            fileInfo['checksums'] = {'adler32': adler32, 'cksum': cksum}
            fileInfo['size'] = os.stat(fileInfo['pfn']).st_size


def AddPsetHash(report, opts):
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
            if fileInfo.get('ouput_module_class') != 'PoolOutputModule':
                continue
            if 'pfn' not in fileInfo:
                continue
            print "== Filename: %s" % fileInfo['pfn']
            if not os.path.exists(fileInfo['pfn']):
                print "== Output file missing!"
                continue
            filename = fileInfo['pfn']
            m = re.match(r"^[A-Za-z0-9\-._]+$", filename)
            if not m:
                print "== EDM output filename (%s) must match RE ^[A-Za-z0-9\\-._]+$" % filename
                continue
            print "==== PSet Hash lookup STARTING at %s ====" % time.asctime(time.gmtime())
            lines = getProv(filename, opts)
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
            print "==== PSet Hash lookup FINISHED at %s ====" % time.asctime(time.gmtime())
            if matches:
                max_depth = max(matches.keys())
                pset_hash = matches[max_depth]
                print "== edmProvDump pset hash %s" % pset_hash
                fileInfo['pset_hash'] = pset_hash
            else:
                print "ERROR: PSet Hash missing from edmProvDump output.  Full dump below."
                print lines
                raise Exception("PSet hash missing from edmProvDump output.")


if __name__ == "__main__":
    print "==== CMSRunAnalysis.py STARTING at %s ====" % time.asctime(time.gmtime())
    print "Local time : %s" % time.ctime()
    starttime = time.time()

    ad = {}
    try:
        ad = parseAd()
    except:
        print "==== FAILURE WHEN PARSING HTCONDOR CLASSAD AT %s ====" % time.asctime(time.gmtime())
        print traceback.format_exc()
        ad = {}
    if ad and not "CRAB3_RUNTIME_DEBUG" in os.environ:
        dashboardId = earlyDashboardMonitoring(ad)
        if dashboardId:
            os.environ['DashboardJobId'] = dashboardId

    #WMCore import here
    # Note that we may fail in the imports - hence we try to report to Dashboard first
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
        if ad and not "CRAB3_RUNTIME_DEBUG" in os.environ:
            earlyDashboardFailure(ad)
        print "==== FAILURE WHEN LOADING WMCORE AT %s ====" % time.asctime(time.gmtime())
        print traceback.format_exc()
        mintime()
        sys.exit(10043)

    # At this point, all our dependent libraries have been loaded; it's quite
    # unlikely python will see a segfault.  Drop a marker file in the working
    # directory; if we encounter a python segfault, the wrapper will look to see if
    # this file exists and report to Dashboard accordingly.
    with open("wmcore_initialized", "w") as fd:
        fd.write("wmcore initialized.\n")

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
        print "==== CMSSW Stack Execution STARTING at %s ====" % time.asctime(time.gmtime())
        try:
            jobExitCode = None
            if not opts.scriptExe:
                cmssw = executeCMSSWStack(opts)
                jobExitCode = cmssw.step.execution.exitStatus
            else:
                jobExitCode = executeScriptExe(opts)
        except:
            print "==== CMSSW Stack Execution FAILED at %s ====" % time.asctime(time.gmtime())
            logCMSSW()
            raise
        print "Job exit code: %s" % str(jobExitCode)
        print "==== CMSSW Stack Execution FINISHING at %s ====" % time.asctime(time.gmtime())
        logCMSSW()
    except WMExecutionFailure, WMex:
        print "ERROR: Caught WMExecutionFailure - code = %s - name = %s - detail = %s" % (WMex.code, WMex.name, WMex.detail)
        exmsg = WMex.name

        # Try to recover what we can from the FJR.  handleException will use this if possible.
        if os.path.exists('FrameworkJobReport.xml'):
            try:
                report = Report("cmsRun")
                report.parse('FrameworkJobReport.xml', "cmsRun")
                try:
                    jobExitCode = report.getExitCode()
                except:
                    jobExitCode = WMex.code
                report = report.__to_json__(None)
                report['jobExitCode'] = jobExitCode

                with open('jobReport.json','w') as of:
                    json.dump(report, of)
            except:
                print "WARNING: Failure when trying to parse FJR XML after job failure.  Traceback follows."
                print traceback.format_exc()

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
        print "==== Report file creation STARTING at %s ====" % time.asctime(time.gmtime())
        report = Report("cmsRun")
        report.parse('FrameworkJobReport.xml', "cmsRun")
        jobExitCode = report.getExitCode()
        #import pdb;pdb.set_trace()
        report = report.__to_json__(None)
        # Record the payload process's exit code separately; that way, we can distinguish
        # cmsRun failures from stageout failures.  The initial use case of this is to
        # allow us to use a different LFN on job failure.
        report['jobExitCode'] = jobExitCode
        AddChecksums(report)
        try:
            AddPsetHash(report, opts)
        except:
            handleException("FAILED", EC_PsetHash, "Unable to compute pset hash for job output")
            mintime()
            sys.exit(EC_PsetHash)
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
        if ad and not "CRAB3_RUNTIME_DEBUG" in os.environ:
            stopDashboardMonitoring(ad)
        print "==== Report file creation FINISHING at %s ====" % time.asctime(time.gmtime())
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

    print "==== CMSRunAnalysis.py FINISHING at %s ====" % time.asctime(time.gmtime())
    print "Local time : %s" % time.ctime()
    sys.exit(jobExitCode)
