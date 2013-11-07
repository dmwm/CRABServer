import os
import shutil
import socket
import os.path
import getopt
import time
import commands
import sys
import re
import json
import traceback
import pickle
from ast import literal_eval

import WMCore.Storage.SiteLocalConfig as SiteLocalConfig

EC_MissingArg  =        50113 #10 for ATLAS trf
EC_CMSRunWrapper =      10040
EC_MoveOutErr =         99999 #TODO define an error code
EC_ReportHandlingErr =  50115
EC_WGET =               99998 #TODO define an error code

print "=== start ==="
print time.ctime()

from optparse import (OptionParser,BadOptionError,AmbiguousOptionError)

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
                OptionParser._process_args(self,largs,rargs,values)
            except (BadOptionError,AmbiguousOptionError), e:
                largs.append(e.opt_str)


def handleException(exitAcronymn, exitCode, exitMsg):
    report = {}
    report['exitAcronym'] = exitAcronymn
    report['exitCode'] = exitCode
    report['exitMsg'] = exitMsg
    print traceback.format_exc()
    with open('jobReport.json','w') as of:
        json.dump(report, of)
    with open('jobReportExtract.pickle','w') as of:
        pickle.dump(report, of)


parser = PassThroughOptionParser()
parser.add_option('-a',\
                  dest='archiveJob',\
                  type='string')
parser.add_option('-o',\
                  dest='outFiles',\
                  type='string')
parser.add_option('-r',\
                  dest='runDir',\
                  type='string')
parser.add_option('--inputFile',\
                  dest='inputFile',\
                  type='string')
parser.add_option('--sourceURL',\
                  dest='sourceURL',\
                  type='string')
parser.add_option('--jobNumber',\
                  dest='jobNumber',\
                  type='string')
parser.add_option('--cmsswVersion',\
                  dest='cmsswVersion',\
                  type='string')
parser.add_option('--scramArch',\
                  dest='scramArch',\
                  type='string')
parser.add_option('--runAndLumis',\
                  dest='runAndLumis',\
                  type='string',\
                  default='{}')
parser.add_option('--userFiles',\
                  dest='userFiles',\
                  type='string')
parser.add_option('--oneEventMode',\
                  dest='oneEventMode',\
                  default=0)

(opts, args) = parser.parse_args(sys.argv[1:])

try:
    print "=== parameters ==="
    print "archiveJob:    ", opts.archiveJob
    print "runDir:        ", opts.runDir
    print "sourceURL:     ", opts.sourceURL
    print "jobNumber:     ", opts.jobNumber
    print "cmsswVersion:  ", opts.cmsswVersion
    print "scramArch:     ", opts.scramArch
    print "inputFile      ", opts.inputFile
    print "outFiles:      ", opts.outFiles
    print "runAndLumis:   ", opts.runAndLumis
    print "userFiles:     ", opts.userFiles
    print "oneEventMode:  ", opts.oneEventMode
    print "==================="
except:
    type, value, traceBack = sys.exc_info()
    print 'ERROR: missing parameters : %s - %s' % (type,value)
    handleException("FAILED", EC_MissingArg, 'CMSRunAnalysisERROR: missing parameters : %s - %s' % (type,value))
    sys.exit(EC_MissingArg)
    
#clean workdir ?

#wget sandnox
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
            if re.search('--no-check-certificate',line) != None:
                wgetCommand = 'wget --no-check-certificate'
                break
        com = '%s %s/cache/%s' % (wgetCommand, opts.sourceURL, opts.archiveJob)
        nTry = 3
        for iTry in range(nTry):
            print 'Try : %s' % iTry
            status,output = commands.getstatusoutput(com)
            print output
            if status == 0:
                break
            if iTry+1 == nTry:
                print "ERROR : cound not get jobO files from panda server"
                handleException("FAILED", EC_WGET, 'CMSRunAnalysisERROR: cound not get jobO files from panda server')
                sys.exit(EC_WGET)
            time.sleep(30)
    print commands.getoutput('tar xvfzm %s' % opts.archiveJob)

#move the pset in the right place
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

#WMCore import here
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
from PSetTweaks.WMTweak import makeTweak

def executeCMSSWStack(opts):

    def getOutputModules():
        config = __import__('WMTaskSpace.cmsRun.PSet', globals(), locals(), ["process"], -1)
        tweakJson = makeTweak(config.process).jsondictionary()
        return tweakJson["process"]["outputModules_"]

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
    cmssw.step.section_("builder")
    cmssw.step.builder.workingDir = os.getcwd()
    cmssw.step.runtime.invokeCommand = 'python'
    cmssw.step.runtime.preScripts = []
    cmssw.step.runtime.scramPreScripts = ['%s/TweakPSet.py Analy %s \'%s\' \'%s\' --oneEventMode=%s' % (os.getcwd(), os.getcwd(), opts.inputFile, opts.runAndLumis, opts.oneEventMode)]
    cmssw.step.section_("execution") #exitStatus of cmsRun is set here
    cmssw.report = Report("cmsRun") #report is loaded and put here
    cmssw.execute()
    return cmssw

def AddChecksums(report):
    if 'steps' not in report: return
    if 'cmsRun' not in report['steps']: return
    if 'output' not in report['steps']['cmsRun']: return

    for module, outputMod in report['steps']['cmsRun']['output'].items():
        for fileInfo in outputMod:
            if 'checksums' in fileInfo: continue
            if 'pfn' not in fileInfo: continue
            cksum = FileInfo.readCksum(fileInfo['pfn'])
            adler32 = FileInfo.readAdler32(fileInfo['pfn'])
            fileInfo['checksums'] = {'adler32': adler32, 'cksum': cksum}
            fileInfo['size'] = os.stat(fileInfo['pfn']).st_size

def parseAd(ad):
    fd = open(os.environ['_CONDOR_JOB_AD'])
    ad = {}
    for line in fd.readlines():
        info = line.split(" = ", 1)
        if len(info) != 2:
            continue
        if info[1].startswith('"'):
            val = info[1].strip()[1:-1]
        else:
            try:
                val = int(info[1].strip())
            except ValueError:
                continue
        ad[info[0]] = val
    return ad

def startDashboardMonitoring(ad):
    params = {
        'WNHostName': socket.getfqdn(),
        #'MonitorJobID': '3_https://submit-6.t2.ucsd.edu//131103//53912.2',
        'SyncGridJobId': '%d_https://glidein.%d:%s_0' % (ad['CRAB_Id'], ad['CRAB_Id'], ad['CRAB_ReqName']),
        'SyncSite': ad['JOB_CMSSite'],
        'SyncCE': ad['Used_Gatekeeper'].split(":")[0],
        'ExeStart': 'cmsRun',
    }

def stopDashboardMonitoring(ad):
    params = {
        'SyncGridJobId': '%d_https://glidein.%d:%s_0' % (ad['CRAB_Id'], ad['CRAB_Id'], ad['CRAB_ReqName']),
        'ExeEnd': 'cmsRun',
    }

try:
    setupLogging('.')
    cmssw = executeCMSSWStack(opts)
    jobExitCode = cmssw.step.execution.exitStatus
except WMExecutionFailure, WMex:
    print "caught WMExecutionFailure - code = %s - name = %s - detail = %s" % (WMex.code, WMex.name, WMex.detail)
    exmsg = WMex.name
    #exmsg += WMex.detail
    #print "jobExitCode = %s" % jobExitCode
    handleException("FAILED", WMex.code, exmsg)
    #sys.exit(WMex.code)
    sys.exit(0)
except Exception, ex:
    #print "jobExitCode = %s" % jobExitCode
    handleException("FAILED", EC_CMSRunWrapper, "failed to generate cmsRun cfg file at runtime")
    #sys.exit(EC_CMSRunWrapper)
    sys.exit(0)

#Create the report file
try:
    report = Report("cmsRun")
    report.parse('FrameworkJobReport.xml', "cmsRun")
    jobExitCode = report.getExitCode()
    #import pdb;pdb.set_trace()
    report = report.__to_json__(None)
    AddChecksums(report)
    if jobExitCode: #TODO check exitcode from fwjr
        report['exitAcronym'] = "FAILED"
        report['exitCode'] = jobExitCode #TODO take exitcode from fwjr
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
    with open('jobReport.json','w') as of:
        json.dump(report, of)
    with open('jobReportExtract.pickle','w') as of:
        pickle.dump(report, of)
except FwkJobReportException, FJRex:
    msg = "BadFWJRXML"
    handleException("FAILED", EC_ReportHandlingErr, msg)
    sys.exit(EC_ReportHandlingErr)
except Exception, ex:
    msg = "Exception while handling the job report."
    handleException("FAILED", EC_ReportHandlingErr, msg)
    sys.exit(EC_ReportHandlingErr)

# rename output files. Doing this after checksums otherwise outfile is not found.
if jobExitCode == 0:
    try:
        for oldName,newName in literal_eval(opts.outFiles).iteritems():
            os.rename(oldName, newName)
    except Exception, ex:
        handleException("FAILED", EC_MoveOutErr, "Exception while moving the files.")
        sys.exit(EC_MoveOutErr)


#create the Pool File Catalog (?)
pfcName = 'PoolFileCatalog.xml'
pfcFile = open(pfcName,'w')
pfcFile.write("""<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- Edited By POOL -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>

</POOLFILECATALOG>
""")
pfcFile.close()

