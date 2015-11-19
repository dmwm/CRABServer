"""
    CRABQuality - Entry module for CRAB testing functionality
"""
from __future__ import print_function
import nose
from nose.tools import with_setup
import os
import os.path
import shutil
import subprocess
import sys
import tempfile
import time
import CRABInterface.CRABServerBase as CRABServerBase

def getTestRoot():
    return os.path.join(CRABServerBase.getCRABServerBase(),
                        'test', 'python')

def runTests(mode='default',integrationHost=None):
    """
    Top-level function
    """
    if mode not in ['default', 'integration']:
        raise ArgumentError("Invalid mode selected")

    testBaseDir = getTestRoot()
    if not os.path.exists(testBaseDir):
        raise RuntimeError("Test tree not found")

    extraArgs = []
    if mode == 'default':
        extraArgs = ['-a', '!integration,!broken']
    elif mode == 'integration':
        extraArgs = ['-a', 'integration,!broken']

    args = [__file__, '--with-xunit', '-v', '-s',
                '-m', '(_t.py$)|(_t$)|(^test)',
                testBaseDir,
                __file__]

    args.extend(extraArgs)
    return nose.run(argv=args)

# Code needed for integration tests
# Do it this way (not in a class) so we can use a test generator
taskNamePrefix = "integration_%s" % round(time.time())
integrationStatus = {}
integrationTempDir = ""
integrationCMSSWVer = 'CMSSW_5_3_11'
def runCommand(command, cwd):
    p = subprocess.Popen(command,
                         cwd=cwd,
                         shell = True,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.STDOUT)
    stdout, _ = p.communicate()
    return stdout, p.returncode

def sendTestJobsToAllSites():
    global integrationTempDir
    integrationTempDir = tempfile.mkdtemp(prefix='crab3-int')
    # make CMSSW checkout
    print("Making CMSSW release area")
    command = makeCMSSWSnippet % {'cmsswVersion' : integrationCMSSWVer}
    stdout, returncode = runCommand(command, integrationTempDir)
    if returncode != 0 or not os.path.exists("%s/%s" %
                                    (integrationTempDir, integrationCMSSWVer)):
        raise RuntimeError("Couldn't get CMSSW install: \n%s" % stdout)
    cmsswEnv = getCMSSWEnvironment % {'cmsswVersion':integrationCMSSWVer}

    # Write cmsRun configuration
    open('%s/pset.py' % integrationTempDir, 'w').write(psetSnippet)
    skipProxy = ""
    for site in sorted(allSites):
        print("Submitting to %s" % site)
        # make configuration
        configOpts = { 'requestName' : '%s_%s' % (taskNamePrefix, site),
                       'serverUrl' : 'crab3-gwms-1.cern.ch',
                       'targetSite' : site }
        configPath = '%s/config_%s.py' % (integrationTempDir, site)
        fh = open(configPath, 'w')
        fh.write(configSnippet % configOpts)
        fh.close()
        # crab -submit it and wait two hours
        state = { 'desc' : 'Test job to %s' % site,
                  'timeout' : time.time() + 60 * 120,
                  'path' : '%s/crab_%s' % (integrationTempDir, configOpts['requestName'])}
        integrationStatus[site] = state
        cmdLine = "unset LD_LIBRARY_PATH ; %s ; crab -d %s submit -c %s" % \
                                    (cmsswEnv, skipProxy, configPath)
        stdout, returncode = runCommand(cmdLine, integrationTempDir)
        if not os.path.exists(state['path']):
            state['error'] = "Couldn't submit CRAB task:%s\n%s" %\
                                       (state['path'], stdout)
        integrationStatus[site] = state
        skipProxy = "-p"

def cleanupTestJobs():
    global integrationTempDir
    # TODO: clean up output files
    cmsswEnv = getCMSSWEnvironment % {'cmsswVersion':integrationCMSSWVer}
    for site in sorted(integrationStatus.keys()):
        if 'path' not in integrationStatus[site]:
            continue
        taskPath = integrationStatus[site]['path']
        print("Killing task of %s" % taskPath)
        cmdLine = "unset LD_LIBRARY_PATH ; %s ; crab -d -p kill -t %s" % (cmsswEnv, taskPath)
        #stdout, returncode = runCommand(cmdLine, integrationTempDir)
        #if not returncode:
        #    print "Couldn't kill CRAB task:%s\n%s" % (taskPath,stdout)

    if integrationTempDir and os.path.exists(integrationTempDir):
        #shutil.rmtree(integrationTempDir)
        pass

def dummySetup():
    pass

@with_setup(dummySetup, cleanupTestJobs)
def testJobsToAllSites():
    """
        Check status of the jobs we've sent
    """
    sendTestJobsToAllSites()
    for site in sorted(integrationStatus.keys()):
        getJobStatus.description = integrationStatus[site]['desc']
        getJobStatus.name = site
        if 'error' in integrationStatus[site]:
            isError = integrationStatus[site]['error']
        else:
            isError = False
        yield getJobStatus, integrationStatus[site]['path'],\
                            integrationStatus[site]['timeout'],\
                            isError

def getJobStatus(taskDir, timeout, isError):
    global integrationCMSSWVer, integrationTempDir
    if isError:
        raise RuntimeError(isError)
    stdout = "Status not checked yet"
    triesRemaining = 5
    # If a previous status check takes too long, it can cause
    # a timeout before we get a chance to check this current task
    firstLoop = True
    while firstLoop or time.time() < timeout:
        firstLoop = False
        cmsswEnv = getCMSSWEnvironment % {'cmsswVersion':integrationCMSSWVer}
        print("Getting status of %s" % taskDir)
        cmdLine = "unset LD_LIBRARY_PATH ; %s ; crab -d -p status -t %s" % (cmsswEnv, taskDir)
        stdout, exitCode = runCommand(cmdLine, integrationTempDir)
        hasStatus = False
        for line in stdout.split('\n'):
            if line.startswith('Task status:'):
                assert line.split()[2] != 'FAILED'
                hasStatus = True
                triesRemaining = 5
                if line.split()[2] == 'COMPLETE':
                    # TODO check more
                    return
                elif line.split()[2] == 'SUBMITTED':
                    # nothing
                    pass
                break
        if not hasStatus:
            triesRemaining -= 1
            if triesRemaining <= 0:
                raise RuntimeError("Status call didn't return a status")
        time.sleep(5)
    # timeout
    assert time.time() < timeout, "Job timed out. Last status: %s" % stdout

testJobsToAllSites.integration = 1

def testImport_t():
    importRoot = CRABServerBase.getCRABServerBase() + '/src/python'
    for (dirpath, dirnames, filenames) in os.walk(importRoot):
        dirpath = os.path.relpath(dirpath, importRoot)
        baseModule = dirpath.replace(os.sep, '.')
        for filename in filenames:
            if not filename.endswith('.py'):
                continue
            moduleName = filename.replace('.py', '')
            if baseModule != '.':
                toImport = "%s.%s" % (baseModule, moduleName)
            else:
                toImport = moduleName
            importOne.name = "Import.%s" % toImport
            importOne.description = importOne.name
            yield importOne, toImport

def importOne(val):
    __import__(val)

# https://cmsweb.cern.ch/das/request?view=plain&limit=1000&instance=cms_dbs_prod_global&input=site+dataset%3D%2FGenericTTbar%2FHC-CMSSW_5_3_1_START53_V5-v1%2FGEN-SIM-RECO
allSitesRaw = """T1_CH_CERN_Buffer
T1_DE_KIT_Buffer
T3_IT_Bologna
T1_FR_CCIN2P3_Buffer
T1_IT_CNAF_Buffer
T1_CH_CERN_MSS
T1_DE_KIT_Disk
T1_DE_KIT_MSS
T1_ES_PIC_Buffer
T1_IT_CNAF_Disk
T1_ES_PIC_MSS
T1_FR_CCIN2P3_MSS
T1_RU_JINR_Disk
T1_TW_ASGC_MSS
T1_UK_RAL_MSS
T1_US_FNAL_Buffer
T1_US_FNAL_MSS
T1_IT_CNAF_MSS
T1_TW_ASGC_Buffer
T2_AT_Vienna
T2_BE_IIHE
T2_BE_UCL
T2_BR_SPRACE
T1_UK_RAL_Buffer
T1_UK_RAL_Disk
T2_FR_GRIF_IRFU
T2_CH_CERN
T2_CH_CSCS
T2_CN_Beijing
T2_DE_DESY
T2_DE_RWTH
T2_EE_Estonia
T2_ES_CIEMAT
T2_ES_IFCA
T2_FI_HIP
T2_UK_London_Brunel
T2_UK_London_IC
T2_UK_SGrid_Bristol
T2_FR_CCIN2P3
T2_UK_SGrid_RALPP
T2_FR_GRIF_LLR
T2_FR_IPHC
T2_US_Vanderbilt
T2_GR_Ioannina
T2_HU_Budapest
T2_IN_TIFR
T2_IT_Bari
T2_IT_Legnaro
T2_IT_Pisa
T2_IT_Rome
T2_BR_UERJ
T2_KR_KNU
T2_PK_NCP
T2_PL_Warsaw
T2_PT_NCG_Lisbon
T2_RU_IHEP
T2_RU_INR
T2_RU_ITEP
T2_RU_JINR
T2_RU_PNPI
T2_RU_RRC_KI
T2_RU_SINP
T2_TH_CUNSTDA
T2_TR_METU
T2_TW_Taiwan
T2_UA_KIPT
T2_US_Caltech
T2_US_Florida
T2_US_MIT
T2_US_Nebraska
T2_US_Purdue
T2_US_UCSD
T2_US_Wisconsin
T3_CH_PSI
T3_FR_IPNL
T3_IR_IPM
T3_IT_Perugia
T3_IT_Trieste
T3_KR_UOS
T3_NZ_UOA
T3_RU_FIAN
T3_TW_NTU_HEP
T3_US_Brown
T3_US_Colorado
T3_US_Cornell
T3_US_FIU
T3_UK_SGrid_Oxford
T3_UK_ScotGrid_ECDF
T3_UK_ScotGrid_GLA
T3_US_Minnesota
T3_US_OSU
T3_US_PuertoRico
T3_UK_London_UCL
T3_US_FIT
T3_US_FSU
T3_US_Rutgers
T3_US_TAMU
T3_US_Omaha
T3_UK_London_RHUL
T3_UK_London_QMUL
T3_MX_Cinvestav
T3_US_TTU
T3_US_UCD
T3_US_UCR
T3_US_UMD
T3_US_UMiss
T3_US_UVA"""
allSites = list(set([x.replace('_Buffer', '').replace('_Disk', '').replace('_MSS', '')
                     for x in allSitesRaw.split('\n')]))

configSnippet = """from WMCore.Configuration import Configuration
config = Configuration()
config.section_("General")
config.General.requestName   = '%(requestName)s'
config.General.serverUrl     = '%(serverUrl)s'
config.General.instance = 'private'
config.section_("JobType")
config.JobType.pluginName  = 'Analysis'
config.JobType.psetName    = 'pset.py'
config.section_("Data")
config.Data.inputDataset = '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 20
## User options
config.section_("User")
config.User.email = ''
config.section_("Site")
config.Site.storageSite = 'T2_US_Nebraska'
config.Site.removeT1Blacklisting = 1
config.Site.whitelist = ['%(targetSite)s']
config.section_("Debug")
config.Debug.oneEventMode = True
"""
psetSnippet = """
import FWCore.ParameterSet.Config as cms
process = cms.Process('NoSplit')
process.source = cms.Source('PoolSource',
                              fileNames = cms.untracked.vstring("/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0010/FC85224E-EAAD-E111-AB01-0025901D629C.root"),
                            )

process.dump = cms.EDAnalyzer("EventContentAnalyzer", listContent=cms.untracked.bool(False), getData=cms.untracked.bool(True))
process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 10

process.Timing = cms.Service("Timing",
                                 useJobReport = cms.untracked.bool(True),
                                 summaryOnly = cms.untracked.bool(True),
                             )

process.o = cms.OutputModule("PoolOutputModule", fileName = cms.untracked.string("dumper.root"), outputCommands=cms.untracked.vstring("drop *"))
process.out = cms.EndPath(process.o)
process.p = cms.Path(process.dump)
"""
makeCMSSWSnippet = "scram p %(cmsswVersion)s ; cd %(cmsswVersion)s/src/ ; eval `scramv1 runtime -sh`"
getCMSSWEnvironment = "cd %(cmsswVersion)s ; eval `scramv1 runtime -sh` ; cd -"
