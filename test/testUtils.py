from __future__ import division
from __future__ import print_function

def writePset():
    psetFileContent = """
    import FWCore.ParameterSet.Config as cms
    
    process = cms.Process('NoSplit')
    
    process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring('root://cms-xrd-global.cern.ch///store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0010/00CE4E7C-DAAD-E111-BA36-0025B32034EA.root'))
    process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(10))
    process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))
    process.output = cms.OutputModule("PoolOutputModule",
        outputCommands = cms.untracked.vstring("drop *", "keep recoTracks_globalMuons_*_*"),
        fileName = cms.untracked.string('output.root'),
    )
    process.out = cms.EndPath(process.output)
    """

    with open('PSET.py', 'w') as fp:
        fp.write(psetFileContent)
    return

def changeInConf(configuration=None, paramName=None, paramValue=None, configSection=None):
    newParamLine = 'config.%s.%s = %s\n' % (configSection, paramName, paramValue)
    # is this param already present in the configuration ?
    if paramName in configuration:
        # override:
        for line in configuration.split('\n'):
            if paramName in line :
                existingLine = line
                break
        configuration = configuration.replace(existingLine, newParamLine)
    else:
        # add below section header
        cfgSectionHeader = "config.section_('%s')\n" % configSection
        newCfgLines = cfgSectionHeader + newParamLine
        configuration = configuration.replace(cfgSectionHeader, newCfgLines)
    return configuration

def writeConfigFile(testName=None, listOfDicts=None):
    standardConfig = """
# a simple configuration which is customized for each test
#
import time
from WMCore.Configuration import Configuration
config = Configuration()

config.section_('General')
config.General.instance = 'prod'
config.General.workArea = '/tmp/crabTestConfig'
config.General.requestName = REQUESTNAME

config.section_('JobType')
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'PSET.py'
config.JobType.maxJobRuntimeMin = 60

config.section_('Data')
config.Data.inputDataset = '/GenericTTbar/HC-CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/AODSIM'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 1
config.Data.totalUnits = 1
config.Data.publication = False
config.Data.outputDatasetTag = 'CrabAutoTest_' + config.General.requestName

config.section_('User')

config.section_('Site')
config.Site.storageSite = 'T2_CH_CERN'

config.section_('Debug')
"""
    conf = standardConfig
    for d in listOfDicts:
        param = d['param']
        value = d['value']
        section = d['section']
        conf = changeInConf(configuration=conf, paramName=param, paramValue=value, configSection=section)
    # also set the requestName (do it now to avoid confusing changeInConf)
    conf = conf.replace('REQUESTNAME', '"'+testName+'"')
    with open(testName + '.py', 'w') as fp:
        fp.write(conf)
    return

def writeValidationScript(testName=None, validationScript=None):
    validationScriptHeader = """#!/bin/bash
function checkStatus {
  # check that taskName has reached targetStatus and writes statusLog.txt
  # if target = SUBMITTED, accepts status COMPLETED as well
  # if target = COMPLETED and status is SUBMITTED, ask for retry after delay
  # Fail test if command fails or status is not good
  local taskName="$1"
  local targetStatus="$2"

  crab remake --task ${taskName} 2>&1 | tee remakeLog.txt 
  [ $? -ne 0 ] && exit -1  # if remake fails, abort
  grep -q Success remakeLog.txt || exit -1  # if log does not contain "Success" string, abort
  workDir=`grep Success remakeLog.txt | awk '{print $NF}'`
  crab status -d $workDir 2>&1 | tee  statusLog.txt
  [ $? -ne 0 ] && exit -1  # if crab status fails, abort

  local isSub=0
  local isDone=0
  grep -q "Status on the scheduler:.*SUBMITTED" statusLog.txt 2>&1 && isSub=1
  grep -q "Status on the scheduler:.*COMPLETED" statusLog.txt 2>&1 && isDone=1

  case $targetStatus in
    NONE)
      # no check is needed
      ;;
    SUBMITTED)
      # both SUBMITTED or COMPLETED are OK
      [ ${isSub} -eq 0 ] && [ ${isDone} -eq 0 ] && exit 2
      ;;
    COMPLETED)
      [ ${isSub} -eq 1 ] && exit 1  # ask for a check later on
      [ ${isDone} -eq 0 ] && exit 2
  esac
  return 0
}

function lookFor {
  # looks for string in file. Fail test if not found
  local string="$1"
  local file="$2"
  grep -q "${string}" ${file}
  [ $? -ne 0 ] && exit 2
  return 0
}

function crabCommand() {
  # execute crab command and write commandLog.txt.
  # Fails test if command exit code is non 0
  local cmd="$1"
  local params="$2"
  crab $cmd $params 2>&1 | tee commandLog.txt
  [ $? -ne 0 ] && exit 2
  return 0
}
taskName="$1"
"""
    with open(testName + '.sh', 'w') as fp:
        fp.write(validationScriptHeader)
        fp.write(validationScript)
    return
