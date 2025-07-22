"""
An almost standalone python script which creates a set of files for CRAB validation
Requires the testUtils.py file to be placed in same directory
Requires the REST_instance environment variable to be set to a known instance
The script creates three files for each test, named as the name of
the configuration parameter they aim to checkout as per
https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile
The three files are:
 <testName>.py is the configuration file to be used in crab submit
 <testName>-testSubmit.sh is a script to execute an immediate check on
    submission output and/or files created in the work directory
    (in most cases this is dummy, since "it was submitted" is good enough)
 <testName>-check.sh is a script to executed independently AFTER the previous one
    succeeded (typically 30min or 1h later), and possibily executed again until
    the submitted task has reached the required status (SUBMITTED or COMPLETED)
    This script relies on crab remake to create a new work directory

 Tasks needs to be submitted from the directory where they have been
  created, since they may need additional external files created by this script.
  *-submit.sh script takes no argument and exit with code
    0 if all OK
    1 if fail
  *-check scripts take a task name as only argument, and exit with code
    0 if all OK
    1 if FAIL
    2 to main "wait and run me again later"
   there is no validation of the task name argument, a bad one results in exit code 1
   These script need to be executed like:
     bash <scriptName> <taskName>     or
     bash -x <scriptName> <taskName>

"""
from __future__ import division
from __future__ import print_function

import os

from testUtils import writePset, writePset8cores, writeScriptExe, writeLumiMask, \
    writeConfigFile, writeTestSubmitScript, writeValidationScript

# some tests needs changing if running on SL6
# rely on $singularity which is set by Jenkins as
# export singularity=`echo ${SCRAM_ARCH:3:1}`
SL6 = os.environ.get('singularity', '7') == '6'

# some tests need a small tweak on CMSSW_8
CMSSW8 = os.environ.get('CMSSW_VERSION').split('_')[1] == '8'

writePset()
writePset8cores()
writeScriptExe()
writeLumiMask()


dummyTestScript = "\nexit 0\n"  #  a test which always returns success
#
# test CRAB Configuration file parameters
#

#=============================
# SECTION GENERAL
#=============================

# transferOutputs
name = 'transferOutputs'
changeDict = {'param': name, 'value': 'False', 'section': 'General'}  # default is True
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "transferOutputs: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: CRAB_TransferOutputs = 0" "${workDir}/results/job_out.1.*.txt" || echo "transferOutputs: Retrieval failure JOB AD"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# transferLogs
name = 'transferLogs'
changeDict = {'param': name, 'value': 'True', 'section': 'General'}  # default is False
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "transferLogs: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: CRAB_SaveLogsFlag = 1" "${workDir}/results/job_out.1.*.txt" || echo "transferLogs: Retrieval failure JOB AD"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# activity
name = 'activity'
changeDict = {'param': name, 'value': '"hctestnew"', 'section': 'General'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "JOB AD: CMS_Type = \\"Test\\"" "${workDir}/results/job_out.1.*.txt" || echo "activity: Retrieval failure JOB AD"
lookFor "JOB AD: CMS_TaskType = \\"hctestnew\\"" "${workDir}/results/job_out.1.*.txt" || echo "activity: Retrieval failure JOB AD"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

#=============================
# SECTION JOB TYPE
#=============================

# inputFiles
name = 'inputFiles'
inFile1 = '/etc/hosts'
inFile2 = '/etc/centos-release'
changeDict = {'param': name, 'section': 'JobType', 'value': [inFile1, inFile2]}
confChangesList = [changeDict]
testSubmitScript = """
lookInTarFor "^hosts" ${workDir}/inputs/*default.tgz || echo "inputFiles: Retrieval failure in Tarlookup of hosts"
lookInTarFor "^centos-release" ${workDir}/inputs/*default.tgz || echo "inputFiles: Retrieval failure in Tarlookup of centos-release"
"""
validationScript = """
checkStatus ${taskName} SUBMITTED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# disableAutomaticOutputCollection
name = 'disableAutomaticOutputCollection'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "disableAutomaticOutputCollection: Retrieval failure job_out.1.*.txt"
lookFor "^Output files.*: \$" "${workDir}/results/job_out.1.*.txt" || echo "disableAutomaticOutputCollection: Retrieval failure Output files"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# outputFiles
name = 'outputFiles'
# use scriptExe to add a custom output file which is not .root
confChangesList = []
changeDict = {'param': 'scriptExe', 'value': '"SIMPLE-SCRIPT.sh"', 'section': 'JobType'}
confChangesList.append(changeDict)
changeDict = {'param': 'disableAutomaticOutputCollection', 'value': 'True', 'section': 'JobType'}
confChangesList.append(changeDict)
changeDict = {'param': 'outputFiles', 'value': '["output.root", "My_output.txt"]', 'section': 'JobType'}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getoutput "--jobids=1 --proxy=$PROXY"
lookFor "Success in retrieving output_1.root " commandLog.txt || echo "outputFiles: Retrieval failure output_1.root"
lookFor "Success in retrieving My_output_1.txt " commandLog.txt  || echo "outputFiles: Retrieval failure My_output_1.txt"
"""
if SL6:  # skip: singularity, no gfal_copy, crab getoutput can't work
    validationScript = dummyTestScript
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# allowUndistributedCMSSW
name = 'allowUndistributedCMSSW'
#TODO need a real test here, e.g. using a non-prod version of CMSSW
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} SUBMITTED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# maxMemoryMB
name = 'maxMemoryMB'
changeDict = {'param': name, 'value': '2500', 'section': 'JobType'} # default is 2000
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "maxMemoryMB: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: RequestMemory = 2500" "${workDir}/results/job_out.1.*.txt" || echo "maxMemoryMB: Retrieval failure JOB AD"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# maxJobRuntimeMin
name = 'maxJobRuntimeMin'
changeDict = {'param': name, 'value': '100', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "maxJobRuntimeMin: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: MaxWallTimeMins_RAW = 100" "${workDir}/results/job_out.1.*.txt" || echo "maxJobRuntimeMin: Retrieval failure JOB AD"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# numCores
name = 'numCores'
confChangesList = []
changeDict = {'param': name, 'value': '8', 'section': 'JobType'}
confChangesList.append(changeDict)
changeDict = {'param': 'psetName', 'value': '"PSET-8cores.py"', 'section': 'JobType'}
confChangesList.append(changeDict)
changeDict = {'param': 'maxMemoryMB', 'value': '4000', 'section': 'JobType'}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "numCores: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: RequestCpus = 8" "${workDir}/results/job_out.1.*.txt" || echo "numCores: Retrieval failure JOB AD"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# scriptExe
name = 'scriptExe'
changeDict = {'param': name, 'value': '"SIMPLE-SCRIPT.sh"', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = """
lookInTarFor "^SIMPLE-SCRIPT.sh" ${workDir}/inputs/*default.tgz || echo "scriptExe: Tarlookup failure"
"""
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "scriptExe: Retrieval failure job_out.1.*.txt"
lookFor "SB CMSRUN starting" "${workDir}/results/job_out.1.*.txt" || echo "scriptExe: Retrieval failure SB CMSRUN start"
lookFor "====== arg checking: \$1 = 1" "${workDir}/results/job_out.1.*.txt" || echo "scriptExe: Retrieval failure arg checking"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# scriptArgs
name = 'scriptArgs'
confChangesList = []
changeDict = {'param': 'scriptExe', 'value': '"SIMPLE-SCRIPT.sh"', 'section': 'JobType'}
confChangesList.append(changeDict)
changeDict = {'param': name, 'value': ['exitCode=666', 'gotArgs=Yes'], 'section': 'JobType'}
confChangesList.append(changeDict)
testSubmitScript = """
lookInTarFor "^SIMPLE-SCRIPT.sh" ${workDir}/inputs/*default.tgz || echo "scriptArgs: Tarlookup failure"
"""
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "scriptArgs: Retrieval failure job_out.1.*.txt"
lookFor "SB CMSRUN starting" "${workDir}/results/job_out.1.*.txt" || echo "scriptArgs: Retrieval failure SB CMSRUN start"
lookFor "====== arg checking: \$1 = 1" "${workDir}/results/job_out.1.*.txt" || echo "scriptArgs: Retrieval failure arg checking"
lookFor "====== arg checking: \$2 = exitCode=666" "${workDir}/results/job_out.1.*.txt" || echo "scriptArgs: Retrieval failure arg checking"
lookFor "====== arg checking: \$3 = gotArgs=Yes" "${workDir}/results/job_out.1.*.txt" || echo "scriptArgs: Retrieval failure arg checking"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# sendVenvFolder
name = 'sendVenvFolder'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = """
lookInTarFor "^venv/" ${workDir}/inputs/*default.tgz || echo "sendVenvFolder: Tarlookup failure"
"""
validationScript = """
checkStatus ${taskName} SUBMITTED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)


# sendExternalFolder
name = 'sendExternalFolder'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = """
lookInTarFor "^external/" ${workDir}/inputs/*default.tgz || echo "sendExternalFolder: Tarlookup failure"
"""
validationScript = """
checkStatus ${taskName} SUBMITTED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

#=============================
# SECTION DATA
#=============================

# inputDBS
name = 'inputDBS'
confChangesList = []
changeDict = {'param': name, 'value': '"phys03"', 'section': 'Data'}
confChangesList.append(changeDict)
changeDict = {'param': 'inputDataset', 'section': 'Data',
              'value': '"/GenericTTbar/belforte-Stefano-Test-bb695911428445ed11a1006c9940df69/USER"'}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
# data for that dataset are not on disk anymore, can't expect this task to complete
# but if it was sumitted, it means that DBS lookup was OK
validationScript = """
checkStatus ${taskName} SUBMITTED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# useParent
name = 'useParent'
changeDict = {'param': name, 'value': 'True', 'section': 'Data'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
# make sure that parents were really read by cmsRun
validationScript = """
checkStatus ${taskName} COMPFAIL
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "useParent: Retrieval failure job_out.1.*.txt"
lookFor "request to open.*GenericTTbar/GEN-SIM-RAW" "${workDir}/results/job_out.1.*.txt" || echo "useParent: Retrieval failure"
"""
if CMSSW8:  # skip: needed parent dataset for the sample that we can read witn CMSSW_8 is not on disk
    validationScript = dummyTestScript
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# secondaryInputDataset
name = 'secondaryInputDataset'
changeDict = {'param': name, 'section': 'Data',
              'value': "'/GenericTTbar/HC-CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/GEN-SIM-RAW'"}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
# make sure that the secondary dataset was really used in cmsRun
validationScript = """
checkStatus ${taskName} COMPFAIL
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "secondaryInputDataset: Retrieval failure job_out.1.*.txt"
lookFor "request to open.*GenericTTbar/GEN-SIM-RAW" "${workDir}/results/job_out.1.*.txt" || echo "secondaryInputDataset: Retrieval failure"
"""
if SL6:  # skip: primary input used for CMSSW_7 has different lumisection numbers from the dataset above
    validationScript = dummyTestScript
if CMSSW8:  # skip: needed parent dataset for the sample that we can read witn CMSSW_8 is not on disk
    validationScript = dummyTestScript
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)


# lumiMask-File
name = 'lumiMaskFile'
changeDict = {'param': 'lumiMask', 'value': '"lumiMask.json"', 'section': 'Data'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
# make sure that the lumimask was really applied
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "lumiMask-File: Retrieval failure job_out.1.*.txt"
lookFor "== JOB AD: CRAB_AlgoArgs.*1,10,20,25" "${workDir}/results/job_out.1.*.txt" || echo "lumiMask-File: Retrieval failure JOB AD"
"""
if SL6:  # skip: our lumiMask does not work on the primary input used for CMSSW_7 tests
    validationScript = dummyTestScript
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# lumiMask-URL
if not SL6:  # skip on SL6, can't fetch lumMask from URL inside singularity
    name = 'lumiMaskUrl'
    confChangesList = []
    changeDict = {'param': 'lumiMask', 'section': 'Data',
              'value': '"https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions16/13TeV/ReReco/Final/Cert_271036-284044_13TeV_ReReco_07Aug2017_Collisions16_JSON.txt"'}
    confChangesList.append(changeDict)
    changeDict = {'param': 'inputDataset', 'section': 'Data',
              'value': '"/MuonEG/Run2016B-23Sep2016-v3/MINIAOD"'}
    confChangesList.append(changeDict)
    testSubmitScript = dummyTestScript
    # make sure that the lumimask was really applied
    validationScript = """
    checkStatus ${taskName} COMPLETED
    statusCode=$?
    ([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
    crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
    lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "lumiMask-URL: Retrieval failure job_out.1.*.txt"
    lookFor "== JOB AD: CRAB_AlgoArgs.*273158" "${workDir}/results/job_out.1.*.txt" || echo "lumiMask-URL: Retrieval failure JOB AD"
    """
    writeConfigFile(testName=name, listOfDicts=confChangesList)
    writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
    writeValidationScript(testName=name, validationScript=validationScript)

# outLFNDirBase
name = 'outLFNDirBase'
changeDict = {'param': name, 'value': "'/store/user/%s/OLFNtest/Adir'%getUsername()", 'section': 'Data'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getoutput "--dump --jobids=1 --proxy=$PROXY"
lookFor "OLFNtest/Adir" commandLog.txt || echo "outLFNDirBase: Retrieval failure"
"""
if SL6:  # skip: singularity, no gfal_copy, crab getoutput can't work
    validationScript = dummyTestScript
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# runRange
name = 'runRange'
confChangesList = []
changeDict = {'param': name, 'section': 'Data',
              'value': "'273150-273300,273410-273420'"}
confChangesList.append(changeDict)
changeDict = {'param': 'inputDataset', 'section': 'Data',
              'value': '"/MuonEG/Run2016B-23Sep2016-v3/MINIAOD"'}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
# make sure that the run range was really applied
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "runRange: Retrieval failure job_out.1.*.txt"
lookFor "== JOB AD: CRAB_AlgoArgs.*273150" "${workDir}/results/job_out.1.*.txt"|| echo "runRange: Retrieval failure JOB AD"
"""
if SL6:  # skip: our runRange does not work on the primary input used for CMSSW_7 tests
    validationScript = dummyTestScript
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# ignoreLocality
name = 'ignoreLocality'
confChangesList = []
changeDict = {'param': name, 'section': 'Data', 'value': "True"}
confChangesList.append(changeDict)
if not SL6:  # pick a dataset which is NOT at CERN (the one for CMSSW is only at FNAL)
    changeDict = {'param': 'inputDataset', 'section': 'Data',
                  'value': '"/MuonEG/Run2016B-23Sep2016-v3/MINIAOD"'}
    confChangesList.append(changeDict)
changeDict = {'param': 'whitelist', 'section': 'Site', 'value': "['T2_CH_CERN']"}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand status "--long --proxy=$PROXY"
lookFor "T2_CH_CERN" commandLog.txt || echo "ignoreLocality: Retrieval failure"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# userInputFiles
name = 'userInputFiles'
confChangesList = []
changeDict = {'param': name, 'section': 'Data', 'value':
    "['/store/mc/HC/GenericTTbar/AODSIM/CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/00000/00B29645-2B76-E711-8802-FA163EB9B8B4.root',"
    "'/store/mc/HC/GenericTTbar/AODSIM/CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/00000/0EC77D94-0976-E711-8D8A-FA163E75A20F.root']"}
confChangesList.append(changeDict)
changeDict = {'param': 'inputDataset', 'section': 'Data', 'value': 'REMOVE'}
confChangesList.append(changeDict)
changeDict = {'param': 'splitting', 'section': 'Data', 'value': "'FileBased'"}
confChangesList.append(changeDict)
changeDict = {'param': 'whitelist', 'section': 'Site', 'value': "['T2_CH_CERN']"}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
"""
if SL6:  # skip: those input files can't be read with CMSSW_7
    validationScript = dummyTestScript
if CMSSW8:  # skip: those input files can't be read with CMSSW_8
    validationScript = dummyTestScript
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)


#=============================
# SECTION SITE
#=============================

# whitelist
name = 'whitelist'
changeDict = {'param': name, 'section': 'Site', 'value': "['T2_DE_DESY']"}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "whitelist: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: DESIRED_SITES = \\"T2_DE_DESY\\"" "${workDir}/results/job_out.1.*.txt" || echo "whitelist: Retrieval failure JOB AD"
"""
if SL6:  # skip: old dataset for CMSSW_7 has not enough locations to test this
    validationScript = dummyTestScript
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)


# blacklist
name = 'blacklist'
# blacklist all sites but T1_US_ and disable overflow !
confChangesList = []
changeDict = {'param': name, 'section': 'Site',
              'value': "['T1_IT*','T1_DE*','T1_ES*','T1_FR*','T1_RU*','T1_UK*','T2_*','T3_*']"}
confChangesList.append(changeDict)
changeDict = {'section':'Debug', 'param': 'extraJDL', 'value': "['My.CMS_ALLOW_OVERFLOW=False']"}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "blacklist: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: JOB_CMSSite = \\"T1_US_FNAL\\"" "${workDir}/results/job_out.1.*.txt" || echo "blacklist: Retrieval failure JOB AD"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# ignoreGlobalBlacklist
name = 'ignoreGlobalBlacklist'
changeDict = {'param': name, 'value': 'True', 'section': 'Site'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
# there is no good way to check that the central black list is ignored,
# mostly because it is a list which continuously changes. Best way
# is to check TW log, which is not (easily) accessible from client
# let's simply make sure that task completes
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

#=============================
# SECTION USER
#=============================

# voGroup
name = 'voGroup'
changeDict = {'param': name, 'value': '"itcms"', 'section': 'User'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "voGroup: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: CRAB_UserGroup = \\"itcms\\"" "${workDir}/results/job_out.1.*.txt" || echo "voGroup: Retrieval failure JOB AD"
lookFor "attribute : /cms/itcms/Role=NULL/Capability=NULL" "${workDir}/results/job_out.1.*.txt" || echo "voGroup: Retrieval failure attribute"
# now that condor does not fill x509UserProxyFirstFQAN anymore we lack a good way to check the FirstFQAN
#lookFor "JOB AD: x509UserProxyFirstFQAN = \\"/cms/itcms/Role=NULL/Capability=NULL\\"" "${workDir}/results/job_out.1.*.txt"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

#=============================
# SECTION DEBUG
#=============================

# scheddName
name = 'scheddName'
changeDict = {'param': name, 'value': '"crab3@vocms059.cern.ch"', 'section': 'Debug'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} SUBMITTED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
lookFor "crab3@vocms059.cern.ch" statusLog.txt || echo "scheddName: Retrieval failure"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# collector
name = 'collector'
confChangesList = []
# temporarily remove FNAL ITB collector https://mattermost.web.cern.ch/cms-o-and-c/pl/5hgkkmg4jfge5b5umkq6jc968w
# changeDict = {'param': name, 'value': '"cmsgwms-collector-itb.cern.ch,cmsgwms-collector-itb.fnal.gov"', 'section': 'Debug'}
changeDict = {'param': name, 'value': '"cmsgwms-collector-itb.cern.ch"', 'section': 'Debug'}
confChangesList.append(changeDict)
changeDict = {'param': 'scheddName', 'value': '"crab3@vocms068.cern.ch"', 'section': 'Debug'}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} SUBMITTED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# extraJDL
name = 'extraJDL'
changeDict = {'param': name, 'value': "['My.CMS_ALLOW_OVERFLOW=False', 'My.CRAB_StageoutPolicy=\"remote\"']", 'section': 'Debug'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
statusCode=$?
([ $statusCode -eq 0 ] || [ $statusCode -eq 2 ]) || exit 1
crabCommand getlog "--short --jobids=1 --proxy=$PROXY"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt || echo "extraJDL: Retrieval failure job_out.1.*.txt"
lookFor "JOB AD: CMS_ALLOW_OVERFLOW = false" "${workDir}/results/job_out.1.*.txt" || echo "extraJDL: Retrieval failure JOB AD"
lookFor "JOB AD: CRAB_StageoutPolicy = \\"remote\\"" "${workDir}/results/job_out.1.*.txt" || echo "extraJDL: Retrieval failure JOB AD"
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)
