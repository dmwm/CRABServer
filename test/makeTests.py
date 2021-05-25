"""
An almost standalone python script which creates a set of files for CRAB validation
Requires the testUtils.py file to be placed in same directory
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

from testUtils import writePset, writePset8cores, writeScriptExe,\
    writeConfigFile, writeTestSubmitScript, writeValidationScript

#from testUtils import *

writePset()
writePset8cores()
writeScriptExe()


dummyTestScript = "\nexit 0\n"  #  a test which always returns success
#
# test CRAB Configuration file parameters
#

#=============================
# SECTION GENERAL
#=============================

# transferOutputs
name = 'transferOutputs'
changeDict = {'param': name, 'value': 'False', 'section': 'General'} # default is True
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "JOB AD: CRAB_TransferOutputs = 0" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# transferLogs
name = 'transferLogs'
changeDict = {'param': name, 'value': 'True', 'section': 'General'} # default is False
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "JOB AD: CRAB_SaveLogs = 0" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# activity
name = 'activity'
changeDict = {'param': name, 'value': '"hctestnew"', 'section': 'General'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
#TODO this is not the correct check, need to dig into how activity is passed around
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "JOB AD: CMS_Type = \\"Test\\"" ${workDir}/results/job_out.1.*.txt
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
inFile2 = '/etc/os-release'
changeDict = {'param': name, 'section': 'JobType', 'value': [inFile1, inFile2]}
confChangesList = [changeDict]
testSubmitScript = """
lookInTarFor "^hosts" ${workDir}/inputs/*default.tgz
lookInTarFor "^os-release" ${workDir}/inputs/*default.tgz
"""
validationScript = """
checkStatus ${taskName} SUBMITTED
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
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "^Output files.*: \$" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# outputFiles
name = 'outputFiles'
confChangesList = []
changeDict = {'param': 'disableAutomaticOutputCollection', 'value': 'True', 'section': 'JobType'}
confChangesList.append(changeDict)
changeDict = {'param': 'outputFiles', 'value': '["output.root"]', 'section': 'JobType'}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getoutput --jobids=1"
lookFor "Success in retrieving output_1.root " commandLog.txt
"""
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
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "JOB AD: RequestMemory = 2500" ${workDir}/results/job_out.1.*.txt
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
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "JOB AD: MaxWallTimeMins_RAW = 100" ${workDir}/results/job_out.1.*.txt
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
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "JOB AD: RequestCpus = 8" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# scriptExe
name = 'scriptExe'
changeDict = {'param': name, 'value': '"SIMPLE-SCRIPT.sh"', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = """
lookInTarFor "^SIMPLE-SCRIPT.sh" ${workDir}/inputs/*default.tgz
"""
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "SB CMSRUN starting" ${workDir}/results/job_out.1.*.txt
lookFor "====== arg checking: \$1 = 1" ${workDir}/results/job_out.1.*.txt
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
lookInTarFor "^SIMPLE-SCRIPT.sh" ${workDir}/inputs/*default.tgz
"""
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "SB CMSRUN starting" ${workDir}/results/job_out.1.*.txt
lookFor "====== arg checking: \$1 = 1" ${workDir}/results/job_out.1.*.txt
lookFor "====== arg checking: \$2 = exitCode=666" ${workDir}/results/job_out.1.*.txt
lookFor "====== arg checking: \$3 = gotArgs=Yes" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# sendPythonFolder
name = 'sendPythonFolder'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = """
lookInTarFor "^python/" ${workDir}/inputs/*default.tgz
"""
validationScript = """
checkStatus ${taskName} SUBMITTED
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)


# sendExternalFolder
name = 'sendExternalFolder'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
testSubmitScript = """
lookInTarFor "^external/" ${workDir}/inputs/*default.tgz
"""
validationScript = """
checkStatus ${taskName} SUBMITTED
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

#=============================
# SECTION DATA
#=============================


#=============================
# SECTION SITE
#=============================


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
lookFor "crab3@vocms059.cern.ch" statusLog.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# collector
name = 'collector'
confChangesList = []
changeDict = {'param': name, 'value': '"cmsgwms-collector-itb.cern.ch"', 'section': 'Debug'}
confChangesList.append(changeDict)
changeDict = {'param': 'scheddName', 'value': '"crab3@vocms068.cern.ch"', 'section': 'Debug'}
confChangesList.append(changeDict)
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} SUBMITTED
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)

# extraJDL
name = 'extraJDL'
changeDict = {'param': name, 'value': "['+CMS_ALLOW_OVERFLOW=False', '+CRAB_StageoutPolicy=\"remote\"']", 'section': 'Debug'}
confChangesList = [changeDict]
testSubmitScript = dummyTestScript
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "JOB AD: CMS_ALLOW_OVERFLOW = false" ${workDir}/results/job_out.1.*.txt
lookFor "JOB AD: CRAB_StageoutPolicy = \\"remote\\"" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeTestSubmitScript(testName=name, testSubmitScript=testSubmitScript)
writeValidationScript(testName=name, validationScript=validationScript)
