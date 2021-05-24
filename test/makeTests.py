"""
An almost standalone python script which creates a set of files for CRAB validation
Requires the testUtils.py file to be placed in same directory
The script output is a set of files written to current directory
 A simple PSET.py file to be used in all tests
 A list of crab configuration files, one for each test, named: <testName>.py
 A list of checking script to be executed to validate each test, named:  <testName>.sh

 Tasks needs to be submitted from the directory where they have been
  created, since they may need additional external files created by this script.

 Checking scripts take a task name as only argument, and exit with code
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

from testUtils import writePset, writeConfigFile, writeValidationScript

writePset()

#
# test CRAB Configuration file parameters
#

#=============================
# SECTION GENERAL
#=============================

# transferOutputs
name ='transferOutputs'
changeDict = {'param': name, 'value': 'False', 'section': 'General'}
confChangesList = [changeDict]
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "disabled" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

# transferLogs
name ='transferLogs'
changeDict = {'param': name, 'value': 'False', 'section': 'General'}
confChangesList = [changeDict]
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "disabled" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

#=============================
# SECTION JOB TYPE
#=============================

# inputFiles
#TODO to be filled
name = 'inputFiles'
inFile1 = '/etc/hosts'
inFile2 = '/etc/os-release'
changeDict = {'param': name, 'section': 'JobType', 'value': [inFile1, inFile2]}
confChangesList = [changeDict]
#TODO need a way to check sandbox content
validationScript = """
checkStatus ${taskName} SUBMITTED
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

# disableAutomaticOutputCollection
name = 'disableAutomaticOutputCollection'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "^Output file : \$" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

# outputFiles
name ='outputFiles'
confChangesList = []
changeDict = {'param': 'disableAutomaticOutputCollection', 'value': 'True', 'section': 'JobType'}
confChangesList.append(changeDict)
changeDict = {'param': 'outputFiles', 'value': '"output.root"', 'section': 'JobType'}
confChangesList.append(changeDict)
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getoutput --jobids=1"
lookFor "Success in retrieving output_1.root " commandLog.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

# allowUndistributedCMSSW
name ='allowUndistributedCMSSW'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
validationScript = """
checkStatus ${taskName} SUBMITTED
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

# maxMemoryMB
name = 'maxMemoryMB'
confChangesList = []
changeDict = {'param': name, 'value': '2500', 'section': 'JobType'}
confChangesList.append(changeDict)
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "JOB AD: RequestMemory = 2500" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

# maxJobRuntimeMin
name = 'maxJobRuntimeMin'
changeDict = {'param': name, 'value': '100', 'section': 'JobType'}
confChangesList = [changeDict]
validationScript = """
checkStatus ${taskName} COMPLETED
crabCommand getlog "--short --jobids=1"
lookFor "Retrieved job_out.1.*.txt" commandLog.txt
lookFor "JOB AD: MaxWallTimeMins_RAW = 100" ${workDir}/results/job_out.1.*.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

# numCores
#TODO to be filled

# scriptExe
#TODO to be filled

# scriptArgs
#TODO to be filled (used ony with scriptExe)

# sendPythonFolder
name = 'sendPythonFolder'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
#TODO need a way to check sandbox content
validationScript = """
checkStatus ${taskName} SUBMITTED
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)


# sendExternalFolder
name = 'sendExternalFolder'
changeDict = {'param': name, 'value': 'True', 'section': 'JobType'}
confChangesList = [changeDict]
#TODO need a way to check sandbox content
validationScript = """
checkStatus ${taskName} SUBMITTED
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
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

# extraJDL
# to be filled

# scheddName
name = 'scheddName'
confChangesList = []
changeDict = {'param': name, 'value': '"crab3@vocms059.cern.ch"', 'section': 'Debug'}
confChangesList.append(changeDict)
validationScript = """
checkStatus ${taskName} SUBMITTED
lookFor "crab3@vocms059.cern.ch" statusLog.txt
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

# collector
name = 'collector'
confChangesList = []
changeDict = {'param': name, 'value': '"cmsgwms-collector-itb.cern.ch"', 'section': 'Debug'}
confChangesList.append(changeDict)
changeDict = {'param': 'scheddName', 'value': '"crab3@vocms068.cern.ch"', 'section': 'Debug'}
confChangesList.append(changeDict)
validationScript = """
checkStatus ${taskName} SUBMITTED
"""
writeConfigFile(testName=name, listOfDicts=confChangesList)
writeValidationScript(testName=name, validationScript=validationScript)

