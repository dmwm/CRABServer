#!/usr/bin/python3

import os
import subprocess

# this identifies the line with the resubmit information
RESUBMIT_TAG = "DEBUG:DagmanResubmitter:Task info: "

def parseResubmissionLog(fName, verbose=False):
    """
    input: fName : string: path of a log containing Resubmit commands
    output: changedDict : dict : recap of what's done
            keys: noJids, changedBList, changedWList, (booleans)
            changedMaxMem, changedMaxTime, (int = number of time changed from initial)
            initialMaxMem, initialMaxTime, finalMaxMem, finalMaxTime (integers)
    """
    # find resubmission lines
    cmd = f"grep  {RESUBMIT_TAG} {fName}"
    result = subprocess.run(cmd, shell=True, capture_output=True, encoding='utf-8')
    lines = result.stdout.rstrip().split('\n')
    # get initial values from first line
    taskDict = eval(lines[0].split(RESUBMIT_TAG)[1])
    taskName = taskDict['tm_taskname']
    initialMaxMem = taskDict['tm_maxmemory']
    initialMaxTime = taskDict['tm_maxjobruntime']
    initialBlackList = taskDict['tm_site_blacklist']
    initialWhiteList = taskDict['tm_site_whitelist']
    # init counters and flags before looping
    noJids = False
    changedMaxMem = 0
    changedMaxTime = 0
    finalMaxMem = 0
    finalMaxTime = 0
    changedBList = False
    changedWList = False
    # parse all lines
    for line in lines:
        taskDict = eval(line.split(RESUBMIT_TAG)[1])
        if not 'resubmit_jobids' in taskDict or not taskDict['resubmit_jobids']:
            noJids = True
        if taskDict['resubmit_maxmemory']:
            resubMaxMem = taskDict['resubmit_maxmemory']
            if resubMaxMem > initialMaxMem and resubMaxMem > finalMaxMem:
                changedMaxMem += 1
                finalMaxMem = resubMaxMem
        if taskDict['resubmit_maxjobruntime']:
            resubMaxTime = taskDict['resubmit_maxjobruntime']
            if resubMaxTime > initialMaxTime and resubMaxTime > finalMaxTime:
                changedMaxTime += 1
                finalMaxTime = resubMaxTime
        if taskDict['resubmit_site_blacklist'] and set(initialBlackList) != set(taskDict['resubmit_site_blacklist']):
            changedBList = True
            if verbose:
                print(f"Black List changed from:\n{initialBlackList}\nto:\n{taskDict['resubmit_site_blacklist']}")
        if taskDict['resubmit_site_whitelist'] and set(initialWhiteList) != set(taskDict['resubmit_site_whitelist']):
            changedWList = True
            if verbose:
                print(f"White List changed from:\n{initialWhiteList}\nto:\n{taskDict['resubmit_site_whitelist']}")
            #print(initialWhiteList)
            #print(taskDict['resubmit_site_whitelist'])
    if noJids:
        print(f"**** Task resubmitted w/o list of jobids: {taskName} ****")
    if changedMaxMem or changedMaxTime or changedBList or changedWList:
        changedDict = {
            'changedMaxMem' : changedMaxMem, 'initialMaxMem': initialMaxMem, 'finalMaxMem': finalMaxMem,
            'changedMaxTime': changedMaxTime, 'initialMaxTime': initialMaxTime, 'finalMaxTime': finalMaxTime,
            'changedBList': changedBList, 'changedWList': changedWList,
        }
    else:
        changedDict = {}

    return changedDict

def parseDir(directory, verbose=False):
    # count logs in this directory
    if verbose:
        print(f"Processing directory {directory}")
    cmd = f"ls -1 {directory}| wc -l"
    result = subprocess.run(cmd, shell=True, capture_output=True, encoding='utf-8')
    nLogs=int(result.stdout.rstrip())
    if verbose:
        print(f"Parsing {nLogs} logfiles")

    # find task logs where DagmanResubmitter was used
    cmd =f"grep -l 'DagmanResubmitter' {directory}/*"
    result = subprocess.run(cmd, shell=True, capture_output=True, encoding='utf-8')
    # beware empty result
    if not result.stdout:
        logs = []
    else:
        logs=result.stdout.rstrip().split('\n')
    #print(logs)

    # prepare counters
    nRes = len(logs)
    nChg = 0
    nMem = 0
    nTim = 0
    nTM  = 0
    nWL = 0
    nBL = 0
    nBW = 0

    # parse one log at a time
    for log in logs:
        #print(log.split('.')[0])
        changedDict = parseResubmissionLog(log)
        if changedDict:
            nChg += 1
            cMem = changedDict['changedMaxMem']
            iMem = changedDict['initialMaxMem']
            fMem = changedDict['finalMaxMem']
            cTim = changedDict['changedMaxTime']
            iTim = changedDict['initialMaxTime']
            fTim = changedDict['finalMaxTime']
            #print(f"{cMem} {iMem} {fMem} {cTim} {iTim} {fTim} {changedDict['changedBList']} {changedDict['changedWList']}")
            if cMem:
                nMem += 1
                if verbose:
                    print(f"MaxMem changed from {iMem} to {fMem}")
            if cTim:
                nTim += 1
                if verbose:
                    print(f"MaxTim changed from {iTim} to {fTim}")
            if cMem and cTim:
                nTM += 1
            if changedDict['changedBList']:
                nBL += 1
            if changedDict['changedWList']:
                nWL += 1
            if changedDict['changedBList'] and changedDict['changedWList']:
                nBW += 1

    if verbose:
        # print summary
        print(f"tasks with resubmissions = {nRes}")
        if not nChg:
            print("No changes requested")
        else:
            print(f"tasks with changes in resubmissions: {nChg}")
            print(f"changed MaxMem: {nMem}")
            print(f"changed MaxTim: {nTim}")
            print(f"changed both Time and Mem: {nTM}")
            print(f"changed Black List: {nBL}")
            print(f"changed White List: {nWL}")
            print(f"changed BothBW Lists: {nBW}")

    return (nLogs, nRes, nChg, nMem, nTim, nTM, nBL, nWL, nBW)

def parseAllDirs(initialDir,verbose=False):

    os.chdir(initialDir)

    # Initialize counters
    users = 0
    tasks = 0
    resub = 0
    change = 0
    maxMem = 0
    maxTim = 0
    maxTM = 0
    bList = 0
    wList = 0
    BWList = 0

    # scan all directories parsing logs as they are found
    dirs = os.listdir(initialDir)
    if 'recurring' in dirs:
        dirs.remove('recurring')
    users = len(dirs)
    for userName in dirs:
        (nTasks, nRes, nChg, nMem, nTim, nTM, nBL, nWL, nBW) = parseDir(userName, verbose)
        tasks += nTasks
        resub += nRes
        change += nChg
        maxMem += nMem
        maxTim += nTim
        maxTM += nTM
        bList += nBL
        wList += nWL
        BWList += nBW

    # print grand summary
    print(f"Total users                 : {users}")
    print(f"Total tasks                 : {tasks}")
    print(f"Tasks with resubmissions    : {resub}")
    print(f"Task with changes in resubm : {change}")
    print(f"Changes to MaxMemory        : {maxMem}")
    print(f"Changes to MaxRunningTime   : {maxTim}")
    print(f"Changes to both Mem and Time: {maxMem}")
    print(f"Changes to site Black List  : {bList}")
    print(f"Changes to site White List  : {wList}")
    print(f"Changes to both B and W List: {BWList}")

def main():
    # process one task subdirectory (i.e. a user specific one)
    verbose=False
    if os.environ.get('Verbose'):
        verbose = True
    print(verbose)
    initialDir = os.getcwd()
    initialDir = '/home/belforte/STEFANO/WORK/Jupyter/twlogs/tasks'
    parseAllDirs(initialDir, verbose)

if __name__ == "__main__":
    main()
