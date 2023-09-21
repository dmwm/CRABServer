"""
Peocesses files with info about corrupted/suspicious input files
Gather statistics and optionally report to Rucio
ref. https://github.com/dmwm/CRABServer/issues/7548#issuecomment-1714567989
"""
import os
import json
import datetime
import shutil


ERROR_KINDS = [
    'not a ROOT file',
    'truncated',
    ]


def main():
    """" description is at line 1 """
    # location of files prepared by RetryJobs
    topDir = '/eos/cms/store/temp/user'
    # topDir = '.'  # `for testing locally
    topDir = f"{topDir}/BadInputFiles"

    for problemType in ['corrupted']:  # maybe later on add suspicious files, initially ignore them
        totals = {}
        doneFiles = []
        myDir = f"{topDir}/{problemType}"
        newFilesDir = myDir + '/new'
        doneFilesDir = myDir + '/done'
        newFiles = os.listdir(newFilesDir)
        for aFile in newFiles:
            filePath = f"{newFilesDir}/{aFile}"
            result = parse(filePath)
            accumulate(totals, result)
            doneFiles.append(filePath)
        print(totals)

        for file in doneFiles:
            shutil.move(file, doneFilesDir)

        # now that we have the totals for this run of the script, update the
        # daily tally
        summariesDir = f"{topDir}/summaries/{problemType}"
        addToDailyLog(logDir=summariesDir, totals=totals)


def addToDailyLog(logDir=None, totals=None):
    """
    dir is the directory where to place the daily Log in JSON format
    totals is a dictionary in the format prepared by accumulate()
    """
    if not totals:
        return
    today = datetime.date.today().strftime('%y%m%d')  # YYMMDD format
    dailyLog = f"{logDir}/{today}-log.json"
    if not os.path.exists(dailyLog):
        logs = {}
    else:
        with open(dailyLog, 'r', encoding='utf-8') as fp:
            logs = json.load(fp)
            # convert lists to sets to collect unique fields
            for did in logs.keys():
                logs[did]['RSEs'] = set(logs[did]['RSEs'])
                logs[did]['kind'] = set(logs[did]['kind'])
    for did, info in totals.items():
        if did in logs:
            # add to existing info
            logs[did]['errors'] += info['errors']
            logs[did]['RSEs'] |= info['RSEs']  # set union
            logs[did]['kind'] |= info['kind']
        else:
            # create new entry
            logs[did] = totals[did]
    # persist new log
    # need to convert set to lists to put in JSON
    for did in logs.keys():
        logs[did]['RSEs'] = list(logs[did]['RSEs'])
        logs[did]['kind'] = list(logs[did]['kind'])
    newLog = dailyLog + '.NEW'
    with open(newLog, 'w', encoding='utf-8') as fp:
        json.dump(logs, fp)
    shutil.move(newLog, dailyLog)


def parse(file=None):
    """
    returns a dictionary
    result{'DID', 'RSE', 'errorKind'}
    """
    with open(file, 'r', encoding='utf8') as fp:
        info = json.load(fp)
    result = {}
    result['errorKind'] = ''
    result['DID'] = info['DID']
    result['RSE'] = info['RSE']
    errorLine = info['message'][1]
    for error in ERROR_KINDS:
        if error in errorLine:
            result['errorKind'] = error
            break
    return result


def accumulate(totals=None, result=None):
    """
    totals is a dictionary with a single key (the DID) pointing to a dictionary of info
    {'DID': { 'numberOfErrors': count of how many tims a job failed on this
              'RSEs' : ( a set listing the RSE's were corruption was detected (sites where jobs ran) ]
              'kind' : ( a set listing the error kinds found for this did, hopefully just 1 !! )
            }, ... }
    """
    did = result['DID']
    rse = result['RSE']
    kind = result['errorKind']
    print("kind= ", kind)
    if did not in totals:
        totals[did] = {'errors': 0, 'RSEs': set(), 'kind': set()}
    totals[did]['errors'] += 1
    totals[did]['RSEs'].add(rse)
    totals[did]['kind'].add(kind)
    print(totals[did]['kind'])


main()
