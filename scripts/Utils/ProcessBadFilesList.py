"""
Peocesses files with info about corrupted/suspicious input files
Gather statistics and optionally report to Rucio
ref. https://github.com/dmwm/CRABServer/issues/7548#issuecomment-1714567989
"""
import os
import json
import pprint
import shutil

ERROR_KINDS = [
    'not a ROOT file',
    'truncated',
]


def main():
    """" """
    # location of files prepared by RetryJobs
    topDir = '/eos/cms/store/temp/user'
    topDir = '.'  # `for testing locally
    # topDir = topDir + '/filechecker'  # currently not used`

    for subDir in ['corrupted']:  # maybe later on add suspicious files, initially ignore them
        totals = {}
        doneFiles = []
        myDir = f"{topDir}/{subDir}"
        newFilesDir = myDir + '/new'
        doneFilesDir = myDir + '/done'
        newFiles = os.listdir(newFilesDir)
        for aFile in newFiles:
            filePath = f"{newFilesDir}/{aFile}"
            result = parse(filePath)
            accumulate(totals, result)
            doneFiles.append(filePath)
        pprint.pprint(totals)

        for file in doneFiles:
            shutil.move(file, doneFilesDir)

    # now that we have the totals for this run of the script, update the
    # daily tally

def parse(file=None):
    """
    returns a dictionary
    result{'DID', 'RSE', 'errorKind'}
    """
    with open(file, 'r', encoding='utf8') as fp:
        info = json.load(fp)
    result = {}
    result['errorKind'] = None
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
    totals is a dictionary with a single key (the DID) pointing to a dictionary of into
    {'DID': { 'numnerOfErrors': count of how many tims a job failed on this
               'RSEs' : [ a list (set) of the RSE's were corruption was detected (sites where jobs ran) ]
            }, ... }
    """
    did = result['DID']
    rse = result['RSE']
    if not did in totals:
        totals[did] = {'errors': 0, 'RSEs': set()}
    totals[did]['errors'] += 1
    totals[did]['RSEs'].add(rse)


main()
