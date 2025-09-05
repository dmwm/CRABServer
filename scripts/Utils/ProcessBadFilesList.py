"""
Peocesses files with info about corrupted/suspicious input files
Gather statistics and optionally report to Rucio
ref. https://github.com/dmwm/CRABServer/issues/7548#issuecomment-1714567989

writes these files in current directory
BadInputFiles.html
TruncatedFiles.list
NotRootFiles.list

"""
import os
import json
import datetime
import time
import shutil
import subprocess
import pandas as pd

ERROR_KINDS = [
    'not a ROOT file',
    'truncated',
    ]

DAYS = 3  # count errors over last DAYS for the final summary


def main():
    """" description is at line 1 """
    # location of files prepared by RetryJobs
    topDir = '/eos/cms/store/temp/user'
    #topDir = '..'  # `for testing locally
    topDir = f"{topDir}/BadInputFiles"

    totals = []
    for problemType in ('truncated', 'corrupted', 'suspicious'):
        doneTasks = []
        reportDir = f"{topDir}/{problemType}"
        newReportsDir = reportDir + '/new'
        doneReportsDir = reportDir + '/done'
        fakeReportsDir = reportDir + '/falsePositives'
        for dir in [reportDir, newReportsDir, doneReportsDir, fakeReportsDir]:
            if not os.path.exists(dir):
                os.makedirs(dir)
        troubledTasks = os.listdir(newReportsDir)
        for task in troubledTasks:
            if 'sciaba' in task:
                continue
            taskDir = os.path.join(newReportsDir, task)
            doneTaskDir = os.path.join(doneReportsDir, task)
            # in doneReportsDir we move one file at a time in a subdir named as the task
            if not os.path.exists(doneTaskDir):
                os.makedirs(doneTaskDir)
            # false positive tasks may have zilion of files, move entired directory but beware overwrite
            fakeTaskDir = os.path.join(fakeReportsDir, task)
            sub = 1
            while os.path.exists(fakeTaskDir):
                fakeTaskDir = fakeTaskDir + f"-{sub}"
                sub += 1
            # count files in the taskDir, EOS on Fuse has a limit so use eos command
            result = subprocess.run(f"eos ls {taskDir} |wc -l",shell=True, stdout=subprocess.PIPE, check=False)
            nBadFileReports = int(result.stdout.decode('utf-8'))
            if nBadFileReports > 30 and not problemType == 'truncated':
                # likely code, not files, can't fix whole datasets
                # but we always trust truncated files to be really bad
                shutil.move(taskDir, fakeTaskDir)
                continue
            newFiles = os.listdir(taskDir)
            for aFile in newFiles:
                filePath = f"{taskDir}/{aFile}"
                result = parse(filePath)
                accumulate(totals, result)
                shutil.move(filePath, doneTaskDir)
            doneTasks.append(taskDir)


    if not totals:
        print("No Bad File Report found. Exit")
        return

    # now that we have the totals for this run of the script, update the
    # daily tally
    summaryDir = f"{topDir}/summaries/"
    addToDailyLog(logDir=summaryDir, totals=totals)

    everything = getListOfBadDIDsAsDataFrame(summaryDir, days=DAYS)
    truncated, rest = selectDataFrameByErrorKind(everything, 'truncated')
    notRoot, rest = selectDataFrameByErrorKind(rest, 'not a ROOT file')
    unknown, rest = selectDataFrameByErrorKind(rest, 'unknown')
    misc = rest

    df = pd.concat([truncated, notRoot, unknown, misc]).reset_index(drop=True)
    if not df.empty:
        writeOutHtmlTable(df)

    # write out "clearly bad files" Lists
    # limit to at least 3 errors (avoid cases where automatic retry solved)
    if not truncated.empty:
        topTrunc= truncated[truncated['errors'] > 3]
        with open('TruncatedFiles.list', 'w', encoding='utf-8') as fh:
            fh.write(topTrunc['DID'].to_csv(header=False, index=False))
    if not notRoot.empty:
        topNotRoot = notRoot[notRoot['errors'] > 3]
        with open('NotRootFiles.list', 'w', encoding='utf-8') as fh:
            fh.write(topNotRoot['DID'].to_csv(header=False, index=False))

    # also suspicious files list, but limit to at least 3 errors (avoid cases where
    # CRAB 3 automatic retries solved)
    if not unknown.empty:
        suspicious = unknown[unknown['errors'] > 3]
        with open('SuspiciousFiles.list', 'w', encoding='utf-8') as fh:
            fh.write(suspicious['DID'].to_csv(header=False, index=False))



def selectDataFrameByErrorKind(df, errorKind):
    selected = df[df['kind'] == {errorKind}].sort_values(by=['errors'], ascending=False)
    rest = df[df['kind'] != {errorKind}].sort_values(by=['errors'], ascending=False)
    return selected, rest

def writeOutHtmlTable(dataFrame):

    beginningOfDoc = '<!DOCTYPE html>\n<html>\n'
    header = htmlHeader()
    now = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    title = f"\n<center><b>Bad Input Files count over last {DAYS} days - (at {now}</b></center><hr>\n"
    endOfDoc = '\n</html>'
    table=dataFrame[['kind','errors', 'DID']].to_html()
    with open('BadInputFiles.html', 'w', encoding='utf-8') as fh:
        fh.write(beginningOfDoc)
        fh.write(header)
        fh.write(title)
        fh.write(table)
        fh.write(endOfDoc)

def getListOfBadDIDsAsDataFrame(summaryDir, days):
    listOfDicts = []
    for summary in sorted(os.listdir(summaryDir))[-days:]:
        with open(os.path.join(summaryDir, summary), 'r', encoding='utf-8') as fh:
            totals = json.load(fh)
            # convert lists to sets to collect unique fields
            for did in totals:
                did['RSEs'] = set(did['RSEs'])
                did['kind'] = set(did['kind'])
            listOfDicts = addTotals(listOfDicts, totals)
    reportDF=pd.DataFrame(listOfDicts)
    return reportDF


def parse(file=None):
    """
    parse error report written by PostJob and returns a dictionary
    result{'DID', 'RSE', 'errorKind'}
    """
    result = {}
    try:
        with open(file, 'r', encoding='utf8') as fp:
            info = json.load(fp)
    except Exception:  # pylint: disable=broad-except
        print(f"Failed to parse {file}")
        return result
    if '/store/group' in info['DID'] or '/store/user' in info['DID'] and \
        not 'rucio' in info['DID']:
        return result
    if info['DID'] == "cms:NotAvailable":
        return result
    # make sure thre's nothing after .root since sometimes we get LFN's of the form
    # cms:/store/data/.../AB8F5CB8-2B5B-1945-967F-89EDEC3346AD.root?source=glow
    did = info['DID']
    did = did.split('.root')[0] + '.root'
    result['DID'] = did
    result['RSE'] = info['RSE']
    result['errorKind'] = 'unknown'
    errorLine = info['message'][1]
    for error in ERROR_KINDS:
        if error in errorLine:
            result['errorKind'] = error
            break
    return result


def accumulate(totals=None, result=None):
    """
    totals is a list of dictionaries with same keys as result, plus 'errors' to count errors but
    where values are accumulated: error count is increased, RSE and kind become sets
    [{'DID': the DID,
      'numberOfErrors': count of how many tims a job failed on this
      'RSEs' : ( a set listing the RSE's were corruption was detected (sites where jobs ran) ]
      'kind' : ( a set listing the error kinds found for this did, hopefully just 1 !! )
     }, {}, .... ]
    """
    if not result:
        return
    did = result['DID']
    rse = result['RSE']
    kind = result['errorKind']
    found = False
    for didDict in totals:
        if didDict['DID'] == did:
            didDict['RSEs'].add(rse)
            didDict['kind'].add(kind)
            didDict['errors'] += 1
            found = True
            break
    if not found:
        didDict={}
        didDict['DID'] = did
        didDict['errors'] = 1
        didDict['kind'] = {kind}
        didDict['RSEs'] = {rse}
        totals.append(didDict)


def addToDailyLog(logDir=None, totals=None):
    """
    dir is the directory where to place the daily Log in JSON format
    totals is a list of dictionaries in the format prepared by accumulate()
    with keys: DID, RSEs, errors, kind

    REWRITE: read json list of dicts, add current lit of dicts
    using slightly modification of accumulate. save as json.

    FACTORIZE: use a helper function to add list of dicts that can
    be used in getListOfBadDIDs to combine several logs before turning to a dataframe
    """
    if not totals:
        return
    today = datetime.date.today().strftime('%y%m%d')  # YYMMDD format
    dailyLog = f"{logDir}/{today}-log.json"
    if not os.path.exists(dailyLog):
        logs = []
    else:
        with open(dailyLog, 'r', encoding='utf-8') as fp:
            logs = json.load(fp)
            #import pdb; pdb.set_trace()
            # convert lists to sets to collect unique fields
            for did in logs:
                did['RSEs'] = set(did['RSEs'])
                did['kind'] = set(did['kind'])
    newTotals = addTotals(logs, totals)
    # need to convert set to lists to put in JSON
    for did in newTotals:
        did['RSEs'] = list(did['RSEs'])
        did['kind'] = list(did['kind'])
    newLog = dailyLog + '.NEW'
    with open(newLog, 'w', encoding='utf-8') as fp:
        json.dump(newTotals, fp)
    shutil.move(newLog, dailyLog)


def addTotals(tot1, tot2):
    """
    adds two list of dictionaries with keys: DID, RSEs, kind, errors
    by combining same DID into a single record
    returns the new list
    """
    listOfNewDicts = []
    # combine when possible
    for dict1 in tot1:
        for dict2 in tot2:
            if dict2['DID'] == dict1['DID']:
                # combine
                dict1['RSEs'] |= dict2['RSEs']  # set union
                dict1['kind'] |= dict2['kind']  # set union
                dict1['errors'] += dict2['errors']
                break
        listOfNewDicts.append(dict1)
    # add those in tot2 w/o match in tot1
    for dict2 in tot2:
        found =False
        for dict1 in tot1:
            if dict1['DID'] == dict2['DID']:
                found = True
                break
        if not found:
            listOfNewDicts.append(dict2)

    return listOfNewDicts


def htmlHeader():
    """
    something to make a prettier HTML table, stolen from Ceyhun's
    https://cmsdatapop.web.cern.ch/cmsdatapop/eos-path-size/size.html
    and trimmed down a lot
    """
    head = """<head>
    <!-- prepared using https://datatables.net/download/ -->
    <link rel="stylesheet" type="text/css"
     href="https://cdn.datatables.net/v/dt/jq-3.6.0/jszip-2.5.0/dt-1.12.1/b-2.2.3/b-colvis-2.2.3/b-html5-2.2.3/b-print-2.2.3/cr-1.5.6/date-1.1.2/kt-2.7.0/rr-1.2.8/sc-2.0.6/sb-1.3.3/sp-2.0.1/sl-1.4.0/sr-1.1.1/datatables.min.css"/>

    <!--  Please do not delete below CSSes, important for pretty view -->
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/v/dt/dt-1.12.1/datatables.min.css"/>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.11.4/css/dataTables.bootstrap.min.css">

    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {
            font-family: 'Trebuchet MS', sans-serif;
            font-size: 12px;
        }
        table td {
            word-break: break-all;
        }
        /* First row bold */
        table td:nth-child(1) {
            font-weight: bold;
        }
        /* Header rows, total and titles, align left */
        table th:nth-child(n+2) {
            text-align: left !important;
            color:  #990000 !important;
        }
        /* First column color */
        table th:nth-child(1) {
            color: #990000;
        }
        /* No carriage return for values, no break lines */
        table tr td {
          white-space: nowrap;
        }
    </style>
</head>"""
    return head


if __name__ == '__main__':
    main()
