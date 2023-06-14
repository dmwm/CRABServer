#!/usr/bin/python3
"""
Standalone (Rucio and pandas dependent) script to check status
of pending CRAB tape recalls. Needs also access to CRAB REST
usage:  python3 CheckTapeRecall.py
example output:
"""

import os
import sys
import time
import pandas as pd

from RESTInteractions import CRABRest
from ServerUtilities import encodeRequest

def main():
    """
        get all rules for this account, find pending ones,
        order, pretty format, print them and write an HTML file
    """
    rucio, crab = ensureEnvironment()
    account = 'crab_tape_recall'

    # get rules for this account
    ruleGen = rucio.list_replication_rules({'account': account})
    rules = list(ruleGen)
    print(f"{len(rules)} rules exist for account: {account}")

    # make a DataFrame
    df = pd.DataFrame(rules)
    print(df.groupby('state').size())  # count by state

    # transform created_at to number of days
    today = pd.Timestamp.now()
    df['days'] = df.apply(lambda x: (today-x['created_at']).days, axis=1)

    # select non-OK states
    stuck = df[df['state'] == 'STUCK'].sort_values(by=['days'], ascending=False)
    replicating = df[df['state'] == 'REPLICATING'].sort_values(by=['days'], ascending=False)

    # combine all pending rules in a single dataframe
    pending = pd.concat([stuck, replicating]).reset_index(drop=True)

    # use standard compact format (OK/Rep/Stucl) for lock counts
    pending['locks'] = pending.apply(lambda x: f"{x.locks_ok_cnt}/{x.locks_replicating_cnt}/{x.locks_stuck_cnt}", axis=1)

    # extract CRAB user name from rule comment (ouch ! FRAGILE !)
    pending['user'] = pending.apply(lambda x: findUserNameForRule(rucio, x.id), axis=1)

    # add and URL pointing to rule in Rucio UI
    urlBase = '<a href="https://cms-rucio-webui.cern.ch/rule?rule_id=%s">%s</a>'
    pending['idUrl'] = pending.apply(lambda x: (urlBase % (x.id, x.id)), axis=1)

    # add tape locations
    print("finding tape source for all pending rules (takes some time...)")
    pending['tape'] = pending.apply(lambda x: findTapesForRule(rucio, x.id), axis=1)

    print('Add (DBS) dataset name ...')
    pending['dataset'] = pending.apply(lambda x: findDatasetForRule(rucio, x.id), axis=1)
    print('... and size')
    pending['size'] = pending.apply(lambda x: findDatasetSize(rucio, x.dataset), axis=1)
    print("Done!")

    print('compute a short Url for each dataset')
    pending['datasetUrl'] = pending.apply(lambda x: createDatasetUrl(x.dataset), axis=1)

    print('find tasks using these rules')
    pending['tasks'] = pending.apply(lambda x: findTasksForRule(crab, x.id), axis=1)
    print("Done!")

    print('compute a short Url for each task')
    pending['taskUrls'] = pending.apply(lambda x: createTaskUrl(x.tasks), axis=1)
    print("Done!")

    # select interesting columns
    selected = pending[['id', 'state', 'user', 'locks', 'days', 'tape', 'size', 'datasetUrl', 'taskUrls']]
    rulesToPrint = selected.rename(columns={'locks': 'locks ok/rep/st', 'size': 'size TB'})
    print(rulesToPrint.to_string())

    # create an HTML table
    selected = pending[['idUrl', 'state', 'user', 'locks', 'days', 'tape', 'size', 'datasetUrl', 'taskUrls']]
    renamed = selected.rename(columns={'locks': 'locks ok/rep/st', 'size': 'size TB'})
    rulesToHtml = renamed.to_html(escape=False)
    beginningOfDoc = '<!DOCTYPE html>\n<html>\n'
    header = htmlHeader()
    now = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    title = f"\n<center><b>Status of CRAB Tape Recall rules at {now}</b></center><hr>\n"
    endOfDoc = '\n</html>'
    with open('RecallRules.html', 'w', encoding='utf-8') as fh:
        fh.write(beginningOfDoc)
        fh.write(header)
        fh.write(title)
        fh.write(rulesToHtml)
        fh.write(endOfDoc)


def createTaskUrl(tasks):
    """
    create an URL reference string for HTML pointing to task info in CRABRest UI
    use ahortened task name (timstamp:user) as link in the table
    """
    urlBase = '<a href="https://cmsweb.cern.ch/crabserver/ui/task/%s">%s</a>'
    hrefList = []
    for task in tasks:
        taskHeader = '_'.join(task.split('_')[0:2])
        href = urlBase % (task, taskHeader)
        hrefList.append(href)
    return hrefList


def createDatasetUrl(dataset):
    """
    create an URL reference string for HTML pointing to dataset info in DAS Web UI
    use shortened dataset name as link in the table
    """
    urlBase = '<a href="https://cmsweb.cern.ch/das/ui/task/%s">%s</a>'
    dasUrl = f"https://cmsweb.cern.ch/das/request?view=list&instance=prod/global&input={dataset}"

    # shorten dataset name picking only first word of A and B in /A/B/Tier
    # use either _ or - as word separators
    primaryDS = dataset.split('/')[1]
    processedDS = dataset.split('/')[2]
    datatier = dataset.split('/')[3]
    prim = primaryDS.replace('_','-').split('-')[0]
    proc = processedDS.replace('_','-').split('-')[0]
    shortDS = '/'.join(['', prim, proc, datatier])

    href = f'<a href="{dasUrl}">{shortDS}</a>'
    return href


def findUserNameForRule(rucioClient=None, ruleId=None):
    """
    find which username was this rule created for
    takes care of different formats of comments field in rule
    """
    if not ruleId:
        return None
    rule = rucioClient.get_replication_rule(ruleId)
    comment = rule['comments']
    if not comment:
        return None
    tokens = comment.split(' ')
    if tokens[0] == 'Staged':
        # comment of form: Staged from tape for <username>
        user = tokens[4]
    elif tokens[0] == 'Recall':
        # comment of form: Recall <n> TBytes for user: <username> dataset: <dataset>
        user = tokens[5]
    else:
        user = None
    return user


def findTapesForRule(rucioClient=None, ruleId=None):
    """
    Returns a list of tape RSE's where the files which are object of the rule are located
    Assumes that all files in the container described in the rule have the same origin
    so it will be enough to look up the first file
    """
    rule = rucioClient.get_replication_rule(ruleId)
    aFile = next(rucioClient.list_files(scope=rule['scope'], name=rule['name']))
    aDID = {'scope': aFile['scope'], 'name': aFile['name']}
    aReplica = next(rucioClient.list_replicas([aDID]))
    tapes = []
    for rse in aReplica['rses']:
        if 'Tape' in rse:
            tapes.append(rse.replace("_Tape", ""))
    return tapes

def findDatasetForRule(rucioClient=None, ruleId=None):
    """
    returns the DBS dataset name
    if rule refers to a dataset in cms scope, this is trivial, otherwise
    assume all files in the container described in the rule have same origin
    so it will be enough to look up the first one:
    """
    dataset = None
    datasets = []
    rule = rucioClient.get_replication_rule(ruleId)
    if rule['scope'] == 'cms':
        dataset = rule['name']
    else:
        aFile = next(rucioClient.list_files(scope=rule['scope'], name=rule['name']))
        # to find original dataset need to travel up from file to block to dataset
        # at the container level and make sure to pick scope cms:
        block = next(rucioClient.list_parent_dids(scope=aFile['scope'], name=aFile['name']))
        if block:
            datasets = rucioClient.list_parent_dids(scope=block['scope'], name=block['name'])
        for ds in datasets:
            if ds['scope'] == 'cms':
                dataset = ds['name']
                break
    return dataset

def findTasksForRule(crabRest=None, ruleId=None):
    """
    returns the list of task names which have stored the given Rucio
    rule as ddmreqid in DB Tasks table
    """
    data = {'subresource': 'taskbyddmreqid', 'ddmreqid': ruleId}
    res = crabRest.get(api='task', data=encodeRequest(data))
    tasks = res[0]['result']  # for obscure reasons this has the form [['task1'],['task2']...]
    taskList = [t[0] for t in tasks]
    return taskList

def findDatasetSize(rucioClient=None, dataset=None):
    """
    returns dataset size in TB as a string in format
    1234 if size > 1TB, 0.123 if size < 1TB)
    """
    if not dataset:
        return None
    info = rucioClient.get_did(scope='cms', name=dataset, dynamic='DATASET')
    datasetBytes = info['bytes']
    teraBytes = datasetBytes//1e12
    size = f"{datasetBytes/1e12:.0f}" if teraBytes > 0 else f"{datasetBytes/1e12:.3f}"
    return size

def ensureEnvironment():
    """ make sure we can run Rucio client and talk with CRAB"""
    #if os.getenv("CMSSW_BASE"):
    #    print("Must use a shell w/o CMSSW environent")
    #    sys.exit()
    try:
        from rucio.client import Client
    except ModuleNotFoundError:
        print("Setup Rucio first via:\n" +
              "export PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/slc7/py3/current/lib/python3.6/site-packages/\n" +
              "export RUCIO_HOME=/cvmfs/cms.cern.ch/rucio/current/\n" +
              "export RUCIO_ACCOUNT=`whoami`")
        sys.exit()
    # make sure Rucio client is initialized, this also ensures X509 proxy
    rucio = Client()

    proxy = os.environ['X509_USER_PROXY']
    crab = CRABRest(hostname='cmsweb-testbed.cern.ch', localcert=proxy,
                    localkey=proxy, userAgent='CheckTapeRecall')
    crab.setDbInstance('prod')

    return rucio, crab

def htmlHeader():
    """
    something to make a prettier HTML table, stolen from Ceyhun's
    https://cmsdatapop.web.cern.ch/cmsdatapop/eos-path-size/size.html
    """
    head = """<head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- prepared using https://datatables.net/download/ -->
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/v/dt/jq-3.6.0/jszip-2.5.0/dt-1.12.1/b-2.2.3/b-colvis-2.2.3/b-html5-2.2.3/b-print-2.2.3/cr-1.5.6/date-1.1.2/kt-2.7.0/rr-1.2.8/sc-2.0.6/sb-1.3.3/sp-2.0.1/sl-1.4.0/sr-1.1.1/datatables.min.css"/>

    <!--  Please do not delete below CSSes, important for pretty view -->
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/v/dt/dt-1.12.1/datatables.min.css"/>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.11.4/css/dataTables.bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.2.2/css/buttons.bootstrap.min.css">

    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {
	        font-family: 'Trebuchet MS', sans-serif;
	        font-size: 12px;
        }
        /* Search bar */
        .dataTables_filter input {
          border: 7px solid Tomato;
          width: 400px;
          font-size: 14px;
          font-weight: bold;
        }
        table td {
            word-break: break-all;
        }
        /* From 7th column, align to left */
        table td:nth-child(n+7) {
            text-align: left;
        }
        /* First row bold */
        table td:nth-child(1) {
            font-weight: bold;
        }
        /* Path title */
        table th:nth-child(1) {
            color: #990000;
        }
        /* Header rows, total and titles, align left */
        table th:nth-child(n+2) {
            text-align: left !important;
            color:  #990000 !important;
        }
        /* Different background color for even and odd columns  */
        .table.display tr:nth-child(even) {
          /* background-color: #dddfff; */
        }
        /* No carriage return for values, no break lines */
        table tr td {
          width: 1%;
          white-space: nowrap;
        }
        /* button */
        div.dt-buttons {
          float: right;
        }
        .dt-button.buttons-columnVisibility.active {
              background: #FF0000 !important;
              color: white !important;
              opacity: 0.5;
           }
        .dt-button.buttons-columnVisibility {
              background: black !important;
              color: white !important;
              opacity: 1;
           }

        div.dataTables_wrapper {
          margin-bottom: 3em;
        }

        <!--         Divider   -->
        .divider {
<!--            width:500px;-->
            text-align:center;
        }
        .divider hr {
            margin-left:auto;
            margin-right:auto;
            width:100%;
        }
    </style>
</head>"""
    return head



if __name__ == '__main__':
    main()
