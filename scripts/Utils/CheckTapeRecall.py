#!/usr/bin/python3
"""
Standalone (Rucio and pandas dependent) script to check status
of pending CRAB tape recalls. Needs also access to CRAB REST
usage:  python3 CheckTapeRecall.py
example output:
"""

import sys
import time
import logging
import datetime
import json
from socket import gethostname

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from RESTInteractions import CRABRest
from ServerUtilities import encodeRequest

WORKDIR = '/data/srv/monit/'
LOGDIR = '/data/srv/monit/logs/'
LOGFILE = f'GenMonit-{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

def readpwd():
    """
    Reads password from disk
    """
    with open("/data/certs/monit.d/MONIT-CRAB.json", encoding='utf-8') as f:
        credentials = json.load(f)
    return credentials["url"], credentials["username"], credentials["password"]
MONITURL, MONITUSER, MONITPWD = readpwd()

def send(document):
    """
    sends this document to Elastic Search via MONIT
    the document may contain InfluxDB data, but those will be ignored unless the end point
    in MONIT is changed. See main code body for more
    Currently there is no need for using InfluxDB, see discussion in
    https://its.cern.ch/jira/browse/CMSMONIT-72?focusedCommentId=2920389&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-2920389
    :param document:
    :return:
    """
    return requests.post(f"{MONITURL}",
                        auth=HTTPBasicAuth(MONITUSER, MONITPWD),
                         data=json.dumps(document),
                         headers={"Content-Type": "application/json; charset=UTF-8"},
                         verify=False
                         )


def sendAndCheck(document):
    """
    commend the `##PROD` section when developing, not to duplicate the data inside elasticsearch
    """
    ## DEV
    # print(type(document), document)
    # PROD
    response = send(document)
    msg = 'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)
    print(msg)

def main():
    """
        get all rules, find pending ones,
        order, pretty format, print them and write an HTML file
    """

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler1 = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler1)
    handler2 = logging.FileHandler(LOGDIR + LOGFILE)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s",
                                  datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler2.setFormatter(formatter)
    logger.addHandler(handler2)
    logger.setLevel(logging.INFO)

    rucio, crab = ensureEnvironment(logger)
    activity = "Analysis TapeRecall"

    # get rules for this activity
    ruleGen = rucio.list_replication_rules({'activity': activity})
    rules = list(ruleGen)
    msg = f"{len(rules)} rules exist for activity: {activity}"
    logger.info(msg)

    # make a DataFrame
    df = pd.DataFrame(rules)
    logger.info(df.groupby('state').size())  # count by state

    # transform created_at to number of days
    today = pd.Timestamp.now()
    df['days'] = df.apply(lambda x: (today - x['created_at']).days, axis=1)

    # select non-OK states
    stuck = df[df['state'] == 'STUCK'].sort_values(by=['days'], ascending=False)
    replicating = df[df['state'] == 'REPLICATING'].sort_values(by=['days'], ascending=False)
    suspended = df[df['state'] == 'SUSPENDED'].sort_values(by=['days'], ascending=False)


    # combine all pending rules in a single dataframe
    pending = pd.concat([stuck, replicating, suspended]).reset_index(drop=True)

    if not pending.empty:
        pendingCompact = createRulesDataframe(pending, rucio, crab, logger)
    else:
        pendingCompact = pd.DataFrame()

    # prepare an HTML table
    if pendingCompact.empty:
        rulesTable = "<tr><td><center>No rules either in replicating nor stuck</center></td></tr>"
        rulesJson = [{}]
    else:
        rulesTable = createRulesHtmlTable(pendingCompact)
        rulesJson = createRulesJson(pendingCompact)

    # write an HTML doc
    beginningOfDoc = '<!DOCTYPE html>\n<html>\n'
    header = htmlHeader()
    now = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    title = f"\n<center><b>Status of CRAB Tape Recall rules at {now}</b></center><hr>\n"
    endOfDoc = '\n</html>'
    with open('RecallRules.html', 'w', encoding='utf-8') as fh:
        fh.write(beginningOfDoc)
        fh.write(header)
        fh.write(title)
        fh.write(rulesTable)
        fh.write(endOfDoc)

    # send document to opensearch
    sendAndCheck(rulesJson)

def createRulesDataframe(pending, rucio, crab, logger=None):
    """
    reformat the datafram into a compact-looking HTML table
    """
    # use standard compact format (OK/Rep/Stucl) for lock counts
    pending['locks'] = pending.apply(lambda x: f"{x.locks_ok_cnt}/{x.locks_replicating_cnt}/{x.locks_stuck_cnt}", axis=1)

    # extract CRAB user name from rule comment (ouch ! FRAGILE !)
    pending['user'] = pending.apply(lambda x: findUserNameForRule(rucio, x.id), axis=1)

    # # add and URL pointing to rule in Rucio UI
    # urlBase = '<a href="https://cms-rucio-webui.cern.ch/rule?rule_id=%s">%s</a>'
    # pending['idUrl'] = pending.apply(lambda x: (urlBase % (x.id, x.id)), axis=1)

    # add tape locations
    logger.info("finding tape source for all pending rules (takes some time...)")
    pending['tape'] = pending.apply(lambda x: findTapesForRule(rucio, x.id), axis=1)

    logger.info('Add (DBS) dataset name ...')
    pending['dataset'] = pending.apply(lambda x: findDatasetForRule(rucio, x.id), axis=1)
    logger.info('... and size')
    pending['size'] = pending.apply(lambda x: findDatasetSize(rucio, x.dataset), axis=1)
    logger.info("Done!")

    # logger.info('compute a short Url for each dataset')
    # pending['datasetUrl'] = pending.apply(lambda x: createDatasetUrl(x.dataset), axis=1)

    logger.info('find tasks using these rules')
    pending['tasks'] = pending.apply(lambda x: findTasksForRule(crab, x.id), axis=1)
    logger.info("Done!")

    # logger.info('compute a short Url for each task')
    # pending['taskUrls'] = pending.apply(lambda x: createTaskUrlHtml(x.tasks), axis=1)
    # logger.info("Done!")

    # create an HTML table
    # rulesToHtml = renamed.to_html(escape=False)

    # return rulesToHtml
    return pending

def createRulesHtmlTable(df):
    # add and URL pointing to rule in Rucio UI
    urlBase = '<a href="https://cms-rucio-webui.cern.ch/rule?rule_id=%s">%s</a>'
    df['idUrl'] = df.apply(lambda x: (urlBase % (x.id, x.id)), axis=1)

    # logger.info('compute a short Url for each task')
    df['taskUrls'] = df.apply(lambda x: createTaskUrlHtml(x.tasks), axis=1)

    # logger.info('compute a short Url for each dataset')
    df['datasetUrl'] = df.apply(lambda x: createDatasetUrlHtml(x.dataset), axis=1)

    selected = df[['idUrl', 'state', 'user', 'locks', 'days', 'tape', 'size', 'datasetUrl', 'taskUrls']]
    pendingCompact = selected.rename(columns={'locks': 'locks ok/rep/st', 'size': 'size TB'})
    _ = selected.rename(columns={'locks': 'locks ok/rep/st', 'size': 'size TB'})

    return pendingCompact.to_html(escape=False)

def createRulesJson(df):
    df = df[['id', 'state', 'user', 'locks', 'days', 'tape', 'size', 'dataset', 'tasks']]

    # logger.info('compute a short Url for each task')
    df['task0'] = df.apply(lambda x: x.tasks[-1], axis=1)
    df['tasks_1toN'] = df.apply(lambda x: x.tasks[:-1] if len(x.tasks) >= 2 else [], axis=1)
    df['tasks_num'] = df.apply(lambda x: len(x.tasks), axis=1)

    df['dataset_short'] = df.apply(lambda x: createDatasetShortJson(x.dataset), axis=1)

    rulesString = df.to_json(orient="split")
    rulesDict = json.loads(rulesString)
    rulesOpensearch = []
    for rule in rulesDict["data"]:
        ruleOpensearch = dict(
            producer=MONITUSER,
            type='checktaperecall',
            hostname=gethostname(),
        )
        for key, value in zip(rulesDict["columns"], rule):
            ruleOpensearch[key] = value
        rulesOpensearch.append(ruleOpensearch)
    return rulesOpensearch

def createTaskUrlHtml(tasks):
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

def createTaskUrlJson(tasks):
    """
    create an URL reference string for HTML pointing to task info in CRABRest UI
    use ahortened task name (timstamp:user) as link in the table
    """
    urlBase = 'https://cmsweb.cern.ch/crabserver/ui/task/%s'
    hrefList = []
    for task in tasks:
        href = urlBase % (task)
        hrefList.append(href)
    return hrefList

def createDatasetUrlHtml(dataset):
    """
    create an URL reference string for HTML pointing to dataset info in DAS Web UI
    use shortened dataset name as link in the table
    """
    dasUrl = f"https://cmsweb.cern.ch/das/request?view=list&instance=prod/global&input={dataset}"

    # shorten dataset name picking only first word of A and B in /A/B/Tier
    # use either _ or - as word separators
    primaryDS = dataset.split('/')[1]
    processedDS = dataset.split('/')[2]
    datatier = dataset.split('/')[3]
    prim = primaryDS.replace('_', '-').split('-')[0]
    proc = processedDS.replace('_', '-').split('-')[0]
    shortDS = '/'.join(['', prim, proc, datatier])

    href = f'<a href="{dasUrl}">{shortDS}</a>'
    return href

def createDatasetShortJson(dataset):
    """
    create an URL reference string for HTML pointing to dataset info in DAS Web UI
    use shortened dataset name as link in the table
    """

    # shorten dataset name picking only first word of A and B in /A/B/Tier
    # use either _ or - as word separators
    primaryDS = dataset.split('/')[1]
    processedDS = dataset.split('/')[2]
    datatier = dataset.split('/')[3]
    prim = primaryDS.replace('_', '-').split('-')[0]
    proc = processedDS.replace('_', '-').split('-')[0]
    shortDS = '/'.join(['', prim, proc, datatier])

    return shortDS

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
    teraBytes = datasetBytes // 1e12
    size = f"{datasetBytes/1e12:.0f}" if teraBytes > 0 else f"{datasetBytes/1e12:.3f}"
    return size


def ensureEnvironment(logger=None):
    """
    make sure we can run Rucio client and talk with CRAB
    """
    try:
        from rucio.client import Client
    except ModuleNotFoundError:
        logger.info("Setup Rucio first via:\n" +
              "export PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/slc7/py3/current/lib/python3.6/site-packages/\n" +
              "export RUCIO_HOME=/cvmfs/cms.cern.ch/rucio/current/\n" +
              "export RUCIO_ACCOUNT='crab_server'")
        sys.exit()
    # make sure Rucio client is initialized, this also ensures X509 proxy
    # our robot certificate can access multiple Rucio account, use the non-privileged one here
    rucio = Client(account="crab_server",
        creds={"client_cert": "/data/certs/robotcert.pem", "client_key": "/data/certs/robotkey.pem"},
        auth_type='x509',
    )

    crab = CRABRest(hostname='cmsweb.cern.ch', localcert="/data/certs/servicecert.pem",
                    localkey="/data/certs/servicekey.pem", userAgent='CheckTapeRecall')
    crab.setDbInstance('prod')

    return rucio, crab


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
