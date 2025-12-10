"""
report used Rucio quota to ElasticSearch via MONIT
"""
import sys
import json
import logging
from socket import gethostname

import datetime

import requests
from requests.auth import HTTPBasicAuth
from RucioUtils import getRucioUsage, Client

FMT = "%Y-%m-%dT%H:%M:%S%z"
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

def createQuotaReport(rucioClient=None, account=None, activity=None):
    """
    create a dictionary with the quota report to be sent to MONIT
    returns {'totalTB':TBypte}
    """
    totalBytes = getRucioUsage(rucioClient=rucioClient,account=account,activity=activity)
    report = {}
    totalTB = totalBytes // 1e12
    report['totalTB'] = totalTB
    return report

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

def send_and_check(document, should_fail=False):
    """
    commend the `##PROD` section when developing, not to duplicate the data inside elasticsearch
    """
    ## DEV
    # print(type(document), document)
    # PROD
    response = send(document)
    msg = 'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)
    assert ((response.status_code in [200]) != should_fail), \
        msg

def main(log):
    rucioClient = Client(
        creds={"client_cert": "/data/certs/robotcert.pem", "client_key": "/data/certs/robotkey.pem"},
        auth_type='x509',
    )
    log.info("rucio client initialized: %s %s", rucioClient.ping(), rucioClient.whoami() )

    # prepare a JSON to be sent to MONIT
    jsonDoc = {'producer': MONITUSER, 'type': 'reportrecallquota', 'hostname': gethostname()}
    # get info for the used account, each will be sent with ad-hoc tag in the JSON
    # different strategies need to be used for tape reall and input locking
    # tape recall rules are created on the various user accounts, so need to sum by activity
    # input locking rules are all created by crab_input account and used quota can be obained very simply
    accounts = [{'name': None, 'activity': 'Analysis TapeRecall', 'tag': 'tape_recall_total_TB'},
                {'name': 'crab_input', 'activity': None,  'tag': 'crab_input_total_TB'}]
    for account in accounts:
        report = createQuotaReport(rucioClient=rucioClient,
                                   account=account['name'],
                                   activity=account['activity'])
        jsonDoc[account['tag']] = report['totalTB']
    send_and_check(jsonDoc)


if __name__ == '__main__':
    # Simple main to execute the action standalone. You just need to set the task worker environment.
    #  The main is set up to work with the production task worker. If you want to use it on your own
    #  instance you need to change resthost, resturi, and TWCONFIG.

    # TWCONFIG = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler1 = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler1)
    handler2 = logging.FileHandler(LOGDIR + LOGFILE)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler2.setFormatter(formatter)
    logger.addHandler(handler2)
    logger.setLevel(logging.DEBUG)

    main(logger)
