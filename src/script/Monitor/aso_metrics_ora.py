#!/usr/bin/env python
"""
Kibana monitor script for OracleAso
"""
from __future__ import print_function
from __future__ import division

import json
import datetime
import logging
import sys

from socket import gethostname
import requests
from requests.auth import HTTPBasicAuth

from RESTInteractions import CRABRest
from ServerUtilities import encodeRequest, oracleOutputMapping
from ServerUtilities import TRANSFERDB_STATES, PUBLICATIONDB_STATES

# MONIT = 'http://monit-metrics:10012/'
# CMSWEB = 'cmsweb.cern.ch'
# DBINSTANCE = 'prod'
# CERTIFICATE = '/data/certs/servicecert.pem'
# KEY = '/data/certs/servicekey.pem'

WORKDIR = '/data/srv/monit/'
LOGDIR = '/data/srv/monit/logs/'
LOGFILE = f'GenMonit-{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

CMSWEB = 'cmsweb.cern.ch'
DBINSTANCE = 'prod'
CMSWEB_CERTIFICATE = '/data/certs/servicecert.pem'
CMSWEB_KEY = '/data/certs/servicekey.pem'

def readpwd():
    """
    Reads password from disk
    """
    with open("/data/certs/monit.d/MONIT-CRAB.json", encoding='utf-8') as f:
        credentials = json.load(f)
    return credentials["url"], credentials["username"], credentials["password"]
MONITURL, MONITUSER, MONITPWD = readpwd()

#======================================================================================================================================
def getData(subresource):
    """This function will fetch data from Oracle table"""

    crabserver = CRABRest(hostname=CMSWEB,
                          localcert=CMSWEB_CERTIFICATE,
                          localkey=CMSWEB_KEY,
                          retry=3, userAgent='CRABTaskWorker')
    crabserver.setDbInstance(dbInstance=DBINSTANCE)
    result = crabserver.get(api='filetransfers', data=encodeRequest({'subresource': subresource, 'grouping': 0}))

    return oracleOutputMapping(result)

#======================================================================================================================================

def printData(data, header, logger=None):
    """This function will print table on screen for debuging purpose"""

    i = 1
    logger.info("="*70)
    logger.info("%-4s%-15s%-10s%-10s", header)
    logger.info("-"*70)
    for row in data:
        logger.info("%-4d%-15s%-10d%-10d", (i, row[header[1]], row[header[2]], row[header[3]]))
        i = i+1

    logger.info("="*70)
#======================================================================================================================================

def fillData(data, dictKey, asoWorker, state, inputdoc):
    "This function will fill fetched data from Oracle table in JSON dict."""

    if state == 'transfer_state':
        STATES = TRANSFERDB_STATES
    if state == 'publication_state':
        STATES = PUBLICATIONDB_STATES

    for row in data:
        if row['aso_worker'] == asoWorker:
            inputdoc[dictKey][STATES[row[state]]]['count'] = row['nt']

#======================================================================================================================================

def sendDocument(document, logger=None):
    """This function upload JSON to MONIT"""

    response = requests.post(f"{MONITURL}",
                             auth=HTTPBasicAuth(MONITUSER, MONITPWD),
                             data=json.dumps(document),
                             headers={"Content-Type": "application/json; charset=UTF-8"},
                             verify=False)
    msg = 'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)
    if not response.status_code in [200]:
        logger.info(msg)
    return response.status_code
#======================================================================================================================================

def main():
    logger = logging.getLogger()
    handler1 = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler1)
    handler2 = logging.FileHandler(LOGDIR + LOGFILE)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s",
                                  datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler2.setFormatter(formatter)
    logger.addHandler(handler2)
    logger.setLevel(logging.INFO)

    inputDoc = {
        'producer': MONITUSER,
        'type': 'asoless',
        'hostname': gethostname(),
        'transfers':{
            'NEW':{'count':0, 'size':0},
            'ACQUIRED':{'count':0, 'size':0},
            'FAILED':{'count':0, 'size':0},
            'DONE':{'count':0, 'size':0},
            'RETRY':{'count':0, 'size':0},
            'SUBMITTED':{'count':0, 'size':0},
            'KILL':{'count':0, 'size':0},
            'KILLED':{'count':0, 'size':0}
            },
        'publications':{
            'NEW':{'count':0},
            'ACQUIRED':{'count':0},
            'FAILED':{'count':0},
            'DONE':{'count':0},
            'RETRY':{'count':0},
            'NOT_REQUIRED':{'count':0}
            }
    }
    results = getData('groupedTransferStatistics')
    printData(results, ('Row', 'aso_worker', 'nt', 'transfer_state'), logger)
    logger.info("TRANSFERDB_STATES = {0: 'NEW', 1: 'ACQUIRED', 2: 'FAILED', 3: 'DONE', 4: 'RETRY', 5: 'SUBMITTED', 6: 'KILL', 7: 'KILLED'}")
    fillData(results, 'transfers', 'schedd', 'transfer_state', inputDoc)

    results = getData('groupedPublishStatistics')
    printData(results, ('Row', 'aso_worker', 'nt', 'publication_state'), logger)
    logger.info("PUBLICATIONDB_STATES={0: 'NEW', 1: 'ACQUIRED', 2: 'FAILED', 3: 'DONE', 4: 'RETRY', 5: 'NOT_REQUIRED'}")
    fillData(results, 'publications', 'schedd', 'publication_state', inputDoc)

    logger.info("Filled json doc with   where aso_worker=asoless")
    logger.info("-"*70)
    logger.info(json.dumps([inputDoc]))

    logger.info("="*70)
    if sendDocument([inputDoc], logger) == 200:
        logger.info('successfully uploaded')
    else:
        logger.info('errors in  uploaded')

if __name__ == "__main__":
    main()
