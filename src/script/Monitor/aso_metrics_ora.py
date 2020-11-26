#!/usr/bin/env python
"""
Kibana monitor script for OracleAso
"""
from __future__ import print_function
from __future__ import division

import os
import sys
import json
import time
import pycurl 
import urllib
import urllib2
import httplib
import logging
import datetime
import subprocess
import requests
from urlparse import urljoin
from socket import gethostname
from optparse import OptionParser

from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping
from ServerUtilities import TRANSFERDB_STATES, PUBLICATIONDB_STATES

def send(document):
    return requests.post('http://monit-metrics:10012/', data=json.dumps(document), headers={ "Content-Type": "application/json; charset=UTF-8"})

def send_and_check(document, should_fail=False):
    response = send(document)
    assert( (response.status_code in [200]) != should_fail), 'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)


if __name__ == "__main__":
    server = HTTPRequests('cmsweb.cern.ch',
                          '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy',
                          '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy')

    result = server.get('/crabserver/prod/filetransfers', 
                        data=encodeRequest({'subresource': 'groupedTransferStatistics', 'grouping': 0}))

    results = oracleOutputMapping(result)

    tmp = {
        'producer': 'crab',
        'type': 'aso_total',
        'hostname': gethostname(),
        'transfers':{ 'DONE':{'count':0,'size':0}, 'ACQUIRED':{'count':0,'size':0}, 'SUBMITTED':{'count':0,'size':0}, 'FAILED':{'count':0,'size':0}, 'RETRY':{'count':0,'size':0} }, 
        'publications':{'DONE':{'count':0}, 'ACQUIRED':{'count':0}, 'NEW':{'count':0}, 'FAILED':{'count':0}, 'RETRY':{'count':0}}
    }
    status=tmp


    for doc in results:
        if doc['aso_worker']=="asoprod1":
            status['transfers'][TRANSFERDB_STATES[doc['transfer_state']]]['count'] = doc['nt']
            tmp['transfers'][TRANSFERDB_STATES[doc['transfer_state']]]['count'] = doc['nt']

    while True:
        try:
            tmp_transfer = open("tmp_transfer","w")
            tmp_transfer.write(json.dumps(tmp))
            tmp_transfer.close()
            break
        except Exception as ex:
            print(ex)
            continue

    ## redo query
    """
    tmp = {
        'producer': 'crab',
        'type': 'aso_v2',
        'hostname': gethostname(),
        'transfers':{ 'DONE':{'count':0,'size':0}, 'ACQUIRED':{'count':0,'size':0}, 'SUBMITTED':{'count':0,'size':0}, 'FAILED':{'count':0,'size':0}, 'RETRY':{'count':0,'size':0} }, 
        'publications':{'DONE':{'count':0}, 'ACQUIRED':{'count':0}, 'NEW':{'count':0}, 'FAILED':{'count':0}, 'RETRY':{'count':0}}
    }
    status=tmp

    for doc in results:
        if doc['aso_worker']=="asov2":
            status['transfers'][TRANSFERDB_STATES[doc['transfer_state']]]['count'] = doc['nt']
            tmp['transfers'][TRANSFERDB_STATES[doc['transfer_state']]]['count'] = doc['nt']
    """
    #result = server.get('/crabserver/prod/filetransfers', 
    #                    data=encodeRequest({'subresource': 'getGroupedPublicationQuery', 'username': 'asda', 'taskname': '12390_123:crab_dasda_dasdasd', 'asoworker': 'asoprod1' ,'grouping': 0}))

    #results = oracleOutputMapping(result)
    #result = server.get('/crabserver/prod/filetransfers', data=encodeRequest({'subresource': 'getGroupedPublicationQuery', 'grouping': 0}))
                            #data=encodeRequest({'subresource': 'groupedPublishStatistics', 'grouping': 2}))

    #results = oracleOutputMapping(result)
    #print(results)

    #for doc in results:
    #    if doc['aso_worker']=="asoprod1" and not PUBLICATIONDB_STATES[doc['publication_state']]=="NOT_REQUIRED":
    #        status['publications'][PUBLICATIONDB_STATES[doc['publication_state']]]['count'] = doc['nt']
    #        tmp['publications'][PUBLICATIONDB_STATES[doc['publication_state']]]['count'] = doc['nt']

    #past.close()

    print (json.dumps([tmp]))
    try:
        send_and_check([tmp])
    except Exception as ex:
        print(ex)

    tmp = {
        'producer': 'crab',
        'type': 'aso_total',
        'hostname': gethostname(),
        'transfers':{ 'DONE':{'count':0,'size':0}, 'ACQUIRED':{'count':0,'size':0}, 'SUBMITTED':{'count':0,'size':0}, 'FAILED':{'count':0,'size':0}, 'RETRY':{'count':0,'size':0} }, 
        'publications':{'DONE':{'count':0}, 'ACQUIRED':{'count':0}, 'NEW':{'count':0}, 'FAILED':{'count':0}, 'RETRY':{'count':0}}
    }
    status=tmp

    tmp = {
        'producer': 'crab',
        'type': 'asoless',
        'hostname': gethostname(),
        'transfers':{ 'DONE':{'count':0,'size':0}, 'ACQUIRED':{'count':0,'size':0}, 'SUBMITTED':{'count':0,'size':0}, 'FAILED':{'count':0,'size':0}, 'RETRY':{'count':0,'size':0} }, 
        'publications':{'DONE':{'count':0}, 'ACQUIRED':{'count':0}, 'NEW':{'count':0}, 'FAILED':{'count':0}, 'RETRY':{'count':0}}
    }
    status=tmp

    for doc in results:
        if doc['aso_worker']=="asoless":
            status['transfers'][TRANSFERDB_STATES[doc['transfer_state']]]['count'] = doc['nt']
            tmp['transfers'][TRANSFERDB_STATES[doc['transfer_state']]]['count'] = doc['nt']

    while True:
        try:
            tmp_transfer = open("tmp_transfer","w")
            tmp_transfer.write(json.dumps(tmp))
            tmp_transfer.close()
            break
        except Exception as ex:
            print(ex)
            continue

    print (json.dumps([tmp]))
    try:
        send_and_check([tmp])
    except Exception as ex:
        print(ex)

    sys .exit(0)
