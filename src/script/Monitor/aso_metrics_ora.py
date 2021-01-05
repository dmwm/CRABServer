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

MONIT           ='http://monit-metrics:10012/'
CMSWEB          ='cmsweb.cern.ch'
CERTIFICATE     ='/data/certs/servicecert.pem'
KEY             ='/data/certs/servicekey.pem'
DATA_SOURCE_URL ='/crabserver/prod/filetransfers'

#======================================================================================================================================
def getData(subresource):

        server = HTTPRequests(CMSWEB,CERTIFICATE,KEY)
        result = server.get(DATA_SOURCE_URL,data=encodeRequest({'subresource': subresource, 'grouping': 0}))

        return oracleOutputMapping(result)

#======================================================================================================================================
def printData(data,header):

        i=1
        print("="*70)
        print("%-4s%-15s%-10s%-10s" % header)
        print("-"*70)
        for row in data:
                print("%-4d%-15s%-10d%-10d" % (i,row[header[1]],row[header[2]],row[header[3]]))
                i=i+1

        print("="*70)
#======================================================================================================================================

def fillData(data,dict_key,aso_worker,state,inputDoc):

        if state=='transfer_state':
                STATES=TRANSFERDB_STATES
        if state=='publication_state':
                STATES=PUBLICATIONDB_STATES

        for row in data:
                if row['aso_worker'] == aso_worker:
                        inputDoc[dict_key][STATES[row[state]]]['count'] = row['nt']

#======================================================================================================================================
def sendDocument(document):
        response = requests.post(MONIT, data=json.dumps(document), headers={ "Content-Type": "application/json; charset=UTF-8"})
        return response.status_code
#======================================================================================================================================

inputDoc = {
        'producer': 'crab',
        'type': 'asoless',
        'hostname': gethostname(),
        'transfers':{
                        'NEW':{'count':0,'size':0},
                        'ACQUIRED':{'count':0,'size':0},
                        'FAILED':{'count':0,'size':0},
                        'DONE':{'count':0,'size':0},
                        'RETRY':{'count':0,'size':0},
                        'SUBMITTED':{'count':0,'size':0},
                        'KILL':{'count':0,'size':0},
                        'KILLED':{'count':0,'size':0}
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
results=getData('groupedTransferStatistics')
printData(results,('Row','aso_worker','nt','transfer_state'))
print("TRANSFERDB_STATES={0: 'NEW', 1: 'ACQUIRED', 2: 'FAILED', 3: 'DONE', 4: 'RETRY', 5: 'SUBMITTED', 6: 'KILL', 7: 'KILLED'}")
fillData(results,'transfers','schedd','transfer_state',inputDoc)

results=getData('groupedPublishStatistics')
printData(results,('Row','aso_worker','nt','publication_state'))
print("PUBLICATIONDB_STATES={0: 'NEW', 1: 'ACQUIRED', 2: 'FAILED', 3: 'DONE', 4: 'RETRY', 5: 'NOT_REQUIRED'}")
fillData(results,'publications','schedd','publication_state',inputDoc)


print("Filled json doc with   where aso_worker=asoless")
print("-"*70)
print (json.dumps([inputDoc]))

print("="*70)
if sendDocument([inputDoc])==200:
        print('successfully uploaded')
else:
        print('errors in  uploaded')
