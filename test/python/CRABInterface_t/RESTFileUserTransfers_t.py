#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
__DashboardAPI_t__
Unit tests for the FileTransfer API.
Created on Tue Mar 23 13:30:04 2016

@author: jbalcas
"""
from __future__ import print_function
from __future__ import division
import unittest

import os
import time
import string
import random
import urllib

from RESTInteractions import HTTPRequests
from ServerUtilities import getHashLfn, generateTaskName, PUBLICATIONDB_STATUSES, encodeRequest, oracleOutputMapping

class FileTransfersTest(unittest.TestCase):
    """
    _DashboardAPITest_

    Unit tests for the FileTransfers API
    """
    def setUp(self):
        """
        Setup for unit tests
        """
        self.server = HTTPRequests(os.environ['SERVER_HOST'], os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY'])
        self.lfnBase = '/store/temp/user/%s/my_cool_dataset-%s/file-%s-%s.root'
        self.fileDoc = {'id': 'OVERWRITE',
                        'username': 'OVERWRITE',
                        'taskname': 'OVERWRITE',
                        'start_time': 0,
                        'destination': 'T2_CH_CERN',
                        'destination_lfn': 'OVERWRITE',
                        'source': 'T2_US_Caltech',
                        'source_lfn': 'OVERWRITE',
                        'filesize': random.randint(1, 9999),
                        'publish': 1,
                        'transfer_state': 'OVERWRITE',
                        'publication_state': 'OVERWRITE',
                        'job_id': 1,
                        'job_retry_count': 0,
                        'type': 'log',
                        'rest_host': 'cmsweb.cern.ch',
                        'rest_uri': '/crabserver/prod/'}
        self.ids = []
        self.users = ['jbalcas', 'mmascher', 'dciangot', 'riahi', 'erupeika', 'sbelforte']  # just random users for tests
        self.tasks = {}
        self.totalFiles = 10

    def testFileTransferPUT(self):
        """
        _testFileTransferPUT_

        Just test simple testFileTransferPUT with fake data
        """
        # We just sent fake data which is not monitored by dashboard.
        # Also only the first time to decide is publication ON or NOT
        for user in self.users:
            timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
            for i in range(self.totalFiles):
                now = int(time.time())
                # Generate a taskname
                workflowName = ""
                taskname = ""
                if user not in self.tasks:
                    workflowName = "".join([random.choice(string.ascii_lowercase) for _ in range(20)]) + "_" + str(now)
                    publicationState = random.choice(['NEW', 'NOT_REQUIRED'])
                else:
                    workflowName = self.tasks[user]['workflowName']
                    publicationState = self.tasks[user]['publication']
                transferState = random.choice(['NEW', 'DONE'])
                taskname = generateTaskName(user, workflowName, timestamp)
                finalLfn = self.lfnBase % (user, workflowName, i, random.randint(1, 9999))
                idHash = getHashLfn(finalLfn)
                self.fileDoc['id'] = idHash
                self.fileDoc['job_id'] = i
                self.fileDoc['username'] = user
                self.fileDoc['taskname'] = taskname
                self.fileDoc['start_time'] = int(time.time())
                self.fileDoc['source_lfn'] = finalLfn
                self.fileDoc['destination_lfn'] = finalLfn
                self.fileDoc['transfer_state'] = transferState
                self.fileDoc['publication_state'] = publicationState
                print(self.fileDoc)
                self.server.put('/crabserver/dev/fileusertransfers', data=encodeRequest(self.fileDoc))
                # if I will put the same doc twice, it should raise an error.
                # self.server.put('/crabserver/dev/fileusertransfers', data=urllib.urlencode(self.fileDoc))
                # This tasks are for the future and next calls
                if user not in self.tasks:
                    self.tasks[user] = {'workflowName': workflowName, 'taskname': taskname, 'listOfIds': [],
                                        'publication': publicationState, 'toTransfer': 0, 'toPublish': 0, 'total': self.totalFiles}
                if self.tasks[user]['publication'] == 'NEW':
                    self.tasks[user]['toPublish'] += 1
                if transferState == 'NEW':
                    self.tasks[user]['toTransfer'] += 1
                self.tasks[user]['listOfIds'].append(idHash)
        # This should raise an error
        for username in self.tasks:
            taskname = self.tasks[username]['taskname']
            for query in ['getTransferStatus', 'getPublicationStatus']:
                result = self.server.get('/crabserver/dev/fileusertransfers', data=encodeRequest({'subresource': query,
                                                                                    'username': username,
                                                                                    'taskname': taskname}))
                print(result)
                print(result[0]['result'])
                taskInfoDict = oracleOutputMapping(result, 'id')
                print(taskInfoDict)
                for key, docDict in taskInfoDict.items():
                    result = self.server.get('/crabserver/dev/fileusertransfers', data=encodeRequest({'subresource': 'getById', 'id': key}))

        randomUsers = random.sample(set(self.users), 3)  # Take half of the users and kill their transfers for specific task
        for username in randomUsers:
            taskname = self.tasks[username]['taskname']
            result = self.server.post('/crabserver/dev/fileusertransfers', data=encodeRequest({'subresource': 'killTransfers',
                                                                                                  'username': username,
                                                                                                  'taskname': taskname}))
            print(result)
        # oneUser is left for killing a list of IDs
        # leftUsers will be killing transfers one by one for specific id.
        leftUsers = list(set(self.users) - set(randomUsers))
        oneUser = random.sample(set(leftUsers), 1)
        leftUsers = list(set(leftUsers) - set(oneUser))
        for username in leftUsers:
            # First get all left ids for this users
            result = self.server.get('/crabserver/dev/fileusertransfers', data=encodeRequest({'subresource': 'getTransferStatus',
                                                                                                 'username': username,
                                                                                                 'taskname': self.tasks[username]['taskname']}))
            resultOut = oracleOutputMapping(result, None)
            print("**"*50)
            for outDict in resultOut:
                print(outDict)
                result = self.server.post('/crabserver/dev/fileusertransfers', data=encodeRequest({'subresource': 'killTransfersById',
                                                                                                      'username': username,
                                                                                                      'listOfIds': outDict['id']}))
                print(result)
            print(resultOut)
            print(result)
        for username in oneUser:
            result = self.server.post('/crabserver/dev/fileusertransfers', data=encodeRequest({'subresource': 'killTransfersById',
                                                                                               'username': username,
                                                                                               'listOfIds': self.tasks[username]['listOfIds']}, ['listOfIds']))
            # As it asks to kill all which are in new, need to double check what we submitted before and if the output of killed is correct
            print(result)
            print(self.tasks[username])


if __name__ == '__main__':
    unittest.main()
