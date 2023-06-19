# monitor_locks_status get list of dataset from list_content() api from publishname
# FOR EACH dataset, get rules name
# ================================
#
# for source of truth (input), get it from bookkeeping file. not passing down
# in memory from register replicas because register replicas already skip some
# entry.
# maybe do it at top
# input is something like {ruleID1: datasetName1, ruleID2: datasetName2}
# outputs are
# 1. replicas state is ok: same as ReegisterReplicas.register(), but plus blockCompletes for rule state is ok
# 2. replicas state is replication: need rule id and id (no need for dataset)
import json
import pytest
import datetime
from argparse import Namespace
from unittest.mock import patch, Mock, call

import ASO.Rucio.config as config
from ASO.Rucio.Actions.MonitorLocksStatus import MonitorLocksStatus
from ASO.Rucio.Actions.RegisterReplicas import RegisterReplicas

@pytest.fixture
def mock_Transfer():
    username = 'cmscrab'
    rucioScope = f'user.{username}'
    publishname = '/TestPrimary/test-dataset/RAW'
    currentDatasetUUID = 'c9b28b96-5d16-41cd-89af-2678971132c9'
    currentDataset = f'{publishname}#{currentDatasetUUID}'
    logsDataset = f'{publishname}#LOG'
    return Mock(publishname=publishname, currentDataset=currentDataset, rucioScope=rucioScope, logsDataset=logsDataset, currentDatasetUUID=currentDatasetUUID, username=username)

@pytest.fixture
def mock_rucioClient():
    with patch('rucio.client.client.Client', autospec=True) as m_rucioClient:
        return m_rucioClient

@pytest.fixture
def loadDatasetMetadata():
    with open('test/assets/dataset_metadata.json') as r:
        return json.load(r)

def test_checkLockStatus_all_ok(mock_Transfer, mock_rucioClient):
    listRuleIDs = ['b43a554244c54dba954aa29cb2fdde0a']
    outputAllOK = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#c9b28b96-5d16-41cd-89af-2678971132c9',
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    listReplicaLocksReturnValue = [{
        'name': '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root',
        'state': 'OK',
    }]

    mock_rucioClient.list_replica_locks.side_effect = ((x for x in listReplicaLocksReturnValue), ) # list_replica_locks return generator
    mock_Transfer.replicaLFN2IDMap = {
        '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root' : '98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca'
    }
    mock_Transfer.replicasInContainer = {
        '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root' : '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#c9b28b96-5d16-41cd-89af-2678971132c9'
    }
    mock_Transfer.containerRuleID = 'b43a554244c54dba954aa29cb2fdde0a'
    config.args = Namespace(max_file_per_dataset=1)
    m = MonitorLocksStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.checkLocksStatus() == (outputAllOK, [])

def test_checkLockStatus_all_replicating(mock_Transfer, mock_rucioClient):
    listRuleIDs = ['b43a554244c54dba954aa29cb2fdde0a']
    outputNotOK = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#c9b28b96-5d16-41cd-89af-2678971132c9',
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    getReplicationRuleReturnValue = {
        'id': 'b43a554244c54dba954aa29cb2fdde0a',
        'name': '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#c9b28b96-5d16-41cd-89af-2678971132c9',
        'state': 'REPLICATING',
    }
    listReplicaLocksReturnValue = [{
        'name': '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root',
        'state': 'REPLICATING',
    }]
    mock_rucioClient.get_replication_rule.return_value = getReplicationRuleReturnValue
    mock_rucioClient.list_replica_locks.side_effect = ((x for x in listReplicaLocksReturnValue), ) # list_replica_locks return generator
    mock_Transfer.getIDFromLFN.return_value = '98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca'
    m = MonitorLocksStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.checkLocksStatus(listRuleIDs) == ([], outputNotOK, [])

@pytest.mark.skip(reason="Skip it for now due deadline.")
def test_checkLockStatus_mix():
    assert True == False

# bookkeeping rule
# - bookkeeping per dataset
# - save rule id if rule ok to another file
@patch.object(MonitorLocksStatus, 'checkLocksStatus')
def test_execute_bookkeeping_none(mock_checkLockStatus, mock_Transfer):
    allRules = ['b43a554244c54dba954aa29cb2fdde0a']
    okRules = []
    mock_Transfer.allRules = allRules
    mock_Transfer.okRules = okRules
    m = MonitorLocksStatus(mock_Transfer, mock_rucioClient, Mock())
    mock_checkLockStatus.return_value = ([], [], okRules)
    m.execute()
    mock_checkLockStatus.assert_called_once_with(allRules)
    mock_Transfer.updateOKRules.assert_called_once()

@patch.object(MonitorLocksStatus, 'checkLocksStatus')
def test_execute_bookkeeping_all(mock_checkLockStatus, mock_Transfer):
    allRules = ['b43a554244c54dba954aa29cb2fdde0a']
    okRules = ['b43a554244c54dba954aa29cb2fdde0a']
    mock_Transfer.allRules = allRules
    mock_Transfer.okRules = okRules
    m = MonitorLocksStatus(mock_Transfer, mock_rucioClient, Mock())
    mock_checkLockStatus.return_value = ([], [], [])
    m.execute()
    mock_checkLockStatus.assert_called_once_with([])
    mock_Transfer.updateOKRules.assert_called_once()


def generateExpectedOutput(doctype):
    if doctype == 'complete':
        return {
            'asoworker': 'rucio',
            'list_of_ids': ['98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca'], # hmm, how do we get this
            'list_of_transfer_state': ['DONE'],
            'list_of_dbs_blockname': ['/TestDataset/cmscrab-unittest-1/USER#c9b28b96-5d16-41cd-89af-2678971132ca'],
            'list_of_block_complete': ['OK'],
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/'],
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': ['NA'],
        }
    elif doctype == 'notcomplete':
        return {
            'asoworker': 'rucio',
            'list_of_ids': ['98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20cb'], # hmm, how do we get this
            'list_of_transfer_state': ['SUBMITTED'],
            'list_of_dbs_blockname': None,
            'list_of_block_complete': None,
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/'],
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': ['b43a554244c54dba954aa29cb2fdde0b'],
        }

def test_prepareOKFileDoc(mock_Transfer):
    okFileDoc = generateExpectedOutput('complete')
    outputOK = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": '/TestDataset/cmscrab-unittest-1/USER#c9b28b96-5d16-41cd-89af-2678971132ca',
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    m = MonitorLocksStatus(mock_Transfer, Mock(), Mock())
    assert okFileDoc == m.prepareOKFileDoc(outputOK)


def test_prepareNotOKFileDoc(mock_Transfer):
    notOKFileDoc = generateExpectedOutput('notcomplete')
    outputNotOK = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20cb",
            "dataset": '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#c9b28b96-5d16-41cd-89af-2678971132c9',
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0b",
        }
    ]
    m = MonitorLocksStatus(mock_Transfer, Mock(), Mock())
    assert notOKFileDoc == m.prepareNotOKFileDoc(outputNotOK)

def test_addReplicasToPublishContainer():
    outputOK = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": '/TestDataset/cmscrab-unittest-1/USER#c9b28b96-5d16-41cd-89af-2678971132ca',
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    m = MonitorLocksStatus(mock_Transfer, mock_rucioClient, Mock())
    m.addReplicasToPublishContainer(outputOK)


@patch.object(RegisterReplicas, 'addReplicasToDataset')
def test_updateBlockCompleteStatus(mock_addReplicasToDataset, mock_Transfer, mock_rucioClient, loadDatasetMetadata):
    outputOK = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20cb",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20cc",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
    ]
    retAddReplicasToDataset = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": '/TestPrimary/test-dataset_TRANSFER-bc8b2558/USER#c3800048-d946-45f7-9e83-1f420b4fc32e',
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20cb",
            "dataset": '/TestPrimary/test-dataset_TRANSFER-bc8b2558/USER#c3800048-d946-45f7-9e83-1f420b4fc32e',
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20cc",
            "dataset": '/TestPrimary/test-dataset_TRANSFER-bc8b2558/USER#b74d9bde-9a36-4e40-af17-3d614f19d380',
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
    ]
    expectedOutput = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": '/TestPrimary/test-dataset_TRANSFER-bc8b2558/USER#c3800048-d946-45f7-9e83-1f420b4fc32e',
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20cb",
            "dataset": '/TestPrimary/test-dataset_TRANSFER-bc8b2558/USER#c3800048-d946-45f7-9e83-1f420b4fc32e',
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20cc",
            "dataset": '/TestPrimary/test-dataset_TRANSFER-bc8b2558/USER#b74d9bde-9a36-4e40-af17-3d614f19d380',
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
    ]
    mock_addReplicasToDataset.return_value = retAddReplicasToDataset
    datasetMetadata = loadDatasetMetadata[:2]
    datasetMetadata[0]['is_open'] = False
    datasetMetadata[1]['is_open'] = True
    mock_rucioClient.get_metadata.side_effect = datasetMetadata
    m = MonitorLocksStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.updateBlockCompleteStatus(outputOK) == expectedOutput
