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
from unittest.mock import patch, Mock, call, create_autospec

import ASO.Rucio.config as config
from ASO.Rucio.Transfer import Transfer
from ASO.Rucio.Actions.MonitorLockStatus import MonitorLockStatus
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
    outputAllOK = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    listReplicaLocksReturnValue = [{
        'name': '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root',
        'state': 'OK',
    }]

    mock_rucioClient.list_replica_locks.side_effect = ((x for x in listReplicaLocksReturnValue), ) # list_replica_locks return generator
    mock_Transfer.replicasInContainer = {
        '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root' : '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#c9b28b96-5d16-41cd-89af-2678971132c9'
    }
    mock_Transfer.containerRuleID = 'b43a554244c54dba954aa29cb2fdde0a'
    mock_Transfer.LFN2transferItemMap = {
        '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root': {
            'id': '98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca',
        }
    }
    config.args = Namespace(max_file_per_dataset=1)
    m = MonitorLockStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.checkLockStatus() == (outputAllOK, [])

def test_checkLockStatus_all_replicating(mock_Transfer, mock_rucioClient):
    outputNotOK = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
    ]
    listReplicaLocksReturnValue = [{
        'name': '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root',
        'state': 'REPLICATING',
    }]
    mock_Transfer.containerRuleID = 'b43a554244c54dba954aa29cb2fdde0a'
    mock_Transfer.LFN2transferItemMap = {
        '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root': {
            'id': '98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca',
        }
    }
    mock_rucioClient.list_replica_locks.side_effect = ((x for x in listReplicaLocksReturnValue), ) # list_replica_locks return generator
    mock_Transfer.getIDFromLFN.return_value = '98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca'
    m = MonitorLockStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.checkLockStatus() == ([], outputNotOK)

@pytest.mark.skip(reason="Skip it for now due deadline.")
def test_checkLockStatus_mix():
    assert True == False

@patch.object(RegisterReplicas, 'addReplicasToContainer')
def test_registerToPublishContainer(mock_addReplicasToContainer, mock_Transfer, mock_rucioClient):
    mock_Transfer.publishContainer = '/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER'
    outputAllOK = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    result = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    mock_addReplicasToContainer.return_value = result
    m = MonitorLockStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.registerToPublishContainer(outputAllOK) == result

def test_checkBlockCompleteStatus_close(mock_Transfer, mock_rucioClient):
    fileDocs = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    result = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    config.args = Namespace(open_dataset_timeout=1*60*60)
    mock_rucioClient.get_metadata.side_effect = [{
        'is_open': False,
        'updated_at': datetime.datetime.now()
    }]
    m = MonitorLockStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.checkBlockCompleteStatus(fileDocs) == result

def test_checkBlockCompleteStatus_shouldClose(mock_Transfer, mock_rucioClient):
    fileDocs = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    result = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    config.args = Namespace(open_dataset_timeout=1*60*60)
    mock_rucioClient.get_metadata.side_effect = [{
        'is_open': False,
        'updated_at': datetime.datetime.now() - datetime.timedelta(seconds=config.args.open_dataset_timeout - 1) # 1 hour and 1 second ago
    }]
    m = MonitorLockStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.checkBlockCompleteStatus(fileDocs) == result

def test_checkBlockCompleteStatus_notclose(mock_Transfer, mock_rucioClient):
    fileDocs = [
        {
            "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    config.args = Namespace(open_dataset_timeout=1*60*60)
    mock_rucioClient.get_metadata.side_effect = [{
        'is_open': True,
        'updated_at': datetime.datetime.now() - datetime.timedelta(seconds=1) # 1 sec ago
    }]
    m = MonitorLockStatus(mock_Transfer, mock_rucioClient, Mock())
    assert m.checkBlockCompleteStatus(fileDocs) == []

@pytest.mark.skip(reason="We did not use it, for now. Likely to revisit again in the future.")
def test_filterFilesNeedToPublish():
    assert True == False


@patch('ASO.Rucio.Actions.MonitorLockStatus.updateToREST')
def test_updateRESTFileDocsStateToDone(mock_updateToREST):
    expectedRestFileDocs = {
        'asoworker': 'rucio',
        'list_of_ids': ['98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca', "0fbdabe9311c07ad901652dc998af04c3f16997ba62f03bf5a13e769"],
        'list_of_transfer_state': ['DONE', 'DONE'],
        'list_of_dbs_blockname': None,
        'list_of_block_complete': None,
        'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/', 'https://fts3-cms.cern.ch:8446/'],
        'list_of_failure_reason': None, # omit
        'list_of_retry_value': None, # omit
        'list_of_fts_id': ['b43a554244c54dba954aa29cb2fdde0a', 'b43a554244c54dba954aa29cb2fdde0a'],
    }
    outputOK = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "name": '/store/user/rucio/tseethon/random/dir/output_9.root',
            "dataset": '/TestDataset/cmscrab-unittest-1/USER#c9b28b96-5d16-41cd-89af-2678971132ca',
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "0fbdabe9311c07ad901652dc998af04c3f16997ba62f03bf5a13e769",
            "name": '/store/user/rucio/tseethon/random/dir/output_10.root',
            "dataset": '/TestDataset/cmscrab-unittest-1/USER#ebe712e4-d53a-48e4-87d8-32c582ef4fab',
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    rest = Mock()
    m = MonitorLockStatus(Mock(), Mock(), rest)
    m.updateRESTFileDocsStateToDone(outputOK)
    mock_updateToREST.assert_called_with(rest, 'filetransfers', 'updateTransfers', expectedRestFileDocs)


@patch('ASO.Rucio.Actions.MonitorLockStatus.updateToREST')
def test_updateRESTFileDocsBlockCompletionInfo(mock_updateToREST):
    expectedRestFileDocs = {
        'asoworker': 'rucio',
        'list_of_ids': ['98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca', "0fbdabe9311c07ad901652dc998af04c3f16997ba62f03bf5a13e769"],
        'list_of_transfer_state': ['DONE', 'DONE'],
        'list_of_dbs_blockname': ['/TestDataset/cmscrab-unittest-1/USER#c9b28b96-5d16-41cd-89af-2678971132ca', '/TestDataset/cmscrab-unittest-1/USER#ebe712e4-d53a-48e4-87d8-32c582ef4fab'],
        'list_of_block_complete': ['OK', 'OK'],
        'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/', 'https://fts3-cms.cern.ch:8446/'],
        'list_of_failure_reason': None, # omit
        'list_of_retry_value': None, # omit
        'list_of_fts_id': None,
    }
    outputOK = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "name": '/store/user/rucio/tseethon/random/dir/output_9.root',
            "dataset": '/TestDataset/cmscrab-unittest-1/USER#c9b28b96-5d16-41cd-89af-2678971132ca',
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "0fbdabe9311c07ad901652dc998af04c3f16997ba62f03bf5a13e769",
            "name": '/store/user/rucio/tseethon/random/dir/output_10.root',
            "dataset": '/TestDataset/cmscrab-unittest-1/USER#ebe712e4-d53a-48e4-87d8-32c582ef4fab',
            "blockcomplete": 'OK',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        }
    ]
    rest = Mock()
    m = MonitorLockStatus(Mock(), Mock(), rest)
    m.updateRESTFileDocsBlockCompletionInfo(outputOK)
    mock_updateToREST.assert_called_with(rest, 'filetransfers', 'updateRucioInfo', expectedRestFileDocs)


@patch.object(RegisterReplicas, 'addReplicasToContainer')
def test_registerToMutiPubContainers(mock_addReplicasToContainer):
    mock_Transfer.publishContainer = '/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER'
    outputAllOK = [
        {
            "id": "7652449e07afeaf00abe804e8507f4172e5b04f09a2c5e0d883a3193",
            "name": "/store/user/rucio/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/log/cmsRun_3.log.tar.gz",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "10ba0d321da1a9d7ecc17e2bf411932ec5268ae12d5be76b5928dc29",
            "name": "/store/user/rucio/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/miniaodfake_3.root",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "091bfc9fb03fe326b1ace7cac5b71e034ce4b44ed46be14ae88b472a",
            "name": "/store/user/rucio/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/output_3.root",
            "dataset": None,
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
    ]

    returnValue = [
        {
            "id": "7652449e07afeaf00abe804e8507f4172e5b04f09a2c5e0d883a3193",
            "name": "/store/user/rucio/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/log/cmsRun_3.log.tar.gz",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "10ba0d321da1a9d7ecc17e2bf411932ec5268ae12d5be76b5928dc29",
            "name": "/store/user/rucio/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/miniaodfake_3.root",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
        {
            "id": "091bfc9fb03fe326b1ace7cac5b71e034ce4b44ed46be14ae88b472a",
            "name": "/store/user/rucio/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/output_3.root",
            "dataset": "/GenericTTbar/integration-test-30_TRANSFER.befe3559/USER#a86fca3a-1e38-467d-a2ac-98a8ff4299bd",
            "blockcomplete": 'NO',
            "ruleid": "b43a554244c54dba954aa29cb2fdde0a",
        },
    ]

    multiPubContainers = [
        '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d__cmsRun.log.tar.gz/USER',
        '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d__miniaodfake.root/USER',
        '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d__output.root/USER',
    ]

    def side_effect(*args):
        container = args[1]
        index = multiPubContainers.index(container)
        return [returnValue[index]]

    t = create_autospec(Transfer, instance=True)
    t.multiPubContainers = multiPubContainers
    mock_addReplicasToContainer.side_effect = side_effect
    m = MonitorLockStatus(t, Mock(), Mock())
    ret = m.registerToMutiPubContainers(outputAllOK)


    # check args pass to addReplicasToContainer()
    allcall = []
    for i in range(len(returnValue)):
        allcall.append(call([outputAllOK[i]], multiPubContainers[i]))
    mock_addReplicasToContainer.assert_has_calls(allcall, any_order=True)

    # check return value
    assert sorted(ret, key=lambda d: d['id']) == sorted(returnValue, key=lambda d: d['id'])


def test_checkBlockCompleteStatus():
    assert 1 == 0
