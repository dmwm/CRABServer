# should test detect provided wrong path to open function?

import pytest
import builtins
import json
from unittest.mock import patch, mock_open, MagicMock
from argparse import Namespace

from ASO.Rucio.Transfer import Transfer
from ASO.Rucio.exception import RucioTransferException
import ASO.Rucio.config as config
#from .fixtures import mock_rucioClient


@pytest.fixture
def transfersTxtContent():
    path = 'test/assets/transfers.txt'
    with open(path, 'r', encoding='utf-8') as r:
        return r.read()

@pytest.fixture
def restInfoForFileTransfersJsonContent():
    path = 'test/assets/RestInfoForFileTransfers.json'
    with open(path, 'r', encoding='utf-8') as r:
        return r.read()

@pytest.fixture
def bookkeepingRulesJSONContent():
    path = 'test/assets/bookkeeping_rules.json'
    with open(path, 'r', encoding='utf-8') as r:
        return json.load(r)

@pytest.fixture
def listContentDatasets():
    path = 'test/assets/rucio_list_content_datasets.json'
    with open(path, 'r', encoding='utf-8') as r:
        return json.load(r)

@pytest.fixture
def listContentFiles():
    path = 'test/assets/rucio_list_content_datasets.json'
    with open(path, 'r', encoding='utf-8') as r:
        return json.load(r)

@pytest.fixture
def transferDicts():
    path = 'test/assets/transferDicts.json'
    with open(path, 'r', encoding='utf-8') as r:
        return json.load(r)

@pytest.fixture
def containerRuleIDJSONContent():
    path = 'test/assets/container_ruleid.json'
    with open(path, 'r', encoding='utf-8') as r:
        return r.read()

# https://stackoverflow.com/a/57015304
@pytest.fixture(name='LFN2PFNMapJSON')
def fixture_LFN2PFNMapJSON():
    path = 'test/assets/LFN2PFNMap.json'
    with open(path, 'r', encoding='utf-8') as r:
        return json.load(r)

@pytest.fixture(name='cleanedFiles')
def fixture_cleanedFiles():
    path = 'test/assets/transferDicts.json'
    with open(path, 'r', encoding='utf-8') as r:
        transfersItem = json.load(r)
    return [xdict['destination_lfn'] for xdict in transfersItem]

#old relic
#def test_Transfer_readInfo():
#    restInfoForFileTransfersJson = {
#        "host": "cmsweb-test12.cern.ch:8443",
#        "dbInstance": "devthree",
#        "proxyfile": "9041f6500ff40aaca33737316a2dbfb57116e8e0"
#    }
#    transfersTxt = {
#        "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
#        "username": "tseethon",
#        "taskname": "230324_151740:tseethon_crab_rucio_transfer_whitelist_cern_test12_20230324_161736",
#        "start_time": 1679671544,
#        "destination": "T2_CH_CERN",
#        "destination_lfn": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
#        "source": "T2_CH_CERN",
#        "source_lfn": "/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
#        "filesize": 628054,
#        "publish": 0,
#        "transfer_state": "NEW",
#        "publication_state": "NOT_REQUIRED",
#        "job_id": "9",
#        "job_retry_count": 0,
#        "type": "output",
#        "publishname": "autotest-1679671056-00000000000000000000000000000000",
#        "checksums": {"adler32": "812b8235", "cksum": "1236675270"},
#        "outputdataset": "/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER"
#    }
#    config.config = Namespace(force_publishname=None, rest_info_path='/a/b/c', transfer_info_path='/d/e/f')
#    with patch('builtins.open', new_callable=mock_open, read_data=f'{json.dumps(restInfoForFileTransfersJson)}\n') as mo:
#        # setup config
#        mo.side_effect = (mo.return_value, mock_open(read_data=f'{json.dumps(transfersTxt)}\n').return_value,)
#        # run config
#        t = Transfer()
#        t.readInfo()
#        assert t.proxypath == '9041f6500ff40aaca33737316a2dbfb57116e8e0'
#        assert t.username == 'tseethon'
#        assert t.rucioScope == 'user.tseethon'
#        assert t.destination == 'T2_CH_CERN'
#        assert t.publishname == '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER'
#        assert t.logsDataset == '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#LOGS'
#        assert t.currentDataset == ''

#def test_Transfer_readInfo_filenotfound():
#    # setup config
#
#    # run transfer
#    config.config = Namespace(rest_info_path='/a/b/c', transfer_info_path='/d/e/f')
#    t = Transfer()
#    with pytest.raises(RucioTransferException):
#        t.readInfo()

def test_readInfoFromTransferItems():
    transferDict = {
        'source_lfn': '/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_7.root',
        'destination_lfn': '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_7.root',
        'id': '5b5c6d9f2e99ae32191e2c702ca9bba32951d69027289a7cde884468',
        'source': 'T2_CH_CERN_Temp',
        'destination': 'T2_CH_CERN',
        'checksum': 'cde8011f',
        'filesize': 628826,
        'publishname': '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER',
        'username': 'tseethon',
    }
    t = Transfer()
    t.transferItems = [transferDict]
    t.readInfoFromTransferItems()
    assert t.username == 'tseethon'
    assert t.rucioScope == 'user.tseethon'
    assert t.destination == 'T2_CH_CERN'
    assert t.publishname == '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER'
    assert t.logsDataset == '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#LOGS'
    assert t.currentDataset == ''


def test_readRESTInfo(restInfoForFileTransfersJsonContent):
    t = Transfer()
    t.lastTransferLine = 0
    config.args = Namespace(rest_info_path='/path/to/RestInfoForFileTransfers.json')
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open, read_data=restInfoForFileTransfersJsonContent) as mo:
        t.readRESTInfo()
        assert mo.call_args.args[0] == '/path/to/RestInfoForFileTransfers.json'
        assert t.restHost == "cmsweb-test12.cern.ch:8443"
        assert t.restDBinstance == 'devthree'
        assert t.restProxyFile == '9041f6500ff40aaca33737316a2dbfb57116e8e0'

def test_readRESTInfo_FileNotFoundError():
    t = Transfer()
    t.lastTransferLine = 0
    config.args = Namespace(rest_info_path='/path/should/not/found')
    with pytest.raises(RucioTransferException):
        t.readRESTInfo()


def test_readTransferItems(transfersTxtContent):
    t = Transfer()
    t.lastTransferLine = 0
    path = '/path/to/transfers.txt'
    config.args = Namespace(transfers_txt_path=path)
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open, read_data=transfersTxtContent) as mo:
        t.readTransferItems()
        assert mo.call_args.args[0] == path
        assert t.transferItems[5]['id'] == '5b5c6d9f2e99ae32191e2c702ca9bba32951d69027289a7cde884468'
        assert t.transferItems[5]['source'] == 'T2_CH_CERN'
        assert t.transferItems[5]['checksums']['adler32'] == 'cde8011f'

def test_readTransferItems_FileNotFoundError():
    t = Transfer()
    t.lastTransferLine = 0
    path = '/path/to/transfers.txt'
    config.args = Namespace(transfers_txt_path=path)
    with pytest.raises(RucioTransferException):
        t.readTransferItems()

def test_readTransferItems_no_new_item(transfersTxtContent):
    # maybe another exception class to seperate between filenotfound and no new entry
    t = Transfer()
    t.lastTransferLine = 20
    path = '/path/to/transfers.txt'
    config.args = Namespace(transfers_txt_path=path)
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open, read_data=transfersTxtContent) as mo:
        with pytest.raises(RucioTransferException):
            t.readTransferItems()

def test_readLastTransferLine():
    config.args = Namespace(last_line_path='/path/to/last_transfer.txt')
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open, read_data='5\n') as mo:
        t = Transfer()
        t.readLastTransferLine()
        assert t.lastTransferLine == 5
        assert mo.call_args.args[0] == '/path/to/last_transfer.txt'

def test_readLastTransferLine_file_not_found():
    config.args = Namespace(last_line_path='/path/should/not/found')
    t = Transfer()
    t.readLastTransferLine()
    assert t.lastTransferLine == 0

# do we need to test this thing?
# ======================
# if not os.path.exists('task_process/transfers'):
#     os.makedirs('task_process/transfers')



def test_readBookkeepingRules(bookkeepingRulesJSONContent):
    config.args = Namespace(bookkeeping_rules_path='/path/to/bookkeeping_rules.json')
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open, read_data=json.dumps(bookkeepingRulesJSONContent)) as mo:
        t = Transfer()
        t.readBookkeepingRules()
        assert t.allRules == bookkeepingRulesJSONContent['all']
        assert t.okRules == bookkeepingRulesJSONContent['ok']


def test_readBookkeepingRules_FileNotFoundError():
    config.args = Namespace(bookkeeping_rules_path='/path/to/bookkeeping_rules.json')
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open) as mo:
        mo.side_effect = FileNotFoundError
        t = Transfer()
        t.readBookkeepingRules()
        assert t.allRules == []
        assert t.okRules == []


def test_updateOKRules(bookkeepingRulesJSONContent):
    config.args = Namespace(bookkeeping_rules_path='/path/to/bookkeeping_rules.json')
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open) as mo:
        with patch('ASO.Rucio.Transfer.somecontextlibfunc') as mock_somecontextlibfunc:
            t = Transfer()
            t.allRules = list(bookkeepingRulesJSONContent['all'])
            t.okRules = list(bookkeepingRulesJSONContent['ok'])
            writePath = '/path/to/tmp'
            mock_somecontextlibfunc.return_value.__enter__.return_value = writePath
            newOK = ['e609d75e4a7a4fa3a880ea0bb6681371']
            t.updateOKRules(newOK)
            mo.assert_called_once_with(writePath, 'w', encoding='utf-8')
            assert t.okRules == bookkeepingRulesJSONContent['ok'] + newOK
            # TODO: need to check content but I do not know how to do it

@pytest.mark.skip(reason='Skip for now due to deadline')
def test_updateOKRules_ok_rules_not_in_all(bookkeepingRulesJSONContent):
    assert 0 == 1


def test_addNewRule(bookkeepingRulesJSONContent):
    config.args = Namespace(bookkeeping_rules_path='/path/to/bookkeeping_rules.json')
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open) as mo:
        with patch('ASO.Rucio.Transfer.somecontextlibfunc') as mock_somecontextlibfunc:
            t = Transfer()
            t.allRules = list(bookkeepingRulesJSONContent['all'])
            t.okRules = list(bookkeepingRulesJSONContent['ok'])
            writePath = '/path/to/tmp'
            mock_somecontextlibfunc.return_value.__enter__.return_value = writePath
            newAll = ['e609d75e4a7a4fa3a880ea0bb6681999']
            t.addNewRule(newAll)
            mo.assert_called_once_with(writePath, 'w', encoding='utf-8')
            assert t.allRules == bookkeepingRulesJSONContent['all'] + newAll
            assert t.okRules == bookkeepingRulesJSONContent['ok']
            # TODO: need to check content but I do not know how to do it

def test_getContainerInfo(mock_rucioClient, listContentDatasets, listContentFiles):
    t = Transfer()
    t.publishname = '/GenericTTbar/tseethon-integrationtest-1/USER'
    t.rucioScope = 'user.tseethon'
    mock_rucioClient.list_content.side_effect = (
        (x for x in listContentDatasets),
        (x for x in listContentFiles[:3]),
        (x for x in listContentFiles[3:]),
    )
    t.getContainerInfo(mock_rucioClient)
    expectedReplicas = [x['name'] for x in listContentFiles]
    assert t.replicasInContainer == expectedReplicas

@pytest.mark.skip(reason='I am really lazy')
def test_buildReplica2IDMap():
    assert 0 == 1

def test_buildMultiPubContainerNames(transferDicts):
    t = Transfer()
    t.transferItems = transferDicts
    t.publishContainer = '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d/USER'
    t.populateMultiPubContainers()
    assert sorted(t.multiPubContainers) == sorted([
        '/FakeDataset/fakefile-FakePublish-befe3559057072761674520fdaee5005_cmsRun.log.tar.gz/USER',
        '/FakeDataset/fakefile-FakePublish-befe3559057072761674520fdaee5005_miniaodfake.root/USER',
        '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d/USER',
    ])

def test_updateContainerRuleID(containerRuleIDJSONContent, fs):
    path = '/path/to/container_ruleid.json'
    config.args = Namespace(container_ruleid_path=path)
    # we should mock writePath(), not function inside writePath()
    fs.create_file(path)
    with open(path, 'w', encoding='utf-8') as mock_file:
        with patch('ASO.Rucio.Transfer.writePath') as mock_writePath:
            mock_writePath.return_value.__enter__.return_value = mock_file
            t = Transfer()
            t.containerRuleID = 'c88abb899f744efb8f33fd197ee77ecd'
            t.publishRuleID = '141a41b6b54f45f59dc0703182f1257f'
            t.multiPubRuleIDs = {
                '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d_cmsRun.log.tar.gz/USER': '9653f39f686944128028fd25888ae2d3',
                '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d_output.root/USER': 'e609d75e4a7a4fa3a880ea0bb6681371',
                '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d_miniaodfake.root/USER': '6b159d7e5dc940daa2188658a68c4b23',
            }
            t.updateContainerRuleID()
            mock_writePath.assert_called_once_with(path)
    with open(path, 'r', encoding='utf-8') as mock_file:
        x = json.loads(mock_file.read())
        y = json.loads(containerRuleIDJSONContent)
        assert x == y

def test_readContainerRuleID(containerRuleIDJSONContent):
    path = '/path/to/container_ruleid.json'
    config.args = Namespace(container_ruleid_path=path, force_publishname=False)
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open, read_data=containerRuleIDJSONContent):
        t = Transfer()
        t.readContainerRuleID()
        assert t.containerRuleID == 'c88abb899f744efb8f33fd197ee77ecd'
        assert t.publishRuleID == '141a41b6b54f45f59dc0703182f1257f'
        assert t.multiPubRuleIDs == {
            '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d_cmsRun.log.tar.gz/USER': '9653f39f686944128028fd25888ae2d3',
            '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d_output.root/USER': 'e609d75e4a7a4fa3a880ea0bb6681371',
            '/GenericTTbar/tseethon-ruciotransfers-1697125324-94ba0e06145abd65ccb1d21786dc7e1d_miniaodfake.root/USER': '6b159d7e5dc940daa2188658a68c4b23',
        }

def test_readLFN2PFNMap(LFN2PFNMapJSON):
    path = '/path/to/lfn2pfn.json'
    config.args = Namespace(lfn2pfn_map_path=path, ignore_lfn2pfn_map=False)
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open, read_data=json.dumps(LFN2PFNMapJSON)) as mo:
        t = Transfer()
        t.readLFN2PFNMap()
        # check if open correct file
        mo.assert_called_once_with(path, 'r', encoding='utf-8')
        assert t.LFN2PFNMap == LFN2PFNMapJSON

def test_updateLFN2PFNMap(LFN2PFNMapJSON, fs):
    path = '/path/to/lfn2pfn.json'
    config.args = Namespace(lfn2pfn_map_path=path)
    fs.create_file(path)
    with open(path, 'w', encoding='utf-8') as mock_file:
        with patch('ASO.Rucio.Transfer.writePath') as mock_writePath:
            mock_writePath.return_value.__enter__.return_value = mock_file
            t = Transfer()
            t.LFN2PFNMap = LFN2PFNMapJSON
            t.updateLFN2PFNMap()
            mock_writePath.assert_called_once_with(path)
    with open(path, 'r', encoding='utf-8') as mock_file:
        fileContent = json.loads(mock_file.read())
        assert fileContent == LFN2PFNMapJSON

def test_readCleanedFiles(cleanedFiles):
    path = '/path/to/files.txt'
    config.args = Namespace(cleaned_files_path=path, ignore_cleaned_files=False)
    with patch('ASO.Rucio.Transfer.open', new_callable=mock_open, read_data=json.dumps(cleanedFiles)) as mo:
        t = Transfer()
        t.readCleanedFiles()
        # check if open correct file
        mo.assert_called_once_with(path, 'r', encoding='utf-8')
        assert t.cleanedFiles == cleanedFiles

def test_updateCleanedFiles(cleanedFiles, fs):
    path = '/path/to/files.txt'
    config.args = Namespace(cleaned_files_path=path, ignore_cleaned_files=False)
    fs.create_file(path)
    with open(path, 'w', encoding='utf-8') as mock_file:
        with patch('ASO.Rucio.Transfer.writePath') as mock_writePath:
            mock_writePath.return_value.__enter__.return_value = mock_file
            t = Transfer()
            t.cleanedFiles = cleanedFiles
            t.updateCleanedFiles()
            mock_writePath.assert_called_once_with(path)
    with open(path, 'r', encoding='utf-8') as mock_file:
        fileContent = json.loads(mock_file.read())
        assert fileContent == cleanedFiles
