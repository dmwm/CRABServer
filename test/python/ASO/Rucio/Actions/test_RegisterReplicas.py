import pytest
import json
from unittest.mock import patch, Mock
from argparse import Namespace

from ASO.Rucio.Actions.RegisterReplicas import RegisterReplicas
import ASO.Rucio.config as config

# somehow register return [[item1,item2]] while it should return [item1, item2], but unittest fail to detect this

# prepare_replicas
# input is just transfers.txt start from last lines
# {"id": "7e6d075f7434f1307a764d491d86ab1192554b76106d139846810834", "username": "tseethon", "taskname": "230227_174038:tseethon_crab_rucio_transfer_test12_20230227_184034", "start_time": 1677520683, "destination": "T2_CH_CERN", "destination_lfn": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1677519634/230227_174038/0000/output_2.root", "source": "T3_US_FNALLPC", "source_lfn": "/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1677519634/230227_174038/0000/output_2.root", "filesize": 630710, "publish": 0, "transfer_state": "NEW", "publication_state": "NOT_REQUIRED", "job_id": "2", "job_retry_count": 1, "type": "output", "publishname": "autotest-1677519634-00000000000000000000000000000000", "checksums": {"adler32": "5cbf440e", "cksum": "3473488862"}, "outputdataset": "/GenericTTbar/tseethon-autotest-1677519634-94ba0e06145abd65ccb1d21786dc7e1d/USER"}
# expect output is MAP OF RSE key replicas in RSE
# {rse1: [replica1,replicas2], rse2: [replica3, replica4]}
# where
# replica = {'scope': glob.rucio_scope, 'pfn': pfn_map[rse][source_lfn],
#            'name': destination_lfn, 'bytes': size, 'adler32': checksum}
# and
# pfn_map is map of ALL lfn of input in format of
# pfn_map = {rse1: {source_lfn1: pfn}}
# well let change to make it more simpler

@pytest.fixture
def mock_rucioClient():
    with patch('rucio.client.client.Client', autospec=True) as m_rucioClient:
        return m_rucioClient

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
def loadTransferList():
    with open('test/assets/transfers.txt', encoding='utf-8') as r:
        return [json.loads(line) for line in r]

@pytest.fixture
def loadPrepareExpectedOutput():
    with open('test/assets/prepare_expectedoutput.json', encoding='utf-8') as r:
        return json.load(r)

def generateExpectedOutput(doctype):
    if doctype == 'success':
        return {
            'asoworker': 'rucio',
            'list_of_ids': ['98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca'], # hmm, how do we get this
            'list_of_transfer_state': ['SUBMITTED'],
            'list_of_dbs_blockname': ['/TestPrimary/test-dataset/RAW#c9b28b96-5d16-41cd-89af-2678971132c9'],
            'list_of_block_complete': ['NO'],
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/'],
            'list_of_failure_reason': None, # omit
            'list_of_retry_value': None, # omit
            'list_of_fts_id': ['NA'], # maybe we should put ruleid of container here
        }
    elif doctype == 'fail':
        return {
            'asoworker': 'rucio',
            'list_of_ids': ['98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca'], # hmm, how do we get this
            'list_of_transfer_state': ['FAILED'],
            'list_of_dbs_blockname': None,  # omit
            'list_of_block_complete': None, # omit
            'list_of_fts_instance': ['https://fts3-cms.cern.ch:8446/'],
            'list_of_failure_reason': ['Failed to register files within RUCIO'],
            # No need for retry -> delegate to RUCIO
            'list_of_retry_value': [0],
            'list_of_fts_id': ['NA'],
        }


def test_getSourcePFN(mock_Transfer, mock_rucioClient):
    srcRSE = 'T3_US_FNALLPC'
    dstRSE = 'T2_CH_CERN'
    srcLFN = '/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_17.root'
    srcPFN = 'root://eoscms.cern.ch//eos/cms/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_17.root'
    srcDid = f'{mock_Transfer.rucioScope}:{srcLFN}'
    returnLfns2pfns = {srcDid: srcPFN}
    mock_rucioClient.lfns2pfns.return_value = returnLfns2pfns
    with patch('ASO.Rucio.Actions.RegisterReplicas.find_matching_scheme', autospec=True) as mock_find_matching_scheme:
        mock_find_matching_scheme.return_value = ('', 'davs', '', '')
        # TODO: ensure RSE pass as argument should not have suffix with '_Temp'
        r = RegisterReplicas(mock_Transfer, mock_rucioClient, Mock())
        assert r.getSourcePFN(srcLFN, srcRSE, dstRSE) == srcPFN
        mock_find_matching_scheme.assert_called_once()
    mock_rucioClient.get_protocols.assert_called()
    mock_rucioClient.lfns2pfns.assert_called_once()

def test_prepare_single_xdict(mock_Transfer, mock_rucioClient):
    # input
    prepareInput = [{
        "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
        "username": "cmscrab",
        "taskname": "230324_151740:tseethon_crab_rucio_transfer_whitelist_cern_test12_20230324_161736",
        "start_time": 1679671544,
        "destination": "T2_CH_CERN",
        "destination_lfn": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
        "source": "T2_CH_CERN",
        "source_lfn": "/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
        "filesize": 628054,
        "publish": 0,
        "transfer_state": "NEW",
        "publication_state": "NOT_REQUIRED",
        "job_id": "9",
        "job_retry_count": 0,
        "type": "output",
        "publishname": "autotest-1679671056-00000000000000000000000000000000",
        "checksums": {
            "adler32": "812b8235",
            "cksum": "1236675270"
        },
        "outputdataset": "/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER"
    }]

    # expectedOuput should be
    getSourcePFNReturnValue = 'davs://eoscms.cern.ch:443/eos/cms/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root'
    expectedOutput = {
        "T2_CH_CERN_Temp": [
            {
                "scope": "user.cmscrab",
                "pfn": getSourcePFNReturnValue,
                "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
                "bytes": 628054,
                "adler32": "812b8235",
                "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca"
            }
        ]
    }
    with patch('ASO.Rucio.Actions.RegisterReplicas.RegisterReplicas.getSourcePFN', autospec=True) as mock_getSourcePFN:
        config.args = Namespace(force_replica_name_suffix=None)
        mock_getSourcePFN.return_value = getSourcePFNReturnValue
        mock_Transfer.replicasInContainer = []
        r = RegisterReplicas(mock_Transfer, mock_rucioClient, Mock())
        assert r.prepare(prepareInput) == expectedOutput

@pytest.mark.skip(reason="skip it for now due to deadline.")
def test_prepare_last_line_is_not_zero(mock_Transfer, mock_rucioClient):
    assert 0 == 1


@pytest.mark.skip(reason="Need to implement (or not?) but skip it for now.")
def test_prepare_skip_direct_stageout(mock_Transfer, mock_rucioClient, loadTransferList):
    assert 0 == 1

#def test_prepare_duplicate_replicas(mock_Transfer, mock_rucioClient, loadTransferList, loadPrepareExpectedOutput):
#    prepareInput = loadTransferList[:3]
#    mock_Transfer.rucioScope = f"user.{loadTransferList[0]['username']}"
#    mock_Transfer.replicasInContainer = [prepareInput[0]['destination_lfn']] # skip first
#    expectedOutput = loadPrepareExpectedOutput
#    expectedOutput['T2_CH_CERN_Temp'] = loadPrepareExpectedOutput['T2_CH_CERN_Temp'][1:] # expect 2 items
#    getSourcePFNReturnValue = [x['pfn'] for x in expectedOutput['T2_CH_CERN_Temp']]
#    config.args = Namespace(force_replica_name_suffix=None)
#    with patch('ASO.Rucio.Actions.RegisterReplicas.RegisterReplicas.getSourcePFN', autospec=True) as mock_getSourcePFN:
#        mock_getSourcePFN.side_effect = getSourcePFNReturnValue
#        r = RegisterReplicas(mock_Transfer, mock_rucioClient, Mock())
#        assert r.prepare(prepareInput) == expectedOutput

def test_removeRegisteredReplicas(mock_Transfer, mock_rucioClient, loadPrepareExpectedOutput):
    fInput = {
        'T2_CH_CERN_Temp': loadPrepareExpectedOutput['T2_CH_CERN_Temp'][:3]
    }
    mock_Transfer.replicasInContainer = {
        '/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root': '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#ec579451-65f1-484f-a345-8c29916c3715'
    }
    expectedReplicasToRegister = {
        'T2_CH_CERN_Temp': loadPrepareExpectedOutput['T2_CH_CERN_Temp'][1:3]
    }
    expectedRegisteredReplicas = [{
        "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
        "dataset": '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#ec579451-65f1-484f-a345-8c29916c3715',
        "blockcomplete": 'NO',
    }]
    expectedOutput = (expectedReplicasToRegister, expectedRegisteredReplicas)
    config.args = Namespace(force_replica_name_suffix=None)
    r = RegisterReplicas(mock_Transfer, Mock(), Mock())
    assert r.removeRegisteredReplicas(fInput) == expectedOutput

def test_register_success(mock_Transfer, mock_rucioClient):
    prepareReplicasByRSE = {
        "T2_CH_CERN_Temp": [
            {
                "scope": "user.cmscrab",
                "pfn": 'davs://eoscms.cern.ch:443/eos/cms/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root',
                "name": "/store/user/rucio/tseethon/test-workflow/GenericTTbar/autotest-1679671056/230324_151740/0000/output_9.root",
                "bytes": 628054,
                "adler32": "812b8235",
                "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            }
        ]
    }
    expectedSuccess = [
        {
            "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
            "dataset": mock_Transfer.currentDataset,
        }
    ]
    expectedFail = []
    config.config = Namespace(replicas_chunk_size=2, dataset_file_limit=9)
    r = RegisterReplicas(mock_Transfer, mock_rucioClient, Mock())
    s, f = r.register(prepareReplicasByRSE)
    # FIXME: should we add replica one at a time instead of bulk?
    # to prevent one file cause fail whole bulk and prevent postjob to retry
    # TODO: ensure add_replicas only have all key exception id (id is crab id we store in rest)
    mock_rucioClient.add_replicas.assert_called()
    mock_rucioClient.add_files_to_datasets.assert_called()

@pytest.mark.skip(reason="Skip it for now due deadline.")
def test_register_fail(mock_Transfer, mock_rucioClient):
    assert 0 == 1

@pytest.mark.skip(reason="Skip it for now due deadline.")
def test_register_mix_fail_and_success(mock_Transfer, mock_rucioClient):
    assert 0 == 1

@pytest.mark.skip(reason="Skip it for now due deadline.")
def test_register_rerun_from_last_crash(mock_Transfer, mock_rucioClient):
    assert 0 == 1

@pytest.mark.skip(reason="Skip it for now due deadline.")
def test_register_create_new_dataset(mock_Transfer, mock_rucioClient):
    assert 0 == 1


@pytest.mark.skip(reason="Skip it for now due deadline.")
def test_register_ensure_bookkeeping_to_last_transfers(mock_Transfer, mock_rucioClient):
    assert 0 == 1


def test_prepareSuccessFileDoc(mock_Transfer):
    successFileDoc = generateExpectedOutput('success')
    outputSuccess = [
        {
            "id": successFileDoc['list_of_ids'][0],
            "dataset": successFileDoc['list_of_dbs_blockname'][0],
            "blockcomplete": 'NO',
        }
    ]
    r = RegisterReplicas(mock_Transfer, Mock(), Mock())
    assert successFileDoc == r.prepareSuccessFileDoc(outputSuccess)

def test_prepareFailFileDoc(mock_Transfer):
    failFileDoc = generateExpectedOutput('fail')
    outputFail = [
        {
            "id": failFileDoc['list_of_ids'][0],
            "dataset": '',
            "blockcomplete": 'NO',
        }
    ]
    r = RegisterReplicas(mock_Transfer, Mock(), Mock())
    assert failFileDoc == r.prepareFailFileDoc(outputFail)
