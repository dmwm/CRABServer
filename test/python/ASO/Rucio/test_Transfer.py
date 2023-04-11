# should test detect provided wrong path to open function?

import pytest
import builtins
import json
from unittest.mock import patch, mock_open
from argparse import Namespace

from ASO.Rucio.Transfer import Transfer
from ASO.Rucio.exception import RucioTransferException
import ASO.Rucio.config as config


def test_Transfer_readInfo():

    restInfoForFileTransfersJson = {
        "host": "cmsweb-test12.cern.ch:8443",
        "dbInstance": "devthree",
        "proxyfile": "9041f6500ff40aaca33737316a2dbfb57116e8e0"
    }
    transfersTxt = {
        "id": "98f353b91ec84f0217da80bde84d6b520c0c6640f60ad9aabb7b20ca",
        "username": "tseethon",
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
        "checksums": {"adler32": "812b8235", "cksum": "1236675270"},
        "outputdataset": "/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER"
    }
    config.config = Namespace(force_publishname=None, rest_info_path='/a/b/c', transfer_info_path='/d/e/f')
    with patch('builtins.open', new_callable=mock_open, read_data=f'{json.dumps(restInfoForFileTransfersJson)}\n') as mo:
        # setup config
        mo.side_effect = (mo.return_value, mock_open(read_data=f'{json.dumps(transfersTxt)}\n').return_value,)
        # run config
        t = Transfer()
        t.readInfo()
        assert t.proxypath == '9041f6500ff40aaca33737316a2dbfb57116e8e0'
        assert t.username == 'tseethon'
        assert t.rucioScope == 'user.tseethon'
        assert t.destination == 'T2_CH_CERN'
        assert t.publishname == '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER'
        assert t.logsDataset == '/GenericTTbar/tseethon-autotest-1679671056-94ba0e06145abd65ccb1d21786dc7e1d/USER#LOGS'
        assert t.currentDataset == ''

def test_Transfer_readInfo_filenotfound():
    # setup config

    # run transfer
    config.config = Namespace(rest_info_path='/a/b/c', transfer_info_path='/d/e/f')
    t = Transfer()
    with pytest.raises(RucioTransferException):
        t.readInfo()
