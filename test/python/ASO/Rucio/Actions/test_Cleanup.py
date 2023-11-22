"""
unittest
"""

import json
import pytest
from argparse import Namespace
from unittest.mock import patch, Mock, create_autospec

import ASO.Rucio.config as config
from ASO.Rucio.Transfer import Transfer
from ASO.Rucio.Actions.Cleanup import Cleanup

# copy from test_Transfers
def cleanedFiles():
    path = 'test/assets/transferDicts.json'
    with open(path, 'r', encoding='utf-8') as r:
        transfersItem = json.load(r)
    return [xdict['destination_lfn'] for xdict in transfersItem]

@pytest.fixture(name='bookkeepingOKLock')
def fixture_bookkeepingOKLock():
    """
    Same fixture as fixture_cleanedFiles.
    Will find the way to share those later.
    """
    return cleanedFiles()

@pytest.fixture(name='LFN2transferItemMap')
def fixture_LFN2transferItemMap():
    with open('test/assets/LFN2transferItemMap.json') as r:
        return json.load(r)


def test_deleteFileFromTempArea(bookkeepingOKLock, LFN2transferItemMap):
    t = create_autospec(Transfer, instance=True)
    # input1
    # input2 fileDocs
    # return nothing but have log.
    # mock subprocess.run
    LFN2PFNMap = {
        'T2_CH_CERN_Temp': {
            '/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/log/cmsRun_3.log.tar.gz': 'davs://eoscms.cern.ch:443/eos/cms/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/log/cmsRun_3.log.tar.gz',
            '/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/miniaodfake_3.root': 'davs://eoscms.cern.ch:443/eos/cms/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/miniaodfake_3.root',
            '/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/output_3.root': 'davs://eoscms.cern.ch:443/eos/cms/store/temp/user/tseethon.d6830fc3715ee01030105e83b81ff3068df7c8e0/tseethon/test-rucio/ruciotransfers-1697125324/GenericTTbar/ruciotransfers-1697125324/231012_154207/0000/output_3.root',
        }
    }

    t.LFN2PFNMap = LFN2PFNMap
    t.LFN2transferItemMap = LFN2transferItemMap
    proxyPath = '/path/to/proxy'
    t.restProxyFile = proxyPath
    logPath = '/path/to/gfal.log'
    config.args = Namespace(gfal_log_path=logPath)
    expectedPFNs = LFN2PFNMap['T2_CH_CERN_Temp'].values()
    with patch('ASO.Rucio.Actions.Cleanup.callGfalRm', autospec=True) as mo_callGfalRm:
        m = Cleanup(t)
        m.deleteFileInTempArea(bookkeepingOKLock)
        mo_callGfalRm.assert_called_once()
        calledPFNs, calledProxy, calledLogPath = mo_callGfalRm.call_args.args
        assert calledProxy == proxyPath
        assert calledLogPath == logPath
        for pfn in expectedPFNs:
            assert pfn in calledPFNs
