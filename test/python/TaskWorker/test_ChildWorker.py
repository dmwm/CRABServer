import time
import pytest
import ctypes
from argparse import Namespace
from unittest.mock import Mock

from WMCore.Configuration import ConfigurationEx
from TaskWorker.ChildWorker import startChildWorker
from TaskWorker.WorkerExceptions import ChildUnexpectedExitException, ChildTimeoutException



@pytest.fixture
def config_ChildWorker():
    config = ConfigurationEx()
    config.section_("FeatureFlags")
    config.FeatureFlags.childWorkerTimeout = 1
    return config

@pytest.fixture
def mock_logger():
    logger = Mock()
    logger.name = '1'
    return logger

def fn(n, timeSleep=0, mode='any'):
    """
    function to test startChildWorker contains 4 behaviors
    1. normal: normal function
    2. exception: any exception occur in child should raise to parent properly
    3. timeout: should raise ChildTimeoutException
    4. coredump: should raise ChildUnexpectedExitException
    """
    print(f'executing function with n={n},timeSleep={timeSleep},mode={mode}')
    if mode == 'exception':
        raise TypeError('simulate raise generic exception')
    elif mode == 'timeout':
        time.sleep(timeSleep)
    elif mode == 'coredump':
        #https://codegolf.stackexchange.com/a/22383
        ctypes.string_at(1)
    else:
        pass
    return n*5

testList = [
    (17, 0, 'any', None),
    (17, 0, 'exception', TypeError),
    (17, 5, 'timeout', ChildTimeoutException),
    (17, 0, 'coredump', ChildUnexpectedExitException),
]
@pytest.mark.parametrize("n, timeSleep, mode, exceptionObj", testList)
def test_executeTapeRecallPolicy_allow(n, timeSleep, mode, exceptionObj, config_ChildWorker, mock_logger):
    if not exceptionObj:
        startChildWorker(config_ChildWorker, fn, (n, timeSleep, mode), mock_logger)
    else:
        with pytest.raises(exceptionObj):
            startChildWorker(config_ChildWorker, fn, (n, timeSleep, mode), mock_logger)
