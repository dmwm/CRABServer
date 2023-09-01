"""
Test DBSDataDiscovery

Tape recall policy
1. If recall (partial)dataset are in (|MINI|NANO)AOD(|SIM) tier allow recall any size
2. allow to recall any (partial)dataset with size less than 50 TB
"""

import pytest
import json
import os
from unittest.mock import patch, Mock
from argparse import Namespace

from WMCore.Configuration import ConfigurationEx
from TaskWorker.Actions.DBSDataDiscovery import DBSDataDiscovery
from TaskWorker.WorkerExceptions import TaskWorkerException


@pytest.fixture
def config_DBSDataDiscovery():
    config = ConfigurationEx()
    config.section_("TaskWorker")
    # policy
    config.TaskWorker.tiersToRecall = ['AOD', 'AODSIM', 'MINIAOD', 'MINIAODSIM', 'NANOAOD', 'NANOAODSIM']
    config.TaskWorker.maxTierToBlockRecallSizeTB = 50
    config.TaskWorker.maxAnyTierRecallSizeTB = 50
    return config

testdataAllow = [
    ('/GenericTTbar/Run3-Unittest-dataset/AODSIM', None, 1e15),   # anysize, in tierToRecall
    ('/GenericTTbar/Run3-Unittest-dataset/ALCARECO', None, 1e12), # 1TB, not in tierToRecall, less than maxAnyTierRecallSizeTB
    ('/GenericTTbar/Run3-Unittest-dataset/ANY', ['/GenericTTbar/Run3-Unittest-dataset/ANY#2b4bf54e-ebad-4948-8730-432434bbcb2f'], 1e12),
]

testdataDeny = [
    ('/GenericTTbar/Run3-Unittest-dataset/ALCARECO', None, 1e15), # 1PB, not in tierToRecall
    ('/GenericTTbar/Run3-Unittest-dataset/ANY', ['/GenericTTbar/Run3-Unittest-dataset/ANY#2b4bf54e-ebad-4948-8730-432434bbcb2f'], 100e12),
    ('/GenericTTbar/Run3-Unittest-dataset/ANY', ['/GenericTTbar/Run3-Unittest-dataset/ANY#2b4bf54e-ebad-4948-8730-432434bbcb2f'], 70e12),
]

@pytest.mark.parametrize("inputDataset, inputBlocks, totalSizeBytes", testdataAllow)
def test_executeTapeRecallPolicy_allow(inputDataset, inputBlocks, totalSizeBytes, config_DBSDataDiscovery):
    config = config_DBSDataDiscovery
    d = DBSDataDiscovery(config)
    d.executeTapeRecallPolicy(inputDataset, inputBlocks, totalSizeBytes)

@pytest.mark.parametrize("inputDataset, inputBlocks, totalSizeBytes", testdataDeny)
def test_executeTapeRecallPolicy_deny(inputDataset, inputBlocks, totalSizeBytes, config_DBSDataDiscovery):
    config = config_DBSDataDiscovery
    d = DBSDataDiscovery(config)
    with pytest.raises(TaskWorkerException):
        d.executeTapeRecallPolicy(inputDataset, inputBlocks, totalSizeBytes)
