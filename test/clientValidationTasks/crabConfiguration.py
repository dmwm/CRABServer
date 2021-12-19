from __future__ import division
from datetime import datetime
from WMCore.Configuration import Configuration
import os

config = Configuration()

config.section_("General")
config.General.transferOutputs = True
config.General.transferLogs = True
config.General.instance = os.getenv('REST_Instance','test2')
config.General.workArea = '/tmp/crabClientValidation'

config.section_("JobType")
config.JobType.allowUndistributedCMSSW = True
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'pset.py'

config.section_("Data")
config.Data.inputDataset = os.getenv('inputDataset','/GenericTTbar/HC-CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/AODSIM')
config.Data.inputDBS = 'global'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 2
config.Data.totalUnits = 20
config.Data.publication = True
config.Data.outputDatasetTag = 'CRAB3_analysis-%d' % int(datetime.now().strftime('%y%m'))

config.section_("Site")
config.Site.storageSite = 'T2_CH_CERN'

config.section_("Debug")
config.Debug.scheddName = 'crab3@vocms059.cern.ch'
