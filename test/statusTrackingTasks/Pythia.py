from __future__ import division
import time
from WMCore.Configuration import Configuration
config = Configuration()

config.section_("General")
config.General.instance = 'test2'
config.General.restHost = ''
config.General.dbInstance = ''
config.General.workArea = '/tmp/crabStatusTracking'
config.General.requestName = 'Pythia'

config.section_("JobType")
config.JobType.pluginName = 'PrivateMC'
config.JobType.psetName = 'psetdemoMC.py'
config.JobType.maxJobRuntimeMin = 60
config.JobType.allowUndistributedCMSSW = True

config.section_("Data")
config.Data.splitting = 'EventBased'
config.Data.unitsPerJob = 1
config.JobType.maxJobRuntimeMin = 60
config.Data.totalUnits = 10

config.Data.publication = True
testName = "autotest-%d" % int(time.time())
config.Data.outputDatasetTag = testName
config.Data.outputPrimaryDataset = 'CRABTest'

config.section_("User")

config.section_("Site")
config.Data.ignoreLocality = True
config.Site.whitelist = ['T1_*','T2_US_*','T2_IT_*','T2_DE_*','T2_ES_*','T2_FR_*','T2_UK_*']
config.Site.blacklist = ['T2_ES_IFCA']

config.Site.storageSite = 'T2_CH_CERN'
 
config.section_("Debug")
config.Debug.scheddName = 'crab3@vocms059.cern.ch'


