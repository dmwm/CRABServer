import time
from WMCore.Configuration import Configuration
config = Configuration()

config.section_("General")
config.General.instance = 'other'
config.General.restHost = 'cmsweb-test2.cern.ch'
config.General.dbInstance = 'dev'


config.section_("JobType")
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'pset.py'

config.section_("Data")
config.Data.inputDataset = '/GenericTTbar/HC-CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/AODSIM'

config.Data.splitting = 'EventAwareLumiBased'
config.Data.unitsPerJob = 1
config.JobType.maxJobRuntimeMin = 60
config.Data.totalUnits = 100

config.Data.publication = True
testName = "autotest-%d" % int(time.time())
config.Data.outputDatasetTag = testName

config.section_("User")

config.section_("Site")
config.Site.whitelist = ['T1_*','T2_US_*','T2_IT_*','T2_DE_*','T2_ES_*','T2_FR_*','T2_UK_*']

config.Site.storageSite = 'T2_CH_CERN'
 
config.section_("Debug")
config.Debug.scheddName = 'crab3@vocms059.cern.ch'


