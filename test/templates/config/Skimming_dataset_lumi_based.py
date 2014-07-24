from WMCore.Configuration import Configuration
config = Configuration()

#General Section
config.section_("General")
config.General.requestName = 'CHANGE'
config.General.workArea = 'CHANGE'
config.General.transferOutput = True
config.General.saveLogs = True
config.General.instance = 'preprod'

#Job Type Section
config.section_("JobType")
config.JobType.pluginName  = 'Analysis'
config.JobType.psetName    = 'psets/skimming_dataset.py'

#Data Section
config.section_("Data")
config.Data.inputDataset = '/DoubleMuParked/Run2012D-22Jan2013-v1/AOD'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 200 
config.Data.publication = True
config.Data.publishDbsUrl = 'phys03'
config.Data.publishDataName = 'CHANGE'

# Site options
config.section_("Site")
config.Site.storageSite = 'CHANGE'
