from WMCore.Configuration import Configuration
config = Configuration()

#General Section
config.section_("General")
config.General.requestName = 'CHANGE'
config.General.workArea = 'CHANGE'
config.General.transferOutputs = True
config.General.transferLogs = True
config.General.instance = 'preprod'
config.General.activity = 'analysistest'

#Job Type Section
config.section_("JobType")
config.JobType.pluginName  = 'Analysis'
config.JobType.psetName    = 'psets/skimming_dataset.py'
config.JobType.disableAutomaticOutputCollection = False

#Data Section
config.section_("Data")
config.Data.inputDataset = '/DoubleMuParked/Run2012D-22Jan2013-v1/AOD'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 200 
config.Data.publication = True
config.Data.ignoreLocality = False
config.Data.publishDBS = 'phys03'
config.Data.publishDataName = 'CHANGE'

# Site options
config.section_("Site")
config.Site.storageSite = 'CHANGE'
