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
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'psets/pset_tutorial_analysis.py'

#Data Section
config.section_("Data")
config.Data.inputDataset = '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO'
config.Data.dbsUrl = 'global'
config.Data.splitting = 'FileBased'
config.Data.unitsPerJob = 10
config.Data.ignoreLocality = False
config.Data.publication = True
config.Data.publishDbsUrl = 'phys03'
config.Data.publishDataName = 'CHANGE'

#Site Section
config.section_("Site")
config.Site.storageSite = 'CHANGE' 
