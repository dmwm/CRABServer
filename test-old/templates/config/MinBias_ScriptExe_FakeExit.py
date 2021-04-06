from WMCore.Configuration import Configuration
config = Configuration()

#General Section
config.section_("General")
config.General.requestName = 'CHANGE'
config.General.workArea = 'CHANGE'
config.General.transferOutputs = False
config.General.transferLogs = False
config.General.instance = 'preprod'
config.General.activity = 'analysistest'

#Job Type Section
config.section_("JobType")
config.JobType.pluginName = 'PrivateMC'
config.JobType.psetName = 'psets/pset_tutorial_MC_generation.py'
config.JobType.scriptExe = 'input_files/fake_exit_code.sh'
config.JobType.disableAutomaticOutputCollection = False

#Data Section
config.section_("Data")
config.Data.outputPrimaryDataset = 'MinBias'
config.Data.splitting = 'EventBased'
config.Data.unitsPerJob = 10
config.Data.totalUnits = 10000
config.Data.ignoreLocality = False
config.Data.publication = False
config.Data.publishDBS = 'phys03'
config.Data.outputDatasetTag = 'CHANGE'

#Site Section
config.section_("Site")
config.Site.storageSite = 'CHANGE'
