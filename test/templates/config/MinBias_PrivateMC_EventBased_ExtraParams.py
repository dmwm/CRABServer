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
config.JobType.pluginName = 'PrivateMC'
config.JobType.psetName = 'psets/pset_tutorial_MC_generation.py'
config.JobType.maxJobRuntimeMin = 1000
config.JobType.numCores = 1
config.JobType.maxMemoryMB = 2000
config.JobType.disableAutomaticOutputCollection = False

#Data Section
config.section_("Data")
config.Data.primaryDataset = 'MinBias'
config.Data.splitting = 'EventBased'
config.Data.unitsPerJob = 100 
config.Data.totalUnits = 10000
config.Data.ignoreLocality = False
config.Data.publication = True
config.Data.publishDBS = 'phys03'
config.Data.publishDataName = 'CHANGE'

#User Section
config.section_("User")
config.User.voRole = ''
config.User.voGroup = ''

#Site Section
config.section_("Site")
config.Site.storageSite = 'CHANGE'

#Debug Section
config.section_("Debug")
#config.Debug.extraJDL = ['+CRAB_NoWNStageout=1', '+CRAB_HC=True'] # %%HammerCloud%%
#config.Debug.extraJDL = ['CRAB_StageoutPolicy = "remote"'] # %%stageout%%

