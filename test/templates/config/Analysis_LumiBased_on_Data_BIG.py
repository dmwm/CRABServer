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
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'psets/pset_tutorial_analysis.py'
config.JobType.disableAutomaticOutputCollection = False

#Data Section
config.section_("Data")
config.Data.inputDataset = '/DoubleMuParked/Run2012B-22Jan2013-v1/AOD'
config.Data.inputDBS = 'global'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 1500 # 200
config.Data.ignoreLocality = False
#config.Data.lumiMask = 'https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions12/8TeV/Prompt/Cert_190456-208686_8TeV_PromptReco_Collisions12_JSON.txt'
#config.Data.lumiMask = 'lumimask/Cert_190456-208686_8TeV_PromptReco_Collisions12_JSON.txt' # if you downloaded the file in the working directory
#config.Data.runRange = '193093-193999' # '193093-194075'
config.Data.publication = True
config.Data.publishDBS = 'phys03'
config.Data.publishDataName = 'CHANGE'

#Site Section
config.section_("Site")
config.Site.storageSite = 'CHANGE'
