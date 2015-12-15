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
config.Data.userInputFiles = open('input_files/userinputfiles.txt').readlines()
config.Data.inputDBS = 'global'
config.Data.splitting = 'FileBased'
config.Data.unitsPerJob = 10
config.Data.ignoreLocality = False
config.Data.publication = True
config.Data.publishDBS = 'phys03'
config.Data.outputPrimaryDataset='CHANGE'
config.Data.outputDatasetTag = 'CHANGE'

#Site Section
config.section_("Site")
config.Site.storageSite = 'CHANGE' 
