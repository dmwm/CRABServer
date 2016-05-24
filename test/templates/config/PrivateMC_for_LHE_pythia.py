from WMCore.Configuration import Configuration
config = Configuration()

config.section_("General")
config.General.requestName = 'CHANGE'
config.General.workArea = 'CHANGE' 
config.General.transferOutputs = True
config.General.transferLogs = True
config.General.instance = 'preprod' 
config.General.activity = 'analysistest'

config.section_("JobType")
config.JobType.pluginName = 'PrivateMC'
config.JobType.generator = 'pythia'
config.JobType.psetName = 'psets/pset_no_lhe_source.py'
config.JobType.inputFiles = ['input_files/dynlo.lhe']
config.JobType.disableAutomaticOutputCollection = False

config.section_("Data")
config.Data.outputPrimaryDataset = 'MinBias'
config.Data.splitting = 'EventBased'
config.Data.ignoreLocality = False
config.Data.unitsPerJob = 50
NJOBS = 200
config.Data.totalUnits = config.Data.unitsPerJob * NJOBS - 50
config.Data.publishDBS = 'phys03'
config.Data.outputDatasetTag = 'CHANGE' 

config.section_("Site")
config.Site.storageSite = 'CHANGE' 
