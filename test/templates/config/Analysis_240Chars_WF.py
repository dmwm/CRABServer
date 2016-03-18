from CRABClient.UserUtilities import config
config = config()

from time import time
runNumber = str(time()).replace('.','-')

head='HG1512e-rc4-old-client-2-240CharsWF'
size=240
padd="X"*(size-len(head)-4)
version='01'
reqName='%s-%s-%s'%(head,padd,version)

config.section_("General")
config.General.requestName = reqName

config.General.workArea = 'HG1512e-rc4-old-client-1'
config.General.instance = 'preprod'

config.section_("JobType")
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'psets/pset_tutorial_analysis.py'

config.section_("Data")
config.Data.inputDataset = '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO'
config.Data.inputDBS = 'global'
config.Data.splitting = 'FileBased'
config.Data.unitsPerJob = 10
#config.Data.outLFNDirBase = '/store/user/jmsilva'

config.Data.publication = False
config.Data.outputDatasetTag = "%s-%s"%(head,runNumber)

config.section_("Site")
config.Site.storageSite = 'T2_CH_CERN'

