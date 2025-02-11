from __future__ import division
import time
from WMCore.Configuration import Configuration
import os
import datetime
from CRABClient.UserUtilities import getUsername

now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
filename_nopy =  __file__.split('/')[-1][:-3]

config = Configuration()

config.section_("General")
config.General.instance = os.getenv('REST_Instance','test12')
#config.General.restHost = ''
#config.General.dbInstance = ''
config.General.requestName = filename_nopy + '_' + now_str
config.General.workArea = f'/tmp/crabStatusTracking_{os.environ["CMSSW_release"]}'
config.General.transferLogs=False

config.section_("JobType")
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'psetManyEDM.py'

config.section_("Data")
config.Data.inputDataset = os.getenv('inputDataset','/GenericTTbar/HC-CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/AODSIM')

config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 1
config.JobType.maxJobRuntimeMin = 60
config.Data.totalUnits = 40

config.Data.publication = False
testName = "ruciotransfers-%d" % int(time.time())
#testName = 'ruciotransfers-1'
config.Data.outputDatasetTag = testName
# rucio
config.Data.outLFNDirBase = '/store/%s/rucio/%s/%s' % ('user', getUsername(), testName,)

config.section_("User")

config.section_("Site")
config.Site.whitelist = ['T1_*','T2_US_*','T2_IT_*','T2_DE_*','T2_ES_*','T2_FR_*','T2_UK_*']
#config.Site.blacklist = ['T2_ES_IFCA']
config.Site.storageSite = 'T2_CH_CERN'

config.section_("Debug")
config.Debug.scheddName = 'crab3@vocms059.cern.ch'
