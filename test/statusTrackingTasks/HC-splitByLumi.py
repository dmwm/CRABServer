from __future__ import division
import time
from WMCore.Configuration import Configuration
import os

config = Configuration()

config.section_("General")
config.General.instance = os.getenv('REST_Instance','test2')
config.General.restHost = ''
config.General.dbInstance = ''
config.General.workArea = '/tmp/crabStatusTracking'

config.section_("JobType")
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'pset.py'

config.section_("Data")
config.Data.inputDataset = os.getenv('inputDataset','somedefaultdset')

config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 1
config.JobType.maxJobRuntimeMin = 60
config.Data.totalUnits = 100

config.Data.publication = True
testName = "autotest-%d" % int(time.time())
config.Data.outputDatasetTag = testName

config.section_("User")

config.section_("Site")
config.Site.whitelist = ['T1_*','T2_US_*','T2_IT_*','T2_DE_*','T2_ES_*','T2_FR_*','T2_UK_*']
config.Site.blacklist = ['T2_ES_IFCA']

config.Site.storageSite = 'T2_CH_CERN'
 
config.section_("Debug")
config.Debug.scheddName = 'crab3@vocms059.cern.ch'


