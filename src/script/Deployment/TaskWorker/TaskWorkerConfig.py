# TaskWorker configuration example
# original file from https://gitlab.cern.ch/ai/it-puppet-hostgroup-vocmsglidein/-/blob/bc61a79da85d72e4db90b3fbca07a369f456e9ee/code/templates/crabtaskworker/taskworker/TaskWorkerConfig.py.erb

import os
from WMCore.Configuration import ConfigurationEx

config = ConfigurationEx()

## External services url's
config.section_("Services")
## Which DBS instance should be used
config.Services.DBSHostName = 'cmsweb-prod.cern.ch'
config.Services.MyProxy= 'myproxy.cern.ch'
config.Services.Rucio_host = 'https://cms-rucio.cern.ch'
config.Services.Rucio_account = 'crab_server'
config.Services.Rucio_authUrl = 'https://cms-rucio-auth.cern.ch'
config.Services.Rucio_caPath = '/etc/grid-security/certificates/'
config.Services.Rucio_cert = '/data/certs/robotcert.pem'
config.Services.Rucio_key = '/data/certs/robotkey.pem'


config.section_("TaskWorker")
############################################################################
## Following parameters NEED to be customized for every TW installation
## values here default to a generic development instance, values
## REMEMBER TO UPDATE THIS WHEN USING THIS FILE
############################################################################
config.TaskWorker.name = 'crab-dev-tw01' #Remember to update this!


## How much work can be done in parallel
config.TaskWorker.nslaves = 1


## recurring actions: only the simplest one for minimal testing
## other make no sense (or do not work) other than in Production
## anyhow Recurring Actions can be tested as standalone scripts
config.TaskWorker.recurringActions = ['RemovetmpDir', 'BanDestinationSites', 'GetAcceleratorSite']


## Which CRAB SERVICE instance is this TW part of
## Possible values for instance are:
#   - preprod   : uses cmsweb-testbed RESt with preprod DB
#   - prod      : uses cmsweb,cern.ch production REST with production DB
#   - other     : allows to use any restHost with andy dbInstance, those needs to be
#                  specified in the two following parameters
config.TaskWorker.instance = 'test2'
config.TaskWorker.restURInoAPI = '/crabserver/test2/'


config.TaskWorker.restHost = ''
config.TaskWorker.dbInstance = ''


## Decide if this TW is going to use v1 or v2 of HTC python bindings
config.TaskWorker.useHtcV2 = True



############################################################################
## Following parameters are good defaults for any TW and should not be changed
## lightly nor without a godd reason
############################################################################

config.TaskWorker.polling = 30 #seconds
config.TaskWorker.scratchDir = '/data/srv/tmp' #make sure this directory exists
config.TaskWorker.logsDir = './logs'

## the parameters here below are used to contact cmsweb services for the REST-DB interactions
config.TaskWorker.cmscert = '/data/certs/robotcert.pem'
config.TaskWorker.cmskey = '/data/certs/robotkey.pem'

config.TaskWorker.backend = 'glidein'

# for connection to HTCondor scheds
config.TaskWorker.SEC_TOKEN_DIRECTORY = '/data/certs/tokens.d'

#Retry policy
config.TaskWorker.max_retry = 4
config.TaskWorker.retry_interval = [30, 60, 120, 0]

#Default False. If true dagman will not retry the job on ASO failures
config.TaskWorker.retryOnASOFailures = True
#Dafault 0. If -1 no ASO timeout, if transfer is stuck in ASO we'll retry the postjob FOREVER (well, eventually a dagman timeout for the node will be hit).
#If 0 default timeout of 4 to 6 hours will be used. If specified the timeout set will be used (minutes).
config.TaskWorker.ASOTimeout = 86400

#When Rucio is used for ASO, we should be prepared to wait for a longer time
config.TaskWorker.ASORucioTimeout = 86400 * 7  # seven days

# Control the ordering of stageout attempts.
# - remote means a copy from the worker node to the final destination SE directly.
# - local means a copy from the worker node to the worker node site's SE.
# One can include any combination of the above, or leaving one of the methods out.
# This is the CRAB3 default: ["local", "remote"]:
config.TaskWorker.stageoutPolicy = ["local", "remote"]

# This is needed for Site Metrics
config.TaskWorker.dashboardTaskType = 'analysis'

# HammerCloud and other submissions for Site Metrics should always run
config.TaskWorker.ActivitiesToRunEverywhere = ['hctest', 'hcdev']

config.TaskWorker.maxIdle = 1000
config.TaskWorker.maxPost = 20

## Decide if this TW is going to use v1 or v2 of HTC python bindings
## Before the first import, must set an env. var which will be used to
## in every file to decide if to do "import htcondor" or "import htcondor2 as htcondor"
## and same for classad
if getattr(config.TaskWorker, 'useHtcV2', None):
    os.environ['useHtcV2'] = 'True'
else:
    os.environ.pop('useHtcV2', None)
import HTCondorLocator
config.TaskWorker.scheddPickerFunction = HTCondorLocator.capacityMetricsChoicesHybrid

config.TaskWorker.tiersToRecall = ['AOD', 'AODSIM', 'MINIAOD', 'MINIAODSIM', 'NANOAOD', 'NANOAODSIM']
config.TaskWorker.maxTierToBlockRecallSizeTB = 50
config.TaskWorker.maxAnyTierRecallSizeTB = 50
config.TaskWorker.maxRecallPerUserTB = 500

config.section_("Sites")
config.Sites.DashboardURL = "https://cmssst.web.cern.ch/cmssst/analysis/usableSites.json"

config.section_("MyProxy")
config.MyProxy.serverhostcert = '/data/certs/robotcert.pem'
config.MyProxy.serverhostkey = '/data/certs/robotkey.pem'
config.MyProxy.cleanEnvironment = True
config.MyProxy.credpath = '/data/certs/creds' #make sure this directory exists

# Setting the minimum runtime requirements in minutes for automatic splitting
config.TaskWorker.minAutomaticRuntimeMins = 60
config.TaskWorker.highPrioEgroups = ['cms-crab-HighPrioUsers']
config.TaskWorker.bannedUsernames = ['mickeymouse', 'donaldduck']
config.TaskWorker.rejectedCommands = ['NONE', 'NONE']  # commands are upper case e.g. 'SUBMIT'

# DEPRECATED: use config.TaskWorker.acceleratorSitesCollector instead.
# glidein pool for get accelerator sites
config.TaskWorker.glideinPool = 'vocms0207.cern.ch'

# collector endpoint for getting list of accelerator sites
config.TaskWorker.acceleratorSitesCollector = 'cmsgwms-factory.cern.ch'

# Task scheduling
config.TaskWorker.task_scheduling_limit = 10
config.TaskWorker.task_scheduling_dry_run = False

config.section_("FeatureFlags")
config.FeatureFlags.childWorker = True
config.FeatureFlags.childWorkerTimeout = 3600
