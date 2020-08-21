from WMCore.Configuration import ConfigurationEx
import HTCondorLocator

config = ConfigurationEx()

## External services url's
config.section_("Services")
config.Services.DBSHostName = 'cmsweb.cern.ch'
config.Services.MyProxy= 'myproxy.cern.ch'
config.Services.Rucio_host = 'https://cms-rucio.cern.ch'
config.Services.Rucio_account = 'crab_server'
config.Services.Rucio_authUrl = 'https://cms-rucio-auth.cern.ch'
config.Services.Rucio_caPath = '/etc/grid-security/certificates/'

config.section_("TaskWorker")
############################################################################
## Following parameters NEED to be customized for every TW installation
## values here default to a generic development instance, values
## REMEMBER TO UPDATE THIS WHEN USING THIS FILE
## currently used in production are marked with ##PROD
############################################################################

## This TaskWorker identity:
config.TaskWorker.name = 'DevelopmentTaskWorker'
##PROD config.TaskWorker.name = 'prod-tw01'

## How much work can be done in parallel
config.TaskWorker.nslaves = 1
##PROD config.TaskWorker.nslaves = 6

## recurring actions: only the simplest one for minimal testing
## other make no sense (or do not work) other than in Production
## anyhow Recurring Actions can be tested as standalone scripts
config.TaskWorker.recurringActions = ['RemovetmpDir']
##PROD config.TaskWorker.recurringActions = ['RenewRemoteProxies', 'RemovetmpDir',
##PROD                                       'BanDestinationSites', 'TapeRecallStatus']

## Which CRAB SERVICE instance is this TW part of
## Possible values for instance are:
#   - preprod   : uses cmsweb-testbed RESt with preprod DB
#   - prod      : uses cmsweb,cern.ch production REST with production DB
#   - K8s       : uses cmsweb-k8s-testbed REST with preprod DB
#   - other     : allows to use any restHost with andy dbInstance, those needs to be
#                  specified in the two following parameters
config.TaskWorker.instance = 'other'
config.TaskWorker.restHost = 'cmsweb-test.cern.ch'
config.TaskWorker.dbInstance = 'dev'
##PROD config.TaskWorker.instance = 'prod'


############################################################################
## Following parameters are good defaults for any TW and should not be changed
## lightly nor without a godd reason
############################################################################

config.TaskWorker.polling = 30 #seconds

config.TaskWorker.scratchDir = '/data/srv/tmp' #make sure this directory exists
config.TaskWorker.logsDir = './logs'

## the parameters here below are used to contact cmsweb services for the REST-DB interactions
config.TaskWorker.cmscert = '/data/certs/servicecert.pem'
config.TaskWorker.cmskey = '/data/certs/servicekey.pem'

config.TaskWorker.backend = 'glidein'
#Retry policy
config.TaskWorker.max_retry = 4
config.TaskWorker.retry_interval = [30, 60, 120, 0]


#Default False. If true dagman will not retry the job on ASO failures
config.TaskWorker.retryOnASOFailures = True
#Dafault 0. If -1 no ASO timeout, if transfer is stuck in ASO we'll retry the postjob FOREVER (well, eventually a dagman timeout for the node will be hit).
#If 0 default timeout of 4 to 6 hours will be used. If specified the timeout set will be used (minutes).
config.TaskWorker.ASOTimeout = 86400

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

config.TaskWorker.scheddPickerFunction = HTCondorLocator.capacityMetricsChoicesHybrid

config.TaskWorker.DDMServer = 'dynamo.mit.edu'

config.section_("Sites")
config.Sites.DashboardURL = "https://cmssst.web.cern.ch/cmssst/analysis/usableSites.json"

config.section_("MyProxy")
config.MyProxy.serverhostcert = '/data/certs/hostcert.pem'
config.MyProxy.serverhostkey = '/data/certs/hostkey.pem'
config.MyProxy.cleanEnvironment = True
config.MyProxy.credpath = '/data/certs/creds' #make sure this directory exists

# Setting the minimum runtime requirements in minutes for automatic splitting
config.TaskWorker.minAutomaticRuntimeMins = 60

config.TaskWorker.highPrioEgroups = ['cms-crab-HighPrioUsers']
config.TaskWorker.bannedUsernames = ['mickeymouse','donaldduck']

# Setting the list of users for the highprio accounting group
# not usually needed since the list if automatically populated from e-group
# config.TaskWorker.highPrioUsers = ['sethzenz']

