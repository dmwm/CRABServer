"""
Configuration file for CRAB standalone Publisher
"""
from __future__ import division
from WMCore.Configuration import Configuration

config = Configuration()
config.section_('General')

config.General.asoworker = 'asoless'

## Which CRAB SERVICE instance is this Publisher using
## Possible values for instance are:
#   - preprod   : uses cmsweb-testbed RESt with preprod DB
#   - prod      : uses cmsweb,cern.ch production REST with production DB
#   - K8s       : uses cmsweb-k8s-testbed REST with preprod DB
#   - other     : allows to use any restHost with andy dbInstance, those needs to be
#                  specified in the two following parameters
config.General.instance = 'preprod'
#config.General.restHost = 'stefanovm.cern.ch'
#config.General.dbInstance = 'dev'

config.General.RestHostName = 'cmsweb-testbed.cern.ch'

#config.General.oracleFileTrans = '/crabserver/preprod/filetransfers'
#config.General.oracleUserTrans = '/crabserver/preprod/fileusertransfers'
config.General.pollInterval = 1800
config.General.publish_dbs_url = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'
config.General.block_closure_timeout = 9400
config.General.workflow_expiration_time = 3
config.General.max_files_per_block = 100
config.General.serviceCert = '/data/certs/servicecert.pem'
config.General.serviceKey = '/data/certs/servicekey.pem'
config.General.taskFilesDir = '/data/srv/Publisher_files/'
config.General.logsDir = './logs'
config.General.logMsgFormat = '%(asctime)s:%(levelname)s:%(module)s:%(name)s: %(message)s'
config.General.logLevel = 'INFO'
config.General.dryRun = False

config.section_('TaskPublisher')
config.TaskPublisher.logMsgFormat = '%(asctime)s:%(levelname)s: %(message)s'
