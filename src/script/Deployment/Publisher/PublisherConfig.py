# Publisher_schedd example configuration file
# original file from https://gitlab.cern.ch/ai/it-puppet-hostgroup-vocmsglidein/-/blob/bc61a79da85d72e4db90b3fbca07a369f456e9ee/code/templates/crabtaskworker/publisher/Publisher_scheddConfig.py.erb
"""
Configuration file for CRAB standalone Publisher
"""
from __future__ import division
from WMCore.Configuration import Configuration

config = Configuration()
config.section_('General')

# never change following line
config.General.asoworker = 'schedd'

## Which CRAB SERVICE instance is this Publisher using
## Possible values for instance are:
#   - preprod   : uses cmsweb-testbed RESt with preprod DB
#   - prod      : uses cmsweb,cern.ch production REST with production DB
#   - other     : allows to use any restHost with andy dbInstance, those needs to be
#                  specified in the two following parameters
config.General.instance = 'test2'

#config.General.restHost = ''
#config.General.dbInstance = ''

config.General.pollInterval = 1800
config.General.block_closure_timeout = 9400
config.General.max_files_per_block = 100
config.General.max_slaves = 5
config.General.serviceCert = '/data/certs/servicecert.pem'
config.General.serviceKey = '/data/certs/servicekey.pem'
config.General.taskFilesDir = '/data/srv/Publisher_files/'
config.General.logsDir = '/data/srv/Publisher/logs'
config.General.logMsgFormat = '%(asctime)s:%(levelname)s:%(module)s:%(name)s: %(message)s'
config.General.logLevel = 'INFO'
config.General.skipUsers = ['mickeymouse', 'donaldduck']

config.section_('REST')
config.REST.instance = 'test2'

#config.REST.restHost = ''
#config.REST.dbInstance = ''
config.REST.cert = '/data/certs/servicecert.pem'
config.REST.key = '/data/certs/servicekey.pem'

config.section_('TaskPublisher')
config.TaskPublisher.DBShost = 'cmsweb-prod.cern.ch'
config.TaskPublisher.logMsgFormat = '%(asctime)s:%(levelname)s: %(message)s'
config.TaskPublisher.dryRun = False
config.TaskPublisher.cert = '/data/certs/servicecert.pem'
config.TaskPublisher.key = '/data/certs/servicekey.pem'
