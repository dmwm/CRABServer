"""
Configuration file for CRAB standalone Publisher
"""
from __future__ import division
from WMCore.Configuration import Configuration

config = Configuration()
config.section_('General')

config.General.asoworker = 'asoless'
config.General.isOracle = True
config.General.oracleDB = 'cmsweb-testbed.cern.ch'
config.General.oracleFileTrans = '/crabserver/preprod/filetransfers'
config.General.oracleUserTrans = '/crabserver/preprod/fileusertransfers'
config.General.logLevel = 'INFO'
config.General.pollInterval = 1800
config.General.publish_dbs_url = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'
config.General.block_closure_timeout = 9400
#config.General.cache_area = 'https://cmsweb-testbed.cern.ch/crabserver/preprod/filemetadata'
#config.General.cache_area = 'https://cmsweb.cern.ch/crabserver/prod/filemetadata'
config.General.workflow_expiration_time = 3
config.General.serviceCert = '/data/certs/hostcert.pem'
config.General.serviceKey = '/data/certs/hostkey.pem'
config.General.logMsgFormat = '%(asctime)s:%(levelname)s:%(module)s:%(name)s: %(message)s'
config.General.max_files_per_block = 100
config.General.opsCert = '/data/certs/servicecert.pem'
config.General.opsKey = '/data/certs/servicekey.pem'
config.General.cache_path = '/crabserver/preprod/filemetadata'
config.General.task_path = '/crabserver/preprod/task'

config.section_('Publisher')
config.Publisher.logMsgFormat = '%(asctime)s:%(levelname)s: %(message)s'
