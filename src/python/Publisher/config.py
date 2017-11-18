from WMCore.Configuration import Configuration
config = Configuration()
config.section_('General')

config.General.asoworker = 'asodciangot1'
config.General.isOracle = True 
config.General.oracleDB = 'vocms035.cern.ch'
config.General.oracleFileTrans = '/crabserver/dev/filetransfers'
config.General.oracleUserTrans = '/crabserver/dev/fileusertransfers'
config.General.logLevel = 'INFO'
config.General.pollInterval = 1800
config.General.componentDir = '/data/srv/asyncstageout/v1.0.9pre1/install/asyncstageout/DBSPublisher'
config.General.publish_dbs_url = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'
config.General.block_closure_timeout = 9400
config.General.serverDN = '/DC=ch/DC=cern/OU=computers/CN=vocms0109.cern.ch'
config.General.cache_area = 'https://vocms035.cern.ch/crabserver/dev/filemetadata'
config.General.workflow_expiration_time = 3
config.General.serviceCert = '/data/certs/hostcert.pem'
config.General.serviceKey = '/data/certs/hostkey.pem'
config.General.logMsgFormat = '%(asctime)s:%(levelname)s:%(module)s:%(name)s: %(message)s'
config.General.credentialDir = '/data/srv/asyncstageout/state/asyncstageout/creds'
config.General.max_files_per_block = 100
config.General.opsProxy = '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy'
