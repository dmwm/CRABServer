from WMCore.Configuration import Configuration
config = Configuration()
config.section_('General')

config.General.asoworker = 'asodciangot1'
config.General.asoworker = 'asodciangot1'
config.General.isOracle = True 
config.General.oracleDB = 'vocms035.cern.ch'
#config.General.oracleDB = 'vocms035.cern.ch'
config.General.oracleFileTrans = '/crabserver/dev/filetransfers'
config.General.logLevel = 'INFO'
config.General.publication_pool_size = 80
config.General.publication_max_retry = 3
config.General.pollInterval = 600
config.General.log_level = 10
config.General.componentDir = '/data/srv/asyncstageout/v1.0.9pre1/install/asyncstageout/DBSPublisher'
config.General.namespace = 'AsyncStageOut.DBSPublisher'
config.General.config_database = 'asynctransfer_config'
config.General.config_couch_instance = 'http://user:password@127.0.0.1:5984'
config.General.publish_dbs_url = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'
config.General.block_closure_timeout = 18800
config.General.serverDN = '/DC=ch/DC=cern/OU=computers/CN=vocms0109.cern.ch'
config.General.cache_area = 'https://vocms035.cern.ch/crabserver/dev/filemetadata'
config.General.algoName = 'FIFOPriority'
config.General.workflow_expiration_time = 3
config.General.serviceCert = '/data/certs/hostcert.pem'
config.General.schedAlgoDir = 'AsyncStageOut.SchedPlugins'
config.General.serviceKey = '/data/certs/hostkey.pem'
config.General.logMsgFormat = '%(asctime)s:%(levelname)s:%(module)s:%(name)s: %(message)s'
config.General.credentialDir = '/data/srv/asyncstageout/state/asyncstageout/creds'
config.General.files_database = 'asynctransfer'
config.General.UISetupScript = '/data/srv/tmp.sh'
config.General.max_files_per_block = 100
config.General.opsProxy = '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy'
config.General.couch_instance = 'https://vocms035.cern.ch/couchdb'
