from WMCore.Configuration import Configuration
from CRABServerAuth import connectUrl

conf = Configuration()
main = conf.section_('main')
srv = main.section_('server')
srv.thread_pool = 5
main.application = 'crabserver'
main.port = 8270
main.index = 'data'

main.authz_defaults = { 'role': None, 'group': None, 'site': None }
main.section_('tools').section_('cms_auth').key_file = "%s/auth/wmcore-auth/header-auth-key" % __file__.rsplit('/', 3)[0]

app = conf.section_('crabserver')
app.admin = 'cms-service-webtools@cern.ch'
app.description = 'CRABServer RESTFull API'
app.title = 'CRABRESTFull'

views = conf.section_('views')

data = views.section_('data')
data.object = 'CRABInterface.RESTBaseAPI.RESTBaseAPI'

#depending on deployment type using a different url for monitoring
import re, socket
HOST = socket.gethostname().lower()
if re.match(r"^vocms(136|16[13])", HOST):
    data.monurl = "https://cmsweb.cern.ch/couchdb/"
    data.asomonurl = 'https://cmsweb.cern.ch/couchdb/'
    data.configcacheurl = 'https://cmsweb.cern.ch/couchdb'
    data.reqmgrurl = 'https://cmsweb.cern.ch/couchdb'
elif re.match(r"^vocms13[23]", HOST):
    data.monurl = "https://cmsweb-testbed.cern.ch/couchdb/"
    data.asomonurl = 'https://cmsweb-testbed.cern.ch/couchdb/'
    data.configcacheurl = 'https://cmsweb-testbed.cern.ch/couchdb'
    data.reqmgrurl = 'https://cmsweb-testbed.cern.ch/couchdb'
elif re.match(r"^vocms127", HOST):
    data.monurl = "https://cmsweb-dev.cern.ch/couchdb/"
    data.asomonurl = 'https://cmsweb-dev.cern.ch/couchdb/'
    data.configcacheurl = 'https://cmsweb-dev.cern.ch/couchdb'
    data.reqmgrurl = 'https://cmsweb-dev.cern.ch/couchdb'
else:
    data.monurl = "http://localhost:5984"
    data.asomonurl = 'http://localhost:5984'
    data.configcacheurl = 'https://%s/couchdb' % HOST
    data.reqmgrurl = 'https://%s/couchdb' % HOST
data.monname = 'analysis_wmstats'
data.asomonurl = 'http://dashboard73.cern.ch:5184'
data.asomonname = 'user_monitoring_asynctransfer'
data.configcachename = 'analysis_reqmgr_config_cache'
data.reqmgrname = 'analysis_reqmgr_workload_cache'
# these two below are currently needed for only to startup
# if the value is not set, resubmission won't be possible
data.acdcurl = 'http://host:5984'
data.acdcdb = 'wmagent_acdc'

data.serverhostcert = '/data/current/auth/crabserver/hostcert.pem'
data.serverhostkey = '/data/current/auth/crabserver/hostkey.pem'
data.phedexurl = 'https://cmsweb.cern.ch/phedex/datasvc/xml/prod/'
data.dbsurl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
data.defaultBlacklist = ['T0_CH_CERN']
data.delegatedn = ['/DC=ch/DC=cern/OU=computers/CN=mmascher-poc.cern.ch']

data.serverdn = '/DC=ch/DC=cern/OU=computers/CN=mmascher-poc.cern.ch'

data.connectUrl = connectUrl

data.db = 'CRABServerAuth.dbconfig'

conf.section_("CoreDatabase")
conf.CoreDatabase.connectUrl = connectUrl

data.uisource = '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh'
data.credpath = '%s/state/crabserver/proxy/' % __file__.rsplit('/', 4)[0]

data.loggingLevel = 10
data.loggingFile = '/tmp/CRAB.log'
