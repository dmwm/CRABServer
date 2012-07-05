from WMCore.Configuration import Configuration
from CRABServerAuth import connectUrl

conf = Configuration()
main = conf.section_('main')
srv = main.section_('server')
srv.thread_pool = 5
main.application = 'crabserver'
main.port = 8270
main.index = 'ui'

main.authz_defaults = { 'role': None, 'group': None, 'site': None }
main.section_('tools').section_('cms_auth').key_file = '/data/certs/hostkey.pem'

app = conf.section_('crabserver')
app.admin = 'cms.analysis.ops@cern.ch'
app.description = 'CRABServer RESTFull API'
app.title = 'CRABRESTFull'

views = conf.section_('views')

data = views.section_('ui')
data.object = 'CRABInterface.RESTBaseAPI.RESTBaseAPI'

data.monurl = 'http://localhost:5984'
data.monname = 'wmstats'
data.asomonurl = 'http://localhost:5984'
data.asomonname = 'user_monitoring_asynctransfer'
data.configcacheurl = 'http://localhost:5984'
data.configcachename = 'wmagent_configcache'
data.reqmgrurl = 'http://localhost:5984'
data.reqmgrname = 'reqmgrdb'
data.phedexurl = 'https://cmsweb.cern.ch/phedex/datasvc/xml/prod/'
data.dbsurl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
data.delegatedn = ['/dn/of/the/agent/for.myproxy.delegation']
data.acdcurl = 'http://localhost:5984'
data.acdcdb = 'wmagent_acdc'

data.connectUrl = connectUrl
#data.loggingLevel = 10
#data.loggingFile = '/tmp/CRAB.log'

conf.section_("CoreDatabase")
conf.CoreDatabase.connectUrl = connectUrl
