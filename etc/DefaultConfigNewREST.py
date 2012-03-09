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
app.admin = 'marco.mascheroni@cern.ch'
app.description = 'CRABServer RESTFull API'
app.title = 'CRABRESTFull'

views = conf.section_('views')

data = views.section_('data')
data.object = 'CRABInterface.RESTBaseAPI.RESTBaseAPI'

data.monurl = 'http://crabas.lnl.infn.it:5284'
#data.monname = 'test_summary_central'
data.monname = 'summary_backup3'
data.configcacheurl = 'http://crabas.lnl.infn.it:5284'
data.configcachename = 'wmagent_configcache'
data.reqmgrurl = 'http://crabas.lnl.infn.it:5284'
data.reqmgrname = 'reqmgrdb'

data.connectUrl = connectUrl
#data.loggingLevel = 10

conf.section_("CoreDatabase")
conf.CoreDatabase.connectUrl = connectUrl
