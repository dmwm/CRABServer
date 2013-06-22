from WMCore.Configuration import Configuration

serverdn = 'FILL ME' #grid-cert-info -file /data/certs/hostcert.pem -subject

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
data.serverhostcert = '/data/current/auth/crabserver/hostcert.pem'
data.serverhostkey = '/data/current/auth/crabserver/hostkey.pem'
data.phedexurl = 'https://cmsweb.cern.ch/phedex/datasvc/xml/prod/'
data.dbsurl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
data.defaultBlacklist = ['T0_CH_CERN']
data.delegatedn = []
data.serverdn = serverdn
data.db = 'CRABServerAuth.dbconfig'
data.uisource = '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh'
data.credpath = '%s/state/crabserver/proxy/' % __file__.rsplit('/', 4)[0]
data.transformation = 'http://common-analysis-framework.cern.ch/CMSRunAnalysis.sh'
data.loggingLevel = 10
data.loggingFile = '/tmp/CRAB.log'
