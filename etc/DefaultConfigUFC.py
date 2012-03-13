from WMCore.Configuration import Configuration

conf = Configuration()
main = conf.section_('main')
srv = main.section_('server')
srv.thread_pool = 5
main.application = 'crabcache'
main.port = 8271
main.index = 'ui'

main.authz_defaults = { 'role': None, 'group': None, 'site': None }
main.section_('tools').section_('cms_auth').key_file = '/data/auth/wmcore/header-auth-key'

app = conf.section_('crabcache')
app.admin = 'opsmail@cern.ch'
app.description = 'CRABCache RESTFull API'
app.title = 'CRABCacheAPIs'

views = conf.section_('views')

data = views.section_('data')
data.object = 'UserFileCache.RESTBaseAPI.RESTBaseAPI'
data.cachedir = '/tmp'
