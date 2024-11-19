# the original file is in https://github.com/dmwm/CMSKubernetes/blob/8bde12f667d03b0033c05d33699e427385dc2b4e/helm/crabserver/config/test/config.py

from WMCore.Configuration import Configuration
import re
import socket
import time
import os

myhost = socket.getfqdn().lower()

conf = Configuration()
main = conf.section_('main')
srv = main.section_('server')
srv.thread_pool = int(os.getenv('CRABSERVER_THREAD_POOL', 25))
main.application = 'crabserver'
main.port = 8270
main.index = 'data'

main.authz_defaults = { 'role': None, 'group': None, 'site': None }
main.section_('tools').section_('cms_auth').key_file = "%s/auth/crabserver/header-auth-key" % __file__.rsplit('/', 3)[0]

app = conf.section_('crabserver')
app.admin = 'cms-service-webtools@cern.ch'
app.description = 'CRABServer RESTFull API'
app.title = 'CRABRESTFull'

views = conf.section_('views')
ui = views.section_('ui')
ui.object = 'CRABInterface.Pages.FrontPage.FrontPage'

data = views.section_('data')
data.object = 'CRABInterface.RESTBaseAPI.RESTBaseAPI'
data.phedexurl = 'https://cmsweb.cern.ch/phedex/datasvc/xml/prod/'
data.dbsurl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
data.defaultBlacklist = ['T0_CH_CERN']
data.serverhostcert = "%s/auth/crabserver/dmwm-service-cert.pem" % __file__.rsplit('/', 3)[0]
data.serverhostkey = "%s/auth/crabserver/dmwm-service-key.pem" % __file__.rsplit('/', 3)[0]
data.credpath = '%s/state/crabserver/proxy/' % __file__.rsplit('/', 4)[0]
data.backend = 'oracle'
data.db = 'CRABServerAuth.dbconfig'
data.s3 = 'CRABServerAuth.s3'
data.workflowManager = 'HTCondorDataWorkflow'

data.extconfigurl = 'http://gitlab.cern.ch/crab3/CRAB3ServerConfig/raw/master/cmsweb-rest-config.json'

data.loggingLevel = 10
data.loggingFile = '%s/logs/crabserver/CRAB-%s.log' % (__file__.rsplit('/', 4)[0], myhost)
data.keptLogDays = 7
data.mode = "cmsweb-test"

data.enableQueryLoadAllRows = os.getenv('CRABSERVER_ENABLE_QUERY_LOAD_ALL_ROWS', 'True').lower() in ('true', '1', 't')

data.delegateDN = "/DC=ch/DC=cern/OU=computers/CN=crab-(preprod|prod)-tw(01|02).cern.ch|/DC=ch/DC=cern/OU=computers/CN=crab-dev-tw(01|02|03|04).cern.ch|/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cmscrab/CN=(817881|373708)/CN=Robot: cms crab|/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=crabint1/CN=373708/CN=Robot: CMS CRAB Integration 1"
