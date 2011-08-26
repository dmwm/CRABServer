#!/usr/bin/env python
"""
Example configuration for CRABServer
"""

import os

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration

serverHostName = "HOST_NAME"
CRABInterfacePort = 8888
workDirectory = "/CRABInterface/worgkin/dir"
couchURL = "http://user:passwd@host:5984"
workloadCouchDB = "reqmgrdb"
jsmCacheDBName = "wmagent_jobdump"
configCacheCouchDB = "wmagent_configcache"

databaseUrl = "mysql://root@localhost/ReqMgrDB"
databaseSocket = "/path/mysql.sock"

ufcHostName = 'cms-xen39.fnal.gov'
ufcPort = 7725
ufcBasepath = 'userfilecache/userfilecache'

config = Configuration()

config.section_("General")
config.General.workDir = workDirectory
config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl
config.CoreDatabase.socket = databaseSocket

config.section_("Agent")
config.Agent.hostName = serverHostName
#config.Agent.contact = userEmail
#config.Agent.teamName = teamName
#config.Agent.agentName = agentName
#config.Agent.useMsgService = False
#config.Agent.useTrigger = False
#config.Agent.useHeartbeat = False
config.webapp_("CRABInterface")

config.CRABInterface.componentDir = config.General.workDir + "/CRABInterface"
config.CRABInterface.Webtools.host = serverHostName
config.CRABInterface.Webtools.port = CRABInterfacePort
config.CRABInterface.Webtools.environment = "devel"
config.CRABInterface.templates = os.path.join(os.environ["WMCORE_ROOT"], 'templates/WMCore/WebTools')

config.CRABInterface.configCacheCouchURL = "YourConfigCacheUrl"
config.CRABInterface.configCacheCouchDB = configCacheCouchDB
config.CRABInterface.ACDCCouchURL = 'http://user:passwd@host:5984'
config.CRABInterface.ACDCCouchDB = 'wmagent_acdc'
config.CRABInterface.DBSUrl = 'https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet'

## TODO once the deploy model has been defined.. we will clarify how
##      to deal with these params
config.CRABInterface.serverDN = "/Your/Agent/DN.here/"
config.CRABInterface.clientMapping = '/PATH/configuration/ClientMapping.py'
config.CRABInterface.SandBoxCache_endpoint = ufcHostName
config.CRABInterface.SandBoxCache_port  = ufcPort
config.CRABInterface.SandBoxCache_basepath = ufcBasepath
##

config.CRABInterface.admin = "admin@mail.address"
config.CRABInterface.title = "CRAB REST Interface"
config.CRABInterface.description = "rest interface for crab"
config.CRABInterface.instance = "Analysis WMAGENT"

config.CRABInterface.section_("security")
config.CRABInterface.security.dangerously_insecure = True

config.CRABInterface.section_('views')
config.CRABInterface.views.section_('active')
config.CRABInterface.views.active.section_('crab')
config.CRABInterface.views.active.crab.section_('model')
config.CRABInterface.views.active.crab.section_('formatter')
config.CRABInterface.views.active.crab.object = 'WMCore.WebTools.RESTApi'
config.CRABInterface.views.active.crab.templates = os.path.join(os.environ["WMCORE_ROOT"], 'templates/WMCore/WebTools')
config.CRABInterface.views.active.crab.model.couchUrl = couchURL
config.CRABInterface.views.active.crab.model.workloadCouchDB = workloadCouchDB
config.CRABInterface.views.active.crab.model.object = 'CRABServer.CRABRESTModel'
config.CRABInterface.views.active.crab.formatter.object = 'WMCore.WebTools.RESTFormatter'

## TODO once the deploy model has been defined.. we will clarify how
##      to deal with these params
config.CRABInterface.views.active.crab.jsmCacheCouchURL = couchURL
config.CRABInterface.views.active.crab.jsmCacheCouchDB = jsmCacheDBName

## Configuration to setup UserFileCache for sandbox uploads

config.webapp_("UserFileCache")
userFileCacheUrl = "http://%s:%s" % (ufcHostName, ufcPort)

config.UserFileCache.componentDir = config.General.workDir + "/UserFileCache"

config.UserFileCache.userCacheDir = '/tmp/ufCache'
config.UserFileCache.Webtools.host = ufcHostName
config.UserFileCache.Webtools.port = ufcPort
config.UserFileCache.Webtools.environment = 'devel'
config.UserFileCache.templates = os.path.join(os.environ["WMCORE_ROOT"], 'templates/WMCore/WebTools')

config.UserFileCache.admin = 'admin@mail.address'
config.UserFileCache.title = "User File Cache"
config.UserFileCache.description = "Upload and download of user files"

config.UserFileCache.section_("security")
config.UserFileCache.security.dangerously_insecure = True

views = config.UserFileCache.section_('views')

# These are all the active pages that Root.py should instantiate
active = views.section_('active')

# Main part of userfilecache uses REST model

userfilecache = active.section_('userfilecache')
userfilecache.serviceURL = "%s/userfilecache/userfilecache" % userFileCacheUrl
userfilecache.object = 'WMCore.WebTools.RESTApi'

userfilecache.section_('model')
userfilecache.model.object = 'CRABServer.UserFileCache.UserFileCacheRESTModel'
userfilecache.section_('formatter')
userfilecache.formatter.object = 'WMCore.WebTools.RESTFormatter'
userfilecache.default_expires = 0 # no caching

# Download uses Page model

download = active.section_('download')
download.object = 'CRABServer.UserFileCache.UserFileCachePage'
download.templates = os.path.join(os.environ["WMCORE_ROOT"], 'templates/WMCore/WebTools')
