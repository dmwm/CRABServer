#! /usr/bin/env python
"""
Default config for the UserFileCache Agent
"""

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration
import os

serverHostName = 'localhost'
userFileCachePort = '7987'

config = Configuration()

config.webapp_("UserFileCache")
userFileCacheUrl = "http://%s:%s" % (serverHostName, userFileCachePort)

config.UserFileCache.userCacheDir = '/tmp/UserFileCache'
config.UserFileCache.Webtools.host = serverHostName
config.UserFileCache.Webtools.port = userFileCachePort
config.UserFileCache.Webtools.environment = "testing"
config.UserFileCache.templates = os.path.join(getWMBASE(), "src/templates/WMCore/WebTools")

config.UserFileCache.admin = "No one"
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
userfilecache.model.object = 'UserFileCache.UserFileCacheRESTModel'
userfilecache.section_('formatter')
userfilecache.formatter.object = 'WMCore.WebTools.RESTFormatter'
userfilecache.default_expires = 0 # no caching

# Download uses Page model

download = active.section_('download')
download.object = 'UserFileCache.UserFileCachePage'
download.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools')

