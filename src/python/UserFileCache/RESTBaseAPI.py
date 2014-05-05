# WMCore dependecies here
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import JSONFormat

# CRABServer dependecies here
from UserFileCache.RESTFile import RESTFile, RESTLogFile, RESTFileInfo
import UserFileCache.RESTExtensions

# external dependecies here
import os


class RESTBaseAPI(RESTApi):
    """The UserFileCache REST API module"""

    def __init__(self, app, config, mount):
        RESTApi.__init__(self, app, config, mount)

        self.formats = [ ('application/json', JSONFormat()) ]

        if not os.path.exists(config.cachedir) or not os.path.isdir(config.cachedir):
            raise Exception("Failing to start because of wrong cache directory '%s'" % config.cachedir)

        if hasattr(config, 'powerusers'):
            UserFileCache.RESTExtensions.POWER_USERS_LIST = config.powerusers
        if hasattr(config, 'quota_user_limit'):
            UserFileCache.RESTExtensions.quota_user_limit = config.quota_user_limit * 1024 * 1024
        self._add( {'logfile': RESTLogFile(app, self, config, mount),
                    'file': RESTFile(app, self, config, mount),
                    'fileinfo': RESTFileInfo(app, self, config, mount)} )
