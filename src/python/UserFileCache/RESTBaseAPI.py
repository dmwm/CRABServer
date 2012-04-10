# WMCore dependecies here
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import JSONFormat

# CRABServer dependecies here
from UserFileCache.RESTFile import RESTFile, RESTFileInfo

# external dependecies here
import os


class RESTBaseAPI(RESTApi):
    """The UserFileCache REST API module"""

    def __init__(self, app, config, mount):
        RESTApi.__init__(self, app, config, mount)

        self.formats = [ ('application/json', JSONFormat()) ]

        if not os.path.exists(config.cachedir) or not os.path.isdir(config.cachedir):
            raise Exception("Failing to start because of wrong cache directory '%s'" % config.cachedir)

        self._add( {'file': RESTFile(app, self, config, mount),
                    'fileinfo': RESTFileInfo(app, self, config, mount)} )
