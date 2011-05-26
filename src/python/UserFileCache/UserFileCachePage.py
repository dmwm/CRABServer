#! /usr/bin/env python

"""
Page Model portion for User File Cache.
"""

import cherrypy
import os

from cherrypy.lib.static import serve_file
from cherrypy import expose

from WMCore.WebTools.Page import Page

class UserFileCachePage(Page):
    """
    The download/ URL
    """

    @expose
    def index(self, hashkey):
        """
        The download/ code
        """
        try:
            int(hashkey, 16)
        except ValueError:
            raise cherrypy.NotFound

        fileName = os.path.join(self.config.userCacheDir, hashkey[0:2], hashkey)

        if os.path.isfile(fileName):
            os.utime(fileName, None) # "touch" the file
            return serve_file(fileName, "application/x-download", "attachment")
        else:
            raise cherrypy.NotFound
