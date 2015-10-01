from __future__ import absolute_import
import logging
import cherrypy
from commands import getstatusoutput
from time import mktime, gmtime

# WMCore dependecies here
from WMCore.REST.Server import DatabaseRESTApi, rows
from WMCore.REST.Format import JSONFormat
from WMCore.REST.Error import ExecutionError

# CRABServer dependecies here
from . import Utils
from CRABInterface.RESTUserWorkflow import RESTUserWorkflow
from CRABInterface.RESTTask import RESTTask
from CRABInterface.RESTCampaign import RESTCampaign
from CRABInterface.RESTServerInfo import RESTServerInfo
from CRABInterface.RESTFileMetadata import RESTFileMetadata
from CRABInterface.RESTWorkerWorkflow import RESTWorkerWorkflow
from CRABInterface.DataFileMetadata import DataFileMetadata
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.DataUserWorkflow import DataUserWorkflow
from CRABInterface.DataCampaign import DataCampaign

#In case the log level is not specified in the configuration we use the NullHandler and we do not print messages
#The NullHandler is included as of python 3.1
class NullHandler(logging.Handler):
    def emit(self, record):
        pass

class RESTBaseAPI(DatabaseRESTApi):
    """The CRABServer REST API modules"""

    def __init__(self, app, config, mount):
        DatabaseRESTApi.__init__(self, app, config, mount)

        self.formats = [ ('application/json', JSONFormat()) ]

        status, serverdn = getstatusoutput('openssl x509 -noout -subject -in %s | cut -f2- -d\ ' % config.serverhostcert)
        if status is not 0:
            raise ExecutionError("Internal issue when retrieving crabserver service DN.")

        extconfig = Utils.ConfigCache(centralconfig=Utils.getCentralConfig(extconfigurl=config.extconfigurl, mode=config.mode),
                                      cachetime=mktime(gmtime()))

        #Global initialization of Data objects. Parameters coming from the config should go here
        DataUserWorkflow.globalinit(config)
        DataWorkflow.globalinit(dbapi=self, phedexargs={'endpoint': config.phedexurl},\
                                credpath=config.credpath, centralcfg=extconfig, config=config)
        DataFileMetadata.globalinit(dbapi=self, config=config)
        RESTTask.globalinit(centralcfg=extconfig)
        Utils.globalinit(config.serverhostkey, config.serverhostcert, serverdn, config.credpath)

        ## TODO need a check to verify the format depending on the resource
        ##      the RESTFileMetadata has the specifc requirement of getting xml reports
        self._add( {'workflow': RESTUserWorkflow(app, self, config, mount, extconfig),
                    'campaign': RESTCampaign(app, self, config, mount),
                    'info': RESTServerInfo(app, self, config, mount, serverdn, extconfig),
                    'filemetadata': RESTFileMetadata(app, self, config, mount),
                    'workflowdb': RESTWorkerWorkflow(app, self, config, mount),
                    'task': RESTTask(app, self, config, mount),
                   } )

        self._initLogger( getattr(config, 'loggingFile', None), getattr(config, 'loggingLevel', None) )

    def modifynocheck(self, sql, *binds, **kwbinds):
        """This is the same as `WMCore.REST.Server`:modify method but
           not implementing any kind of checks on the number of modified
           rows.

        :arg str sql: SQL modify statement.
        :arg list binds: Bind variables by position: list of dictionaries.
        :arg dict kwbinds: Bind variables by keyword: dictionary of lists.
        :result: See :meth:`rowstatus` and description in `WMCore.REST.Server`."""
        if binds:
            c, _ = self.executemany(sql, *binds, **kwbinds)
        else:
            kwbinds = self.bindmap(**kwbinds)
            c, _ = self.executemany(sql, kwbinds, *binds)
        trace = cherrypy.request.db["handle"]["trace"]
        trace and cherrypy.log("%s commit" % trace)
        cherrypy.request.db["handle"]["connection"].commit()
        return rows([{ "modified": c.rowcount }])

    def _initLogger(self, logfile, loglevel):
        """
        Setup the logger for all the CRAB API's. If loglevel is not specified (==None) we use the NullHandler which just 'pass' and does not log

        RESTEntities and other parts of the code can retrieve it by calling: logging.getLogger('CRABLogger.ChildName')
        ChildName is the specific name of the logger (child of CRABLogger). Using childs in that way we can configure
        the logging in a flexible way (a module logs at DEBUG level to a file, another module logs at INFO level to stdout, etc)
        """

        logger = logging.getLogger('CRABLogger')
        if loglevel:
            hdlr = logging.FileHandler(logfile)
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s')
            hdlr.setFormatter(formatter)
            logger.addHandler(hdlr)
            logger.setLevel(loglevel)
        else:
            logger.addHandler( NullHandler() )
