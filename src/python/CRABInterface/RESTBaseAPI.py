import logging
from commands import getstatusoutput

# WMCore dependecies here
from WMCore.REST.Server import DatabaseRESTApi
from WMCore.REST.Format import JSONFormat
from WMCore.REST.Error import ExecutionError

# CRABServer dependecies here
import Utils
from CRABInterface.RESTUserWorkflow import RESTUserWorkflow
from CRABInterface.RESTCampaign import RESTCampaign
from CRABInterface.RESTJobMetadata import RESTJobMetadata
from CRABInterface.RESTServerInfo import RESTServerInfo
from CRABInterface.RESTJobMetadata import RESTJobMetadata
from CRABInterface.DataJobMetadata import DataJobMetadata
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.DataJobMetadata import DataJobMetadata
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

        #Global initialization of Data objects. Parameters coming from the config should go here
        DataUserWorkflow.globalinit(config.workflowManager)
        DataWorkflow.globalinit(dbapi=self, phedexargs={'endpoint': config.phedexurl}, dbsurl=config.dbsurl,\
                                        credpath=config.credpath, transformation=config.transformation)
        DataJobMetadata.globalinit(dbapi=self)
        Utils.globalinit(config.serverhostkey, config.serverhostcert, serverdn, config.credpath)

        ## TODO need a check to verify the format depending on the resource
        ##      the RESTJobMetadata has the specifc requirement of getting xml reports
        self._add( {'workflow': RESTUserWorkflow(app, self, config, mount),
                    'campaign': RESTCampaign(app, self, config, mount),
                    'info': RESTServerInfo(app, self, config, mount, serverdn),
                    'jobmetadata': RESTJobMetadata(app, self, config, mount),
                   } )

        self._initLogger( getattr(config, 'loggingFile', None), getattr(config, 'loggingLevel', None) )

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
