import logging

# WMCore dependecies here
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import JSONFormat

# CRABServer dependecies here
from CRABInterface.RESTUserWorkflow import RESTUserWorkflow
from CRABInterface.RESTCampaign import RESTCampaign
from CRABInterface.DataWorkflow import DataWorkflow
from CRABInterface.DataUserWorkflow import DataUserWorkflow
from CRABInterface.DataCampaign import DataCampaign

#In case the log level is not specified in the configuration we use the NullHandler and we do not print messages
#The NullHandler is included as of python 3.1
class NullHandler(logging.Handler):
    def emit(self, record):
        pass

class RESTBaseAPI(RESTApi):
    """The CRABServer REST API modules"""

    def __init__(self, app, config, mount):
        RESTApi.__init__(self, app, config, mount)

        self.formats = [ ('application/json', JSONFormat()) ]

        #Global initialization of Data objects. Parameters coming from the config should go here
        DataWorkflow.globalinit(config.monurl, config.monname, config.asomonurl, config.asomonname,
                                config.reqmgrurl, config.reqmgrname, config.configcacheurl,
                                config.configcachename, config.connectUrl, {'endpoint': config.phedexurl},
                                config.dbsurl)
        DataUserWorkflow.globalinit(config.monurl, config.monname, config.asomonurl, config.asomonname)
        DataCampaign.globalinit(config.monurl, config.monname)

        self._add( {'workflow': RESTUserWorkflow(app, self, config, mount),
                    'campaign': RESTCampaign(app, self, config, mount),
                   } )

        self._initLogger( getattr(config, 'loggingFile', None), getattr(config, 'loggingLevel', None) ) #None can be cahnged to logging.INFO if we realize we can log to a file in the cmsWeb env

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
