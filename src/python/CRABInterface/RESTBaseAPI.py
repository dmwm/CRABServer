import logging

# WMCore dependecies here
from WMCore.REST.Server import RESTApi

# CRABServer dependecies here
from CRABInterface.RESTWorkflow import RESTWorkflow
from CRABInterface.RESTCampaign import RESTCampaign
from CRABInterface.DataWorkflow import DataWorkflow
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

        #Global initialization of Data objects. Parameters coming from the config should go here
        DataWorkflow.globalinit(config.monurl, config.monname, config.reqmgrurl, config.reqmgrname, config.configcacheurl, config.configcachename, config.connectUrl)
        DataCampaign.globalinit(config.monurl, config.monname)

        self._add( {'workflow': RESTWorkflow(app, self, config, mount),
                    'campaign': RESTCampaign(app, self, config, mount),
                   } )

        self._initLogger( getattr(config, 'loggingLevel', None) ) #None can be cahnged to logging.INFO if we realize we can log to a file in the cmsWeb env

    def _initLogger(self, loggingLevel = None):
        """
        Setup the logger for all the CRAB API's. If loggingLevel is not specified (==None) we use the NullHandler which just 'pass' and does not log

        RESTEntities and other parts of the code can retrieve it by calling: logging.getLogger('CRABLogger.ChildName')
        ChildName is the specific name of the logger (child of CRABLogger). Using childs in that way we can configure
        the logging in a flexible way (a module logs at DEBUG level to a file, another module logs at INFO level to stdout, etc)
        """

        logger = logging.getLogger('CRABLogger')
        if loggingLevel:
            hdlr = logging.FileHandler('/tmp/CRAB.log') #TODO temporary path until we figure out where we can put the logfile (if we can have one)
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s')
            hdlr.setFormatter(formatter)
            logger.addHandler(hdlr)
            logger.setLevel(loggingLevel)
        else:
            logger.addHandler( NullHandler() )
