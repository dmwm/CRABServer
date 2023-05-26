import logging
import os
from rucio.client.client import Client as RucioClient

from RESTInteractions import CRABRest
from ASO.Rucio.Transfer import Transfer
from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.Actions.BuildDBSDataset import BuildDBSDataset
from ASO.Rucio.Actions.RegisterReplicas import RegisterReplicas
#from ASO.Rucio.Actions.MonitorLocksStatus import MonitorLocksStatus

class RunTransfer:
    """
    RunTransfer describe 10000 foot view of actions that need to be
    done, as describe in algorithm method.  Action is small set of
    work that consume input, do some processing, and produce output.
    The input is mainly consist of Transfer object and rucio/crabrest
    client.  But output in this case is modified Transfer object and
    state change in RUCIO server instead.
    """
    def __init__(self):
        self.logger = logging.getLogger("RucioTransfer.RunTransfer")
        self.transfer = None
        self.rucioClient = None
        self.crabRESTClient = None

    def algorithm(self):
        """
        Instantiate the action class, and execute it inside algorithm
        method.  Also, initialized Transfer object, and rucioClient to
        use across process.
        """
        # init
        self.transfer = Transfer()
        self.transfer.readInfo()
        self.rucioClient = self._initRucioClient(self.transfer.username, self.transfer.restProxyFile)
        self.transfer.readInfoFromRucio(self.rucioClient)
        self.crabRESTClient = self._initCrabRESTClient(
            self.transfer.restHost,
            self.transfer.restDBInstance,
            self.transfer.restProxyFile,
        )
        # build dataset
        BuildDBSDataset(self.transfer, self.rucioClient).execute()
        # do 1
        RegisterReplicas(self.transfer, self.rucioClient, self.crabRESTClient).execute()
        # do 2
        #MonitorLocksStatus(self.transfer, self.rucioClient, self.crabRESTClient).execute()

    def _initRucioClient(self, username, proxypath=None):
        # maybe we can share with getNativeRucioClient
        rucioLogger = logging.getLogger('RucioTransfer.RucioClient')
        rucioLogger.setLevel(logging.INFO)
        if os.environ.get('X509_USER_PROXY', None):
            creds = None
        else:
            if proxypath and os.path.exists(proxypath):
                creds = {"client_proxy": proxypath}
            else:
                raise RucioTransferException(f'proxy file not found: {proxypath}')
        rc = RucioClient(
            account=username,
            auth_type="x509_proxy",
            creds=creds,
            logger=rucioLogger
        )
        self.logger.debug(f'RucioClient.whoami(): {rc.whoami()}')
        return rc

    def _initCrabRESTClient(self, host, dbInstance, proxypath='/tmp/x509_u999999'):
        """
        Initialize client for CRAB REST
        """
        proxyPathEnv = os.environ.get('X509_USER_PROXY', None)
        if proxyPathEnv:
            proxypath = proxyPathEnv
        if not os.path.isfile(proxypath):
            raise RucioTransferException(f'proxy file not found: {proxypath}')

        crabRESTClient = CRABRest(
            host,
            localcert=proxypath,
            localkey=proxypath,
            userAgent='CRABSchedd'
        )
        crabRESTClient.setDbInstance(dbInstance)
        return crabRESTClient
