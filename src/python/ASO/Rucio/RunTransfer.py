"""
Initialize neccessary clients, put all actions together and execute it.
"""

import logging
import os
from rucio.client.client import Client as RucioClient

from RESTInteractions import CRABRest
from ASO.Rucio.Transfer import Transfer
from ASO.Rucio.exception import RucioTransferException
from ASO.Rucio.Actions.BuildDBSDataset import BuildDBSDataset
from ASO.Rucio.Actions.RegisterReplicas import RegisterReplicas
from ASO.Rucio.Actions.MonitorLockStatus import MonitorLockStatus
from ASO.Rucio.Actions.Cleanup import Cleanup


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
        # Read info from files written by PostJobs and bookkeeping from previous run.
        self.transfer.readInfo()
        self.rucioClient = self._initRucioClient(self.transfer.rucioUsername, self.transfer.restProxyFile)
        # Get info what's already in Rucio containers
        self.transfer.readInfoFromRucio(self.rucioClient)
        self.crabRESTClient = self._initCrabRESTClient(
            self.transfer.restHost,
            self.transfer.restDBInstance,
            self.transfer.restProxyFile,
        )
        # build dataset
        BuildDBSDataset(self.transfer, self.rucioClient, self.crabRESTClient).execute()
        # do 1
        RegisterReplicas(self.transfer, self.rucioClient, self.crabRESTClient).execute()
        # do 2
        MonitorLockStatus(self.transfer, self.rucioClient, self.crabRESTClient).execute()
        # cleanup
        # For now, only delete files in temp RSE
        Cleanup(self.transfer).execute()

    def _initRucioClient(self, username, proxypath='/tmp/x509_uXXXX'):
        """
        Initialize Rucio client. Note that X509_USER_PROXY has higher
        precedence than `proxypath` variable.

        :param username: username
        :type username: string
        :param proxypath: x509 proxyfile path
        :type proxypath: string

        :return: rucio client object
        :rtype: rucio.client.client.Client
        """
        # maybe we can share with getNativeRucioClient
        self.logger.info('Initializing Rucio Client')
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
        return rc

    def _initCrabRESTClient(self, host, dbInstance, proxypath='/tmp/x509_uXXXX'):
        """
        Initialize CRAB REST client.

        :param host: hostname and port of REST instance,
            e.g. `cmsweb-test12.cern.ch:8443`
        :type host: string
        :param dbInstance: database instance name e.g. `devthree`
        :type dbInstance: string
        :param proxypath: x509 proxyfile path
        :type proxypath: string

        :return: rest client object
        :rtype: RESTInteractions.CRABRest
        """
        self.logger.info('Initializing CRAB REST client')
        restLogger = logging.getLogger('RucioTransfer.RESTClient')
        restLogger.setLevel(logging.DEBUG)
        proxyPathEnv = os.environ.get('X509_USER_PROXY', None)
        if proxyPathEnv:
            proxypath = proxyPathEnv
        if not os.path.isfile(proxypath):
            raise RucioTransferException(f'proxy file not found: {proxypath}')

        crabRESTClient = CRABRest(
            host,
            localcert=proxypath,
            localkey=proxypath,
            userAgent='CRABSchedd',
            logger=restLogger,
        )
        crabRESTClient.setDbInstance(dbInstance)
        return crabRESTClient
