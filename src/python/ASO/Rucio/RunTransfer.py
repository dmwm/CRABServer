import logging
import os

from rucio.client.client import Client as RucioClient

from ASO.Rucio.Actions.BuildTaskDataset import BuildTaskDataset
#from ASO.Rucio.Actions.AddFilesToTransfer import AddFilesToTransfer
from ASO.Rucio.Transfer import Transfer
from ASO.Rucio.exception import RucioTransferException

class RunTransfer:
    def __init__(self):
        self.logger = logging.getLogger("RucioTransfer.RunTransfer")

        self.transfer = Transfer()

        self.rucio = self._initRucioClient(self.transfer.username, self.transfer.proxypath)

    def algorithm(self):
        # do nothing
        BuildTaskDataset(self.transfer, self.rucio).execute()
        # do 1
        #AddFilesToTransfer(self.rucios, elf.transfer)
        # do 2

    def _initRucioClient(self, username, proxypath=None):
        # maybe we can share with getNativeRucioClient
        rucioLogger = logging.getLogger('RucioTransfer.RucioClient')
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
