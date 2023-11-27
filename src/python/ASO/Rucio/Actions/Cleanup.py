"""
Clean it up.
"""
import logging

import ASO.Rucio.config as config # pylint: disable=consider-using-from-import
from ASO.Rucio.utils import callGfalRm


class Cleanup:
    """
    Clean up temp RSE and Rucio entry.

    :param transfer: Transfer Object to get infomation.
    :type object: class:`ASO.Rucio.Transfer`
    """
    def __init__(self, transfer):
        self.logger = logging.getLogger("RucioTransfer.Actions.Cleanup")
        self.transfer = transfer

    def execute(self):
        """
        Gets inputs from `self.transfer.bookkeepingOKLocks` where its is updated
        by previous `MonitorLockStatus` action.
        """
        # Currently we need to delete file from Temp RSE by ourselves using `gfal-rm` command.
        # Rucio replicas need to wait until CMSRucio allow delete replicas from user scope.

        # TODO: change the name `bookkeepingOKLocks` in MonitorLockStatus to something else.
        toBeDeleted = []
        for name in self.transfer.bookkeepingOKLocks:
            if not name in self.transfer.cleanedFiles:
                toBeDeleted.append(name)
        self.deleteFileInTempArea(toBeDeleted)
        self.transfer.cleanedFiles += toBeDeleted
        self.transfer.updateCleanedFiles()

    def deleteFileInTempArea(self, fileList):
        """
        This method call `callGfalRm`, fire-and-forget function to delete
        file from temp area from provided PFN.`

        `fileList` is the list of the "key" of fileDocs to lookup original
        transfer dict to get `source_lfn`.

        :param fileList: list of key of fileDoc.
        :type fileList: list
        """
        self.logger.info('Cleaning up temp area.')
        # Return if there is no file to delete, to prevent weird log line in
        # `gfal.log`
        if len(fileList) == 0:
            self.logger.info('No file to clean up.')
            return
        pfns = []
        for key in fileList:
            transferItem = self.transfer.LFN2transferItemMap[key]
            rse = f'{transferItem["source"]}_Temp'
            lfn = transferItem['source_lfn']
            pfn = self.transfer.LFN2PFNMap[rse][lfn]
            pfns.append(pfn)
            self.logger.debug(f'PFN to delete: {pfn}.')
        callGfalRm(pfns, self.transfer.restProxyFile, config.args.gfal_log_path)
