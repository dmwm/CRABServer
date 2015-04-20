
import os
import urllib
import logging
from httplib import HTTPException
from base64 import b64encode

from WMCore.WorkQueue.WorkQueueUtils import get_dbs
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Run import Run

from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.WorkerExceptions import StopHandler
from TaskWorker.DataObjects.Result import Result


class UserDataDiscovery(DataDiscovery):
    """
    "Fake" the data discovery - use any arbitrary list of LFNs or PFNs the user provides.
    """

    def execute(self, *args, **kwargs):
        self.logger.info("Data discovery and splitting for %s using user-provided files" % kwargs['task']['tm_taskname'])

        if 'tm_user_files' in kwargs['task'] and kwargs['task']['tm_user_files']:
            userfiles = kwargs['task']['tm_user_files']
        else: ## For backward compatibility only.
            userfiles = kwargs['task']['tm_arguments'].get('userfiles')
        splitting = kwargs['task']['tm_split_algo']
        total_units = kwargs['task']['tm_totalunits']
        if not userfiles or splitting != 'FileBased':
            if not userfiles:
                msg = "No files specified to process for task %s." % kwargs['task']['tm_taskname']
            if splitting != 'FileBased':
                msg = "Data.splitting must be set to 'FileBased' when using a custom set of files."
            self.logger.error("Setting %s as failed: %s" % (kwargs['task']['tm_taskname'], msg))
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'status': "FAILED",
                         'subresource': 'failure',
                         'failure': b64encode(msg)}
            self.server.post(self.resturi, data = urllib.urlencode(configreq))
            raise StopHandler(msg)

        if hasattr(self.config.Sites, 'available'):
            locations = self.config.Sites.available
        else:
            sbj = SiteDBJSON({"key":self.config.TaskWorker.cmskey,
                              "cert":self.config.TaskWorker.cmscert})
            locations = sbj.getAllCMSNames()

        userFileset = Fileset(name = kwargs['task']['tm_taskname'])
        self.logger.info("There are %d files specified by the user." % len(userfiles))
        if total_units > 0:
            self.logger.info("Will run over the first %d files." % total_units)
        file_counter = 0
        for userfile, idx in zip(userfiles, range(len(userfiles))):
            newFile = File(userfile, size = 1000, events = 1)
            newFile.setLocation(locations)
            newFile.addRun(Run(1, idx))
            newFile["block"] = 'UserFilesFakeBlock'
            newFile["first_event"] = 1
            newFile["last_event"] = 2
            userFileset.addFile(newFile)
            file_counter += 1
            if total_units > 0 and file_counter >= total_units:
                break

        return Result(task = kwargs['task'], result = userFileset)


