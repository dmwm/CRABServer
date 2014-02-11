
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

        userfiles = kwargs['task']['tm_arguments'].get('userfiles')
        if not userfiles or kwargs['task']['tm_split_algo'] != "FileBased":
            if not userfiles:
                msg = "No files specified to process for task %s." % kwargs['task']['tm_taskname']
            if kwargs['task']['tm_split_algo'] != "FileBased":
                msg = "Data.splitting must be set to 'FileBased' when using a custom set of files."
            self.logger.error("Setting %s as failed: %s" % (kwargs['task']['tm_taskname'], msg))
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'status': "FAILED",
                         'subresource': 'failure',
                         'failure': b64encode(msg)}
            self.server.post(self.resturl, data = urllib.urlencode(configreq))
            raise StopHandler(msg)

        if hasattr(self.config.Sites, 'available'):
            locations = self.config.Sites.available
        else:
            sbj = SiteDBJSON({"key":self.config.TaskWorker.cmskey,
                              "cert":self.config.TaskWorker.cmscert})
            locations = sbj.getAllCMSNames()

        userFileset = Fileset(name = kwargs['task']['tm_taskname'])
        self.logger.info("There are %d files specified by the user." % len(userfiles))
        for userfile, idx in zip(userfiles, range(len(userfiles))):
            newFile = File(userfile, size = 1000, events = 1)
            newFile.setLocation(locations)
            newFile.addRun(Run(1, idx))
            newFile["block"] = 'UserFilesFakeBlock'
            newFile["first_event"] = 1
            newFile["last_event"] = 2
            userFileset.addFile(newFile)

        return Result(task=kwargs['task'], result=userFileset)


