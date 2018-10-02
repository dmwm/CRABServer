from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.Services.CRIC.CRIC import CRIC

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.WorkerExceptions import TaskWorkerException


class UserDataDiscovery(DataDiscovery):
    """
    "Fake" the data discovery - use any arbitrary list of LFNs or PFNs the user provides.
    """

    def execute(self, *args, **kwargs):
        self.logger.info("Data discovery and splitting for %s using user-provided files" % kwargs['task']['tm_taskname'])

        userfiles = kwargs['task']['tm_user_files']
        splitting = kwargs['task']['tm_split_algo']
        total_units = kwargs['task']['tm_totalunits']
        if not userfiles or splitting != 'FileBased':
            if not userfiles:
                msg = "No files specified to process for task %s." % kwargs['task']['tm_taskname']
            if splitting != 'FileBased':
                msg = "Data.splitting must be set to 'FileBased' when using a custom set of files."
            raise TaskWorkerException(msg)

        if hasattr(self.config.Sites, 'available'):
            locations = self.config.Sites.available
        else:
            with self.config.TaskWorker.envForCMSWEB :
                resourceCatalog = CRIC(loger=self.logger)
                locations = resourceCatalog.getAllPSNs()

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


