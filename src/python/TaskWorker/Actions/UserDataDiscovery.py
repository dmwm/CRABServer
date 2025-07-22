"""
does data discovery when user submitted a list of files
"""
from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.WorkerExceptions import SubmissionRefusedException


class UserDataDiscovery(DataDiscovery):
    """
    "Fake" the data discovery - use any arbitrary list of LFNs or PFNs the user provides.
    """

    def execute(self, *args, **kwargs):
        self.logger.info("Data discovery and splitting for %s using user-provided files", kwargs['task']['tm_taskname'])

        userfiles = kwargs['task']['tm_user_files']
        splitting = kwargs['task']['tm_split_algo']
        totalUnits = kwargs['task']['tm_totalunits']
        if not userfiles or splitting != 'FileBased':
            if not userfiles:
                msg = f"No files specified to process for task {kwargs['task']['tm_taskname']}."
            if splitting != 'FileBased':
                msg = "Data.splitting must be set to 'FileBased' when using a custom set of files."
            raise SubmissionRefusedException(msg)

        if hasattr(self.config.Sites, 'available'):
            locations = self.config.Sites.available
        else:
            locations = kwargs['task']['all_possible_processing_sites']

        userFileset = Fileset(name = kwargs['task']['tm_taskname'])
        self.logger.info("There are %d files specified by the user.", len(userfiles))
        if totalUnits > 0:
            self.logger.info("Will run over the first %d files.", totalUnits)
        fileCounter = 0
        for userfile, idx in zip(userfiles, range(len(userfiles))):
            newFile = File(userfile, size = 1000, events = 1)
            newFile.setLocation(locations)
            newFile.addRun(Run(1, idx))
            newFile["block"] = 'UserFilesFakeBlock'
            newFile["first_event"] = 1
            newFile["last_event"] = 2
            userFileset.addFile(newFile)
            fileCounter += 1
            if 0 < totalUnits <= fileCounter:
                break

        return Result(task = kwargs['task'], result = userFileset)
