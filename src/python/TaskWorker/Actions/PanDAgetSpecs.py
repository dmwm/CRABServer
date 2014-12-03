from PandaServerInterface import getFullJobStatus

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result


class PanDAgetSpecs(PanDAAction):
    """Given a list of jobs, this action retrieves the specs
       form PanDA and load them in memory."""

    def execute(self, *args, **kwargs):
        self.logger.info("Getting already existing specs ")
        status, pandaspecs = getFullJobStatus(self.backendurls['baseURLSSL'], ids=kwargs['task']['resubmit_jobids'],
                                              proxy=kwargs['task']['user_proxy'])
        return Result(task=kwargs['task'], result=pandaspecs)
