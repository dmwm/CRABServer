from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

class PanDAAction(TaskAction):
    """Generic PanDAAction. Probably not needed at the current stage
       but it since this should not cause a big overhead it would be
       better to leave this here in order to eventually be ready to
       support specific PanDA interaction needs."""

    def __init__(self, pandaconfig, server, resturl):
        TaskAction.__init__(self, pandaconfig, server, resturl)

    def translateSiteName(self, sites):
        return ['ANALY_'+ s for s in sites]
