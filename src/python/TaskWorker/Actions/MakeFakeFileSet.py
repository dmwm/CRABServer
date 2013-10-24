from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Run import Run


class MakeFakeFileSet(TaskAction):
    """This is needed to make WMCore.JobSplitting lib working... 
       do not like very much. Given that all is fake here I am 
       quite sure we only need total number of events said that I 
       set all the other parmas to dummy values. We may want to set 
       them in the future"""
 
    def execute(self, *args, **kwargs):

        totalevents = kwargs['task']['tm_totalunits']
        firstEvent = 1
        lastEvent = totalevents
        firstLumi = 1
        lastLumi = 10
        
        #MC comes with only one MCFakeFile
        singleMCFileset = Fileset(name = "MCFakeFileSet")
        newFile = File("MCFakeFile", size = 1000, events = totalevents)
        newFile.setLocation(self.config.Sites.available)
        newFile.addRun(Run(1, *range(firstLumi, lastLumi + 1)))
        newFile["block"] = 'MCFackBlock'
        newFile["first_event"] = firstEvent
        newFile["last_event"] = lastEvent
        singleMCFileset.addFile(newFile)

        return Result(task=kwargs['task'], result=singleMCFileset)

