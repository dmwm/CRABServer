"""
 uploads to S3 the tarball created by DagmanCreator
"""
import os

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import uploadToS3
from CRABUtils.TaskUtils import updateTaskStatus


class Uploader(TaskAction):
    """
    Upload an archive containing all files needed to run the task to the Cache (necessary for crab submit --dryrun.)
    """
    def executeInternal(self, *args, **kw):
        """ pload InputFiles.tar.gz to s3 """
        task = kw['task']['tm_taskname']
        uploadToS3(crabserver=self.crabserver, filepath='InputFiles.tar.gz',
                   objecttype='runtimefiles', taskname=task,
                   logger=self.logger)
        # report that all tarballs are now in S3
        updateTaskStatus(crabserver=self.crabserver, taskName=task, status='UPLOADED', logger=self.logger)
        return Result(task=kw['task'], result=args[0])

    def execute(self, *args, **kw):
        """ entry point from Handler """
        cwd = os.getcwd()
        try:
            os.chdir(kw['tempDir'])
            return self.executeInternal(*args, **kw)
        except Exception as e:
            msg = f"Failed to run Uploader action for {kw['task']['tm_taskname']}; {e}"
            raise TaskWorkerException(msg) from e
        finally:
            os.chdir(cwd)
