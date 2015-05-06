"""
Upload an archive containing all files needed to run the a to the UserFileCache (necessary for crab submit --dryrun.)
"""
import os
import urllib
import tarfile
import hashlib
import json

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException

class DryRunUploader(TaskAction):
    """
    Upload an archive containing all files needed to run the task to the UserFileCache (necessary for crab submit --dryrun.)
    """

    def packSandbox(self, inputFiles):
        dryRunSandbox = tarfile.open('dry-run-sandbox.tar.gz', 'w:gz')
        for f in inputFiles:
            self.logger.debug('Adding %s to dry run tarball' % f)
            dryRunSandbox.add(f, recursive=True)

        dryRunSandbox.close()

    def executeInternal(self, *args, **kw):
        tempDir = args[0][0]
        inputFiles = args[0][3]
        splitterResult = args[0][4]

        cwd = os.getcwd()
        try:
            os.chdir(tempDir)
            splittingSummary = SplittingSummary(kw['task']['tm_split_algo'])
            for jobgroup in splitterResult:
                jobs = jobgroup.getJobs()
                splittingSummary.addJobs(jobs)
            splittingSummary.dump('splitting-summary.json')
            inputFiles.append('splitting-summary.json')

            self.packSandbox(inputFiles)

            self.logger.info('Uploading dry run tarball to the user file cache')
            ufc = UserFileCache(dict={'cert': kw['task']['user_proxy'], 'key': kw['task']['user_proxy'], 'endpoint': kw['task']['tm_cache_url']})
            result = ufc.uploadLog('dry-run-sandbox.tar.gz')
            os.remove('dry-run-sandbox.tar.gz')
            if 'hashkey' not in result:
                raise TaskWorkerException('Failed to upload dry-run-sandbox.tar.gz to the user file cache: ' + str(result))
            else:
                self.logger.info('Uploaded dry run tarball to the user file cache: ' + str(result))
                update = {'workflow': kw['task']['tm_taskname'], 'subresource': 'state', 'status': 'UPLOADED'}
                self.logger.debug('Updating task status: %s' % str(update))
                self.server.post(self.resturi, data=urllib.urlencode(update))

        finally:
            os.chdir(cwd)

        return Result(task=kw['task'], result=(-1))

    def execute(self, *args, **kw):
        try:
            return self.executeInternal(*args, **kw)
        except Exception as e:
            msg = "Failed to upload dry run tarball for %s; '%s'" % (kw['task']['tm_taskname'], str(e))
            raise TaskWorkerException(msg)

class SplittingSummary(object):
    """
    Class which calculates some summary data about the splitting results.
    """

    def __init__(self, algo):
        self.algo = algo
        self.lumisPerJob = []
        self.eventsPerJob = []

    def addJobs(self, jobs):
        if self.algo == 'FileBased':
            self.lumisPerJob += [sum([x.get('lumiCount', 0) for x in job['input_files']]) for job in jobs]
            self.eventsPerJob += [sum([x['events'] for x in job['input_files']]) for job in jobs]
        elif self.algo == 'EventBased':
            self.lumisPerJob += [job['mask']['LastLumi'] - job['mask']['FirstLumi'] for job in jobs]
            self.eventsPerJob += [job['mask']['LastEvent'] - job['mask']['FirstEvent'] for job in jobs]
        else:
            for job in jobs:
                avgEventsPerLumi = sum([f['avgEvtsPerLumi'] for f in job['input_files']])/float(len(job['input_files']))
                lumis = LumiList(compactList=job['mask']['runAndLumis'])
                self.lumisPerJob.append(len(lumis.getLumis()))
                self.eventsPerJob.append(avgEventsPerLumi * self.lumisPerJob[-1])

    def dump(self, outname):
        """
        Save splitting summary to a json file.
        """

        summary = {'algo': self.algo,
                   'total_jobs': len(self.lumisPerJob),
                   'total_lumis': sum(self.lumisPerJob),
                   'total_events': sum(self.eventsPerJob),
                   'max_lumis': max(self.lumisPerJob),
                   'max_events': max(self.eventsPerJob),
                   'avg_lumis': sum(self.lumisPerJob)/float(len(self.lumisPerJob)),
                   'avg_events': sum(self.eventsPerJob)/float(len(self.eventsPerJob)),
                   'min_lumis': min(self.lumisPerJob),
                   'min_events': min(self.eventsPerJob)}

        with open(outname, 'wb') as f:
            json.dump(summary, f)

