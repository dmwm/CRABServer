"""
Upload an archive containing all files needed to run the task to the Cache (necessary for crab submit --dryrun.)
"""
import os
import json
import tarfile
import time

import sys
if sys.version_info >= (3, 0):
    from urllib.parse import urlencode  # pylint: disable=no-name-in-module
if sys.version_info < (3, 0):
    from urllib import urlencode

from WMCore.DataStructs.LumiList import LumiList

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.WorkerExceptions import TaskWorkerException
from ServerUtilities import uploadToS3, downloadFromS3

class DryRunUploader(TaskAction):
    """
    Upload an archive containing all files needed to run the task to the Cache (necessary for crab submit --dryrun.)
    """

    def packSandbox(self, inputFiles):
        dryRunSandbox = tarfile.open('dry-run-sandbox.tar.gz', 'w:gz')
        for f in inputFiles:
            self.logger.debug('Adding %s to dry run tarball', f)
            dryRunSandbox.add(f, recursive=True)

        dryRunSandbox.close()

    def executeInternal(self, *args, **kw):
        inputFiles = args[0][2]
        splitterResult = args[0][3][0]

        cwd = os.getcwd()
        try:
            os.chdir(kw['tempDir'])
            splittingSummary = SplittingSummary(kw['task']['tm_split_algo'])
            for jobgroup in splitterResult:
                jobs = jobgroup.getJobs()
                splittingSummary.addJobs(jobs)
            splittingSummary.dump('splitting-summary.json')
            inputFiles.append('splitting-summary.json')

            self.packSandbox(inputFiles)

            self.logger.info('Uploading dry run tarball to the user file cache')
            t0 = time.time()
            uploadToS3(crabserver=self.crabserver, filepath='dry-run-sandbox.tar.gz',
                       objecttype='runtimefiles', taskname=kw['task']['tm_taskname'], logger=self.logger)
            os.remove('dry-run-sandbox.tar.gz')
            self.logger.info('Uploaded dry run tarball to the user file cache')
            # wait until tarball is available, S3 may take a few seconds for this (ref. issue #6706 )
            t1 = time.time()
            lt1 = time.strftime("%H:%M:%S", time.localtime(t1))
            uploadTime = t1-t0
            self.logger.debug('runtimefiles upload took %s secs and completed at %s', uploadTime, lt1)
            self.logger.debug('check if tarball is available')
            tarballOK = False
            while not tarballOK:
                try:
                    self.logger.debug('download tarball to /dev/null')
                    downloadFromS3(crabserver=self.crabserver, filepath='/dev/null', objecttype='runtimefiles',
                                   taskname=kw['task']['tm_taskname'], logger=self.logger)
                    self.logger.debug('OK, it worked')
                    tarballOK = True
                except Exception as e:
                    self.logger.debug('runtimefiles tarball not ready yet')
                    self.logger.debug('Exception was raised: %s', e)
                    self.logger.debug('Sleep 5 sec')
                    time.sleep(5)
            update = {'workflow': kw['task']['tm_taskname'], 'subresource': 'state', 'status': 'UPLOADED'}
            self.logger.debug('Updating task status: %s', str(update))
            self.crabserver.post(api='workflowdb', data=urlencode(update))

        finally:
            os.chdir(cwd)

        return Result(task=kw['task'], result=args[0])

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
        self.filesPerJob = []

    def addJobs(self, jobs):
        if self.algo == 'FileBased':
            self.lumisPerJob += [sum([x.get('lumiCount', 0) for x in job['input_files']]) for job in jobs]
            self.eventsPerJob += [sum([x['events'] for x in job['input_files']]) for job in jobs]
            self.filesPerJob += [len(job['input_files']) for job in jobs]
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
        if len(self.filesPerJob) > 0:
            summary.update({'total_files': sum(self.filesPerJob),
                            'max_files': max(self.filesPerJob),
                            'avg_files': sum(self.filesPerJob)/float(len(self.filesPerJob)),
                            'min_files': min(self.filesPerJob)})

        with open(outname, 'w') as f:
            json.dump(summary, f)
