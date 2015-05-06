from PandaServerInterface import submitJobs, refreshSpecs, getSite
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import PanDAIdException, PanDAException, NoAvailableSite

import time
import commands
import traceback
import os
import json
import string
import random
import urllib
from httplib import HTTPException
from base64 import b64encode


# the length of the string to be appended to the every out file (before their extension)
LFNHANGERLEN = 6


class PanDAInjection(PanDAAction):
    """Creating the specs and injecting them into PanDA"""

    def inject(self, task, pandajobspecs):
        """Injects in PanDA the taskbuffer objects containing the jobs specs.

           :arg TaskWorker.DataObject.Task task: the task to work on
           :arg list taskbuffer.JobSpecs pandajobspecs: the list of specs to inject
           :return: dictionary containining the injection resulting id's."""
        #pandajobspecs = pandajobspecs[0:2]
        status, injout = submitJobs(self.backendurls['baseURLSSL'], jobs=pandajobspecs, proxy=task['user_proxy'], verbose=True)
        self.logger.info('PanDA submission exit code: %s' % status)
        jobsetdef = {}
        for jobid, defid, setid in injout:
            if setid['jobsetID'] in jobsetdef:
                jobsetdef[setid['jobsetID']].add(defid)
            else:
                jobsetdef[setid['jobsetID']] = set([defid])
        self.logger.debug('Single PanDA injection resulted in %d distinct jobsets and %d jobdefs.' % (len(jobsetdef), sum([len(jobsetdef[k]) for k in jobsetdef])))
        return jobsetdef

    def makeSpecs(self, task, outdataset, jobgroup, site, jobset, jobdef, startjobid, basejobname, lfnhanger, allsites):
        """Building the specs

        :arg TaskWorker.DataObject.Task task: the task to work on
        :arg str outdataset: the output dataset name where all the produced files will be placed
        :arg WMCore.DataStructs.JobGroup jobgroup: the group containing the jobs
        :arg str site: the borkered site where to run the jobs
        :arg int jobset: the PanDA jobset corresponding to the current task
        :arg int jobdef: the PanDA jobdef where to append the current jobs --- not used
        :arg int startjobid: jobs need to have an incremental index, using this to have
                             unique ids in the whole task
        :arg str basejobname: common string between all the jobs in their job name
        :arg str lfnhanger: random string to append at the file lfn
        :arg list str allsites: all possible sites where the job can potentially run
        :return: the list of job sepcs objects."""
        refreshSpecs(self.backendurls['baseURL'], proxy=task['user_proxy'])
        oldids = []
        pandajobspec = []
        i = startjobid
        for job in jobgroup.jobs:
            #if i > 10:
            #    break
            jobname = "%s-%d" %(basejobname, i)
            jspec = self.createJobSpec(task, outdataset, job, jobset, jobdef, site, jobname, lfnhanger, allsites, job.get('jobnum', i))
            pandajobspec.append(jspec)
            if hasattr(jspec, 'parentID'):
                try:
                    oldids.append(int(jspec.parentID))
                except ValueError:
                    pass
            i += 1
        return pandajobspec, i, oldids

    def createJobSpec(self, task, outdataset, job, jobset, jobdef, site, jobname, lfnhanger, allsites, jobid):
        """Create a spec for one job

        :arg TaskWorker.DataObject.Task task: the task to work on
        :arg str outdataset: the output dataset name where all the produced files will be placed
        :arg WMCore.DataStructs.Job job: the abstract job
        :arg int jobset: the PanDA jobset corresponding to the current task
        :arg int jobdef: the PanDA jobdef where to append the current jobs --- not used
        :arg str site: the borkered site where to run the jobs
        :arg str jobname: the job name
        :arg str lfnhanger: the random string to be added in the output file name
        :arg list str allsites: all possible sites where the job can potentially run
        :arg int jobid: incremental job number
        :return: the sepc object."""

        pandajob = JobSpec()
        ## always setting a job definition ID
        pandajob.jobDefinitionID = jobdef if jobdef else -1
        ## always setting a job set ID
        pandajob.jobsetID = jobset if jobset else -1
        pandajob.jobName = jobname
        pandajob.prodUserID = task['tm_user_dn']
        pandajob.destinationDBlock = outdataset
        pandajob.prodDBlock = task['tm_input_dataset']
        pandajob.prodSourceLabel = 'user'
        pandajob.computingSite = site
        pandajob.cloud = getSite(pandajob.computingSite)
        pandajob.destinationSE = 'local'
        ## need to initialize this
        pandajob.metadata = ''

        def outFileSpec(of=None, log=False):
            """Local routine to create an FileSpec for the an job output/log file

               :arg str of: output file base name
               :return: FileSpec object for the output file."""
            outfile = FileSpec()
            if log:
                outfile.lfn = "job.log_%d_%s.tgz" % (jobid, lfnhanger)
                outfile.type = 'log'
            else:
                outfile.lfn = '%s_%d_%s%s' %(os.path.splitext(of)[0], jobid, lfnhanger, os.path.splitext(of)[1])
                outfile.type = 'output'
            outfile.destinationDBlock = pandajob.destinationDBlock
            outfile.destinationSE = task['tm_asyncdest']
            outfile.dataset = pandajob.destinationDBlock
            return outfile

        alloutfiles = []
        outjobpar = {}
        outfilestring = ''
        for outputfile in task['tm_outfiles']:
            outfilestring += '%s,' % outputfile
            filespec = outFileSpec(outputfile)
            alloutfiles.append(filespec)
            #pandajob.addFile(filespec)
            outjobpar[outputfile] = filespec.lfn
        for outputfile in task['tm_tfile_outfiles']:
            outfilestring += '%s,' % outputfile
            filespec = outFileSpec(outputfile)
            alloutfiles.append(filespec)
            #pandajob.addFile(filespec)
            outjobpar[outputfile] = filespec.lfn
        for outputfile in task['tm_edm_outfiles']:
            outfilestring += '%s,' % outputfile
            filespec = outFileSpec(outputfile)
            alloutfiles.append(filespec)
            #pandajob.addFile(filespec)
            outjobpar[outputfile] = filespec.lfn
        outfilestring = outfilestring[:-1]

        infiles = []
        for inputfile in job['input_files']:
            infiles.append( inputfile['lfn'] )

        pandajob.jobParameters = '-a %s ' % task['tm_user_sandbox']
        pandajob.jobParameters += '--sourceURL %s ' % task['tm_cache_url']
        pandajob.jobParameters += '--jobNumber=%s ' % jobid
        pandajob.jobParameters += '--cmsswVersion=%s ' % task['tm_job_sw']
        pandajob.jobParameters += '--scramArch=%s ' % task['tm_job_arch']
        pandajob.jobParameters += '--inputFile=\'%s\' ' % json.dumps(infiles)

        self.jobParametersSetting(pandajob, job, self.jobtypeMapper[task['tm_job_type']])

        pandajob.jobParameters += '-o "%s" ' % str(outjobpar)
        pandajob.jobParameters += '--dbs_url=%s ' % task['tm_dbs_url']
        pandajob.jobParameters += '--publish_dbs_url=%s ' % task['tm_publish_dbs_url']
        pandajob.jobParameters += '--publishFiles=%s ' % ('True' if task['tm_publication'] == 'T' else 'False')
        pandajob.jobParameters += '--saveLogs=%s ' % ('True' if task['tm_save_logs'] == 'T' else 'False')
        pandajob.jobParameters += '--availableSites=\'%s\' ' %json.dumps(allsites)

        pandajob.jobName = '%s' % task['tm_taskname'] #Needed by ASO and Dashboard

        if 'panda_oldjobid' in job and job['panda_oldjobid']:
            pandajob.parentID = job['panda_oldjobid']

        pandajob.addFile(outFileSpec(log=True))
        for filetoadd in alloutfiles:
            pandajob.addFile(filetoadd)

        return pandajob


    def jobParametersSetting(self, pandajob, job, jobtype):
        """ setup jobtype specific fraction of the jobParameters """

        if jobtype == 'Processing':
            pandajob.jobParameters += '--runAndLumis=\'%s\' ' % json.dumps(job['mask']['runAndLumis'])
        elif jobtype == 'Production':
            # The following two are hardcoded currently.
            # we expect they will be used/useful in the near future
            pandajob.jobParameters += '--seeding=\'%s\' ' % 'AutomaticSeeding'
            pandajob.jobParameters += '--lheInputFiles=\'%s\' ' % 'None'
            #
            pandajob.jobParameters += '--firstEvent=\'%s\' ' % job['mask']['FirstEvent']
            pandajob.jobParameters += '--lastEvent=\'%s\' ' % job['mask']['LastEvent']
            pandajob.jobParameters += '--firstLumi=\'%s\' ' % job['mask']['FirstLumi']
            pandajob.jobParameters += '--firstRun=\'%s\' ' % job['mask']['FirstRun']
        elif jobtype == 'Generic':
            self.logger.info('Generic JobType not yet supported')

    def execute(self, *args, **kwargs):
        self.logger.info("Create specs and inject into PanDA ")
        results = []
        ## in case the jobset already exists it means the new jobs need to be appended to the existing task
        ## (a possible case for this is the resubmission)
        jobset = kwargs['task']['panda_jobset_id'] if kwargs['task']['panda_jobset_id'] else None
        jobdef = None
        startjobid = 0

        basejobname = "%s" % commands.getoutput('uuidgen')
        lfnhanger = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(LFNHANGERLEN))
        ## /<primarydataset>/<yourHyperNewsusername>-<publish_data_name>-<PSETHASH>/USER
        if kwargs['task']['tm_input_dataset']:
            primaryDS = kwargs['task']['tm_input_dataset'].split('/')[1]
        else:
            primaryDS = '-'.join(kwargs['task']['tm_publish_name'].split('-')[:-1])

        outdataset = '/%s/%s-%s/USER' %(primaryDS,
                                        kwargs['task']['tm_username'],
                                        kwargs['task']['tm_publish_name'])
        alloldids = kwargs['task']['panda_resubmitted_jobs']
        resulting = False
        for jobgroup in args[0]:
            jobs, site, allsites = jobgroup.result
            blocks = getattr(jobs, 'blocks', None)
            if blocks is None:
                blocks = [infile['block'] for infile in jobs.jobs[0]['input_files'] if infile['block']]
            # now try/except everything, then need to put the code in smaller try/except cages
            # note: there is already an outer try/except for every action and for the whole handler
            try:
                if not site:
                    msg = "No site available for submission of task %s" %(kwargs['task']['tm_taskname'])
                    raise NoAvailableSite(msg)

                jobgroupspecs, startjobid, oldids = self.makeSpecs(kwargs['task'], outdataset, jobs, site, jobset, jobdef,
                                                                   startjobid, basejobname, lfnhanger, allsites)
                jobsetdef = self.inject(kwargs['task'], jobgroupspecs)
                outjobset = jobsetdef.keys()[0]
                outjobdefs = jobsetdef[outjobset]
                alloldids.extend(oldids)
                if outjobset is None:
                    msg = "Cannot retrieve the job set id for task %s " %(kwargs['task']['tm_taskname'])
                    raise PanDAException(msg)
                elif jobset is None:
                    jobset = outjobset
                elif not outjobset == jobset:
                    msg = "Task %s has jobgroups with different jobsets: %d and %d" %(kwargs['task']['tm_taskname'], outjobset, jobset)
                    raise PanDAIdException(msg)
                else:
                    pass

                for jd in outjobdefs:
                    subblocks = None
                    if len(blocks) > 0:
                        resubmittedjobs = ('&subblocks=') + ('&subblocks=').join( map(urllib.quote, map(str, set(blocks))) )
                    configreq = {'workflow': kwargs['task']['tm_taskname'],
                                 'substatus': "SUBMITTED",
                                 'subjobdef': jd,
                                 'subuser': kwargs['task']['tm_user_dn'],}
                    self.logger.error("Pushing information centrally %s" %(str(configreq)))
                    data = urllib.urlencode(configreq) + '' if subblocks is None else subblocks
                    self.server.put(self.resturl, data=data)
                results.append(Result(task=kwargs['task'], result=jobsetdef))
            except HTTPException as hte:
                self.logger.error("Server could not acquire any work from the server: \n" +
                                  "\tstatus: %s\n" %(hte.headers.get('X-Error-Http', 'unknown')) +
                                  "\treason: %s" %(hte.headers.get('X-Error-Detail', 'unknown')))
                self.logger.error("%s " %(str(traceback.format_exc())))
                self.logger.error("\turl: %s\n" %(getattr(hte, 'url', 'unknown')))
                self.logger.error("\tresult: %s\n" %(getattr(hte, 'result', 'unknown')))
                subblocks = None
                if len(blocks) > 0:
                    resubmittedjobs = ('&subblocks=') + ('&subblocks=').join( map(urllib.quote, map(str, set(blocks))) )
                configreq = {'workflow': kwargs['task']['tm_taskname'],
                             'substatus': "FAILED",
                             'subjobdef': -1,
                             'subuser': kwargs['task']['tm_user_dn'],
                             'subfailure': b64encode(str(hte)),}
                self.logger.error("Pushing information centrally %s" %(str(configreq)))
                data = urllib.urlencode(configreq) + '' if subblocks is None else subblocks
                self.server.put(self.resturl, data=data)
                results.append(Result(task=kwargs['task'], warn=msg))
            except Exception as exc:
                msg = "Problem %s injecting job group from task %s reading data from blocks %s" % (str(exc), kwargs['task']['tm_taskname'], ",".join(blocks))
                self.logger.error(msg)
                self.logger.error(str(traceback.format_exc()))
                subblocks = None
                if len(blocks) > 0:
                    resubmittedjobs = ('&subblocks=') + ('&subblocks=').join( map(urllib.quote, map(str, set(blocks))) ) 
                configreq = {'workflow': kwargs['task']['tm_taskname'],
                             'substatus': "FAILED",
                             'subjobdef': -1,
                             'subuser': kwargs['task']['tm_user_dn'],
                             'subfailure': b64encode(str(exc)),}
                self.logger.error("Pushing information centrally %s" %(str(configreq)))
                try:
                    data = urllib.urlencode(configreq) + '' if subblocks is None else subblocks
                    self.server.put(self.resturl, data=data)
                    results.append(Result(task=kwargs['task'], warn=msg))
                except HTTPException as hte:
                    if not hte.headers.get('X-Error-Detail', '') == 'Required object is missing' or \
                       not hte.headers.get('X-Error-Http', -1) == '400':
                        self.logger.error("Server could not acquire any work from the server: \n" +
                                          "\tstatus: %s\n" %(hte.headers.get('X-Error-Http', 'unknown')) +
                                          "\treason: %s" %(hte.headers.get('X-Error-Detail', 'unknown')))
                    if hte.headers.get('X-Error-Http', 'unknown') in ['unknown']:
                        self.logger.error("%s " %(str(traceback.format_exc())))
                        self.logger.error("\turl: %s\n" %(getattr(hte, 'url', 'unknown')))
                        self.logger.error("\tresult: %s\n" %(getattr(hte, 'result', 'unknown')))
            else:
                resulting = True
        if not jobset: # or resulting is False :
            msg = "No task id available for the task. Setting %s at failed." %(kwargs['task']['tm_taskname'])
            self.logger.error(msg)
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'status': "FAILED",
                         'subresource': 'failure',
                         'failure': b64encode(msg)}
            self.server.post(self.resturl, data = urllib.urlencode(configreq))
            results.append(Result(task=kwargs['task'], err=msg))
        else: #if resulting
            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'status': "SUBMITTED",
                         'jobset': jobset,
                         'subresource': 'success',}
            resubmittedjobs = None
            if len(alloldids) > 0:
                resubmittedjobs = ('&resubmittedjobs=') + ('&resubmittedjobs=').join( map(urllib.quote, map(str, set(alloldids))) )
            self.logger.debug("Setting the task as submitted with %s%s " %(str(configreq), '' if resubmittedjobs is None else resubmittedjobs))
            data = urllib.urlencode(configreq) + ('' if resubmittedjobs is None else resubmittedjobs)
            self.server.post(self.resturl, data = data)
        return results
