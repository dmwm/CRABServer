from WMCore.DataStructs.JobGroup import JobGroup as WMJobGroup
from WMCore.DataStructs.Job import Job as WMJob

from TaskWorker.DataObjects.Result import Result
from TaskWorker.Actions.PanDAAction import PanDAAction

#from urllib import unquote
import urllib
from ast import literal_eval
import shlex
from optparse import (OptionParser, BadOptionError)


class PassThroughOptionParser(OptionParser):
    """
    An unknown option pass-through implementation of OptionParser.

    When unknown arguments are encountered, bundle with largs and try again,
    until rargs is depleted.

    sys.exit(status) will still be called if a known argument is passed
    incorrectly (e.g. missing arguments or bad argument types, etc.)
    """
    def _process_args(self, largs, rargs, values):
        while rargs:
            try:
                OptionParser._process_args(self,largs,rargs,values)
            except (BadOptionError, Exception) as e:
                #largs.append(e.opt_str)
                continue

class PanDASpecs2Jobs(PanDAAction):
    """Given a list of job specs to be resubmitted, transforms the specs
       into jobgroups-jobs structure in order to reflect the splitting output."""

    def execute(self, *args, **kwargs):
        self.logger.debug("Transforming old specs into jobs.")

        # mapping to cache job def - blocks association
        blocks = {}

        regroupjobs = {}
        ## grouping in a dictionary can happen here
        for job in args[0]:
            if job.jobDefinitionID in regroupjobs:
                regroupjobs[job.jobDefinitionID].append(job)
            else:
                regroupjobs[job.jobDefinitionID] = [job]

        jobgroups = []
        ## here converting the grouping into proper JobGroup-Jobs
        for jobdef in regroupjobs:
            jobgroup = blocks.get(jobdef, None)
            if jobgroup is None: 
                configreq = {'subresource': 'jobgroup',
                             'subjobdef': jobdef,
                             'subuser': kwargs['task']['tm_user_dn']}
                self.logger.debug("Retrieving %d jobdef information from task manager db: %s" %(jobdef, str(configreq)))
                jobgroup = self.server.get(self.resturl, data = configreq)
                self.logger.debug("Jobgroup information in task manager: %s" % str(jobgroup))
                jobgroup = jobgroup[0]['result'][0]
                blocks[jobdef] = jobgroup['tm_data_blocks'] 
            jg = WMJobGroup()
            for job in regroupjobs[jobdef]:
                parser = PassThroughOptionParser()
                parser.add_option('--inputFile', dest='inputfiles', type='string')
                parser.add_option('--runAndLumis', dest='runlumis', type='string')
                parser.add_option('--availableSites', dest='allsites', type='string')
                parser.add_option('--jobNumber', dest='jobnum', type='int')
                (options, args) = parser.parse_args(shlex.split(job.jobParameters))
                jj = WMJob()
                jj['input_files'] = []
                for infile in literal_eval(options.inputfiles):
                    jj['input_files'].append({'lfn': infile,
                                              'block': blocks[jobdef],
                                              'locations': [ss for ss in literal_eval(options.allsites)]})
                if options.runlumis:
                    jj['mask']['runAndLumis'] = literal_eval(options.runlumis)
                jj['panda_oldjobid'] = job.PandaID
                jj['jobnum'] = options.jobnum
                jg.add(jj)
            setattr(jg, 'blocks', blocks[jobdef])
            jg.commit()
            jobgroups.append(jg)

        return Result(task=kwargs['task'], result=jobgroups)
