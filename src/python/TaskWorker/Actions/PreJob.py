
import os
import classad

from ApmonIf import ApmonIf

states = ['OK', 'FATAL_ERROR', 'RECOVERABLE_ERROR']

class PreJob:

    def __init__(self):
        self.task_ad = classad.ClassAd()

    def update_dashboard(self, retry, id, reqname, backend):

        if not self.task_ad:
            return

        params = {'tool': 'crab3',
                  'SubmissionType':'direct',
                  'JSToolVersion': '3.3.0',
                  'tool_ui': os.environ.get('HOSTNAME',''),
                  'scheduler': 'GLIDEIN',
                  'GridName': self.task_ad['CRAB_UserDN'],
                  'ApplicationVersion': self.task_ad['CRAB_JobSW'],
                  'taskType': 'analysis',
                  'vo': 'cms',
                  'CMSUser': self.task_ad['CRAB_UserHN'],
                  'user': self.task_ad['CRAB_UserHN'],
                  'taskId': self.task_ad['CRAB_ReqName'],
                  'datasetFull': self.task_ad['CRAB_InputData'],
                  'resubmitter': self.task_ad['CRAB_UserHN'],
                  'exe': 'cmsRun',
                  'jobId': ("%d_https://glidein.cern.ch/%d/%s_%d" % (id, id, self.task_ad['CRAB_ReqName'].replace("_", ":"), retry)),
                  'sid': "https://glidein.cern.ch/%d/%s" % (id, self.task_ad['CRAB_ReqName'].replace("_", ":")),
                  'broker': backend,
                  'bossId': str(id),
                  'localId' : '',
                 }

        apmon = ApmonIf()
        print("Dashboard task info: %s" % str(params))
        apmon.sendToML(params)
        apmon.free()


    def get_task_ad(self):
        self.task_ad = {}
        try:
            self.task_ad = classad.parseOld(open(os.environ['_CONDOR_JOB_AD']))
        except Exception:
            print traceback.format_exc()


    def get_statistics(self):
        results = {}
        try:
            for state in states:
                count = 0
                with open("task_statistics.%s" % state) as fd:
                    for line in fd:
                        count += 1
                results[state] = count
        except:
            return {}
        return results


    def get_site_statistics(self, site):
        results = {}
        try:
            for state in states:
                count = 0 
                with open("task_statistics.%s.%s" % (site, state)) as fd:
                    for line in fd:
                        count += 1
                results[state] = count
        except:
            return {}
        return results


    def calculate_blacklist(self):
        # TODO: before we can do this, we need a more reliable way to pass
        # the site list to the prejob.
        return []


    def alter_submit(self, retry, id):
        new_submit_text = '+CRAB_Retry = %d\n' % retry
        if 'JobPrio' in self.task_ad:
            if id <= 5:
                self.task_ad['JobPrio'] += 10
            new_submit_text += '+JobPrio = %s\n' % str(self.task_ad.lookup('JobPrio'))
        if 'MaxWallTimeMins' in self.task_ad:
            new_submit_text += '+MaxWallTimeMins = %s\n' % str(self.task_ad.lookup('MaxWallTimeMins'))
        if 'RequestCpus' in self.task_ad:
            new_submit_text += '+RequestCpus = %s\n' % str(self.task_ad.lookup('RequestCpus'))
        if 'RequestMemory' in self.task_ad:
            new_submit_text += '+RequestMemory = %s\n' % str(self.task_ad.lookup('RequestMemory'))
        blacklist = self.calculate_blacklist()
        if blacklist:
            self.task_ad['CRAB_SiteAutomaticBlacklist'] = blacklist
            new_submit_text += '+CRAB_SiteAutomaticBlacklist = %s\n' % str(self.task_ad.lookup('CRAB_SiteAutomaticBlacklist'))
        with open("Job.submit", "r") as fd:
            new_submit_text += fd.read()
        with open("Job.%d.submit" % id, "w") as fd:
            fd.write(new_submit_text)


    def execute(self, *args):
        retry_num = int(args[0])
        crab_id = int(args[1])
        reqname = args[2]
        backend = args[3]
        self.get_task_ad()
        self.alter_submit(retry_num, crab_id)
        if retry_num != 0:
            self.update_dashboard(retry_num, crab_id, reqname, backend)
        os.execv("/bin/sleep", ["sleep", str(int(args[0])*60)])

