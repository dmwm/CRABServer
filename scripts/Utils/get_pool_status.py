#!/usr/bin/python

from __future__ import print_function
import re
import pprint
import optparse  # pylint: disable=deprecated-module

import htcondor2 as htcondor

class PoolStatus(object):

    def __init__(self, pool):
        self.pool = pool
        self.se_to_site = {}
        self.coll = htcondor.Collector(pool)
        self.neg = None
        self.neg_ad = None
        self.pool_status = None


    split_re = re.compile(r",\s*")
    def get_pool_status(self):
        results = {}
        for slot in self.coll.query(htcondor.AdTypes.Startd, 'AccountingGroup isnt undefined && GLIDEIN_CMSSite isnt undefined', ['GLIDEIN_CMSSite', 'GLIDEIN_SEs', 'AccountingGroup']):
            site = slot['GLIDEIN_CMSSite']
            for se in self.split_re.split(slot['GLIDEIN_SEs']):
                self.se_to_site[se] = site
            site_info = results.setdefault(site, {})
            grp = slot['AccountingGroup'].split("@")[0]
            if grp not in site_info:
                site_info[grp] = {"Resources": 1}
            else:
                site_info[grp]["Resources"] += 1
        return results


    def get_negotiator_status(self):
        self.neg_ad = self.coll.locate(htcondor.DaemonTypes.Negotiator, self.pool)
        self.neg = htcondor.Negotiator(self.neg_ad)
        for entry in self.neg.getPriorities():
            for site_info in self.pool_status.values():
                grp = entry['Name'].split("@")[0]
                if grp in site_info:
                    site_info[grp]['Priority'] = float(entry['Priority'])

    def get_glidein_status(self):
        factory_info = {}
        # The GlideFactory* attributes are the same for entries with the same GlideFactoryName.
        for entry in self.coll.query(htcondor.AdTypes.Any, 'MyType =?= "glideresource"', ["GlideFactoryMonitorStatusIdle", "GLIDEIN_CMSSite", "GlideFactoryName"]):
            cur = factory_info.setdefault(("GlideFactoryName", "GLIDEIN_CMSSite"), 0)
            if 'GLIDEIN_CMSSite' not in entry:
                continue
            factory_info["GlideFactoryName", entry["GLIDEIN_CMSSite"]] = entry.get("GlideFactoryMonitorStatusIdle", 0)

        for entry, idle in factory_info.items():
            name, site = entry  # pylint: disable=unused-variable
            site_info = self.pool_status.setdefault(site, {})
            cur = site_info.setdefault("IdleGlideins", 0)
            site_info["IdleGlideins"] = cur + int(idle)

    def get_jobs(self, schedd_ad):
        schedd = htcondor.Schedd(schedd_ad)
        try:
            jobs = schedd.query('true', ['CRAB_UserHN', 'AccountingGroup', 'JobPrio', 'BLTaskID', 'CRAB_ReqName', 'DESIRED_SEs', 'DESIRED_SITES', 'JobStatus', 'MATCH_GLIDEIN_CMSSite'])
        except IOError:
            print("Unable to retrieve tasks from schedd %s" % schedd_ad['Name'])
            return
        for job in jobs:
            if 'CRAB_ReqName' in job:
                task_name = job['CRAB_ReqName']
            elif 'BLTaskID' in job:
                task_name = job['BLTaskID']
            else:
                continue
            if 'CRAB_UserHN' in job:
                user_name = job['CRAB_UserHN']
            elif 'AccountingGroup' in job:
                user_name = job['AccountingGroup'].split("@")[0]
            else:
                continue
            if 'JobPrio' not in job:
                continue
            sites = []
            if 'DESIRED_SEs' in job:
                for se in self.split_re.split(job['DESIRED_SEs']):
                    if se in self.se_to_site:
                        sites.append(self.se_to_site[se])
            elif 'DESIRED_Sites' in job:
                sites += list(self.split_re.split(job['DESIRED_Sites']))
            for site in sites:
                site_info = self.pool_status.setdefault(site, {})
                grp_info = site_info.setdefault(user_name, {})
                tasks_info = grp_info.setdefault('tasks', {})
                task_info = tasks_info.setdefault(task_name, {'IdleJobs': 0, 'RunningJobs': 0})
                task_info['Priority'] = int(job['JobPrio'])
                if job['JobStatus'] == 1:
                    task_info['IdleJobs'] += 1
                elif job['JobStatus'] == 2 and job.get('MATCH_GLIDEIN_CMSSite', None) == site:
                    task_info['RunningJobs'] += 1

        for site, site_info in self.pool_status.items():
            for grp, grp_info in site_info.items():
                if grp == "IdleGlideins": continue
                for task, task_info in grp_info.get("tasks", {}).items():
                    if task_info.get("IdleJobs", 0) == 0 and task_info.get("RunningJobs", 0) == 0:
                        del grp_info["tasks"][task]
                if not grp_info.get("tasks", {}) and not grp_info.get("Resources", 0):
                    del site_info[grp]


    def get_all_jobs(self):
        schedds = self.coll.locateAll(htcondor.DaemonTypes.Schedd)
        results = {}
        for schedd_ad in schedds:
            self.get_jobs(schedd_ad)
        return results


    def execute(self):
        self.pool_status = self.get_pool_status()
        self.get_glidein_status()
        self.get_all_jobs()
        self.get_negotiator_status()
        for site, site_info in self.pool_status.items():
            if len(site_info) == 1 and 'IdleGlideins' in site_info and site_info['IdleGlideins'] == 0:
                del self.pool_status[site]


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("-p", "--pool", dest="pool", help="Location of the HTCondor pool")
    opts, args = parser.parse_args()

    p = PoolStatus(opts.pool)
    p.execute()

    pprint.pprint(p.pool_status)

