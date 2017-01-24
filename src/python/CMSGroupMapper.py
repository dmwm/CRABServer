from __future__ import print_function

import os
import re
import ldap
import time

g_cache = {}
g_expire_time = 0


def get_egroup_users(egroup_name):
    """ Given an egroup name it returns the whole list of user contained in this egroup
    """
    res = set()
    searchFilter = "memberOf=CN=%s,OU=e-groups,OU=Workgroups,DC=cern,DC=ch" % egroup_name
    searchAttribute = ["sAMAccountName"]
    l = ldap.initialize("ldaps://xldap.cern.ch:636")
    basedn = "DC=cern,DC=ch"
    searchScope = ldap.SCOPE_SUBTREE

    ldap_result_id = l.search(basedn, searchScope, searchFilter, searchAttribute)

    result_type, result_data = l.result(ldap_result_id, timeout=100)
    for item in result_data:
        user = item[1]['sAMAccountName'][0]
        res.add(user)

    return res


def cache_users(logFunction=print):
    """ Cache the entries in the variuos local-users.txt files
        Those entries are saved in a global variable with this
        format:

        {'username1': set(['T3_IT_Bologna']),
         'username2': set(['T2_US_Nebraska']),
         'username3': set(['T2_ES_CIEMAT', 'T3_IT_Bologna']),
         'userdn1': set(['T2_ES_CIEMAT']),
         'userfqan: set(['T2_ES_CIEMAT', 'T3_IT_Bologna'])
    """
    global g_expire_time
    global g_cache

    base_dir = '/cvmfs/cms.cern.ch/SITECONF'
    cache = {}
    user_re = re.compile(r'[-_A-Za-z0-9.]+')
    sites = None
    try:
        if os.path.isdir(base_dir):
            sites = os.listdir(base_dir)
    except OSError as ose:
        logFunction("Cannot list SITECONF directories in cvmfs:" + str(ose))
    if not sites:
        g_expire_time = time.time() + 60
        return
    for entry in sites:
        if (entry == 'local'):
            continue
        #first take care of users that are directly in local-users.txt
        full_path = os.path.join(base_dir, entry, 'GlideinConfig', 'local-users.txt')
        if os.path.isfile(full_path):
            try:
                fd = open(full_path)
                for user in fd:
                    user = user.strip()
                    if user_re.match(user):
                        group_set = cache.setdefault(user, set())
                        group_set.add(entry)
            except OSError as ose:
                logFunction("Cannot list SITECONF directories in cvmfs:" + str(ose))
                raise
        #then do the same for users in local-egroups.txt
        full_path = os.path.join(base_dir, entry, 'GlideinConfig', 'local-egroups.txt')
        if os.path.isfile(full_path):
            egroup = ''
            try:
                fd = open(full_path)
                for egroup in fd:
                    egroup = egroup.strip()
                    users = get_egroup_users(egroup)
                    for user in users:
                        if user_re.match(user):
                            group_set = cache.setdefault(user, set())
                            group_set.add(entry)
            except OSError as ose:
                logFunction("Cannot list SITECONF egroups in cvmfs:" + str(ose))
                raise
            except ldap.LDAPError as le:
                logFunction("Cannot get user list from egroup: %s. Error: %s\n\t" % (egroup, str(le)))

    g_cache = cache
    g_expire_time = time.time() + 15*60


def map_user_to_groups(user):
    """ Get the sites where user, userdn and userfqan
        are present as local-users and return them

        The list of sites is returned as a set of strings
    """
    if time.time() > g_expire_time:
        cache_users()
    return g_cache.setdefault(user, set([]))

if __name__ == '__main__':
   print(map_user_to_groups("bbockelm"))

