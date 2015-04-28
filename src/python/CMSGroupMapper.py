
import os
import re
import time

g_cache = {}
g_expire_time = 0

def cache_users():
    global g_expire_time
    global g_cache

    base_dir = '/cvmfs/cms.cern.ch/SITECONF'
    cache = {}
    user_re = re.compile(r'[-_A-Za-z0-9.]+')
    sites = None
    try:
        if os.path.isdir(base_dir):
            sites = os.listdir(base_dir)
    except:
        pass
    if not sites:
        g_expire_time = time.time() + 60
        return
    for entry in sites:
        full_path = os.path.join(base_dir, entry, 'GlideinConfig', 'local-users.txt')
        if (entry == 'local') or (not os.path.isfile(full_path)):
        #if not os.path.isfile(full_path):
            continue
        try:
            fd = open(full_path)
            for line in fd:
                line = line.strip()
                if user_re.match(line):
                    group_set = cache.setdefault(line, set())
                    group_set.add(entry)
        except:
            raise
            pass
    for key, val in cache.items():
        cache[key] = ",".join(val)

    g_cache = cache
    g_expire_time = time.time() + 15*60


def map_user_to_groups(user):
    if time.time() > g_expire_time:
        cache_users()
    return g_cache.setdefault(user, "")

if __name__ == '__main__':
   print map_user_to_groups("bbockelm")

