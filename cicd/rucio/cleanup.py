#!/usr/bin/env python

import datetime
import pprint

from rucio.client import Client
from rucio.rse.rsemanager import find_matching_scheme

def deleteRules(client=None, filters=None):
    allRules = client.list_replication_rules(filters=filters)
    formattedRules = [(x['created_at'], x['id'], x['name']) for x in allRules]

    #pprint.pprint(formattedRules)

    matchRules = []
    for x in formattedRules:
        if x[0] > (datetime.datetime.now() - datetime.timedelta(days=4)):
            continue
        if not ('ruciotransfers' in x[2] or 'FakePublish' in x[2]):
            continue
        matchRules.append(x)

    #pprint.pprint(matchRules)

    print(f'Found {len(formattedRules)}, match {len(matchRules)}')

    for x in matchRules:
        try:
            print(f'deleting rules {x[1]}')
            client.delete_replication_rule(x[1], purge_replicas=True)
        except Exception as e:
            print(f'Error: {e}')
            print('Skipping...')


rucio=Client()
rucio.whoami()
deleteRules(client=rucio, filters={'account': 'cmsbot', 'scope': 'user.cmsbot'})
rucioGroup=Client(account='crab_test_group')
rucioGroup.whoami()
deleteRules(client=rucioGroup, filters={'account': 'crab_test_group', 'scope': 'group.crab_test'})
