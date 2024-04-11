"""
Cleanup rucio rules from cmsbot account created by task in testsuite.
"""
import datetime
import os

from rucio.client import Client

DRY_RUN = os.getenv("DRY_RUN", 'True').lower() in ('true', '1', 't')

def deleteRules(client=None, filters=None):
    """
    Delete Rucio rules that older than 30 day. Make sure to delete only rules
    that match name created by CRAB Rucio ASO.

    :param client: rucio client object
    :type client: rucio.client.client.Client
    :param filters: dict filter used by list_replication_rules()
    :type filters: dict

    """
    # get rules, apply filters
    allRules = client.list_replication_rules(filters=filters)
    formattedRules = [(x['created_at'], x['id'], x['name']) for x in allRules]

    #pprint.pprint(formattedRules)
    now = datetime.datetime.now()
    today = datetime.datetime(year=now.year, month=now.month, day=now.day)

    matchRules = []
    for x in formattedRules:
        # skip if rule is newer than 30 day
        if x[0] > (today - datetime.timedelta(days=30)):
            continue
        # failsafe to delete only task created by crab.
        if not ('ruciotransfers' in x[2] or 'FakePublish' in x[2]):
            continue
        matchRules.append(x)

    print(f'Found {len(formattedRules)} rules, match {len(matchRules)} rules')

    # deleting the rules
    for x in matchRules:
        try:
            print(f'Deleting rules {x[1]}')
            if DRY_RUN:
                raise Exception('Dry run.')
            client.delete_replication_rule(x[1], purge_replicas=True)
        except Exception as e:
            print(f'Error: {e}')
            print('Skipping...')


# Cleanup cmsbot account
rucio=Client()
rucio.whoami()
deleteRules(client=rucio, filters={'account': 'cmsbot', 'scope': 'user.cmsbot'})
# Cleanup crab_test_group account
rucioGroup=Client(account='crab_test_group')
rucioGroup.whoami()
deleteRules(client=rucioGroup, filters={'account': 'crab_test_group', 'scope': 'group.crab_test'})
