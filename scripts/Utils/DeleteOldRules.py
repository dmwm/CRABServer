"""
 delete all rules created N months ago
"""

import argparse

from datetime import datetime, timedelta

from rucio.client import Client

parser = argparse.ArgumentParser()
parser.add_argument('--days', help='rules older than this will be deleted', required=True)
parser.add_argument('--dry', help='dry run. print what it will do but does nothing', action='store_true')
args = parser.parse_args()
nDays = int(args.days)
retentionDays = timedelta(days=nDays)
dryRun = args.dry

rucio=Client()
me=rucio.whoami()
account=me['account']
print(f"using Rucio account: {account}")

# get rules for this account
ruleGen = rucio.list_replication_rules({'account': account})
rules = list(ruleGen)
print(f"{len(rules)} rules exist for account: {account}")

# find and delete rules older than retentionDays
now = datetime.now()

for rule in rules:
    created=rule['created_at']
    isOld = (now - created) > retentionDays
    if isOld:
        if dryRun:
            print(f"will delete rule {rule['id']} created on {created}")
        else:
            pass
            res = rucio.delete_replication_rule(rule_id=rule['id'], purge_replicas=True)
            if res:
                print(f"deleted rule {rule['id']} created on {created}")
            else:
                print(f"ERROR deletion of rule {rule['ruleId']} failed")
  
