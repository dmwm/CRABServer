#!/usr/bin/python3
"""
Standalone (Rucio and pandas dependent) script to check status
of pending CRAB's tape recalls
usage:  python3 CheckTapeRecall.py
example output:

"""
import os
import pandas as pd

def main():
    rucio = ensure_environment()
    account = 'crab_tape_recall'

    # get rules for this account
    ruleGen = rucio.list_replication_rules({'account': account})
    rules = list(ruleGen)
    print(f"{len(rules)} rules exist for account: {account}")

    # make a DataFrame
    df = pd.DataFrame(rules)
    print(df.groupby('state').size())  # count by state

    # use standard compact format (OK/Rep/Stucl) for lock counts
    df['locks'] = df.apply(lambda x:f"{x['locks_ok_cnt']}/{x['locks_replicating_cnt']}/{x['locks_stuck_cnt']}",axis=1)

    # extract CRAB user name from rule comment (ouch ! FRAGILE !)
    df['user'] = df.apply(lambda x: x['comments'].replace("Staged from tape for ",""), axis=1)

    # transform created_at to number of days
    today = pd.Timestamp.now()
    df['days'] = df.apply(lambda x: (today-x['created_at']).days, axis=1)

    # add and URL pointing to rule in Rucio UI
    urlBase = '<a href="https://cms-rucio-webui.cern.ch/rule?rule_id=%s">%s</a>'
    df['idUrl'] = df.apply(lambda x: (urlBase%(x['id'],x['id'])), axis=1)

    # select non-OK states
    stuck = df[df['state']=='STUCK'].sort_values(by=['days'],ascending=False)
    replicating = df[df['state']=='REPLICATING'].sort_values(by=['days'],ascending=False)

    # combine all pending rules in a single dataframe
    pending = pd.concat([stuck,replicating])

    # add tape locations
    print("finding tape source for all pending rules (takes some time...)")
    pending['tape'] = pending.apply(lambda x: findTapesForRule(rucio, x['id']), axis=1)
    print("Done!")

    # select interesting columns
    rulesToPrint = pending[['id', 'state', 'user','locks','days', 'tape']]
    print(rulesToPrint.to_string(index=False))

    # create an HTML table
    rulesToHtml = pending[['idUrl', 'state', 'user','locks','days', 'tape']].to_html(index=False, escape=False)
    with open('RecallRules.html', 'w') as fh:
        fh.write(rulesToHtml)


def findTapesForRule(rucioClient=None, ruleId=None):
    """
    Returns a list of tape RSE's where the files which are object of the rule are located
    Assumes that all files in the container described in the rule have the same origin
    so it will be enough to look up the first file
    """
    rule=rucioClient.get_replication_rule(ruleId)
    aFile=next(rucioClient.list_files(scope=rule['scope'], name=rule['name']))
    aDID={'scope': aFile['scope'], 'name': aFile['name']}
    aReplica = next(rucioClient.list_replicas([aDID]))
    tapes=[]
    for rse in aReplica['rses']:
        if 'Tape' in rse:
            tapes.append(rse.replace("_Tape", ""))
    return tapes


def ensure_environment():
    if os.getenv("CMSSW_BASE"):
        print("Must use a shell w/o CMSSW environent")
        exit()
    try:
        from rucio.client import Client
    except ModuleNotFoundError:
        print("Setup Rucio first via:\n source /cvmfs/cms.cern.ch/rucio/setup-py3.sh; export RUCIO_ACCOUNT=`whoami`")
        exit()
    # make sure Rucio client is initialized
    rucio = Client()
    return(rucio)



if __name__ == '__main__':
    main()
