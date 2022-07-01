#!/usr/bin/env python3
# coding: utf-8
from __future__ import print_function
from __future__ import division

import os
from  datetime import datetime
import argparse

from dbs.apis.dbsClient import DbsApi


def readAndParse(csvFile, apiMig):
    """
    read and parse CSV file of terminally failed migrations removing duplicates
      and migrations which may have been removed from DBS, or were restarted and
      are not in terminally failed anymore
    args : file: string, full path to file name
    args : apiMig: a DbsApi object pointing to the DBSMigrate service
    returns: a list of dictionaries with keys
     migrId, status, created, block
    """

    failedMigrations = []
    ids = set()
    with open(csvFile, 'r', encoding='utf8') as fp:
        lines = fp.readlines()

    for line in lines:
        items = line.strip().split(',')
        ids.add(int(items[0]))
    uniqueIDs = list(ids)
    print(f"Found {len(uniqueIDs)} unique migration IDs logged as terminally failed")
    for migId in uniqueIDs:
        print(f" {migId}")
    print("Check current status")
    for migId in uniqueIDs:
        status = apiMig.statusMigration(migration_rqst_id=migId)
        if not status:
            print(f"{migId} has been removed")
            continue
        state = status[0].get("migration_status")
        # values for state:
        # 0-request created; 1-in process; 2-succeeded;
        # 3-failed, but has three chances to try; 9-Permanently failed
        if not state == 9:
            print(f"{migId} is in state {state}, not 9")
            continue
        tFromEpoch = status[0].get("creation_date")
        created = datetime.fromtimestamp(tFromEpoch).strftime('%Y-%m-%d %H:%M:%S')
        block = status[0].get("migration_input") # CRAB migrations are always one block at a time
        migDict = {'id':migId, 'status':state, 'created':created, 'block':block}
        failedMigrations.append(migDict)

    return failedMigrations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='log file of terminally failed migrations in CSV format',
                        default='/data/srv/Publisher/logs/migrations/TerminallyFailedLog.txt')
    args = parser.parse_args()
    logFile = os.path.abspath(args.file)

    # if X509 vars are not defined, use default Publisher location
    userProxy = os.getenv('X509_USER_PROXY')
    if userProxy:
        os.environ['X509_USER_CERT'] = userProxy
        os.environ['X509_USER_KEY'] = userProxy
    if not os.getenv('X509_USER_CERT'):
        os.environ['X509_USER_CERT'] = '/data/certs/servicecert.pem'
    if not os.getenv('X509_USER_KEY'):
        os.environ['X509_USER_KEY'] = '/data/certs/servicekey.pem'

    migUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/phys03/DBSMigrate'
    apiMig = DbsApi(url=migUrl, debug=True)

    failedMigrations = readAndParse(logFile, apiMig)

    print(f"Found {len(failedMigrations)} terminally failed migrations")
    if failedMigrations:
        print("   ID\t\tcreated\t\t\tblock")
        for migDict in failedMigrations:
            print(f"{migDict['id']}\t{migDict['created']}\t{migDict['block']}")

if __name__ == '__main__':
    main()
