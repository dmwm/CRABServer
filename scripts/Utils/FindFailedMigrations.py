#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pprint
import time
from  datetime import datetime
import argparse

import CRABClient
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
# 
    failedMigrations=[]
    ids = set()
    with open(csvFile) as _list:
      lines = _list.readlines()

    for line in lines:
        items = line.strip().split(',')
        ids.add(int(items[0]))
    uniqueIDs = list(ids)
    print("Found %d unique migration IDs logged as terminally failed" % len(uniqueIDs))
    for id in uniqueIDs:
        print(" %d" % id)
    print("Check current status")
    for id in uniqueIDs:
        status = apiMig.statusMigration(migration_rqst_id=id)
        if not status:
            print(" %d has been removed" % id)
            continue
        state = status[0].get("migration_status")
        # values for state:
        # 0-request created; 1-in process; 2-succeeded;
        # 3-failed, but has three chances to try; 9-Permanently failed
        if not state==9:
            print("%id is in state %d, not 9" % (id, state))
            continue
        tFromEpoch = status[0].get("creation_date")
        created = datetime.fromtimestamp(tFromEpoch).strftime('%Y-%m-%d %H:%M:%S')
        block = status[0].get("migration_input") # CRAB migrations are always one block at a time
        migDict = {'id':id, 'status':state, 'created':created, 'block':block}
        failedMigrations.append(migDict)
        
    return failedMigrations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='log file of terminally failed migrations in CSV format',
                        default='TerminallyFailedLog.txt')
    args = parser.parse_args()
    logFile = os.path.abspath(args.file)
    
    migUrl='https://cmsweb.cern.ch/dbs/prod/phys03/DBSMigrate'
    apiMig = DbsApi(url=migUrl)
                   
    failedMigrations = readAndParse(logFile, apiMig)
    
    print("Found %d terminally failed migrations" % len(failedMigrations))
    for id in failedMigrations:
        print(" %d" % id) 
        
if __name__ == '__main__':
    main()

