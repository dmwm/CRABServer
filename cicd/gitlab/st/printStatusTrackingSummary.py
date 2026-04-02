#! /usr/bin/env python3
""" print result.json from statusTracking.py as a table """

import json

with open('result.json', 'r', encoding='utf-8') as f:
    summary = json.load(f)
print("============ TEST SUMMARY ========")
legend = "Jobs: OK=Success, R=Running, X=Trasferring, F=Failed\n"
legend += "Publications: F=Failed, P=Pending (ToBeDone), D=in DBS"
print(f"Legend:\n{legend}")

FORMAT = "%30s%15s%20s%15s"
print(FORMAT % ("Task", "result", "Jobs(OK/R/X/F)", "Pub(F/P/D)"))
for task in summary:
    # strip leading timestamp:crabint1_crab_ and trailing _timestamp from task name
    name = '_'.join(task['TN'].split('crab_')[1].split('_')[:-2])
    result = task['testResult']
    jobs = task['jobsPerStatus']
    jOK = jobs.get('finished',0)
    jR = jobs.get('running', 0)
    jT = jobs.get('transferring', 0)
    jF = jobs.get('failed', 0)
    jobString = f"{jOK}/{jR}/{jT}/{jF}"
    pubString = task['publication (fail/tdb/DBS)']
    print(FORMAT % (name, result, jobString, pubString))
