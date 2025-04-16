#!/usr/bin/python
"""
temporary utility to compare ost relevant info from new and old cache_status.json
"""

import sys
import json

old = 'task_process/status_cache.json'
new = 'task_process/status_cache_new.json'
with open(old) as fp:
    oldJson = json.load(fp)
with open(new) as fp:
    newJson = json.load(fp)

oldNodes = oldJson['nodes']
newNodes = newJson['nodes']

diff = False

for job in oldNodes.keys():
    if job == 'DagStatus':
        continue
    oldState=oldNodes[job]['State']
    newState=newNodes[job]['State']
    if oldState != newState:
        print(f'State diff for job {job}: old: {oldState} new: {newState}')
        diff = True

    oldHistory = set(oldNodes[job]['SiteHistory'])
    newHistory = set(newNodes[job]['SiteHistory'])
    if oldHistory == {'Unknown'} : continue
    if newHistory == {'Unknown'} : continue
    oldHistory = oldHistory - {'Unknown'}
    newHistory = newHistory - {'Unknown'}
    if oldHistory!= newHistory:
        print(f"Site History diff for {job}: old {oldHistory} new {newHistory}")
        diff = True

if diff:
   sys.exit(1)