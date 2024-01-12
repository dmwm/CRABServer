#!/bin/bash

source setupCRABClient.sh

python_cmd="python3"
CMSSW_Major=$(echo $CMSSW_VERSION | cut -d_ -f2)  # e.g. from CMSSW_13_1_x this is "13"
[ $CMSSW_Major -lt 12 ] && python_cmd="python2"

exec $python_cmd ${WORK_DIR}/CRABServer/test/container/testinScripts/statusTracking.py
