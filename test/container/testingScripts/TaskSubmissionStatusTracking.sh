#!/bin/bash

source setupCRABClient.sh

python_cmd="python3"
[ $CMSSW_Major -lt 12 ] && python_cmd="python2"

exec $python_cmd statusTracking.py
