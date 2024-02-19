#!/bin/bash

# TASKWORKER_HOME only use in this script
TASKWORKER_HOME=/data/srv/TaskManager
CONFIG=$TASKWORKER_HOME/cfg/TaskWorkerConfig.py
# CRABTASKWORKER_ROOT is a mandatory variable. Use for getting data directory in `DagmanCreator.getLocation()`
export CRABTASKWORKER_ROOT=/data/srv/current/lib/python/site-packages/
# export PYTHONPATH of app
export PYTHONPATH=$CRABTASKWORKER_ROOT:$PYTHONPATH
python3 $CRABTASKWORKER_ROOT/TaskWorker/MasterWorker.py --config ${CONFIG} --logDebug &
