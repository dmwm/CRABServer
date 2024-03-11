#! /bin/bash

# Same style as crabserver_pypi/manage.sh script, but for crabtaskworker.
# This script needs following environment variables:
#   - DEBUG:   if `true`, setup debug mode environment.
#   - PYTHONPATH: inherit from ./start.sh

set -euo pipefail

##H Usage: manage.sh ACTION [ATTRIBUTE] [SECURITY-STRING]
##H
##H Available actions:
##H   help        show this help
##H   version     get current version of the service
##H   restart     (re)start the service
##H   start       (re)start the service
##H   stop        stop the service


## some variable use in start_srv
TASKWORKER_HOME=/data/srv/TaskManager
CONFIG=$TASKWORKER_HOME/cfg/TaskWorkerConfig.py

# CRABTASKWORKER_ROOT is a mandatory variable for getting data directory in `DagmanCreator.getLocation()`
# Hardcoded the path and use new_updateTMRuntime.sh to build it from source and copy to this path.
export CRABTASKWORKER_ROOT=/data/srv/current/lib/python/site-packages/

# app path. Inherit PYTHONPATH from ./start.sh
PYTHONPATH=${PYTHONPATH:-/data/srv/current/lib/python/site-packages}
export PYTHONPATH

usage()
{
    cat $0 | grep "^##H" | sed -e "s,##H,,g"
}

start_srv() {
    if [[ "$DEBUG" == true ]]; then
        APP_DIR=${APP_DIR:-/data/repos/CRABServer/src/python}
        python3 -m pdb ${APP_DIR}/TaskWorker/SequentialWorker.py ${CONFIG} --logDebug
    else
        APP_DIR=/data/srv/current/lib/python/site-packages
        nohup python3 ${APP_DIR}/TaskWorker/MasterWorker.py --config ${CONFIG} --logDebug &
    fi
}

stop_srv() {
    # This part is copy from https://github.com/dmwm/CRABServer/blob/3af9d658271a101db02194f48c5cecaf5fab7725/src/script/Deployment/TaskWorker/stop.sh
    # TW is given checkTimes*timeout seconds to stop, if it is still running after
    # this period, TW and all its slaves are killed by sending SIGKILL signal.
    echo 'Stopping TaskWorker...'
    checkTimes=12
    timeout=15 #that will give 12*15=180 seconds (3min) for the TW to finish work

    TaskMasterPid=$(ps exfww | grep MasterWorker | grep -v grep | head -1 | awk '{print $1}') || true
    if [[ -z $TaskMasterPid ]]; then
        echo "No master process running."
        return;
    fi
    kill $TaskMasterPid
    echo "SIGTERM sent to MasterWorker pid $TaskMasterPid"

    for (( i=0; i<$checkTimes; ++i)); do
      # get only alive slaves, i.e. exclude defunct processes
      aliveSlaves=$(ps --ppid $TaskMasterPid  -o pid,s | grep -v "Z" | awk '{print $1}' | tail -n +2 | tr '\n' ' ') || true

      if [ -n "$aliveSlaves" ]; then
        echo "slave(s) PID [ ($aliveSlaves) ] are still running, sleeping for $timeout seconds. ($((i+1)) of $checkTimes try)"
        sleep $timeout
      else
        echo "Slaves gracefully stopped after $((i * timeout)) seconds."
        sleep 1
        break
      fi
    done

    runningProcesses=$(pgrep -f TaskWorker | tr '\n' ' ') || true
    if [ -n "$runningProcesses" ]; then
      echo -e "After max allowed time ($((checkTimes * timeout)) seconds) following TW processes are still running: $runningProcesses \nSending SIGKILL to stop it."
      pkill -9 -f TaskWorker
      echo "Running TW processes: `pgrep -f TaskWorker | tr '\n' ' '`"
    else
      echo "TaskWorker master has shutdown successfully."
    fi
}

# Main routine, perform action requested on command line.
case ${1:-help} in
  start | restart )
    stop_srv
    start_srv
    ;;

  stop )
    stop_srv
    ;;

  help )
    usage
    ;;

  * )
    echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
    exit 1
    ;;
esac
