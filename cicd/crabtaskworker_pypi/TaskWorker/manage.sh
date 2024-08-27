#! /bin/bash

# Start the TaskWorker service.

set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# sanity check
if [[ -z ${COMMAND+x} || -z ${MODE+x} || -z ${DEBUG+x} || -z ${SERVICE+x} ]]; then
    >&2 echo "All envvars are not set!."
    exit 1
fi

_getMasterWorkerPid() {
    pid=$(pgrep -f 'crab-taskworker' | grep -v grep | head -1 ) || true
    echo "${pid}"
}

start_srv() {
    script_env
    echo "Starting TaskWorker..."
    markmodify_path=/data/srv/current/data_files_modified
    if [[ ${MODE} = "fromGH" ]]; then
        touch ${markmodify_path}
        ./updateDatafiles.sh
    elif [[ -f ${markmodify_path} ]]; then
        echo "Error: ${markmodify_path} exists."
    fi

    if [[ -n ${DEBUG} ]]; then
        crab-taskworker --config "${CONFIG}" --logDebug --pdb
    else
        nohup crab-taskworker --config "${CONFIG}" --logDebug &> logs/nohup.out
    fi
    echo "Started TaskWorker with MasterWorker pid $(_getMasterWorkerPid)"
}

script_env() {
    # config
    CONFIG="${SCRIPT_DIR}"/current/TaskWorkerConfig.py
    # path where we install crab code
    APP_PATH="${APP_PATH:-/data/srv/current/lib/python/site-packages/}"

    # $CRABTASKWORKER_ROOT is a mandatory for getting data directory in `DagmanCreator.getLocation()`
    export CRABTASKWORKER_ROOT="${APP_PATH}"
    # PYTHONPATH
    if [[ $MODE = 'fromGH' ]]; then
        PYTHONPATH=/data/repos/CRABServer/src/python:/data/repos/WMCore/src/python:${PYTHONPATH:-}
    else
        PYTHONPATH="${APP_PATH}":"${PYTHONPATH:-}"
    fi
    export PYTHONPATH
}

stop_srv() {
    # TW is given checkTimes*timeout seconds to stop, if it is still running after
    # this period, TW and all its slaves are killed by sending SIGKILL signal.
    # Note that stop need to be idempotent because start always
    echo 'Stopping TaskWorker...'
    checkTimes=12
    timeout=15 #that will give 12*15=180 seconds (3min) for the TW to finish work

    TaskMasterPid=$(_getMasterWorkerPid)
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

status_srv() {
    >&2 echo "Error: Not implemented."
    exit 1
}

env_eval() {
    script_env
    echo "export CRABTASKWORKER_ROOT=${CRABTASKWORKER_ROOT}"
    echo "export PYTHONPATH=${PYTHONPATH}"
}

# Main routine, perform action requested on command line.
case ${COMMAND:-help} in
    start )
        stop_srv
        start_srv
        ;;

    stop )
        stop_srv
        ;;

    status )
        status_srv
        ;;

    env )
        env_eval
        ;;
    * )
        echo "Error: Unknown command: $COMMAND"
        exit 1
        ;;
esac
