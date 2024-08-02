#! /bin/bash

# Start the TaskWorker service.


set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

helpFunction() {
    echo " Usage: manage.sh ACTION [OPTION]"
    echo ""
    echo " Available actions:"
    echo "   help        show this help"
    echo "   version     get current version of the service"
    echo "   restart     (re)start the service"
    echo "   start       (re)start the service"
    echo "   stop        stop the service"
    echo "   status      show pid"
    echo "   env         eval"
    echo ""
}

script_env() {
    ## some variable use in start_srv
    CONFIG="${SCRIPT_DIR}"/current/TaskWorkerConfig.py
    # path where we install crab code
    APP_PATH="${APP_PATH:-/data/srv/current/lib/python/site-packages/}"

    # CRABTASKWORKER_ROOT is a mandatory variable for getting data directory in `DagmanCreator.getLocation()`
    # Hardcoded the path and use new_updateTMRuntime.sh to build it from source and copy to this path.
    export CRABTASKWORKER_ROOT="${APP_PATH}"

    if [[ $MODE = 'fromGH' ]]; then
       PYTHONPATH=/data/repos/CRABServer/src/python:/data/repos/WMCore/src/python:${PYTHONPATH:-}
    else
       PYTHONPATH=/data/srv/current/lib/python/site-packages:${PYTHONPATH:-}
    fi
    export PYTHONPATH
}

_getMasterWorkerPid() {
    pid=$(pgrep -f 'crab-taskworker' | grep -v grep | head -1 ) || true
    echo "${pid}"
}

start_srv() {
    script_env
    echo "Starting TaskWorker..."
    markmodify_path=/data/srv/current/data_files_modified
    if [[ $MODE = "fromGH" ]]; then
        touch ${markmodify_path}
        ./updateDatafiles.sh
    elif [[ -f ${markmodify_path} ]]; then
        echo "Error: ${markmodify_path} exists."
    fi

    if [[ $DEBUG ]]; then
        crab-taskworker --config "${CONFIG}" --logDebug --pdb
    else
        crab-taskworker --config "${CONFIG}" --logDebug &
    fi
    echo "Started TaskWorker with MasterWorker pid $(_getMasterWorkerPid)"
}

stop_srv() {
    # This part is copy from https://github.com/dmwm/CRABServer/blob/3af9d658271a101db02194f48c5cecaf5fab7725/src/script/Deployment/TaskWorker/stop.sh
    # TW is given checkTimes*timeout seconds to stop, if it is still running after
    # this period, TW and all its slaves are killed by sending SIGKILL signal.
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

env_eval() {
    script_env
    echo "export CRABTASKWORKER_ROOT=${CRABTASKWORKER_ROOT}"
    echo "export PYTHONPATH=${PYTHONPATH}"
}

# parsing args
ACTION=""
MODE=""
DEBUG="f"

# first args always ACTION
ACTION=${!OPTIND}
((OPTIND++))
if [[ $ACTION =~ /(start/env)/ ]]; then
    while getopts ":hgcd" opt; do
        case "${opt}" in
            g )
                if [[ -n "$MODE" ]]; then
                    echo "Error: only one mode support"
                    helpFunction
                    exit 1
                fi
                MODE="fromGH"
                ;;
            c )
                if [[ -n "$MODE" ]]; then
                    echo "Error: only one mode support"
                    helpFunction
                    exit 1
                fi
                MODE="current"
                ;;
            d )
                DEBUG="t"
                ;;
            * )
                echo "$0: unknown action '$1', please try '$0 help' or documentation."
                exit 1
                ;;
        esac
    done
    if [[ -z $MODE ]]; then
        echo "Error: starting mode not are not provided (add -c or -g option)." && helpFunction;
        exit 1
    fi
else
    echo "Error: action ${ACTION} not support."
    helpFunction && exit 1
fi
# Main routine, perform action requested on command line.
case ${ACTION:-help} in
    start | restart )
        stop_srv
        start_srv
        ;;

    stop )
        stop_srv
        ;;

    help )
        helpFunction
        exit 0
        ;;

    env )
        env_eval
        ;;
    * )
        echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
        exit 1
        ;;
esac
