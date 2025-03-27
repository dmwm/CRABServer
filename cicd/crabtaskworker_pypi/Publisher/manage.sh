#!/bin/bash

# Start the Publisher service.

set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi

# sanity check
if [[ -z ${COMMAND+x} || -z ${MODE+x} || -z ${DEBUG+x} || -z ${SERVICE+x} ]]; then
    >&2 echo "Error: Not all envvars are set!"
    exit 1
fi

# Check if SERVICE is specified
if [[ ! ${SERVICE} =~ ^(Publisher_schedd|Publisher_rucio)$ ]]; then
    >&2 echo "Error: Unknown SERVICE: ${SERVICE}."
    exit 1
fi

_getPublisherPid() {
    pid=$(pgrep -f 'crab-publisher' | grep -v grep | head -1 ) || true
    echo "${pid}"
}

_isPublisherBusy(){
    # find my bearings
    myDir="${SCRIPT_DIR}"
    myLog="${myDir}"/logs/log.txt
    # a function to tell if PublisherMaster is busy or waiting
    #   return 0 = success = Publisher is Busy
    #   return 1 = failure = Publisher can be killed w/o fear
    lastLine=$(tail -1 "${myLog}")
    echo "${lastLine}" | grep -q 'Next cycle will start at'
    cycleDone=$?
    if [[ $cycleDone = 1 ]] ; then
        # inside working cycle
        return 0
    else
        # in waiting mode, checks when it is next start
        start=$(echo "${lastLine}" | awk '{print $NF}')
        startTime=$(date -d ${start} +%s)  # in seconds from Epoch
        now=$(date +%s) # in seconds from Epoch
        delta=$((${startTime}-${now}))
        if [[ $delta -gt -300 && $delta -lt 60 ]]; then
            # next cycle about to start, wait until is done
            return 0
        else
            # no race with starting of next cycle, safe to kill
            return 1
        fi
    fi
}

script_env() {
    # config
    CONFIG="${SCRIPT_DIR}"/current/PublisherConfig.py
    # path where we install crab code
    APP_PATH="${APP_PATH:-/data/srv/current/lib/python/site-packages/}"
    # PYTHONPATH
    if [[ $MODE = 'fromGH' ]]; then
        PYTHONPATH=/data/repos/CRABServer/src/python:/data/repos/WMCore/src/python:${PYTHONPATH:-}
    else
        PYTHONPATH="${APP_PATH}":${PYTHONPATH:-}
    fi
    export PYTHONPATH
}

start_srv() {
    script_env
    if [[ -n ${DEBUG} ]]; then
        crab-publisher --config "${CONFIG}" --service "${SERVICE}" --logDebug --pdb
    else
        nohup crab-publisher --config "${CONFIG}" --service "${SERVICE}" --logDebug &> logs/nohup.out &
    fi
    echo "Started Publisher with Publisher pid $(_getPublisherPid)"
}

stop_srv() {
  nIter=1
  maxIter=60 # 600 secs => 10 mins
  # check if publisher is still processing by look at the pb logs
  while _isPublisherBusy;  do
      # exit loop if publisher process is gone
      if [[ -z $(_getPublisherPid) ]]; then
         break;
      elif [[ $nIter -gt $maxIter ]]; then
          echo "Error: execeed maximum waiting time (10 mins)"
          echo "crab-publisher is still running ${stillrunpid}"
          exit 1
      fi
      [[ $nIter = 1 ]] && echo "Waiting for MasterPublisher to complete cycle: ."
      nIter=$((nIter+1))
      sleep 10
      echo -n "."
      if (( nIter%6  == 0 )); then
        minutes=$((nIter/6))
        echo -n "${minutes}m"
      fi
  done
  echo ""
  echo "Publisher is in waiting now. Killing RunPublisher"
  # ignore retcode in case it got kill by other process or crash
  pkill -f crab-publisher || true
  # ensure there is no crab-publisher process
  stillrunpid=$(_getPublisherPid)
  if [[ -n ${stillrunpid} ]]; then
      echo "Error: crab-publisher is still running ${stillrunpid}"
      exit 1
  fi

}

status_srv() {
    pid=$(_getPublisherPid)
    if [[ -z ${pid} ]]; then
        echo "Publisher's Master process is not running."
        exit 1
    fi
    echo "Publisher's Master process is running with PID ${pid}"
    pypath=$(cat /proc/"${pid}"/environ | tr '\0' '\n' | grep PYTHONPATH | cut -d= -f2-)
    echo "PYTHONPATH=$pypath"
    export PYTHONPATH="$pypath"
    python -c 'from TaskWorker import __version__; print(f"Runnning version {__version__}")'
    exit 0
}


env_eval() {
    script_env
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
        echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
        exit 1
        ;;
esac
