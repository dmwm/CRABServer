#! /bin/bash

# Start the Publisher service.

set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi

echo $COMMAND > /dev/null
echo $MODE > /dev/null
echo $DEBUG > /dev/null
echo $SERVICE > /dev/null

script_env() {
    # some variable use in start_srv
    CONFIG="${SCRIPT_DIR}"/current/PublisherConfig.py
    # just t
    export PYTHONPATH="${PYTHONPATH}"
    export DEBUG
    export SERVICE="${SERVICE}"
}

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


start_srv() {
    script_env
    # hardcode APP_DIR, but if debug mode, APP_DIR can be override
    if [[ "${DEBUG}" = 'true' ]]; then
        crab-publisher --config "${CONFIG}" --service "${SERVICE}" --logDebug --pdb
    else
        crab-publisher --config "${CONFIG}" --service "${SERVICE}" --logDebug &
    fi
    echo "Started Publisher with Publisher pid $(_getPublisherPid)"

}

stop_srv() {
    # This part is copy directly from https://github.com/dmwm/CRABServer/blob/3af9d658271a101db02194f48c5cecaf5fab7725/src/script/Deployment/Publisher/stop.sh

  nIter=1
  # check if publisher is still processing by look at the pb logs
  while _isPublisherBusy;  do
      # exit loop if publisher process is gone
      if [[ -z $(_getPublisherPid) ]]; then
         break;
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

env_eval() {
    script_env
    echo "export PYTHONPATH=${PYTHONPATH}"
    echo "export SERVICE=${SERVICE}"
}

# Main routine, perform action requested on command line.
case ${COMMAND:-help} in
    start | restart )
        stop_srv
        start_srv
        ;;

    stop )
        stop_srv
        ;;

    help )
        helpFunction
        ;;

    env )
        env_eval
        ;;
    * )
        echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
        exit 1
        ;;
esac
