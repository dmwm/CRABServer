#! /bin/bash

# Start the Publisher service.

##H Usage: manage.sh ACTION
##H
##H Available actions:
##H   help        show this help
##H   version     get current version of the service
##H   restart     (re)start the service
##H   start       (re)start the service
##H   stop        stop the service
##H
##H This script needs following environment variables for start action:
##H   - DEBUG:      if `true`, setup debug mode environment.
##H   - PYTHONPATH: inherit from ./start.sh
##H   - SERVICE:    inherit from container environment
##H                 (e.g., `-e SERVICE=Publisher_schedd` when do `docker run`)
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi

# some variable use in start_srv
CONFIG="${SCRIPT_DIR}"/current/PublisherConfig.py

helpFunction() {
    grep "^##H" "${0}" | sed -r "s/##H(| )//g"
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
    # Check require env
    # shellcheck disable=SC2269
    SERVICE="${SERVICE}"
    # shellcheck disable=SC2269
    DEBUG="${DEBUG}"
    export PYTHONPATH="${PYTHONPATH}"

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
    helpFunction
    exit 0
    ;;

  * )
    echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
    exit 1
    ;;
esac
