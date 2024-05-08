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

# some variable use in start_srv
CONFIG="${SCRIPT_DIR}"/current/PublisherConfig.py

helpFunction() {
    grep "^##H" "${0}" | sed -r "s/##H(| )//g"
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
        APP_DIR="${APP_DIR:-/data/repos/CRABServer/src/python}"
        python3 "${APP_DIR}"/Publisher/RunPublisher.py --config "${CONFIG}" --service "${SERVICE}" --debug --testMode
    else
        APP_DIR=/data/srv/current/lib/python/site-packages
        python3 "${APP_DIR}"/Publisher/RunPublisher.py --config "${CONFIG}" --service "${SERVICE}" &
    fi
}

stop_srv() {
    # This part is copy directly from https://github.com/dmwm/CRABServer/blob/3af9d658271a101db02194f48c5cecaf5fab7725/src/script/Deployment/Publisher/stop.sh

  # find my bearings
  myDir="${SCRIPT_DIR}"
  myLog="${myDir}"/logs/log.txt

  PublisherBusy(){
  # a function to tell if PublisherMaster is busy or waiting
  #   return 0 = success = Publisher is Busy
  #   return 1 = failure = Publisher can be killed w/o fear
    lastLine=$(tail -1 "${myLog}")
    echo "${lastLine}" | grep -q 'Next cycle will start at'
    cycleDone=$?
    if [ $cycleDone = 1 ] ; then
      # inside working cycle
      return 0
    else
      # in waiting mode, checks when it is next start
      start=$(echo "${lastLine}" | awk '{print $NF}')
      startTime=$(date -d ${start} +%s)  # in seconds from Epoch
      now=$(date +%s) # in seconds from Epoch
      delta=$((${startTime}-${now}))
      if [[ $delta -gt 60 ]]; then
        # no race with starting of next cycle, safe to kill
        return 1
      else
        # next cycle about to start, wait until is done
        return 0
      fi
    fi
  }

  nIter=1
  while PublisherBusy
  do
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
  pkill -f RunPublisher || true
}

# Main routine, perform action requested on command line.
case ${1:-help} in
  # Separate start/restart because stop_srv does not work when next cycle is
  # less than now and its block the container to start.
  start )
    # check if Publisher process still running.
    rc=0;
    # shellcheck disable=SC2207
    pids=($(pgrep -f RunPublisher 2> /dev/null)) || rc=$?
    if [[ $rc -eq 0 ]]; then
       >&2 echo "Error: Publisher process still running (pid: ${pids[@]})"
       exit 1
    fi
    # start
    start_srv
    ;;
  restart )
    stop_srv
    start_srv
    ;;

  stop )
    stop_srv
    ;;

  help )
    helpFunction
    exit 1
    ;;

  * )
    echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
    exit 1
    ;;
esac
