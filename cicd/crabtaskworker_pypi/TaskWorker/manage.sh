#! /bin/bash

# Start the TaskWorker service.

##H Usage: manage.sh start -c/-g -d
##H
##H Available actions:
##H   help        show this help
##H   version     get current version of the service
##H   restart     (re)start the service
##H   start       (re)start the service
##H   stop        stop the service
##H   status      show pid
##H   env         eval
##H
##H This script needs following environment variables for start action:
##H   - DEBUG:      if `true`, setup debug mode environment.
##H   - PYTHONPATH: inherit from ./start.sh

set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

MODE="current"
DEBUG="f"
ACTION=""

while [[ $OPTIND -le "$#" ]]; do
    if getopts ":hgcd" opt; then
        if [[ ${ACTION} != "start" ]]; then
            helpFunction; exit 1
        fi
        case "${opt}" in
            h )
                helpFunction; exit 0
                ;;
            g )
                if [[ -n "$MODE" ]]; then
                   helpFunction
                   exit 1
                fi
                MODE="fromGH"
                ;;
            c )
                if [[ -n "$MODE" ]]; then
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
    elif [[ -n $ACTION ]]; then
         helpFunction; exit 1
    else
        ACTION="${!OPTIND}"
        ((OPTIND++))
    fi
done

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
        echo "export CONFIG=$CONFIG"
        echo "export APP_PATH=$APP_PATH"
        echo "export PYTHONPATH=$PYTHONPATH"
        echo "export CRABTASKWORKER_ROOT=$CRABTASKWORKER_ROOT"
        ;;
    * )
        echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
        exit 1
        ;;
esac
