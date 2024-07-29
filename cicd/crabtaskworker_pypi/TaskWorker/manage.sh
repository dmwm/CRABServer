#! /bin/bash

# Start the TaskWorker service.


set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source $SCRIPT_DIR/source.sh

ACTION=""
MODE=""
DEBUG="f"

# first args always ACTION
ACTION=${!OPTIND}
((OPTIND++))
if [[ $ACTION = "start" ]]; then
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
