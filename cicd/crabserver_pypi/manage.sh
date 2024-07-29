#! /bin/bash

set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi

source source.sh

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
        echo "env blah 3"
        ;;
    * )
        echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
        exit 1
        ;;
esac
