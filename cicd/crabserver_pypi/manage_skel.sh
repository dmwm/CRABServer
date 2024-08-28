#! /bin/bash

# This is skeleton file for each service that have different env vars
# and different ways of handle the main process. The arguments are passing
# to manage.sh via envvars.

set -euo pipefail

# debugging
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi

# sanity check
if [[ -z ${COMMAND+x} || -z ${MODE+x} || -z ${DEBUG+x} || -z ${SERVICE+x} ]]; then
    >&2 echo "All envvars are not set!."
    exit 1
fi

script_env() {
    >&2 echo "This is where we define env"
    # for example
    export PYTHONPATH=/path/to/pythondir
}

start_srv() {
    script_env
    >&2 echo "This is start_srv"
}

stop_srv() {
    # stop_srv need to be idempotent because start always do stop then start.
    >&2 echo "This is stop_srv"
}

status_srv() {
    >&2 echo "This is status_srv"
}

env_eval() {
    script_env
    >&2 echo "This is where manually echo env for env.sh"
    # for example
    echo "export \"PYTHONPATH=${PYTHONPATH}\""
    # will ouput
    # export PYTHONPATH=/path/to/pythondir
}


# Main routine, perform action requested on command line.
case ${COMMAND:-} in
    start )
        stop_srv
        start_srv
        ;;

    status )
        status_srv
        ;;

    stop )
        stop_srv
        ;;

    help )
        usage
        ;;

    * )
        echo "Error: unknown command '$COMMAND'"
        exit 1
        ;;
esac
