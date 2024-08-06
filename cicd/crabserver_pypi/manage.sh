#! /bin/bash

set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi

script_env() {
    # common settings to prettify output
    echo_e=-e
    COLOR_OK="\\033[0;32m"
    COLOR_WARN="\\033[0;31m"
    COLOR_NORMAL="\\033[0;39m"

    # necessary env settings for all WM services
    ## app path. Inherit PYTHONPATH from parent process.
    PYTHONPATH=${PYTHONPATH:-/data/srv/current/lib/python/site-packages}
    ## secrets
    PYTHONPATH=/data/srv/current/auth/crabserver:${PYTHONPATH}
    # finally export PYTHONPATH
    export PYTHONPATH

    ## service creds
    export X509_USER_CERT=${X509_USER_CERT:-${AUTHDIR}/dmwm-service-cert.pem}
    export X509_USER_KEY=${X509_USER_KEY:-${AUTHDIR}/dmwm-service-key.pem}

    if [[ "${DEBUG:-}" == true ]]; then
        # this will direct WMCore/REST/Main.py to run in the foreground rather than as a demon
        # allowing among other things to insert pdb calls in the crabserver code and debug interactively
        export DONT_DAEMONIZE_REST=True
        # this will start crabserver with only one thread (default is 25) to make it easier to run pdb
        export CRABSERVER_THREAD_POOL=1
    fi
    CFGFILE=/data/srv/current/config/config.py

}

start_srv() {
    script_env
    wmc-httpd -r -d $STATEDIR -l "$STATEDIR/crabserver-fifo" $CFGFILE
}

stop_srv() {
    local pid=$(ps auxwww | egrep "wmc-httpd" | grep -v grep | awk 'BEGIN{ORS=" "} {print $2}') || true
    if [[ -n "${pid}" ]]; then
        echo "Stop crabserver service... ${pid}"
        kill -9 ${pid}
    else
        echo "No $srv service process running."
    fi
}

status_srv() {
    local pid=`ps auxwww | egrep "wmc-httpd" | grep -v grep | awk 'BEGIN{ORS=" "} {print $2}'`
    if  [ -z "${pid}" ]; then
        echo "$srv service is not running"
        return
    fi
    if [ ! -z "${pid}" ]; then
        echo $echo_e "$srv service is ${COLOR_OK}RUNNING${COLOR_NORMAL}, PID=${pid}"
        ps -f -wwww -p ${pid}
    else
        echo $echo_e "$srv service is ${COLOR_WARN}NOT RUNNING${COLOR_NORMAL}"
    fi
}

# Main routine, perform action requested on command line.
case ${COMMAND:-} in
  start | restart )
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
