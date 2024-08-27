#! /bin/bash
# manage.sh for crab rest

set -euo pipefail
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
    ## app path
    PYTHONPATH=${PYTHONPATH:-/data/srv/current/lib/python/site-packages}
    ## secrets
    PYTHONPATH=/data/srv/current/auth/crabserver:${PYTHONPATH}
    # finally export PYTHONPATH
    export PYTHONPATH

    ## service creds
    export X509_USER_CERT=${X509_USER_CERT:-/data/srv/current/auth/dmwm-service-cert.pem}
    export X509_USER_KEY=${X509_USER_KEY:-/data/srv/current/auth/dmwm-service-key.pem}

    if [[ "${DEBUG:-}" == true ]]; then
        # this will direct WMCore/REST/Main.py to run in the foreground rather than as a demon
        # allowing among other things to insert pdb calls in the crabserver code and debug interactively
        export DONT_DAEMONIZE_REST=True
        # this will start crabserver with only one thread (default is 25) to make it easier to run pdb
        export CRABSERVER_THREAD_POOL=1
    fi
    # non exported vars
    CFGFILE=/data/srv/current/config/crabserver/config.py
    STATEDIR=/data/srv/state/crabserver
}

# Good thing is REST/Main.py already handled signal and has start/stop/status
# flag and ready to use.
start_srv() {
    script_env
    wmc-httpd -r -d $STATEDIR -l "$STATEDIR/crabserver-fifo" $CFGFILE
}

stop_srv() {
    script_env
    wmc-httpd -k -d $STATEDIR $CFGFILE
}

status_srv() {
    script_env
    wmc-httpd -s -d $STATEDIR $CFGFILE
}

env_eval() {
    script_env
    echo "export PYTHONPATH=${PYTHONPATH}"
    echo "export X509_USER_CERT=${X509_USER_CERT}"
    echo "export X509_USER_KEY=${X509_USER_KEY}"
}

# Main routine, perform action requested on command line.
case ${COMMAND:-} in
    start | restart )
        # no need to stop then start.
        start_srv
        ;;

    status )
        status_srv
        ;;

    stop )
        stop_srv
        ;;

    env )
        env_eval
        ;;

    help )
        usage
        ;;

    * )
        echo "Error: unknown command '$COMMAND'"
        exit 1
        ;;
esac
