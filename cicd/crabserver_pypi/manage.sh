#!/bin/bash
# manage.sh for crab rest

set -euo pipefail
if [[ -n ${TRACE+x} ]]; then
    set -x
    export TRACE
fi

# sanity check
if [[ -z ${COMMAND+x} || -z ${MODE+x} || -z ${DEBUG+x} || -z ${SERVICE+x} ]]; then
    >&2 echo "Error: Not all envvars are set!"
    exit 1
fi

script_env() {
    # app path
    PYTHONPATH=${PYTHONPATH:-/data/srv/current/lib/python/site-packages}
    # secrets
    PYTHONPATH=/data/srv/current/auth/crabserver:${PYTHONPATH}
    # export PYTHONPATH
    export PYTHONPATH

    # cert and proxy
    # Wa: I am not 100% sure between X509_USER_PROXY (mount to /etc/proxy/proxy)
    # and X509_USER_CERT (mount to /data/srv/current/auth/crabserver/dmwm-service-key.pem),
    # which file REST should use. AFAIK, for current (v3.240809) prod, we use
    # X509_USER_PROXY which export via https://github.com/dmwm/CRABServer/blob/f5687adbf5fddeb21526c33b623b2f5025e17945/cicd/crabserver_pypi/entrypoint.sh#L28
    # This is confirmed by put a wrong X509_USER_CERT path and unset
    # X509_USER_PROXY which make REST unable to start.
    # If REST really use X509_USER_PROXY, I need to confirmed that somehow
    # WMCore keep reread the file when it use.
    #
    # X509_USER_PROXY already set in entrypoint.sh, the script that run before
    # execute this file
    export X509_USER_PROXY=${X509_USER_PROXY:-/etc/proxy/proxy}
    export X509_USER_CERT=${X509_USER_CERT:-/data/srv/current/auth/crabserver/dmwm-service-cert.pem}
    export X509_USER_KEY=${X509_USER_KEY:-/data/srv/current/auth/crabserver/dmwm-service-key.pem}

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
