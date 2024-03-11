#!/bin/bash
# This scripts originate from dmwm-base/manage
# https://github.com/dmwm/CMSKubernetes/blob/2b0454f9205cb8f97fecb91bf6661b59e4b31424/docker/pypi/dmwm-base/manage
# However, scripts is heavily edited to suit new CRAB PyPI.
# 2 things that we may need to keep up with original scripts:
#   - Deployment Convention: WMCore/REST app (CRAB REST) read necessary files from.
#   - `wmc-httpd` line.
# This script needs following environment variables:
#   - DEBUG:   if `true`, setup debug mode environment .
#   - PYTHONPATH: inherit from ./start.sh


set -euo pipefail

##H Usage: manage.sh ACTION [ATTRIBUTE] [SECURITY-STRING]
##H
##H Available actions:
##H   help        show this help
##H   version     get current version of the service
##H   status      show current service's status
##H   restart     (re)start the service
##H   start       (re)start the service
##H   stop        stop the service

# common settings to prettify output
echo_e=-e
COLOR_OK="\\033[0;32m"
COLOR_WARN="\\033[0;31m"
COLOR_NORMAL="\\033[0;39m"

## hardcode service name
srv=crabserver

## service settings
#LOGDIR=/data/srv/logs/$srv # we write logs to fifo instead of logdir
AUTHDIR=/data/srv/current/auth/$srv
STATEDIR=/data/srv/state/$srv
CFGDIR=/data/srv/current/config/$srv
CFGFILE=$CFGDIR/config.py

# necessary env settings for all WM services
## app path. Inherit PYTHONPATH from parent process.
PYTHONPATH=${PYTHONPATH:-/data/srv/current/lib/python/site-packages}
## config
PYTHONPATH=/data/srv/current/config/$srv:${PYTHONPATH}
## secrets
PYTHONPATH=/data/srv/current/auth/$srv:${PYTHONPATH}
# finally export PYTHONPATH
export PYTHONPATH
## wmc-httpd path
# export PATH manually to WMCore/bin directory in case you have custom wmc-httpd
export PATH=/data/srv/current/bin:$PATH

## service creds
export X509_USER_CERT=${X509_USER_CERT:-${AUTHDIR}/dmwm-service-cert.pem}
export X509_USER_KEY=${X509_USER_KEY:-${AUTHDIR}/dmwm-service-key.pem}

## debug mode
if [[ "${DEBUG:-}" == true ]]; then
  # this will direct WMCore/REST/Main.py to run in the foreground rather than as a demon
  # allowing among other things to insert pdb calls in the crabserver code and debug interactively
  export DONT_DAEMONIZE_REST=True
  # this will start crabserver with only one thread (default is 25) to make it easier to run pdb
  export CRABSERVER_THREAD_POOL=1
fi

usage()
{
    cat $0 | grep "^##H" | sed -e "s,##H,,g"
}
start_srv()
{
    # Wa: originally from dmwm-base
    #wmc-httpd -r -d $STATEDIR -l "|rotatelogs $LOGDIR/$srv-%Y%m%d-`hostname -s`.log 86400" $CFGFILE
    wmc-httpd -r -d $STATEDIR -l "$STATEDIR/crabserver-fifo" $CFGFILE
}

stop_srv()
{
    local pid=$(ps auxwww | egrep "wmc-httpd" | grep -v grep | awk 'BEGIN{ORS=" "} {print $2}') || true
    if [[ -n "${pid}" ]]; then
        echo "Stop $srv service... ${pid}"
        kill -9 ${pid}
    else
        echo "No $srv service process running."
    fi
}

status_srv()
{
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
case ${1:-status} in
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
    echo "$0: unknown action '$1', please try '$0 help' or documentation." 1>&2
    exit 1
    ;;
esac
