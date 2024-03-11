#! /bin/bash

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

helpFunction() {
    echo -e "Usage example: ./start.sh -c | -g [-d]"
    echo -e "\t-c start current crabserver instance"
    echo -e "\t-g start crabserver instance from GitHub repo"
    echo -e "\t-d start crabserver in debug mode. Option can be combined with -c or -g"
    exit 1
}

DEBUG=''
MODE=''
while getopts ":dDcCgGhH" o; do
    case "${o}" in
        h|H) helpFunction ;;
        g|G) MODE="fromGH" ;;
        c|C) MODE="current" ;;
        d|D) DEBUG=true ;;
        * ) echo "Unimplemented option: -$OPTARG"; helpFunction ;;
    esac
done
shift $((OPTIND-1))

if ! [[ -v MODE ]]; then
  echo "Please set how you want to start crabserver (add -c or -g option)." && helpFunction
fi

case $MODE in
    current)
        # current mode: run current instance
        PYTHONPATH=/data/srv/current/lib/python/site-packages:${PYTHONPATH:-}
        ;;
    fromGH)
        # private mode: run private instance from GH
        PYTHONPATH=/data/repos/WMCore/src/python:/data/repos/CRABServer/src/python:${PYTHONPATH:-}
        ;;
    *) echo "Unimplemented mode: $MODE\n"; helpFunction ;;
esac

# passing DEBUG/APP_PATH to ./manage.sh scripts
export DEBUG
export PYTHONPATH
"${SCRIPT_DIR}/manage.sh" start
