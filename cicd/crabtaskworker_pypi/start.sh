#!/bin/bash

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
markmodify_path=/data/srv/current/data_files_modified
case $MODE in
    current)
        # current mode: run current instance
        # Refuse to start when data_files directry is modified.
        if [[ -f ${markmodify_path} ]]; then
            echo "data_files directory has been modifed."
            echo "Refuse to start with -c option."
            exit 1
        fi
        PYTHONPATH=/data/srv/current/lib/python/site-packages:${PYTHONPATH:-}
        ;;
    fromGH)
        # private mode: run private instance from GH
        PYTHONPATH=/data/repos/CRABServer/src/python:/data/repos/WMCore/src/python:${PYTHONPATH:-}
        # update runtime (create TaskManagerRun.tar.gz from source)
        touch ${markmodify_path}
        ./updateDatafiles.sh
        ;;
    *) echo "Unimplemented mode: $MODE\n"; helpFunction ;;
esac

# export APP_PATH and DEBUG to ./manage.sh
export PYTHONPATH
export DEBUG
"${SCRIPT_DIR}/manage.sh" start
