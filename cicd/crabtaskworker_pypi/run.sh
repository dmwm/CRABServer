#! /bin/bash

# The entrypoint script to execute actual script base on $SERVICE.
#
# This script require the following environment variables:
#   SERVICE   : the name of the service to be run: TaskWorker, Publisher_schedd, Publisher_rucio etc.

set -euo pipefail

# re-export to check require env
export SERVICE=${SERVICE}

# ensure container has needed mounts
check_link() {
    # function checks if symbolic links required to start service exists and if they are not broken
    if [[ -L "${1}" ]] ; then
        if [[ -e "${1}" ]] ; then
            return 0
        else
            unlink "${1}"
            return 1
        fi
    else
        return 1
    fi
}

# directories/files that should be created before starting the container
if [[ $SERVICE == TaskWorker ]]; then
    declare -A links=(
        ["current/TaskWorkerConfig.py"]="/data/hostdisk/${SERVICE}/cfg/TaskWorkerConfig.py"
    )
    WORKDIR=/data/srv/TaskManager
elif [[ "${SERVICE}" == Publisher* ]]; then
    declare -A links=(
        ["current/PublisherConfig.py"]="/data/hostdisk/${SERVICE}/cfg/PublisherConfig.py"
    )
    WORKDIR=/data/srv/Publisher
else
    echo "Service unknown: ${SERVICE}"; exit 1;
fi

for name in "${!links[@]}";
do
  check_link "${name}" || ln -s "$(realpath "${links[$name]}")" "$name"
done

pushd "${WORKDIR}"

# execute
./start.sh -c

while true; do
    sleep 3600
done
