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
    WORKDIR=/data/srv/TaskManager
    declare -A links=(
        ["current/TaskWorkerConfig.py"]="${WORKDIR}/hostdisk/cfg/TaskWorkerConfig.py"
    )

elif [[ "${SERVICE}" == Publisher* ]]; then
    WORKDIR=/data/srv/Publisher
    declare -A links=(
        ["current/PublisherConfig.py"]="${WORKDIR}/hostdisk/cfg/PublisherConfig.py"
    )
else
    echo "Service unknown: ${SERVICE}"; exit 1;
fi

for name in "${!links[@]}";
do
  check_link "${name}" || ln -s "$(realpath "${links[$name]}")" "$name"
done

pushd "${WORKDIR}"

# execute
./manage.py start -c

while true; do
    sleep 3600
done
