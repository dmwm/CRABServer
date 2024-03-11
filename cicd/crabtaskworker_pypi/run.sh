#! /bin/bash
# Main process of container.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${SCRIPT_DIR}

./start.sh -c

while true; do
    sleep 3600
done
