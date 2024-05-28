#!/bin/bash

##H Usage example: ./start.sh -c | -g [-d]
##H     -c start current crabserver instance
##H     -g start crabserver instance from GitHub repo
##H     -d start crabserver in debug mode. Option can be combined with -c or -g

set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/env.sh "$@"

"${SCRIPT_DIR}/manage.sh" start
