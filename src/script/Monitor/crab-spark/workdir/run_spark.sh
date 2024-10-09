#!/bin/bash

set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd $SCRIPT_DIR

# source the environment for spark submit
set +euo pipefail
source ./bootstrap.sh
set -euo pipefail

$@

popd
