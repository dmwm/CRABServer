#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
if ! voms-proxy-info; then
    voms-proxy-init --rfc --voms cms -valid 192:00
fi

export RUCIO_ACCOUNT=${RUCIO_ACCOUNT:-crabint1}
source /cvmfs/cms.cern.ch/rucio/setup-py3.sh
rucio whoami

export DRY_RUN=false
python3 "${SCRIPT_DIR}"/cleanup.py
