#! /bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
voms-proxy-init --rfc --voms cms -valid 192:00
voms-proxy-info

source /cvmfs/cms.cern.ch/rucio/setup-py3.sh
rucio whoami

export DRY_RUN=${DRY_RUN:-false}
python3 "${SCRIPT_DIR}"/cleanup.py
