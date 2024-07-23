#!/bin/bash
# NOTE: This file suppose to run inside cmssw sigularity (cc7,el8,el9)

set -x
set -euo pipefail

#Script can be used to setup CRABClient:
# 1. dev - CRABClient from Intergration Build (IB);
# 2. GH - CRABClient from CRABClient GH repository master branch.
# 3. prod - production CRABClient from cvmfs;
# Variables ${SCRAM_ARCH}, ${CMSSW_release}, ${CRABClient_version}
# comes from caller/CI variables

# ignore all bash fail from script from /cvmfs/cms-ib.cern.ch
set +euo pipefail
source /cvmfs/cms-ib.cern.ch/latest/cmsset_default.sh
set -euo pipefail

scramv1 project ${CMSSW_release}
cd ${CMSSW_release}/src
eval "$(scramv1 runtime -sh)"
scram build

if echo ${CMSSW_release} | grep -q CMSSW_7; then
  # when using CMSSW_7, need to force latest curl
  if echo $SCRAM_ARCH | grep -q slc6; then
    echo "using CMSSW_7 on SL6. Setup/cvmfs/cms.cern.ch/slc6_amd64_gcc900/external/curl/7.59.0"
    set +euo pipefail
    source /cvmfs/cms.cern.ch/slc6_amd64_gcc700/external/curl/7.59.0/etc/profile.d/init.sh
    set -euo pipefail
  fi
  if echo $SCRAM_ARCH | grep -q slc7; then
    echo "using CMSSW_7 on SL7. Setup /cvmfs/cms.cern.ch/slc7_amd64_gcc630/external/curl/7.59.0"
    set +euo pipefail
    source /cvmfs/cms.cern.ch/slc7_amd64_gcc630/external/curl/7.59.0/etc/profile.d/init.sh
    set -euo pipefail
  fi
fi

case $CRABClient_version in
  dev)
    set +euo pipefail
    source /cvmfs/cms-ib.cern.ch/latest/common/crab-setup.sh dev
    set -euo pipefail
    alias crab='crab-dev'
    ;;
  GH)
    # TODO: specific fork/commit of crabclient repo still not support
    MY_CRAB=${PWD}/CRABClient
    rm -rf CRABClient
    git clone https://github.com/dmwm/CRABClient ${MY_CRAB} -b master
    cp ${ROOT_DIR}/src/python/ServerUtilities.py ${MY_CRAB}/src/python/
    cp ${ROOT_DIR}/src/python/RESTInteractions.py ${MY_CRAB}/src/python/
    # install the fake WMCore dependency for CRABClient, taking inspiration from
    # https://github.com/cms-sw/cmsdist/blob/b38a4b3339f12706513917153a2ec6cdcb23741c/crab-build.file#L37
    mkdir -p ${MY_CRAB}/WMCore/src/python/WMCore
    touch ${MY_CRAB}/WMCore/src/python/__init__.py
    touch ${MY_CRAB}/WMCore/src/python/WMCore/__init__.py
    cp ${MY_CRAB}/src/python/CRABClient/WMCoreConfiguration.py ${MY_CRAB}/WMCore/src/python/WMCore/Configuration.py

    export PYTHONPATH=${MY_CRAB}/src/python:${PYTHONPATH:-}
    export PYTHONPATH=${MY_CRAB}/WMCore/src/python:$PYTHONPATH
    export PATH=${MY_CRAB}/bin:$PATH
    source ${MY_CRAB}/etc/crab-bash-completion.sh
    ;;
  prod)
    set +euo pipefail
	source /cvmfs/cms.cern.ch/common/crab-setup.sh prod
    set -euo pipefail
esac

#cd "${CURRENT_DIR}"
crab --version
