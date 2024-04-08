#!/bin/bash
# NOTE: This file suppose to run inside cmssw sigularity (cc7,el8,el9)

set -x
set -euo pipefail

#Script can be used to setup CRABClient:
# 1. dev - CRABClient from Intergration Build (IB);
# 2. GH - CRABClient from CRABClient GH repository master branch. This option requires to set which CRABServer tag to use;
# 3. prod - production CRABClient from cvmfs;
# Variables ${SCRAM_ARCH}, ${CMSSW_release}, ${CRABServer_tag}, ${CRABClient_version}
# comes from Jenkins job CRABServer_ExecuteTests configuration.

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

#cd ${WORK_DIR}
#[ ! -d 'CRABServer' ] && git clone git@github.com:dmwm/CRABServer

case $CRABClient_version in
  #dev)
  #  source /cvmfs/cms-ib.cern.ch/latest/common/crab-setup.sh dev
  #  alias crab='crab-dev'
  #  ;;
  #GH)
  #  set -euo pipefail
  #  #cd CRABServer; git checkout ${CRABServer_tag}; cd ..
  #  rm -rf CRABClient
  #  git clone https://github.com/${CRABCLIENT_FORK}/CRABClient CRABClient -b $CRABCLIENT_TAG
  #  cp src/python/ServerUtilities.py CRABClient/src/python/
  #  cp src/python/RESTInteractions.py CRABClient/src/python/
  #  #$ghprbPullId is used for PR testing. If this variable is set, that means
  #  #we need to run test against specific commit
  #  #if [ ! -z "$ghprbPullId" ]; then
  #  #	cd CRABClient
  #  #	git fetch origin pull/${ghprbPullId}/merge:PR_MERGE
  #  #	export COMMIT=`git rev-parse "PR_MERGE^{commit}"`
  #  #	git checkout -f ${COMMIT}
  #  #	cd ..
  #  #fi
  #  cd ${WORK_DIR}
  #  GitDir=${WORK_DIR}
  #
  #  MY_CRAB=${GitDir}/CRABClient
  #
  #  # install the fake WMCore dependency for CRABClient, taking inspiration from
  #  # https://github.com/cms-sw/cmsdist/blob/b38a4b3339f12706513917153a2ec6cdcb23741c/crab-build.file#L37
  #  mkdir -p ${GitDir}/WMCore/src/python/WMCore
  #  touch ${GitDir}/WMCore/src/python/__init__.py
  #  touch ${GitDir}/WMCore/src/python/WMCore/__init__.py
  #  cp ${GitDir}/CRABClient/src/python/CRABClient/WMCoreConfiguration.py ${GitDir}/WMCore/src/python/WMCore/Configuration.py
  #
  #  export PYTHONPATH=${MY_CRAB}/src/python:${PYTHONPATH:-}
  #  export PYTHONPATH=${GitDir}/WMCore/src/python:$PYTHONPATH
  #
  #  export PATH=${MY_CRAB}/bin:$PATH
  #  set +euo pipefail
  #  source ${MY_CRAB}/etc/crab-bash-completion.sh
  #  ;;
  prod)
	source /cvmfs/cms.cern.ch/common/crab-setup.sh prod
esac

#cd "${CURRENT_DIR}"
crab --version
