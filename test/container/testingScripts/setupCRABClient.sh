#!/bin/bash

#Script can be used to setup CRABClient:
# 1. dev - CRABClient from Intergration Build (IB);
# 2. GH - CRABClient from CRABClient GH repository master branch. This option requires to set DBS, WMCore and CRABServer tags to use;
# 3. prod - production CRABClient from cvmfs;
#Variables ${SCRAM_ARCHITECTURE}, ${CMSSW_release}, ${CRABServer_tag}, ${CRABClient_version}, ${WMCore_tag}, ${DBS_tag}
# comes from Jenkins job CRABServer_ExecuteTests configuration.

export WORK_DIR=`pwd`
export SCRAM_ARCH=${SCRAM_ARCHITECTURE}
mkdir repos

source /cvmfs/cms-ib.cern.ch/latest/cmsset_default.sh
scramv1 project ${CMSSW_release}
cd ${CMSSW_release}/src
eval `scramv1 runtime -sh`
scram build

cd ${WORK_DIR}/repos
git clone https://github.com/dmwm/CRABServer
chmod -R 777 CRABServer

case $CRABClient_version in
  dev)
	source /cvmfs/cms-ib.cern.ch/latest/common/crab-setup.sh dev
	alias crab='crab-dev'
    ;;
  GH)
	cd CRABServer; git checkout ${CRABServer_tag}; cd ..
	git clone https://github.com/dmwm/CRABClient
	cp CRABServer/src/python/ServerUtilities.py CRABClient/src/python/
	cp CRABServer/src/python/RESTInteractions.py CRABClient/src/python/
	#$ghprbPullId is used for PR testing. If this variable is set, that means
	#we need to run test against specific commit
	if [ ! -z "$ghprbPullId" ]; then
		cd CRABClient
		git fetch origin pull/${ghprbPullId}/merge:PR_MERGE
		export COMMIT=`git rev-parse "PR_MERGE^{commit}"`
		git checkout -f ${COMMIT}
		cd ..
	fi
	git clone https://github.com/dmwm/WMCore
	cd WMCore; git checkout ${WMCore_tag}; cd ..
	git clone https://github.com/dmwm/DBS
	cd DBS; git checkout ${DBS_tag}
	cd ${WORK_DIR}
	GitDir=${WORK_DIR}/repos

	MY_DBS=${GitDir}/DBS
	MY_CRAB=${GitDir}/CRABClient
	MY_WMCORE=${GitDir}/WMCore

	export PYTHONPATH=${MY_DBS}/Client/src/python:${MY_DBS}/PycurlClient/src/python:$PYTHONPATH
	export PYTHONPATH=${MY_WMCORE}/src/python:$PYTHONPATH
	export PYTHONPATH=${MY_CRAB}/src/python:$PYTHONPATH

	export PATH=${MY_CRAB}/bin:$PATH
	source ${MY_CRAB}/etc/crab-bash-completion.sh
    ;;
  prod)
	source /cvmfs/cms.cern.ch/common/crab-setup.sh
esac

cd ${WORK_DIR}
crab --version

