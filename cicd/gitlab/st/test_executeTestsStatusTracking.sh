#! /bin/bash
set -euo pipefail
# test
ROOT_DIR="$(git rev-parse --show-toplevel)"
pushd "${ROOT_DIR}"
# clean workdir
rm -rf "${ROOT_DIR}/workdir"
export X509_USER_PROXY=/tmp/x509up_u1000
# flag submit only hc1kj tasks
export DEBUG_TEST=true
# from .env
export KUBECONTEXT=cmsweb-test12
export TW_MACHINE=crab-dev-tw03
export REST_Instance=test12
# ci
export X509_USER_PROXY="$(cicd/gitlab/credFile.sh $X509_USER_PROXY)"
export CRABClient_version=prod
export REST_Instance # from .env
export CMSSW_release=CMSSW_13_0_2
export Task_Submission_Status_Tracking=true
bash -x cicd/gitlab/executeTests.sh
popd
