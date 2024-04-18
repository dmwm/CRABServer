#! /bin/bash
# Note: prepare $ROOT_DIR/workdir2 with submitted_tasks_TS before run this script
set -euo pipefail
ROOT_DIR="$(git rev-parse --show-toplevel)"
pushd "${ROOT_DIR}"
rm -rf "${ROOT_DIR}/workdir"
cp -r workdir2 workdir
export X509_USER_PROXY=/tmp/x509up_u1000
export RETRY_MAX=1
export RETRY=1
# .env
export REST_Instance=test12
# ci
export CMSSW_release=CMSSW_13_0_2
export SCRAM_ARCH=el8_amd64_gcc11
export Check_Publication_Status=Yes
export CRABClient_version=prod
if [[ -n "${MANUAL_TASKNAME:-}" ]]; then
    echo "${MANUAL_TASKNAME}" > workdir/submitted_tasks_TS
fi
bash -x cicd/gitlab/retry.sh bash -x cicd/gitlab/executeStatusTracking.sh
popd
