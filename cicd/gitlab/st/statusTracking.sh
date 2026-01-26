#! /bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "(DEBUG) X509_USER_PROXY=${X509_USER_PROXY}"
echo "(DEBUG) ROOT_DIR=${ROOT_DIR}"
echo "(DEBUG) WORK_DIR=${WORK_DIR}"
echo "(DEBUG) CMSSW_release=${CMSSW_release}"

source "${ROOT_DIR}"/cicd/gitlab/setupCRABClient.sh
CM=${CMSSW_release#CMSSW_}; $([ "${CM%%_*}" -le 11 ] && echo python2 || echo python3) "${SCRIPT_DIR}/statusTracking.py"
