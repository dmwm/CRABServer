#! /bin/bash
# just a wrapper to launch the python script which lives in same directory

echo "Running st/statusTracking.sh"

set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "(DEBUG) X509_USER_PROXY=${X509_USER_PROXY}"
echo "(DEBUG) ROOT_DIR=${ROOT_DIR}"
echo "(DEBUG) WORK_DIR=${WORK_DIR}"
echo "(DEBUG) CMSSW_release=${CMSSW_release}"
echo "(DEBUG) SCRIPT_DIR=${SCRIPT_DIR}"

source "${ROOT_DIR}"/cicd/gitlab/setupCRABClient.sh
CM=${CMSSW_release#CMSSW_};  python3 "${SCRIPT_DIR}/statusTracking.py"
