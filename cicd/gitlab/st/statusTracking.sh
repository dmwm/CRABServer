#! /bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "(DEBUG) X509_USER_PROXY=${X509_USER_PROXY}"
echo "(DEBUG) ROOT_DIR=${ROOT_DIR}"
echo "(DEBUG) WORK_DIR=${WORK_DIR}"

source "${ROOT_DIR}"/cicd/gitlab/setupCRABClient.sh
python3 "${SCRIPT_DIR}"/statusTracking.py
