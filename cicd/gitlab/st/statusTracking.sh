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
rc=0
python3 "${SCRIPT_DIR}/statusTracking.py" 2>&1 > statusTracking.log || rc=$?
printLog=0
if [ $rc -eq 1 ]; then
  echo "==> FAILED> StatusTracking.py failed. Here's log:"
  printLog=1
fi
if [ $Verbose -ge 2 ]; then
  echo " Verbose is $Verbose. Printing statusTracking.py log "
  printLog=1
fi
if [ $printLog -eq 1 ]; then
  echo "--------------------------------------------------------"
  cat statusTracking.log
  echo "--------------------------------------------------------"
fi
if [ $rc -eq 1 ]; then
  return 1
fi

echo "statusTracking.py executed"

