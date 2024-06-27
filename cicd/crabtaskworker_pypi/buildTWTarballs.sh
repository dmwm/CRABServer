#! /bin/bash
# Simplify version of htcondor_make_runtime.sh to build TaskManagerRun.tar.gz/CMSRunAnalysis.tar.gz
# https://github.com/novicecpp/CRABServer/blob/5dd0a31c9545e5e16a395aba0b1b955f1bf37b23/bin/htcondor_make_runtime.sh
# This script needs
#   - RUNTIME_WORKDIR:   Working directory to build tar files
#   - WMCOREDIR:         WMCore repository path.
#   - CRABSERVERDIR:     CRABServer repository path.
#
# Fetching WMCore/CRABServer is depend on dev/CI step.
# If you are not use default path (i.e. WMCore and CRABServer repo is not in
# the same directory) you can override variable via export as environment
# variables.
#
# Example of running in local machine (use default RUNTIME_WORKDIR and enable trace):
# CRABSERVERDIR=./ WMCOREDIR=../WMCore TRACE=true bash cicd/crabtaskworker/buildTWTarballs.sh):
#

set -euo pipefail

TRACE=${TRACE:-}
if [[ -n $TRACE ]]; then
    set -x
fi

# env var can override.
RUNTIME_WORKDIR="${RUNTIME_WORKDIR:-./make_runtime}"
WMCOREDIR="${WMCOREDIR:-./WMCore}"
CRABSERVERDIR="${CRABSERVERDIR:-./}"

# get absolute path
RUNTIME_WORKDIR="$(realpath "${RUNTIME_WORKDIR}")"
WMCOREDIR="$(realpath "${WMCOREDIR}")"
CRABSERVERDIR="$(realpath "${CRABSERVERDIR}")"

STARTDIR="${RUNTIME_WORKDIR}/runtime"
WMCORE_BUILD_PREFIX="${WMCOREDIR}/build/lib"
CRABSERVER_BUILD_PREFIX="${CRABSERVERDIR}/build/lib"

# cleanup $RUNTIME_WORKDIR
rm -rf "${RUNTIME_WORKDIR}"/{CMSRunAnalysis.tar.gz,TaskManagerRun.tar.gz}
# create neccessary dir
mkdir -p "${RUNTIME_WORKDIR}" "${STARTDIR}" "${CRABSERVER_BUILD_PREFIX}" "${WMCORE_BUILD_PREFIX}"

# Build library.
# `python3 setup.py build_system` will create file in `${setup.py_dir}/build/`
# build wmcore
pushd "${WMCOREDIR}"
python3 setup.py build_system -s crabtaskworker --skip-docs
popd
# build crabserver
pushd "${CRABSERVERDIR}"
python3 setup.py build_system -s TaskWorker --skip-docs=d
popd


pushd "${STARTDIR}"

# ============================================================
# This section is taken from original htcondor_make_runtime.sh
# ============================================================
# make sure there's always a CRAB3.zip to avoid errors in other parts
echo "${STARTDIR}"
touch "${STARTDIR}/dummyFile"
zip -r "${STARTDIR}/CRAB3.zip" "${STARTDIR}/dummyFile"
rm -f "${STARTDIR}/dummyFile"

# Take the libraries from the build environment.
pushd "${WMCORE_BUILD_PREFIX}"
zip -r "${STARTDIR}/WMCore.zip" ./*
zip -rq "${STARTDIR}/CRAB3.zip" WMCore PSetTweaks Utils -x \*.pyc || exit 3
popd

pushd "${CRABSERVER_BUILD_PREFIX}"
zip -rq "${STARTDIR}/CRAB3.zip" RESTInteractions.py HTCondorLocator.py TaskWorker CRABInterface  TransferInterface ASO -x \*.pyc || exit 3
popd

cp -r "${CRABSERVERDIR}/scripts"/{TweakPSet.py,CMSRunAnalysis.py,task_process} .
cp "${CRABSERVERDIR}/src/python"/{ServerUtilities.py,RucioUtils.py,CMSGroupMapper.py,RESTInteractions.py} .

echo "Making TaskManagerRun tarball"
tar zcf "${RUNTIME_WORKDIR}/TaskManagerRun.tar.gz" CRAB3.zip TweakPSet.py CMSRunAnalysis.py task_process ServerUtilities.py RucioUtils.py CMSGroupMapper.py RESTInteractions.py || exit 4
echo "Making CMSRunAnalysis tarball"
tar zcf "${RUNTIME_WORKDIR}/CMSRunAnalysis.tar.gz" WMCore.zip TweakPSet.py CMSRunAnalysis.py ServerUtilities.py CMSGroupMapper.py RESTInteractions.py || exit 4

# cleanup.
# current directory ($RUNTIME_WORKDIR) should only have TaskManagerRun.tar.gz and CMSRunAnalysis.tar.gz
rm -rf "${STARTDIR}" "${WMCORE_BUILD_PREFIX}" "${CRABSERVER_BUILD_PREFIX}"
popd
