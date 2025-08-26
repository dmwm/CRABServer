#!/bin/bash
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
zip -rq "${STARTDIR}/WMCore.zip" ./*
popd

# for CRAB just take all python files and put in a zip to be added to PYTHONPATH
pushd "${CRABSERVERDIR}"/src/python
zip -rq "${STARTDIR}/CRAB3.zip" . -x  \*.pyc || exit 3
popd

cp -r "${CRABSERVERDIR}/scripts/job_wrapper" .
cp -r "${CRABSERVERDIR}/scripts/dagman" .

#cp "${CRABSERVERDIR}/src/python"/{ServerUtilities.py,RucioUtils.py,CMSGroupMapper.py,RESTInteractions.py} .

echo "Making TaskManagerRun tarball"
tar cf "${RUNTIME_WORKDIR}/TaskManagerRun.tar" CRAB3.zip WMCore.zip || exit 4
# add scripts from dagman directory at top level of tarball. Will be in correct place when tar is expanded
pushd dagman
tar rf "${RUNTIME_WORKDIR}/TaskManagerRun.tar" * || exit 4
popd
gzip "${RUNTIME_WORKDIR}/TaskManagerRun.tar"

echo "Making CMSRunAnalysis tarball"
tar cf "${RUNTIME_WORKDIR}/CMSRunAnalysis.tar" WMCore.zip || exit 4
# add scripts from job_wrapper directory at top level of tarball. Will be in correct place when tar is expanded
pushd job_wrapper
tar rf "${RUNTIME_WORKDIR}/CMSRunAnalysis.tar" * || exit 4
popd
gzip "${RUNTIME_WORKDIR}/CMSRunAnalysis.tar"


# cleanup.
# current directory ($RUNTIME_WORKDIR) should only have TaskManagerRun.tar.gz and CMSRunAnalysis.tar.gz
rm -rf "${STARTDIR}" "${WMCORE_BUILD_PREFIX}" "${CRABSERVER_BUILD_PREFIX}"
popd
