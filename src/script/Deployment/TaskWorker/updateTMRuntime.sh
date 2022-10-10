# Run the script inside CRABServer
# this is a hardcoded name in htcondor_make_runtime.sh with no relation
# with actual CRAB version
CRAB3_DUMMY_VERSION=3.3.0-pre1

# Will replace the tarball in whatever is current TW

TW_HOME=/data/srv/TaskManager
TW_CURRENT=${TW_HOME}/current
TW_RELEASE=`ls -l ${TW_CURRENT}|awk '{print $NF}'|tr -d '/'`
TW_ARCH=$(basename $(ls -d ${TW_CURRENT}/sl*))
CRABTASKWORKER_ROOT=${TW_CURRENT}/${TW_ARCH}/cms/crabtaskworker/${TW_RELEASE}


# if GH repositories location is not already defined, set a default
if ! [ -v GHrepoDir ]
then
  GHrepoDir='/data/repos'
fi

logFile=/tmp/updateRuntimeLog.txt

echo "working environment:" > ${logFile}
echo TW_HOME = $TW_HOME >> ${logFile}
echo TW_CURRENT = $TW_CURRENT >> ${logFile}
echo TW_RELEASE = $TW_RELEASE >> ${logFile}
echo TW_ARCH = $TW_ARCH >> ${logFile}
echo CRABTASKWORKER_ROOT = $CRABTASKWORKER_ROOT >> ${logFile}

# CRAB_OVERRIDE_SOURCE tells htcondor_make_runtime.sh where to find the CRABServer repository
export CRAB_OVERRIDE_SOURCE=${GHrepoDir}
echo CRAB_OVERRIDE_SOURCE = $CRAB_OVERRIDE_SOURCE >> ${logFile}
pushd $CRAB_OVERRIDE_SOURCE/CRABServer > /dev/null
echo "current working directory is: " `pwd` >> ${logFile}
sh bin/htcondor_make_runtime.sh >> ${logFile} 2>&1
mv TaskManagerRun-$CRAB3_DUMMY_VERSION.tar.gz TaskManagerRun.tar.gz >> ${logFile} 2>&1
mv CMSRunAnalysis-$CRAB3_DUMMY_VERSION.tar.gz CMSRunAnalysis.tar.gz >> ${logFile} 2>&1

# files to be placed in $CRABTASKWORKER_ROOT/data
filesToCopy=CMSRunAnalysis.tar.gz
filesToCopy="$filesToCopy CRAB3-externals.zip"
filesToCopy="$filesToCopy TaskManagerRun.tar.gz"
filesToCopy="$filesToCopy $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/AdjustSites.py"
filesToCopy="$filesToCopy $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/dag_bootstrap_startup.sh"
filesToCopy="$filesToCopy $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/dag_bootstrap.sh"
filesToCopy="$filesToCopy $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/gWMS-CMSRunAnalysis.sh"
filesToCopy="$filesToCopy $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/env-CMSRunAnalysis.sh"
filesToCopy="$filesToCopy $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/CMSRunAnalysis.sh"
filesToCopy="$filesToCopy $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/cmscp.py"

# clean backup of previous Runtime files
rm -rf  $CRABTASKWORKER_ROOT/data/PreviousRuntime/*
mkdir -p $CRABTASKWORKER_ROOT/data/PreviousRuntime

# move current files to backup are and then update current TW with new ones
for file in $filesToCopy; do
  mv $CRABTASKWORKER_ROOT/data/`basename $file` $CRABTASKWORKER_ROOT/data/PreviousRuntime/
  cp -v $file $CRABTASKWORKER_ROOT/data  >> ${logFile} 2>&1
done

if [ $? -eq 0 ]
then
  echo ""
  echo "OK. New tarballs created and placed inside directory tree: $TW_CURRENT"
  echo "See log in ${logFile}"
  echo "Previous files have been saved in $CRABTASKWORKER_ROOT/data/PreviousRuntime/"
  echo "BEWARE: Safest way to revert to original configuration is to re-deploy container"
else
  echo "*** ERROR ** ERROR ** ERROR ** ERROR ***"
  echo "Tarball creation or copy failed."
  echo "See log in ${logFile}"
  echo "Make sure $TW_CURRENT is not corrupted"
  echo "****************************************"
fi

popd > /dev/null

