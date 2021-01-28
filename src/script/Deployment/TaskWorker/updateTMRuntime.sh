# Run the script inside CRABServer
# this is a hardcoded name in htcondor_make_runtime.sh with no relation
# with actual CRAB version
CRAB3_DUMMY_VERSION=3.3.0-pre1

#
# Will replace the tarball in whatever is current TW

TW_HOME=/data/srv/TaskManager

TW_CURRENT=${TW_HOME}/current

TW_RELEASE=`ls -l ${TW_CURRENT}|awk '{print $NF}'|tr -d '/'`
TW_ARCH=$(basename $(ls -d ${TW_CURRENT}/sl*))
CRABTASKWORKER_ROOT=${TW_CURRENT}/${TW_ARCH}/cms/crabtaskworker/${TW_RELEASE}


# if GH repositories location is not already defined, set a default
if ! [ -v GHrepoDir ]
then
  GHrepoDir='/data/hostdisk/repos'
fi

logFile=/tmp/updateRuntimeLog.txt

# CRAB_OVERRIDE_SOURCE tells htcondor_make_runtime.sh where to find the CRABServer repository
export CRAB_OVERRIDE_SOURCE=${GHrepoDir}
pushd $CRAB_OVERRIDE_SOURCE/CRABServer > /dev/null
sh bin/htcondor_make_runtime.sh > ${logFile} 2>&1
mv TaskManagerRun-$CRAB3_DUMMY_VERSION.tar.gz TaskManagerRun.tar.gz >> ${logFile} 2>&1
mv CMSRunAnalysis-$CRAB3_DUMMY_VERSION.tar.gz CMSRunAnalysis.tar.gz >> ${logFile} 2>&1

cp -v CMSRunAnalysis.tar.gz CRAB3-externals.zip TaskManagerRun.tar.gz $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/AdjustSites.py $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/dag_bootstrap_startup.sh $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/dag_bootstrap.sh $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/gWMS-CMSRunAnalysis.sh $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/CMSRunAnalysis.sh $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/cmscp.py $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/DashboardFailure.sh $CRABTASKWORKER_ROOT/data  >> ${logFile} 2>&1

if [ $? -eq 0 ]
then
  echo ""
  echo "OK. New tarballs created and placed inside directory tree: $TW_CURRENT"
else
  echo "*** ERROR ** ERROR ** ERROR ** ERROR ***"
  echo "Tarball creation or copy failed."
  echo "See log in ${logFile}"
  echo "Make sure $TW_CURRENT is not corrupted"
  echo "****************************************"
fi

popd > /dev/null

