# Run the script inside CRABServer
# this is a hardcoded name in htcondor_make_runtime.sh with no relation
# with actual CRAB version
CRAB3_DUMMY_VERSION=3.3.0-pre1

#
# Will replace the tarball in whatever is current TW
TW_RELEASE=`ls -l /data/srv/TaskManager/current|awk '{print $NF}'|tr -d '/'`
MYTESTAREA=/data/srv/TaskManager/current
TW_ARCH=$(basename $(ls -d /data/srv/TaskManager/current/sl*))
CRABTASKWORKER_ROOT=${MYTESTAREA}/${TW_ARCH}/cms/crabtaskworker/${TW_RELEASE}

# CRAB_OVERRIDE_SOURCE tells htcondor_make_runtime.sh where to find the CRABServer repository
export CRAB_OVERRIDE_SOURCE=/data/user/
pushd $CRAB_OVERRIDE_SOURCE/CRABServer
sh bin/htcondor_make_runtime.sh
mv TaskManagerRun-$CRAB3_DUMMY_VERSION.tar.gz TaskManagerRun.tar.gz
mv CMSRunAnalysis-$CRAB3_DUMMY_VERSION.tar.gz CMSRunAnalysis.tar.gz

cp -v CMSRunAnalysis.tar.gz CRAB3-externals.zip TaskManagerRun.tar.gz $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/AdjustSites.py $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/dag_bootstrap_startup.sh $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/dag_bootstrap.sh $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/gWMS-CMSRunAnalysis.sh $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/CMSRunAnalysis.sh $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/cmscp.py $CRAB_OVERRIDE_SOURCE/CRABServer/scripts/DashboardFailure.sh $CRABTASKWORKER_ROOT/data
popd

