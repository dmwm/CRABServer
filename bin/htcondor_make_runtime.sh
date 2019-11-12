#! /bin/sh

set -x
set -e
BASEDIR=$(cd "$(dirname "$0")"; pwd)

ORIGDIR=$PWD
STARTDIR=$PWD/tmp/runtime

CRAB3_VERSION=3.3.0-pre1

WMCOREDIR=$STARTDIR/WMCore
WMCOREVER=1.1.14.crab1
WMCOREREPO=dmwm

CRABSERVERDIR=$STARTDIR/CRABServer
CRABSERVERVER=3.3.1909.rc1
CRABSERVERREPO=dmwm

[[ -d $STARTDIR ]] || mkdir -p $STARTDIR

cp $BASEDIR/../scripts/gWMS-CMSRunAnalysis.sh $STARTDIR || exit 3


rm -rf $WMCOREDIR && mkdir -p $WMCOREDIR
rm -rf $CRABSERVERDIR && mkdir -p $CRABSERVERDIR

if [[ -n "$CRAB_OVERRIDE_SOURCE" ]]; then
    REPLACEMENT_ABSOLUTE=$(readlink -f $CRAB_OVERRIDE_SOURCE)
elif [[ "x$1" != "x" ]]; then
    REPLACEMENT_ABSOLUTE=$(readlink -f $1)
else
    REPLACEMENT_ABSOLUTE=""
fi
pushd $STARTDIR

#
# cleanup, avoid to keep adding to existing tarballs
#

rm -f $STARTDIR/CRAB3.zip
rm -f $STARTDIR/WMCore.zip
rm -f $STARTDIR/nose.tar.gz


# For developers, we download all our dependencies from the various upstream servers.
# For actual releases, we take the libraries from the build environment RPMs.
if [[ "x$RPM_RELEASE" != "x" ]]; then

    pushd $ORIGDIR/../WMCore-$WMCOREVER/build/lib/
    zip -r $STARTDIR/WMCore.zip *
    zip -rq $STARTDIR/CRAB3.zip WMCore PSetTweaks Utils -x \*.pyc || exit 3
    popd

    pushd $ORIGDIR/build/lib
    zip -rq $STARTDIR/CRAB3.zip RESTInteractions.py HTCondorUtils.py HTCondorLocator.py TaskWorker CRABInterface  TransferInterface -x \*.pyc || exit 3
    popd

    pushd $VO_CMS_SW_DIR/$SCRAM_ARCH/external/cherrypy/*/lib/python2.7/site-packages
    zip -rq $STARTDIR/CRAB3.zip cherrypy -x \*.pyc
    popd

    mkdir -p bin
    cp -r $ORIGDIR/scripts/{TweakPSet.py,CMSRunAnalysis.py,DashboardFailure.sh,task_process} .
    cp $ORIGDIR/src/python/{ApmonIf.py,DashboardAPI.py,Logger.py,ProcInfo.py,apmon.py,ServerUtilities.py,CMSGroupMapper.py,RESTInteractions.py} .

else

    if [[ -d "$REPLACEMENT_ABSOLUTE/WMCore" ]]; then
        echo "Using replacement WMCore source at $REPLACEMENT_ABSOLUTE/WMCore"
        WMCORE_PATH="$REPLACEMENT_ABSOLUTE/WMCore"
    else
        if [[ ! -e $WMCOREVER.tar.gz ]]; then
            curl -L https://github.com/$WMCOREREPO/WMCore/archive/$WMCOREVER.tar.gz > $WMCOREVER.tar.gz || exit 2
        fi
        tar zxf $WMCOREVER.tar.gz || exit 2
        WMCORE_PATH="WMCore-$WMCOREVER"
    fi

    if [[ -d "$REPLACEMENT_ABSOLUTE/CRABServer" ]]; then
        echo "Using replacement CRABServer source at $REPLACEMENT_ABSOLUTE/CRABServer"
        CRABSERVER_PATH="$REPLACEMENT_ABSOLUTE/CRABServer"
    else
        curl -L https://github.com/$CRABSERVERREPO/CRABServer/archive/$CRABSERVERVER.tar.gz | tar zx || exit 2
        CRABSERVER_PATH="CRABServer-$CRABSERVERVER"
    fi

    if [[ ! -e nose.tar.gz ]]; then
        curl -L https://github.com/nose-devs/nose/archive/release_1.3.0.tar.gz > nose.tar.gz || exit 2
    fi

    tar xzf nose.tar.gz || exit 2

    pushd nose-release_1.3.0/
    zip -rq $STARTDIR/CRAB3.zip nose -x \*.pyc || exit 3
    popd


    # up until this point, evertying in CRAB3.zip is an external
    cp $STARTDIR/CRAB3.zip $ORIGDIR/CRAB3-externals.zip

    pushd $WMCORE_PATH/src/python
    zip -rq $STARTDIR/WMCore.zip * || exit 3
    zip -rq $STARTDIR/CRAB3.zip WMCore PSetTweaks Utils -x \*.pyc || exit 3
    popd

    pushd $CRABSERVER_PATH/src/python
    zip -rq $STARTDIR/CRAB3.zip RESTInteractions.py HTCondorUtils.py HTCondorLocator.py TaskWorker CRABInterface TransferInterface -x \*.pyc || exit 3
    popd

    mkdir -p bin
    cp -r $CRABSERVER_PATH/scripts/{TweakPSet.py,CMSRunAnalysis.py,DashboardFailure.sh,task_process} .
    cp $CRABSERVER_PATH/src/python/{ApmonIf.py,DashboardAPI.py,Logger.py,ProcInfo.py,apmon.py,ServerUtilities.py,CMSGroupMapper.py,RESTInteractions.py} .
fi

pwd
echo "Making TaskManagerRun tarball"
tar zcf $ORIGDIR/TaskManagerRun-$CRAB3_VERSION.tar.gz CRAB3.zip TweakPSet.py CMSRunAnalysis.py ApmonIf.py task_process DashboardAPI.py Logger.py ProcInfo.py apmon.py ServerUtilities.py CMSGroupMapper.py RESTInteractions.py || exit 4
echo "Making CMSRunAnalysis tarball"
tar zcf $ORIGDIR/CMSRunAnalysis-$CRAB3_VERSION.tar.gz WMCore.zip TweakPSet.py CMSRunAnalysis.py ApmonIf.py DashboardAPI.py Logger.py ProcInfo.py apmon.py ServerUtilities.py CMSGroupMapper.py RESTInteractions.py DashboardFailure.sh || exit 4
popd

