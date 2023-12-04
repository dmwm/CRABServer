#! /bin/bash

set -x
set -e

ORIGDIR=$PWD
STARTDIR=$PWD/runtime

WMCOREDIR=$ORIGDIR/WMCore
pushd $WMCOREDIR
python3 setup.py build_system -s crabtaskworker --skip-docs
popd
#WMCOREVER=${WMCOREVER:-1.1.14.crab1}
#WMCOREREPO=dmwm

CRABSERVERDIR=$ORIGDIR
pushd $CRABSERVERDIR
python3 setup.py build_system -s TaskWorker --skip-docs=d
popd


#CRABSERVERVER=${CRABSERVERVER:-3.3.1909.rc1}
#CRABSERVERREPO=dmwm

[[ -d $STARTDIR ]] || mkdir -p $STARTDIR

# already copy in setup.py
#cp $CRABSERVERDIR/scripts/cmscp.sh $STARTDIR || exit 3
#cp $CRABSERVERDIR/scripts/submit_env.sh $STARTDIR || exit 3
#cp $CRABSERVERDIR/scripts/gWMS-CMSRunAnalysis.sh $STARTDIR || exit 3


#rm -rf $WMCOREDIR && mkdir -p $WMCOREDIR
#rm -rf $CRABSERVERDIR && mkdir -p $CRABSERVERDIR

#if [[ -n "$CRAB_OVERRIDE_SOURCE" ]]; then
#    REPLACEMENT_ABSOLUTE=$(readlink -f $CRAB_OVERRIDE_SOURCE)
#elif [[ "x$1" != "x" ]]; then
#    REPLACEMENT_ABSOLUTE=$(readlink -f $1)
#else
#    REPLACEMENT_ABSOLUTE=""
#fi

pushd $STARTDIR


# cleanup, avoid to keep adding to existing tarballs
#rm -f $STARTDIR/CRAB3.zip
#rm -f $STARTDIR/WMCore.zip

# make sure there's always a CRAB3.zip to avoid errors in other parts
touch $STARTDIR/dummyFile
zip -r $STARTDIR/CRAB3.zip $STARTDIR/dummyFile
rm -f $STARTDIR/dummyFile

# For developers, we download all our dependencies from the various upstream servers.
# For actual releases, we take the libraries from the build environment RPMs.
RPM_RELEASE=1
if [[ "x$RPM_RELEASE" != "x" ]]; then
    # I am inside  a release building
    pushd $WMCOREDIR/build/lib/
    zip -r $STARTDIR/WMCore.zip *
    zip -rq $STARTDIR/CRAB3.zip WMCore PSetTweaks Utils -x \*.pyc || exit 3
    popd

    pushd $CRABSERVERDIR/build/lib
    zip -rq $STARTDIR/CRAB3.zip RESTInteractions.py HTCondorUtils.py HTCondorLocator.py TaskWorker CRABInterface  TransferInterface ASO -x \*.pyc || exit 3
    popd

    mkdir -p bin
    cp -r $CRABSERVERDIR/scripts/{TweakPSet.py,CMSRunAnalysis.py,task_process} .
    cp $CRABSERVERDIR/src/python/{ServerUtilities.py,RucioUtils.py,CMSGroupMapper.py,RESTInteractions.py} .

#else
#    # building runtime tarballs from development area or GH
#    if [[ -d "$REPLACEMENT_ABSOLUTE/WMCore" ]]; then
#        echo "Using replacement WMCore source at $REPLACEMENT_ABSOLUTE/WMCore"
#        WMCORE_PATH="$REPLACEMENT_ABSOLUTE/WMCore"
#    else
#        if [[ ! -e $WMCOREVER.tar.gz ]]; then
#            curl -L https://github.com/$WMCOREREPO/WMCore/archive/$WMCOREVER.tar.gz > $WMCOREVER.tar.gz || exit 2
#        fi
#        tar zxf $WMCOREVER.tar.gz || exit 2
#        WMCORE_PATH="WMCore-$WMCOREVER"
#    fi
#
#    if [[ -d "$REPLACEMENT_ABSOLUTE/CRABServer" ]]; then
#        echo "Using replacement CRABServer source at $REPLACEMENT_ABSOLUTE/CRABServer"
#        CRABSERVER_PATH="$REPLACEMENT_ABSOLUTE/CRABServer"
#    else
#        curl -L https://github.com/$CRABSERVERREPO/CRABServer/archive/$CRABSERVERVER.tar.gz | tar zx || exit 2
#        CRABSERVER_PATH="CRABServer-$CRABSERVERVER"
#    fi
#
#    # up until this point, evertying in CRAB3.zip is an external
#    cp $STARTDIR/CRAB3.zip $ORIGDIR/CRAB3-externals.zip
#
#    pushd $WMCORE_PATH/src/python
#    zip -rq $STARTDIR/WMCore.zip * || exit 3
#    zip -rq $STARTDIR/CRAB3.zip WMCore PSetTweaks Utils -x \*.pyc || exit 3
#    popd
#
#    pushd $CRABSERVER_PATH/src/python
#    zip -rq $STARTDIR/CRAB3.zip RESTInteractions.py HTCondorUtils.py HTCondorLocator.py TaskWorker CRABInterface TransferInterface ASO -x \*.pyc || exit 3
#    popd
#
#    mkdir -p bin
#    cp -r $CRABSERVER_PATH/scripts/{TweakPSet.py,CMSRunAnalysis.py,task_process} .
#    cp $CRABSERVER_PATH/src/python/{ServerUtilities.py,RucioUtils.py,CMSGroupMapper.py,RESTInteractions.py} .
fi

pwd
echo "Making TaskManagerRun tarball"
tar zcf $ORIGDIR/TaskManagerRun.tar.gz CRAB3.zip TweakPSet.py CMSRunAnalysis.py task_process ServerUtilities.py RucioUtils.py CMSGroupMapper.py RESTInteractions.py || exit 4
echo "Making CMSRunAnalysis tarball"
tar zcf $ORIGDIR/CMSRunAnalysis.tar.gz WMCore.zip TweakPSet.py CMSRunAnalysis.py ServerUtilities.py CMSGroupMapper.py RESTInteractions.py || exit 4
popd
