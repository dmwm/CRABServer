#! /bin/sh

set -x
set -e
BASEDIR=$(cd "$(dirname "$0")"; pwd)

ORIGDIR=$PWD
STARTDIR=$PWD/tmp/runtime

CRAB3_VERSION=3.3.0-pre1

WMCOREDIR=$STARTDIR/WMCore
WMCOREVER=0.9.94d
WMCOREREPO=dmwm

CRABSERVERDIR=$STARTDIR/CRABServer
CRABSERVERVER=3.3.8.rc7
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
# This is the one dep which can't be taken from the CMS RPM install.
# The runtime on the HTCondor node uses the system python which, on SL6, is
# not linked against OpenSSL.  This breaks communications with the frontend.
#
# This libcurl.so.4 is produced by taking the curl SRPM from RHEL and adding
# a flag to use openssl as a backend.
#
# TODO: resolve this situation.
#
if [[ ! -e libcurl.so.4 ]]; then
    curl -L https://github.com/dmwm/CRABServer/raw/master/lib/libcurl.so.4 > $STARTDIR/libcurl.so.4 || exit 2
    curl -L https://github.com/dmwm/CRABServer/raw/master/lib/libcurl.so.4.sha1sum > $STARTDIR/libcurl.so.4.sha1sum || exit 2
    sha1sum -c $STARTDIR/libcurl.so.4.sha1sum || exit 2
fi
chmod +x libcurl.so.4

# For developers, we download all our dependencies from the various upstream servers.
# For actual releases, we take the libraries from the build environment RPMs.
if [[ "x$RPM_RELEASE" != "x" ]]; then

    pushd $ORIGDIR/../WMCore-$WMCOREVER/build/lib/
    zip -r $STARTDIR/WMCore.zip *
    zip -rq $STARTDIR/CRAB3.zip WMCore PSetTweaks -x \*.pyc || exit 3
    popd

    pushd $ORIGDIR/build/lib
    zip -rq $STARTDIR/CRAB3.zip RESTInteractions.py HTCondorUtils.py TaskWorker CRABInterface  -x \*.pyc || exit 3
    popd

    pushd $VO_CMS_SW_DIR/$SCRAM_ARCH/external/py2-httplib2/*/lib/python2.6/site-packages
    zip -rq $STARTDIR/CRAB3.zip httplib2 -x \*.pyc
    zip -r $STARTDIR/WMCore.zip httplib2  -x \*.pyc || exit 3
    popd

    pushd $VO_CMS_SW_DIR/$SCRAM_ARCH/external/cherrypy/*/lib/python2.6/site-packages
    zip -rq $STARTDIR/CRAB3.zip cherrypy -x \*.pyc
    popd

    mkdir -p bin
    cp $ORIGDIR/scripts/{TweakPSet.py,CMSRunAnalysis.py,DashboardFailure.sh} .
    cp $ORIGDIR/src/python/{ApmonIf.py,DashboardAPI.py,Logger.py,ProcInfo.py,apmon.py,ServerUtilities.py,CMSGroupMapper.py} .

else

    if [[ ! -e libcurl.so.4 ]]; then
        curl -L http://hcc-briantest.unl.edu/libcurl.so.4 > $STARTDIR/libcurl.so.4 || exit 2
    fi
    chmod +x libcurl.so.4

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

    if [[ ! -e httplib2.tar.gz ]]; then
        curl -L https://httplib2.googlecode.com/files/httplib2-0.8.tar.gz > httplib2.tar.gz || exit 2
    fi
    if [[ ! -e cherrypy.tar.gz ]]; then
        curl -L http://download.cherrypy.org/cherrypy/3.1.2/CherryPy-3.1.2.tar.gz > cherrypy.tar.gz || exit 2
    fi
    if [[ ! -e nose.tar.gz ]]; then
        curl -L https://github.com/nose-devs/nose/archive/release_1.3.0.tar.gz > nose.tar.gz || exit 2
    fi

    tar xzf httplib2.tar.gz || exit 2
    tar xzf cherrypy.tar.gz || exit 2
    tar xzf nose.tar.gz || exit 2

    pushd httplib2-0.8/python2
    zip -rq $STARTDIR/CRAB3.zip httplib2  -x \*.pyc || exit 3
    zip -r $STARTDIR/WMCore.zip httplib2  -x \*.pyc || exit 3
    popd

    pushd CherryPy-3.1.2/
    zip -rq $STARTDIR/CRAB3.zip cherrypy  -x \*.pyc || exit 3
    popd

    pushd nose-release_1.3.0/
    zip -rq $STARTDIR/CRAB3.zip nose -x \*.pyc || exit 3
    popd

    # up until this point, evertying in CRAB3.zip is an external
    cp $STARTDIR/CRAB3.zip $ORIGDIR/CRAB3-externals.zip

    pushd $WMCORE_PATH/src/python
    zip -rq $STARTDIR/WMCore.zip * || exit 3
    zip -rq $STARTDIR/CRAB3.zip WMCore PSetTweaks -x \*.pyc || exit 3
    popd

    pushd $CRABSERVER_PATH/src/python
    zip -rq $STARTDIR/CRAB3.zip RESTInteractions.py HTCondorUtils.py TaskWorker CRABInterface  -x \*.pyc || exit 3
    popd

    mkdir -p bin
    cp $CRABSERVER_PATH/scripts/{TweakPSet.py,CMSRunAnalysis.py,DashboardFailure.sh} .
    cp $CRABSERVER_PATH/src/python/{ApmonIf.py,DashboardAPI.py,Logger.py,ProcInfo.py,apmon.py,ServerUtilities.py,CMSGroupMapper.py} .
fi

pwd
echo "Making TaskManagerRun tarball"
tar zcf $ORIGDIR/TaskManagerRun-$CRAB3_VERSION.tar.gz CRAB3.zip TweakPSet.py CMSRunAnalysis.py ApmonIf.py DashboardAPI.py Logger.py ProcInfo.py apmon.py ServerUtilities.py CMSGroupMapper.py libcurl.so.4 || exit 4
echo "Making CMSRunAnalysis tarball"
tar zcf $ORIGDIR/CMSRunAnalysis-$CRAB3_VERSION.tar.gz WMCore.zip TweakPSet.py CMSRunAnalysis.py ApmonIf.py DashboardAPI.py Logger.py ProcInfo.py apmon.py ServerUtilities.py CMSGroupMapper.py DashboardFailure.sh || exit 4
popd

