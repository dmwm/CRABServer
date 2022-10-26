#!/bin/bash

save_env() {
    # save the current environment to the file startup_environment.sh 
    # this is intended to be the first function run by 
    # - gWMS-CMSRunAnalysis.sh: when running a job on the global pool
    # - crab preparelocal, crab submit --dryrun: when running a job locally

    # On some sites we know there was some problems with environment cleaning
    # with using 'env -i'. To overcome this issue, whenever we start a job, we have
    # to save full current environment into file, and whenever it is needed we can load
    # it. Be aware, that there are some read-only variables, like: BASHOPTS, BASH_VERSINFO,
    # EUID, PPID, SHELLOPTS, UID, etc.

    # Moreover, src/python/WMCore/Storage/Backends/GFAL2Impl.py 
    # makes use of startup_environment.sh

    export DMDEBUGVAR=dmdebugvalue-env-cmsrunanalysis
    export JOBSTARTDIR=$PWD
    export HOME=${HOME:-$PWD}

    declare -p > startup_environment.sh

    # these lines are for debugging purposes only: start 
    echo "DM DEBUG: cat startup_environment.sh"
    # basename -- "$0"
    # dirname -- "$0"
    # echo $PWD
    # ls -lrth
    cat startup_environment.sh
    # these lines are for debugging purposes only: end 

}

setup_local_env () {
    # when running a job locally, we need to set manually some variables that 
    # are set for us when running on the global pool.

    export SCRAM_ARCH=$(scramv1 arch)
    export REQUIRED_OS=rhel7
    export CRAB_RUNTIME_TARBALL=local
    export CRAB_TASKMANAGER_TARBALL=local
    export CRAB3_RUNTIME_DEBUG=True

}

setup_cmsset() {
    ### source the CMSSW stuff using either OSG or LCG style entry env. or CVMFS
    echo "======== CMS environment load starting at $(TZ=GMT date) ========"
    CMSSET_DEFAULT_PATH=""
    if [ -f "$VO_CMS_SW_DIR"/cmsset_default.sh ]
    then  #   LCG style --
        echo "WN with a LCG style environment, thus using VO_CMS_SW_DIR=$VO_CMS_SW_DIR"
        CMSSET_DEFAULT_PATH=$VO_CMS_SW_DIR/cmsset_default.sh
    elif [ -f "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]
    then  #   OSG style --
        echo "WN with an OSG style environment, thus using OSG_APP=$OSG_APP"
        CMSSET_DEFAULT_PATH=$OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
    elif [ -f "$CVMFS"/cms.cern.ch/cmsset_default.sh ]
    then
        echo "WN with CVMFS environment, thus using CVMFS=$CVMFS"
        CMSSET_DEFAULT_PATH=$CVMFS/cms.cern.ch/cmsset_default.sh
    elif [ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]
    then  # ok, lets call it CVMFS then
        CVMFS=/cvmfs/cms.cern.ch
        echo "WN missing VO_CMS_SW_DIR/OSG_APP/CVMFS environment variable, forcing it to CVMFS=$CVMFS"
        CMSSET_DEFAULT_PATH=$CVMFS/cmsset_default.sh
    else
        echo "Error during job bootstrap: VO_CMS_SW_DIR, OSG_APP, CVMFS or /cvmfs were not found." >&2
        echo "  Because of this, we can't load CMSSW. Not good." >&2
        exit 11003
    fi
    . $CMSSET_DEFAULT_PATH
    # # (dario FIXME): I think I added the folowing line for debugging only,
    # # (dario FIXME): I will remove it and see if anything breaks
    # echo "export CMSSET_DEFAULT_PATH=$CMSSET_DEFAULT_PATH" >> startup_environment.sh
    echo -e "========  CMS environment load finished at $(TZ=GMT date) ========\n"
}

setup_python_comp() {
    echo "======== python bootstrap for stageout at $(TZ=GMT date) STARTING ========"
    # Python library required for Python2/Python3 compatibility through "future"
    PY3_FUTURE_VERSION=0.18.2
    # Saving START_TIME and when job finishes END_TIME.
    START_TIME=$(date +%s)
    WMA_DEFAULT_OS=rhel7
    export JOBSTARTDIR=$PWD

    # First, decide which COMP ScramArch to use based on the required OS and Architecture
    THIS_ARCH=`uname -m`  # if it's PowerPC, it returns `ppc64le`
    # if this job can run at any OS, then use rhel7 as default
    if [ "$REQUIRED_OS" = "any" ]
    then
        WMA_SCRAM_ARCH=${WMA_DEFAULT_OS}_${THIS_ARCH}
    else
        WMA_SCRAM_ARCH=${REQUIRED_OS}_${THIS_ARCH}
    fi
    echo "Job requires OS: $REQUIRED_OS, thus setting ScramArch to: $WMA_SCRAM_ARCH"

    # WMCore
    suffix=etc/profile.d/init.sh
    if [ -d "$VO_CMS_SW_DIR"/COMP/"$WMA_SCRAM_ARCH"/external/python3 ]
    then
        prefix="$VO_CMS_SW_DIR"/COMP/"$WMA_SCRAM_ARCH"/external/python3
    elif [ -d "$OSG_APP"/cmssoft/cms/COMP/"$WMA_SCRAM_ARCH"/external/python3 ]
    then
        prefix="$OSG_APP"/cmssoft/cms/COMP/"$WMA_SCRAM_ARCH"/external/python3
    elif [ -d "$CVMFS"/COMP/"$WMA_SCRAM_ARCH"/external/python3 ]
    then
        prefix="$CVMFS"/COMP/"$WMA_SCRAM_ARCH"/external/python3
    else
        echo "Failed to find a COMP python3 installation in the worker node setup." >&2
        echo "  Without a known python3, there is nothing else we can do with this job. Quiting!" >&2
        exit 11004
    fi
    compPythonPath=`echo $prefix | sed 's|/python3||'`
    echo "WMAgent bootstrap: COMP Python path is: $compPythonPath"
    latestPythonVersion=`ls -t "$prefix"/*/"$suffix" | head -n1 | sed 's|.*/external/python3/||' | cut -d '/' -f1`
    pythonMajorVersion=`echo $latestPythonVersion | cut -d '.' -f1`
    pythonCommand="python"${pythonMajorVersion}
    echo "WMAgent bootstrap: latest python3 release is: $latestPythonVersion"
    source "$prefix/$latestPythonVersion/$suffix"
    echo "Sourcing python future library from: ${compPythonPath}/py3-future/${PY3_FUTURE_VERSION}/${suffix}"
    source "$compPythonPath/py3-future/${PY3_FUTURE_VERSION}/${suffix}"

    command -v $pythonCommand > /dev/null
    rc=$?
    if [[ $rc != 0 ]]
    then
        echo "Error during job bootstrap: python isn't available on the worker node." >&2
        echo "  WMCore/WMAgent REQUIRES at least python2" >&2
        exit 11005
    else
        echo "WMAgent bootstrap: found $pythonCommand at.."
        echo `which $pythonCommand`
    fi
    echo "======== python bootstrap for stageout at $(TZ=GMT date) FINISHED ========"
}
