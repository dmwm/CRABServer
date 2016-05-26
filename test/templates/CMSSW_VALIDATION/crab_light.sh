#!/usr/bin/env bash
if [ -z "$CRAB_SOURCE_SCRIPT" ]; then
    CRAB_SOURCE_SCRIPT="/cvmfs/cms.cern.ch/crab3/crab.sh"
fi

function getVariableValue {
    VARNAME=$1
    SUBDIR=$2
    sh -c "source $CRAB_SOURCE_SCRIPT >/dev/null 2>/dev/null; if [ \$? -eq 0 ] && [ -d $VARNAME ]; \
                    then echo $VARNAME/$SUBDIR; else exit 1; fi"
}

CRAB3_BIN_ROOT=$(getVariableValue \$CRABCLIENT_ROOT bin)
CRAB3_ETC_ROOT=$(getVariableValue \$CRABCLIENT_ROOT etc)
CRAB3_PY_ROOT=$(getVariableValue \$CRABCLIENT_ROOT \$PYTHON_LIB_SITE_PACKAGES)
DBS3_PY_ROOT=$(getVariableValue \$DBS3_CLIENT_ROOT \$PYTHON_LIB_SITE_PACKAGES)
DBS3_PYCURL_ROOT=$(getVariableValue \$DBS3_PYCURL_CLIENT_ROOT \$PYTHON_LIB_SITE_PACKAGES)

if [ $# -gt 0 ]; then
    if [ "x$1" == "x-csh" ]; then
        echo "setenv PYTHONPATH $PYTHONPATH:$CRAB3_PY_ROOT:$DBS3_PY_ROOT:$DBS3_PYCURL_ROOT; \
                        setenv PATH $PATH:$CRAB3_BIN_ROOT"
    else
        export PYTHONPATH=$PYTHONPATH:$CRAB3_PY_ROOT:$DBS3_PY_ROOT:$DBS3_PYCURL_ROOT
        export PATH=$PATH:$CRAB3_BIN_ROOT
        if [ -n "$BASH" ]; then
            source $CRAB3_ETC_ROOT/crab-bash-completion.sh
        fi
    fi
else
    export PYTHONPATH=$PYTHONPATH:$CRAB3_PY_ROOT:$DBS3_PY_ROOT:$DBS3_PYCURL_ROOT
    export PATH=$PATH:$CRAB3_BIN_ROOT
    if [ -n "$BASH" ]; then
        source $CRAB3_ETC_ROOT/crab-bash-completion.sh
    fi
fi
