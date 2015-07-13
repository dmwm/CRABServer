#!/bin/bash

RCFILE=$PWD/paramsrc

## pass it a string containing a variable name
## checks if the variable is defined
function var_check() {
    echo -n "$1: "
    if [ "string${!1}" == "string" ]
    then
	echo "Error. Please specify a $1 variable in $RCFILE"
	return 1
    fi
    echo 'ok'
    return 0
}

echo "Checking paramsrc"

if [ ! -f $RCFILE ]
then
    echo "File $PWD/paramsrc not present"
    exit 1
fi

source $RCFILE

for V in ORACLEUSER ORACLEPASS REST_HOSTNAME COMPREPO ARCH
do
var_check $V || exit 1
done

var_check 'GITUSER'
if [ $? -ne 0 ]
then
    echo "The account should have cloned repo of CRABServer and WMCore (from https://github.com/dmwm)"
fi

var_check 'TW_HOSTNAME'
if [ $? -ne 0 ]
then
    TW_HOSTNAME=${REST_HOSTNAME}
fi

var_check 'GISTEXTURL'
if [ $? -ne 0 ]
then
    echo "If you don't have it yet, considering your current configuration, the content of the gist url should be the following:"
    cat <<EOF
{
    "cmsweb-dev": {
        "delegate-dn": [
            "/DC=ch/DC=cern/OU=computers/CN=vocms(045|052|021|031).cern.ch|/DC=ch/DC=cern/OU=computers/CN=$TW_HOSTNAME"
        ],
        "backend-urls" : {
            "cacheSSL" : "https://cmsweb-testbed.cern.ch/crabcache",
            "baseURL" : "https://cmsweb-testbed.cern.ch/crabcache",
            "htcondorSchedds" : ["crab3test-5@vocms0114.cern.ch"],
            "htcondorPool" : "vocms097.cern.ch,vocms099.cern.ch",
            "ASOURL" : "https://cmsweb-testbed.cern.ch/couchdb"
        },
        "compatible-version" : ["3.3.*"]
    }
}
EOF
    exit 1
fi
