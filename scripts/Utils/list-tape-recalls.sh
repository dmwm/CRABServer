#!/bin/bash
uid=`id -u`
proxy=/tmp/x509up_u${uid}
cat /afs/cern.ch/user/b/belforte/.globus/.stefano | /usr/bin/voms-proxy-init -quiet -pwstdin -rfc -voms cms -valid 192:00 -out ${proxy} 2>/dev/null
rc=$?
if ! [ $rc == "0" ] ; then
    echo "proxy creation failed:"
    cat /afs/cern.ch/user/b/belforte/.globus/.stefano | /usr/bin/voms-proxy-init -quiet -pwstdin -rfc -voms cms -valid 192:00 -out ${proxy}
fi
export X509_USER_PROXY=$proxy

export PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/slc7/py3/current/lib/python3.6/site-packages/
export RUCIO_HOME=/cvmfs/cms.cern.ch/rucio/current/
export RUCIO_ACCOUNT=`whoami`

mkdir -p /tmp/belforte
cd /tmp/belforte
rm -rf CRABServer
rm -rf WMCore

git clone -q https://github.com/dmwm/CRABServer/ > /dev/null
git clone -q https://github.com/dmwm/WMCore > /dev/null

wmcver=`cat CRABServer/requirements.txt|grep wmcver|cut -d= -f3`
cd WMCore; git checkout -q $wmcver; cd - > /dev/null

export PYTHONPATH=`pwd`/CRABServer/src/python:$PYTHONPATH
export PYTHONPATH=`pwd`/WMCore/src/python:$PYTHONPATH

python3 `pwd`/CRABServer/scripts/Utils/CheckTapeRecall.py > check.log 2>&1
rc=$?

if ! [ $rc == "0" ] ; then
    echo "TapeRecalls check failed: here's log"
    cat check.log
else
    cp RecallRules.html /eos/project/c/cmsweb/www/CRAB/RecallRules.html
fi

