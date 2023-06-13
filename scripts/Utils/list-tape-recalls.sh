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
#source /cvmfs/cms.cern.ch/rucio/setup-py3.sh > /dev/null
export PYTHONPATH=$PYTHONPATH:/cvmfs/cms.cern.ch/rucio/x86_64/slc7/py3/current/lib/python3.6/site-packages/
export PYTHONPATH=/afs/cern.ch/user/b/belforte/WORK/CRAB3/CRABServer/src/python:$PYTHONPATH
export PYTHONPATH=/afs/cern.ch/user/b/belforte/WORK/CRAB3/WMCore/src/python:$PYTHONPATH
export RUCIO_HOME=/cvmfs/cms.cern.ch/rucio/current/
export RUCIO_ACCOUNT=`whoami`

mkdir -p /tmp/belforte
cd /tmp/belforte
rm -f CheckTapeRecall.py
wget -q https://github.com/dmwm/CRABServer/raw/master/scripts/Utils/CheckTapeRecall.py 
python3 CheckTapeRecall.py > check.log 2>&1
rc=$?
if ! [ $rc == "0" ] ; then
    echo "TapeRecalls check failed: here's log"
    cat check.log
else
    cp RecallRules.html /eos/home-b/belforte/www/RecallRules.html
fi

