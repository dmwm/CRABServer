#!/bin/bash
uid=`id -u`
proxy=/tmp/x509up_u${uid}
cat /afs/cern.ch/user/b/belforte/.globus/.stefano | /usr/bin/voms-proxy-init -quiet -pwstdin -rfc -voms cms -valid 192:00 -out ${proxy} 2>/dev/null
rc=$?
if ! [ $rc == "0" ] ; then
    echo "proxy creation failed:"
    cat /afs/cern.ch/user/b/belforte/.globus/.stefano | /usr/bin/voms-proxy-init -quiet -pwstdin -rfc -voms cms -valid 192:00 -out ${proxy}
fi
source /cvmfs/cms.cern.ch/rucio/setup-py3.sh > /dev/null
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

