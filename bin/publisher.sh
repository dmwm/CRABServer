#!/bin/bash
source /data/srv/asyncstageout/current/sw.pre.dciangot/slc6_amd64_gcc493/cms/asyncstageout/1.0.9pre1/etc/profile.d/init.sh
export X509_USER_PROXY=/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy
export PYTHONPATH="$PWD/pycurl/python/:$PYTHONPATH"

cd /data/user/MicroASO/microPublisher/python 
echo $1
echo $2
echo $3

python publisher.py $1 $2 $3 
