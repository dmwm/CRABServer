#!/bin/bash

unset X509_USER_PROXY
unset X509_USER_CERT
unset X509_USER_KEY
source /data/srv/TaskManager/env.sh

:> /data/srv/TaskManager/nohup.out

__strip_pythonpath(){
# this function is used to strip the taskworker lines from $PYTHONPATH
# in order for the debug |private calls to be able to add thiers

local strip_reg=".*crabtaskworker.*"
local ppath_init=${PYTHONPATH//:/: }
local ppath_stripped=""

for i in $ppath_init
do
    [[ $i =~ $strip_reg ]] || ppath_stripped="${ppath_stripped}${i}"
done
# echo -e "brfore strip: \n$ppath_init" |sed -e 's/\:/\:\n/g'
# echo -e "after strip: \n$ppath_stripped" |sed -e 's/\:/\:\n/g'
export PYTHONPATH=$ppath_stripped
}

case $1 in
    debug)
	__strip_pythonpath
	export PYTHONPATH=/data/user/CRABServer/src/python:/data/user/WMCore/src/python:$PYTHONPATH
	python -m pdb $MYTESTAREA/${scram_arch}/cms/crabtaskworker/*/lib/python2.7/site-packages/TaskWorker/SequentialWorker.py $MYTESTAREA/TaskWorkerConfig.py --logDebug
	;;
    private)
	__strip_pythonpath
	export PYTHONPATH=/data/user/CRABServer/src/python:/data/user/WMCore/src/python:$PYTHONPATH
	echo
	nohup python /data/user/CRABServer/src/python/TaskWorker/MasterWorker.py --config $MYTESTAREA/TaskWorkerConfig.py --logDebug &
	;;
    test)
        python $MYTESTAREA/${scram_arch}/cms/crabtaskworker/*/lib/python2.7/site-packages/TaskWorker/SequentialWorker.py  $MYTESTAREA/TaskWorkerConfig.py --logDebug
        ;;
    *)
	nohup python $MYTESTAREA/${scram_arch}/cms/crabtaskworker/*/lib/python2.7/site-packages/TaskWorker/MasterWorker.py --config $MYTESTAREA/TaskWorkerConfig.py --logDebug &
	;;
esac
