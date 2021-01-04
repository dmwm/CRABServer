#!/bin/bash

unset X509_USER_PROXY
unset X509_USER_CERT
unset X509_USER_KEY
source /data/srv/TaskManager/env.sh

rm -f /data/hostdisk/${SERVICE}/nohup.out

check_link(){
# function checks if symbolic links required to start service exists and if they are not broken

  if [ -L $1 ] ; then
    if [ -e $1 ] ; then
       return 0
    else
       unlink $1
       return 1
    fi
  else
    return 1
  fi
}

#directories/files that should be created before starting the container (SERVICE will be set to 'TaskWorker'):
# -/data/hostdisk/${SERVICE}/cfg/TaskWorkerConfig.py
# -/data/hostdisk/${SERVICE}/logs
declare -A links=( ["current/TaskWorkerConfig.py"]="/data/hostdisk/${SERVICE}/cfg/TaskWorkerConfig.py" ["logs"]="/data/hostdisk/${SERVICE}/logs" ["nohup.out"]="/data/hostdisk/${SERVICE}/nohup.out")

for name in "${!links[@]}";
do
  check_link "${name}" || ln -s "${links[$name]}" "$name"
done

__strip_pythonpath(){
# this function is used to strip the taskworker lines from $PYTHONPATH
# in order for the debug |private calls to be able to add theirs

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
    # use private instance from /data/user in pdb mode via SequentialWorker
    __strip_pythonpath
    export PYTHONPATH=/data/user/CRABServer/src/python:/data/user/WMCore/src/python:$PYTHONPATH
    python -m pdb /data/user/CRABServer/src/python/TaskWorker/SequentialWorker.py $MYTESTAREA/TaskWorkerConfig.py --logDebug
	;;
  private)
    # run private instance from /data/user
    __strip_pythonpath
    export PYTHONPATH=/data/user/CRABServer/src/python:/data/user/WMCore/src/python:$PYTHONPATH
    nohup python /data/user/CRABServer/src/python/TaskWorker/MasterWorker.py --config $MYTESTAREA/TaskWorkerConfig.py --logDebug &
	;;
  test)
    # use current instance in pdb mode  via SequentialWorker
    python $MYTESTAREA/${scram_arch}/cms/crabtaskworker/*/lib/python2.7/site-packages/TaskWorker/SequentialWorker.py  $MYTESTAREA/TaskWorkerConfig.py --logDebug
  ;;
  help)
    echo "There are 4 ways to run start.sh:"
    echo "  start.sh             without any argument starts current instance"
    echo "  start.sh private     starts the instance from /data/user/CRABServer"
    echo "  start.sh debug       runs private instance in debub mode. For hacking"
    echo "  start.sh test        runs current instance in debug mode. For finding out"
    echo "BEWARE: a misspelled argument is interpreted like no argument"
  ;;
  *)
  # DEFAULT mode: run current instance
	nohup python $MYTESTAREA/${scram_arch}/cms/crabtaskworker/*/lib/python2.7/site-packages/TaskWorker/MasterWorker.py --config $MYTESTAREA/TaskWorkerConfig.py --logDebug &
	;;
esac
