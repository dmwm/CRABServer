#!/bin/bash

helpFunction(){
  echo -e "\nUsage example: ./start.sh -c | -g [-d]"
  echo -e "\t-c start current TW instance"
  echo -e "\t-g start TW instance from GitHub repo"
  echo -e "\t-d start TW in debug mode. Option can be combined with -c or -g"
  exit 1
  }

while getopts ":dDcCgGhH" opt
do
    case "$opt" in
      h|H) helpFunction ;;
      g|G) MODE="private" ;;
      c|C) MODE="current" ;;
      d|D) debug=true ;;
      * ) echo "Unimplemented option: -$OPTARG"; helpFunction ;;
    esac
done

if ! [ -v MODE ]; then
  echo "Please set how you want to start TW (add -c or -g option)." && helpFunction
fi

unset X509_USER_PROXY
unset X509_USER_CERT
unset X509_USER_KEY
source /data/srv/TaskManager/env.sh

rm -f nohup.out

# if GH repositories location is not already defined, set a default
if ! [ -v GHrepoDir ]
then
  GHrepoDir='/data/hostdisk/repos'
fi

__strip_pythonpath(){
  # this function is used to strip the taskworker lines from $PYTHONPATH
  # in order for the debug | private calls to be able to add theirs

  local strip_reg=".*crabtaskworker.*"
  local ppath_init=${PYTHONPATH//:/: }
  local ppath_stripped=""

  for i in $ppath_init
  do
      [[ $i =~ $strip_reg ]] || ppath_stripped="${ppath_stripped}${i}"
  done
  # echo -e "before strip: \n$ppath_init" |sed -e 's/\:/\:\n/g'
  # echo -e "after strip: \n$ppath_stripped" |sed -e 's/\:/\:\n/g'
  export PYTHONPATH=$ppath_stripped
}

ln -s /data/hostdisk/${SERVICE}/nohup.out nohup.out

case $MODE in
  private)
    # private mode: run private instance from ${GHrepoDir}
    __strip_pythonpath
    export PYTHONPATH=${GHrepoDir}/CRABServer/src/python:${GHrepoDir}/WMCore/src/python:$PYTHONPATH
    if [ "$debug" = true ]; then
      python -m pdb ${GHrepoDir}/CRABServer/src/python/TaskWorker/SequentialWorker.py $TASKWORKER_HOME/current/TaskWorkerConfig.py --logDebug
    else
      nohup python ${GHrepoDir}/CRABServer/src/python/TaskWorker/MasterWorker.py --config $TASKWORKER_HOME/current/TaskWorkerConfig.py --logDebug &
    fi
  ;;
  current)
  # current mode: run current instance
  if [ "$debug" = true ]; then
    python $CRABTASKWORKER_ROOT/lib/python2.7/site-packages/TaskWorker/SequentialWorker.py  $TASKWORKER_HOME/current/TaskWorkerConfig.py --logDebug
  else
    nohup python $CRABTASKWORKER_ROOT/lib/python2.7/site-packages/TaskWorker/MasterWorker.py --config $TASKWORKER_HOME/current/TaskWorkerConfig.py --logDebug &
  fi
esac

