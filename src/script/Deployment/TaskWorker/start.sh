#!/bin/bash

#==== PARSE ARGUMENTS
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

#==== DEFINE AN UTILITY FUNCTION
__strip_pythonpath(){
  # this function is used to strip the taskworker lines from $PYTHONPATH
  # in order for the debug | private calls to be able to add theirs

  local strip_reg=".*CRABTASKWORKER.*"
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
#==== SETUP ENVIRONMENT
# if TASKWORKER_HOME is already defined, use it
if [ -v TASKWORKER_HOME ]
then
  echo "TASKWORKER_HOME already set to $TASKWORKER_HOME. Will use that"
else
  thisScript=`realpath $0`
  myDir=`dirname ${thisScript}`
  export TASKWORKER_HOME=${myDir}  # where we run the TASKWORKER and where Config is
  echo "Define environment for TASKWORKER in $TASKWORKER_HOME"
fi

# the env.sh script will define $CRABTASKWORKER_ROOT and $CRABTASKWORKER_VERSION
source ${TASKWORKER_HOME}/env.sh

#==== CLEANUP OLD LOGS
rm `readlink nohup.out`
ln -s /data/hostdisk/${SERVICE}/nohup.out nohup.out

#==== START THE SERVICE

export CRYPTOGRAPHY_ALLOW_OPENSSL_102=true

case $MODE in
    current)
    # current mode: run current instance
    COMMAND_DIR=${TASKWORKER_ROOT}/lib/python3.8/site-packages/TaskWorker/
    CONFIG=${TASKWORKER_HOME}/current/TaskWorkerConfig.py
    if [ "$debug" = true ]; then
      python3 ${COMMAND_DIR}/SequentialWorker.py  $CONFIG --logDebug
    else
      nohup python3 ${COMMAND_DIR}/MasterWorker.py --config ${CONFIG} --logDebug &
    fi
  ;;
  private)
    # private mode: run private instance from ${GHrepoDir}
    __strip_pythonpath
    export PYTHONPATH=${GHrepoDir}/CRABServer/src/python:${GHrepoDir}/WMCore/src/python:$PYTHONPATH
    COMMAND_DIR=${GHrepoDir}/CRABServer/src/python/TaskWorker
    CONFIG=$TASKWORKER_HOME/current/TaskWorkerConfig.py
    if [ "$debug" = true ]; then
      python3 -m pdb ${COMMAND_DIR}/SequentialWorker.py ${CONFIG} --logDebug
    else
      nohup python3 ${COMMAND_DIR}/MasterWorker.py --config ${CONFIG} --logDebug &
    fi
esac

