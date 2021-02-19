#!/bin/bash
# cleanup environment
unset X509_USER_PROXY
unset X509_USER_CERT
unset X509_USER_KEY

# if $SERVICE is defined, I am running in a container
if [ container ]; then
  # ensure container has needed mounts
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
fi

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

source ${TASKWORKER_HOME}/env.sh
# env.sh defines $CRABTASKWORKER_ROOT and $CRABTASKWORKER_VERSION
# those horribly long names derive from names of spec files in CMSDIST, too much history and
# dependencies to think of changing to remove the CRAB string or other simplifications

# run current instance
rm -f ${TASKWORKER_HOME}/nohup.out
nohup python $CRABTASKWORKER_ROOT/lib/python2.7/site-packages/TaskWorker/MasterWorker.py --config $TASKWORKER_HOME/TaskWorkerConfig.py --logDebug &
