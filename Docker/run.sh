#!/bin/bash
# This script prepare the host environment for running one CRAB service based on CRAB
# TaskWorker container image and runs it
# Currently those services include TaskWorker and Publisher_*

# This script is meant to be called in the Dockerfile CMD
# whoever issues the docker run command MUST :
# 1. pass two enviromental variables via the '-e ' option of 'docker run'
#   $SERVICE      : the name of the service to be run: TaskWorker, Publisher_schedd, Publisher_rucio etc.
#   $SERVICE_DIR  : the absolute path to the home directory of the service, where the proper
#                   scripts and configurations will be found, e.g. /data/srv/TaksManager
# 2. set the default current work directory at image start via the '-w' option of 'docker run'
#      so that the image starts in $SERVICE_DIR
# example (see also https://github.com/dmwm/CRABServer/blob/master/src/script/Container/runContainer.sh ) :
#   SERVICE=Publisher_schedd; DIR=Publisher
#   DOCKER_OPT="-e SERVICE=$SERVICE -e SERVICE_DIR=/data/srv/$DIR --w /data/srv/$DIR "
#   docker run  --name Publisher_schedd -d -ti  $DOCKER_OPT
#

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

# directories/files that should be created before starting the container :
# note a trick to do partial string match from https://unix.stackexchange.com/a/465906
if [[ $SERVICE == TaskWorker ]]; then
  # -/data/hostdisk/${SERVICE}/cfg/TaskWorkerConfig.py
  # -/data/hostdisk/${SERVICE}/logs
  declare -A links=( ["current/TaskWorkerConfig.py"]="/data/hostdisk/${SERVICE}/cfg/TaskWorkerConfig.py" ["logs"]="/data/hostdisk/${SERVICE}/logs" ["nohup.out"]="/data/hostdisk/${SERVICE}/nohup.out")
elif [[ $SERVICE == Publisher* ]]; then
  # -/data/hostdisk/${SERVICE}/cfg/PublisherConfig.py
  # -/data/hostdisk/${SERVICE}/logs
  # -/data/hostdisk/${SERVICE}/PublisherFiles
  declare -A links=( ["PublisherConfig.py"]="/data/hostdisk/${SERVICE}/cfg/PublisherConfig.py" ["logs"]="/data/hostdisk/${SERVICE}/logs" ["/data/srv/Publisher_files"]="/data/hostdisk/${SERVICE}/PublisherFiles" ["nohup.out"]="/data/hostdisk/${SERVICE}/nohup.out")
fi

for name in "${!links[@]}";
do
  check_link "${name}" || ln -s "${links[$name]}" "$name"
done

# if GH repositories location is not already defined, set a default
if ! [ -v GHrepoDir ]
then
  GHrepoDir='/data/hostdisk/repos'
fi

# run current instance
sh start.sh -c
