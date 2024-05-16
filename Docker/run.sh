#!/bin/bash
# This script prepare the host environment for running one CRAB service based on CRAB
# TaskWorker container image and runs it
# Currently those services include TaskWorker and Publisher_*

# This script is meant to be called in the Dockerfile CMD and must be run with the current
# working directory set to the home directory of the service, i.e. where the start.sh command is

# Whoever issues the docker run command MUST define the name of the service to be run
#  AND the work directory where it must be run. E.g. via the two env. variables:
#    $SERVICE   : the name of the service to be run: TaskWorker, Publisher_schedd, Publisher_rucio etc.
#    $DIRECTORY : the name of the current work directory where the image will start and where
#                  the proper scripts and configurations will be found, e.g. /data/srv/TaksManager
# which can be used as:
# 1. pass this enviromental variable via the '-e ' option of 'docker run'
# 2. set the default current work directory via the '-w ' option of 'docker run'
#
# example (see also https://github.com/dmwm/CRABServer/blob/master/src/script/Container/runContainer.sh ) :
#   SERVICE=Publisher_schedd; DIRECTORY=/data/srv/Publisher
#   DOCKER_OPT="-e SERVICE=$SERVICE -w $DIRECTORY"
#   docker run  --name ${SERVICE} ${DOCKER_OPT} ... other options
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
# [[...]] is a trick to do partial string match from https://unix.stackexchange.com/a/465906
if [[ $SERVICE == TaskWorker ]]; then
  declare -A links=(
      ["current/TaskWorkerConfig.py"]="./hostdisk/cfg/TaskWorkerConfig.py"
      ["nohup.out"]="./hostdisk/nohup.out"
  )
elif [[ $SERVICE == Publisher* ]]; then
  declare -A links=(
      ["PublisherConfig.py"]="./hostdisk/cfg/PublisherConfig.py"
      ["nohup.out"]="./hostdisk/nohup.out"
  )
fi

for name in "${!links[@]}";
do
  check_link "${name}" || ln -s "$(realpath "${links[$name]}")" "$name"
done

# run current instance
sh start.sh -c
