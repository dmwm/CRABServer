#!/bin/bash

# runContainer.sh: script to pull specified CRAB TW image from defined repo and run it

helpFunction(){
  echo -e "\nUsage example: ./runContainer.sh -v v3.201118 -s TaskWorker"
  echo -e "\t-v TW/Publisher version"
  echo -e "\t-s which service should be started: Publisher, Publisher_schedd, Publisher_asoless, Publisher_rucio or TaskWorker"
  echo -e "\t-r docker hub repo, if not provided, default points to 'cmssw'"
  exit 1
  }

while getopts ":v:s:r:h" opt
do
    case "$opt" in
      h) helpFunction ;;
      v) TW_VERSION="$OPTARG" ;;
      s) SERVICE="$OPTARG" ;;
      r) TW_REPO="$OPTARG" ;;
      :) echo "$0: -$OPTARG needs a value"; helpFunction ;;
      * ) echo "Unimplemented option: -$OPTARG"; helpFunction ;;
    esac
done

if [ -z "${TW_VERSION}" ] || [ -z "${SERVICE}" ]; then
  echo "Make sure to set both -v and -s variables." && helpFunction
fi

#list of directories that should exist on the host machine before container start
dir=("/data/container/${SERVICE}/cfg" "/data/container/${SERVICE}/logs")

case $SERVICE in
  TaskWorker*)
    DIRECTORY='TaskManager'
    ;;
  Publisher*)
    DIRECTORY='Publisher'
    dir+=("/data/container/${SERVICE}/PublisherFiles")
    ;;
  *)
    echo "$SERVICE is not a valid service to start. Specify whether you want to start one of the 'Publisher' variants or 'TaskWorker'." && helpFunction
esac

for d in "${dir[@]}"; do
  if ! [ -e "$d" ]; then
    echo "Make sure to create needed directories before starting container. Missing directory: $d" && exit 1
  fi
done

DOCKER_VOL="-v /data/container/:/data/hostdisk/ -v /data/srv/tmp/:/data/srv/tmp/ -v /cvmfs/cms.cern.ch/SITECONF:/cvmfs/cms.cern.ch/SITECONF -v /etc/grid-security/:/etc/grid-security/ -v /data/certs/:/data/certs/ "
DOCKER_OPT="-e SERVICE=${SERVICE} -w /data/srv/${DIRECTORY} "
docker run --name ${SERVICE} -d -ti --net host --privileged $DOCKER_OPT $DOCKER_VOL ${TW_REPO:-cmssw}/crabtaskworker:${TW_VERSION}

echo -e "Sleeping for 3 seconds.\nRunning containers:"
sleep 3 && docker ps

