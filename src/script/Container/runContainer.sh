#!/bin/bash

# runContainer.sh: script to pull specified CRAB TW image from defined repo and run it

##H  Usage example: ./runContainer.sh -v v3.201118 -s TaskWorker"
##H      -v TW/Publisher version"
##H      -s which service should be started: Publisher, Publisher_schedd, Publisher_asoless, Publisher_rucio or TaskWorker"
##H      -r docker hub repo, if not provided, default points to 'cmssw'"
##H      -c command that overrides CMD specified in the dockerfile"

set -euo pipefail

helpFunction() {
    grep "^##H" "${0}" | sed -r "s/##H(| )//g"
}

while getopts ":v:s:r:h:c:u:" opt
do
    case "$opt" in
      h) helpFunction ;;
      v) TW_VERSION="$OPTARG" ;;
      s) SERVICE="$OPTARG" ;;
      r) TW_REPO="$OPTARG" ;;
      c) COMMAND="$OPTARG" ;;
      u) LOGUUID="$OPTARG" ;;
      :) echo "$0: -$OPTARG needs a value"; helpFunction; exit 1;;
      * ) echo "Unimplemented option: -$OPTARG"; helpFunction; exit 1 ;;
    esac
done

if [ -z "${TW_VERSION}" ] || [ -z "${SERVICE}" ]; then
  echo "Make sure to set both -v and -s variables."; helpFunction; exit 1
fi
# define vars but set initial value to empty strgin to prevent unbound error
TW_REPO=${TW_REPO:-}
COMMAND=${COMMAND:-}
LOGUUID=${LOGUUID:-}

#list of directories that should exist on the host machine before container start
dir=("/data/container/${SERVICE}/cfg" "/data/container/${SERVICE}/logs")

case $SERVICE in
  TaskWorker_monit_*)
    DIRECTORY='monit'
    ;;
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

volumeMounts=()
for d in "${dir[@]}"; do
  if [ -e "$d" ]; then
      volumeMounts+=("-v ${d}:/data/srv/${DIRECTORY}/$(basename "${d}")")
  else
    echo "Make sure to create needed directories before starting container. Missing directory: $d" && exit 1
  fi
done

uuid=$(uuidgen)
if [ ! -z "${LOGUUID}" ]; then
    # if LOGUUID is set, then use it
    uuid=${LOGUUID}
fi
tmpfile=/tmp/monit-${uuid}.txt

if [[ "${SERVICE}" == TaskWorker_monit_*  ]]; then
  if docker ps | grep ${SERVICE} | wc -l ; then
    msg="There already is a running container for $SERVICE. It is likely stuck. Stopping, removing and then starting again."
    echo $msg
    # writing now that the previous execution of the script is hanging.
    # if this execution is ok, then we forget about the previous failure,
    # otherwise we keep track in /tmp/monit-*.txt that we are having problems
    echo $msg > $tmpfile
    docker container stop $SERVICE
  fi
  if docker ps -a | grep ${SERVICE} | wc -l ; then
    docker container rm $SERVICE
  fi
fi

# get os version
OS_Version=$(cat /etc/os-release |grep VERSION_ID|cut -d= -f2|tr -d \"|cut -d. -f1)

DOCKER_VOL="-v /data/container/:/data/hostdisk/ ${volumeMounts[@]} -v /data/srv/tmp/:/data/srv/tmp/"
DOCKER_VOL="${DOCKER_VOL} -v /data/container/${SERVICE}:/data/srv/${DIRECTORY}/hostdisk"
DOCKER_VOL="${DOCKER_VOL} -v /cvmfs:/cvmfs:shared" # https://cvmfs.readthedocs.io/en/stable/cpt-configure.html#bind-mount-from-the-host
DOCKER_VOL="${DOCKER_VOL} -v /etc/grid-security/:/etc/grid-security/"
DOCKER_VOL="${DOCKER_VOL} -v /etc/vomses/:/etc/vomses/"
DOCKER_VOL="${DOCKER_VOL} -v /data/certs/:/data/certs/"
DOCKER_VOL="${DOCKER_VOL} -v /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem:/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
DOCKER_VOL="${DOCKER_VOL} -v /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem:/etc/pki/tls/certs/ca-bundle.crt"
if [[ "$OS_Version" = "7" ]]; then
    DOCKER_VOL="${DOCKER_VOL} -v /var/run/nscd/socket:/var/run/nscd/socket"
fi

DOCKER_OPT="-e SERVICE=${SERVICE} -w /data/srv/${DIRECTORY} "

DOCKER_IMAGE=${TW_REPO:-registry.cern.ch/cmscrab}/crabtaskworker:${TW_VERSION}

if [[ "${SERVICE}" == TaskWorker_monit_*  ]]; then
  echo "TaskWorker_monit_* detected"
  # # the following two bind mounts are necessary to write to eos. it has been superseded by sending data to opensearch
  # DOCKER_OPT="${DOCKER_OPT} -v /eos/project-c/cmsweb/www/CRAB/:/data/eos "
  # DOCKER_OPT="${DOCKER_OPT} -v /tmp/krb5cc_1000:/tmp/krb5cc_1000 "

  # - monit script does not work with `-di` option inside crontab.
  # - in order to get the exit code of the command run inside docker container,
  #   remove the `-d` option for monit crontabs
else
  # start docker container in background when you run TW and Published, not monitoring scripts.
  DOCKER_OPT="${DOCKER_OPT} -d"
fi

docker run --name ${SERVICE} -t --net host --privileged $DOCKER_OPT $DOCKER_VOL $DOCKER_IMAGE $COMMAND > $tmpfile
if [ $? -eq 0 ]; then
  # if the crontab does not fail, remove the log file
  rm -f $tmpfile
fi

echo -e "Sleeping for 3 seconds.\nRunning containers:"
sleep 3 && docker ps
