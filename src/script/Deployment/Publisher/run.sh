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

  #directories/files that should be created before starting the container, (SERVICE is type of Publisher started, e.g.Publisher_rucio):
  # -/data/hostdisk/${SERVICE}/cfg/PublisherConfig.py
  # -/data/hostdisk/${SERVICE}/logs
  # -/data/hostdisk/${SERVICE}/PublisherFiles
  declare -A links=( ["PublisherConfig.py"]="/data/hostdisk/${SERVICE}/cfg/PublisherConfig.py" ["logs"]="/data/hostdisk/${SERVICE}/logs" ["/data/srv/Publisher_files"]="/data/hostdisk/${SERVICE}/PublisherFiles" ["nohup.out"]="/data/hostdisk/${SERVICE}/nohup.out")

  for name in "${!links[@]}";
  do
    check_link "${name}" || ln -s "${links[$name]}" "$name"
  done
fi

# if PUBLISHER_HOME is already defined, use it
if [ -v PUBLISHER_HOME ]
then
  echo "PUBLISHER_HOME already set to $PUBLISHER_HOME. Will use that"
else
  thisScript=`realpath $0`
  myDir=`dirname ${thisScript}`
  export PUBLISHER_HOME=${myDir}  # where we run the Publisher and where Config is
  echo "Define environment for Publisher in $PUBLISHER_HOME"
fi

source ${PUBLISHER_HOME}/env.sh
# env.sh defines $PUBLISHER_ROOT and $PUBLISHER_VERSION

# run current instance
rm -f /data/hostdisk/${SERVICE}/nohup.out
nohup python $PUBLISHER_ROOT/lib/python2.7/site-packages/Publisher/PublisherMaster.py --config $PUBLISHER_HOME/PublisherConfig.py &
