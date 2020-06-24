#!/bin/bash

# runContainer.sh: script to pull specified CRAB TW image from defined repo and run it
# use TW_VERSION to specify TW version
# use TWREPO to specify docker hub repo, default is cmssw/crabtaskworker

# define help
if [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ] || [ "$1" == "help" ]; then
    echo "Usage: <TW_VERSION> runContainer.sh"
    echo "  use TW_VERSION to specify TW version"
    echo "  use TWREPO to specify docker hub repo, default is cmssw/crabtaskworker"
    echo "Examples:"
    echo "  # pull TW image from TWREPO for tag 3.3.2005.rc3 and run it"
    echo "  TW_VERSION=3.3.2005.rc3 ./runContainer.sh"
    echo "  # pull TW image from from your personal docker hub repo for tag 3.3.2005.rc3 and run it, X is name of docker repository"
    echo "  TWREPO=X TW_VERSION=3.3.2005.rc3 ./runContainer.sh"
    exit 1
fi

RED='\033[0;31m'
NC='\033[0m' #No Color

echo "TW_VERSION=$TW_VERSION"
repo=${TWREPO:-cmssw}
echo "repo=$repo"

#before starting new container check if nothing else is running
numContainers=`docker ps | grep crabtaskworker | wc -l`

if [ $numContainers -ne 0 ]; then
        echo -e "${RED}Multiple running containers found ($numContainers containers). Abandon this run!${NC}" && exit 1
else
        echo "Starting new container"
        docker run --name TaskWorker -d -ti --net host --privileged -v /cvmfs/cms.cern.ch/SITECONF:/cvmfs/cms.cern.ch/SITECONF  -v /data/srv/tmp:/data/srv/tmp -v /etc/grid-security/:/etc/grid-security/ -v /data/certs/:/data/certs/  -v /etc/vomses/:/etc/vomses/ -v /data/container/logs/:/data/srv/TaskManager/logs/ -v /data/container/cfg/:/data/srv/TaskManager/cfg/  $repo/crabtaskworker:$TW_VERSION
fi

#sleep for 3 seconds
echo "Sleeping for 3 seconds"
sleep 3

echo "Running containers"
docker ps
