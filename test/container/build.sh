#!/bin/bash

# 0. check parameters
echo "(DEBUG) input parameters:"
# IMAGE_TAG is the only variable used inside this script
echo "(DEBUG)   \- IMAGE_TAG: ${IMAGE_TAG}"
# GH_REPO and BRANCH are used by the jenkins bash script wrapper
echo "(DEBUG)   \- GH_REPO: ${GH_REPO}"
echo "(DEBUG)   \- BRANCH: ${BRANCH}"
echo "(DEBUG) end"


# 1. build and push the image
dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
echo $dir

# the next line is necessary when running the script from jenkins.
# if you build the image manually, just login to registry.cern.ch
# with your usualy CLI token.
docker login registry.cern.ch --username $HARBOR_CMSCRAB_USERNAME --password-stdin <<< $HARBOR_CMSCRAB_PASSWORD

docker build -t registry.cern.ch/cmscrab/crabtesting:$IMAGE_TAG $dir
docker push registry.cern.ch/cmscrab/crabtesting:$IMAGE_TAG 
docker rmi registry.cern.ch/cmscrab/crabtesting:$IMAGE_TAG 

