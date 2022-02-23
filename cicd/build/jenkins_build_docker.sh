#!/bin/bash

set -euo pipefail

echo "(DEBUG) variables from upstream jenkin job (CRABServer_BuildImage_20220127):"
echo "(DEBUG)   \- BRANCH: ${BRANCH}"
echo "(DEBUG)   \- RELEASE_TAG: ${RELEASE_TAG}"
echo "(DEBUG)   \- RPM_RELEASETAG_HASH: ${RPM_RELEASETAG_HASH}"
echo "(DEBUG) jenkin job's env variables:"
echo "(DEBUG)   \- WORKSPACE: $WORKSPACE"
echo "(DEBUG) end"

#build and push crabtaskworker image
git clone https://github.com/dmwm/CRABServer.git
cd CRABServer/Docker

#replace where RPMs are stored
sed -i.bak -e "/export REPO=*/c\export REPO=comp.crab_${BRANCH}" install.sh
echo "(DEBUG) diff dmwm/CRABServer/Docker/install.sh"
diff -u install.sh.bak install.sh
echo "(DEBUG) end"

# use cmscrab robot account credentials
export DOCKER_CONFIG=$PWD/docker_login
docker login registry.cern.ch --username $HARBOR_CMSCRAB_USERNAME --password-stdin <<< $HARBOR_CMSCRAB_PASSWORD

docker build . -t registry.cern.ch/cmscrab/crabtaskworker:${RELEASE_TAG} --network=host \
        --build-arg RELEASE_TAG=${RELEASE_TAG} \
        --build-arg RPM_RELEASETAG_HASH=${RPM_RELEASETAG_HASH}
docker push registry.cern.ch/cmscrab/crabtaskworker:${RELEASE_TAG}
docker rmi registry.cern.ch/cmscrab/crabtaskworker:${RELEASE_TAG}

##build and push crabserver image
#cd $WORKSPACE
#git clone https://github.com/dmwm/CMSKubernetes.git
#cd CMSKubernetes/docker/
#
##get HG version tag from comp.crab_${BRANCH} repo, e.g. HG2201a-cde79778caecdc06e9b316b5530c1da5
#HGVERSION=$(curl -s "http://cmsrep.cern.ch/cmssw/repos/comp.crab_${BRANCH}/slc7_amd64_gcc630/latest/RPMS.json" | grep -oP 'HG\d{4}(.*)(?=":)' | head -1)
#sed -i.bak -e "/REPO=\"comp*/c\REPO=\"comp.crab_${BRANCH}\"" -e "s/VER=HG.*/VER=$HGVERSION/g" -- crabserver/install.sh
#echo "(DEBUG) diff dmwm/CMSKubernetes/docker/crabserver/install.sh"
#diff -u crabserver/install.sh.bak crabserver/install.sh
#echo "(DEBUG) end"
#
#CMSK8STAG=${RELEASE_TAG} ./build.sh "crabserver"
