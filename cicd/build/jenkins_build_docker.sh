#!/bin/bash

set -euo pipefail
set -x

if [ -z ${IMAGE_TAG} ]; then
  IMAGE_TAG=${RELEASE_TAG}
fi

echo "(DEBUG) variables from upstream jenkin job (CRABServer_BuildImage_20220127):"
echo "(DEBUG)   \- BRANCH: ${BRANCH}"
echo "(DEBUG)   \- RELEASE_TAG: ${RELEASE_TAG}"
echo "(DEBUG)   \- RPM_RELEASETAG_HASH: ${RPM_RELEASETAG_HASH}"
echo "(DEBUG)   \- IMAGE_TAG: $IMAGE_TAG"
echo "(DEBUG)   \- CI_CRABSERVER_REPO: ${CI_CRABSERVER_REPO}"
echo "(DEBUG)   \- CI_CRABSERVER_BRANCH: ${CI_CRABSERVER_BRANCH}"
echo "(DEBUG) jenkin job's env variables:"
echo "(DEBUG)   \- WORKSPACE: $WORKSPACE"
echo "(DEBUG) end"

export CI_CRABSERVER_REPO=${CI_CRABSERVER_REPO:-https://github.com/dmwm/CRABServer.git}
export CI_CRABSERVER_BRANCH=${CI_CRABSERVER_BRANCH:-master}
git clone "${CI_CRABSERVER_REPO}" -b "${CI_CRABSERVER_BRANCH}" CRABServer
cd CRABServer/Docker

# use docker config on our WORKSPACE area, avoid replace default creds in ~/.docker that many pipeline depend on it
export DOCKER_CONFIG=$PWD/docker_login

#replace where RPMs are stored
sed -i.bak -e "/export REPO=*/c\export REPO=comp.crab_${BRANCH}" install.sh
echo "(DEBUG) diff dmwm/CRABServer/Docker/install.sh"
ls .
diff -u install.sh.bak install.sh || true
echo "(DEBUG) end"

# use cmscrab robot account credentials
docker login registry.cern.ch --username $HARBOR_CMSCRAB_USERNAME --password-stdin <<< $HARBOR_CMSCRAB_PASSWORD

docker build . -t registry.cern.ch/cmscrab/crabtaskworker:${IMAGE_TAG} --network=host \
        --build-arg RELEASE_TAG=${RELEASE_TAG} \
        --build-arg RPM_RELEASETAG_HASH=${RPM_RELEASETAG_HASH}
docker push registry.cern.ch/cmscrab/crabtaskworker:${IMAGE_TAG}
docker rmi registry.cern.ch/cmscrab/crabtaskworker:${IMAGE_TAG}

#build and push crabserver image
cd $WORKSPACE
git clone https://github.com/dmwm/CMSKubernetes.git
cd CMSKubernetes/docker/

#get HG version tag from comp.crab_${BRANCH} repo, e.g. HG2201a-cde79778caecdc06e9b316b5530c1da5
HGVERSION=$(curl -s "http://cmsrep.cern.ch/cmssw/repos/comp.crab_${BRANCH}/slc7_amd64_gcc630/latest/RPMS.json" | grep -oP 'HG\d{4}(.*)(?=":)' | head -1)
sed -i.bak -e "/REPO=\"comp*/c\REPO=\"comp.crab_${BRANCH}\"" -e "s/VER=HG.*/VER=$HGVERSION/g" -- crabserver/install.sh
echo "(DEBUG) diff dmwm/CMSKubernetes/docker/crabserver/install.sh"
diff -u crabserver/install.sh.bak crabserver/install.sh || true
echo "(DEBUG) end"

# relogin to using cmsweb robot account
docker login registry.cern.ch --username $HARBOR_CMSWEB_USERNAME --password-stdin <<< $HARBOR_CMSWEB_PASSWORD
CMSK8STAG=${IMAGE_TAG} ./build.sh "crabserver"
