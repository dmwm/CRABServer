#!/bin/bash

set -x

echo "(DEBUG) git version: " $(git --version)
echo "(DEBUG) variables from upstream jenkin job (github-webhook):"
# echo "(DEBUG)   \- REPOSITORY: $REPOSITORY" # (not used)
# echo "(DEBUG)   \- EVENT: $EVENT" # (not used)
# echo "(DEBUG)   \- ACTION: $ACTION" # (not used)
# echo "(DEBUG)   \- TITLE: $TITLE" # (not used)
echo "(DEBUG)   \- RELEASE_TAG: $RELEASE_TAG"  # v3.211111, py3.220124
echo "(DEBUG)   \- CRABSERVER_REPO: $CRABSERVER_REPO"  # dmwm, belforte, mapellidario, ...
echo "(DEBUG)   \- WMCORE_REPO: $WMCORE_REPO"  # dmwm, belforte, mapellidario, ...
echo "(DEBUG)   \- WMCORE_TAG: $WMCORE_TAG"  # <empty>, 1.5.7, ...
echo "(DEBUG)   \- BRANCH: $BRANCH" # <empty>, master, ...
echo "(DEBUG)   \- PAYLOAD: $PAYLOAD"
# example of PAYLOAD: https://dmapelli.web.cern.ch/public/crab/20220127/crabserver_github_release_payload_example.json
echo "(DEBUG) end"

if [[ -z $CRABSERVER_REPO ]]; then echo '$CRABSERVER_REPO' "is empty, exiting"; exit 1; fi
if [[ -z $WMCORE_REPO ]]; then echo '$WMCORE_REPO' "is empty, exiting"; exit 1; fi

#do a clean up
docker system prune -af

#clone directories
git clone -b V00-33-XX https://github.com/cms-sw/pkgtools.git
git clone https://github.com/cms-sw/cmsdist.git && cd cmsdist && git checkout comp_gcc630
git clone https://github.com/$CRABSERVER_REPO/CRABServer.git

if [[ -n  ${PAYLOAD} ]]; then
   #if the payload is not empty, get CRABServer branch name from the payload. 
   #the payload is not empty when this job is triggered by github-webhook,
   #if this job is launched manually, then the branch will have the value set
   #as input env variable.
   #paylod has a lot of information, but we are only interested in extracting this info: <...>"target_commitish": "python3"<...>
   #regex would extract 'python3' as a BRANCH name
   export BRANCH=$(echo "${PAYLOAD}" | grep -oP '(?<="target_commitish":\s")([^\s]+)(?=", ")')
else
  # when the payload is empty, it means that this job is launched manually.
  if [[ -z $BRANCH ]] && [[ -z $RELEASE_TAG ]]; then echo "Both BRANCH and RELEASE_TAG are empty, exit"; exit 1; fi
  if [[ -z $BRANCH ]] && [[ -n $RELEASE_TAG ]]; then
    # $RELEASE_TAG non empty: we extract the branch name from the $RELEASE_TAG
    cd CRABServer
    echo "(DEBUG) git branch -a --contains tags/$RELEASE_TAG: $(git branch -a --contains tags/$RELEASE_TAG)"
    export BRANCH=$(git branch -a --contains tags/$RELEASE_TAG | sed "s/*//g" | sed "s/ //g" | grep origin | awk -F "/" '{print $3}')
    cd ..
  fi
  if [[ -n $BRANCH ]] && [[ -z $RELEASE_TAG ]]; then
    # $BRANCH non empty: we use the latest commit hash as $RELEASE_TAG
    echo "this could work in theory, but currently breaks pkgtools/cmsBuild"
    echo "at line: https://github.com/cms-sw/pkgtools/blob/V00-34-XX/cmsBuild#L553"
    echo "exiting"
    exit 1
    ## if they update cmsBuild, then we can do it with the next lines
    # cd CRABServer
    # git checkout $BRANCH
    # export RELEASE_TAG=$(git log --format="%H" -n 1)
    # cd ..
  fi
fi
if [[ -z $RELEASE_TAG ]]; then echo '$RELEASE_TAG' "is empty, exiting"; exit 1; fi
if [[ -z $BRANCH ]]; then echo '$BRANCH' "is empty, exiting"; exit 1; fi
if [[ ! $BRANCH =~ ^[a-zA-Z0-9_]*$ ]]; then 
  # $BRANCH can contain only alfanumeric characters and the underscore "_". for example, the dash "-" is not allowed
  # otherwise, cms-build will fail and no rpm will be uploaded to the repository.
  echo 'ERROR! $BRANCH does not match the regex ^[a-zA-Z0-9_]*$'
  echo '       cmsBuild would fail uploading the rpm to the repo with: '
  echo "       > ERROR: Tmp upload repository name 'crab_20220816-jobwr-sq-0' contains invalid characters. Allowed characters are: a-zA-Z0-9_"
  echo '       Therefore, we are aborting.'
  exit 1
fi
echo "BRANCH=${BRANCH}" >> $WORKSPACE/properties_file

#select the WMCore tag
if [ ${WMCORE_REPO} == "dmwm" ]; then
   WMCORE_TAG=$(grep -oP "wmcver==\K.*" CRABServer/requirements.txt)
fi
if [[ -z $WMCORE_TAG ]]; then echo '$WMCORE_TAG' "is empty, exiting"; exit 1; fi


echo "(DEBUG) env variables that could have been updated from the default:"
echo "(DEBUG)   \- RELEASE_TAG: $RELEASE_TAG"  # v3.211111, py3.220124
echo "(DEBUG)   \- BRANCH (crabserver): $BRANCH"
echo "(DEBUG)   \- WMCORE_TAG: $WMCORE_TAG"

##### All the required env variables are set at this point. Do not set any input below this line
#update .spec files with new CRABServer and  WMCore tags; update from which branch RPMs should be built
cp crabserver.spec crabserver.spec.bak 
cp crabtaskworker.spec crabtaskworker.spec.bak 
sed -i -e "s/### RPM cms crabserver.*/### RPM cms crabserver ${RELEASE_TAG}/g" -- crabserver.spec
sed -i -e "s/### RPM cms crabtaskworker.*/### RPM cms crabtaskworker ${RELEASE_TAG}/g" -- crabtaskworker.spec
sed -i -e "s/^\( *%define crabrepo  *\)[^ ]*\(.*\)*$/\1${CRABSERVER_REPO}\2/" -- crabtaskworker.spec crabserver.spec
sed -i -e "s/^\( *%define wmcrepo  *\)[^ ]*\(.*\)*$/\1${WMCORE_REPO}\2/" -- crabtaskworker.spec crabserver.spec
sed -i -e "s/^\( *%define wmcver  *\)[^ ]*\(.*\)*$/\1${WMCORE_TAG}\2/" -- crabtaskworker.spec crabserver.spec
# we do not need to replace the branch name in the github URLs in the specfiles, for example here:
# https://github.com/cms-sw/cmsdist/blob/aa4897a3d70514b0973008d693e3a6e1009afe4d/crabserver.spec#L22
# i.e. we can leave `obj=master` whatever the value of $BRANCH is.
# read https://github.com/dmwm/CRABServer/issues/7357 for more info.

echo "(DEBUG) diff cms-sw/cmsdist/crabserver.spec"
diff -u crabserver.spec.bak crabserver.spec
echo "(DEBUG) diff cms-sw/cmsdist/crabtaskworker.spec"
diff -u crabtaskworker.spec.bak crabtaskworker.spec
echo "(DEBUG) end"

cd ..

#Build and upload RPMs to comp.crab_${BRANCH} repository
./pkgtools/cmsBuild -c cmsdist --repository comp -a slc7_amd64_gcc630 --builders 8 -j 5 --work-dir w build comp | tee logBuild
./pkgtools/cmsBuild -c cmsdist --repository comp -a slc7_amd64_gcc630 --upload-tmp-repository crab_${BRANCH} --builders 8 -j 5 --work-dir w upload comp | tee logUpload


#Check if RPMs have been uploaded to comp.crab_${BRANCH} repository
#RPM_RELEASETAG_HASH represents full version with hash, i.e. py3.211215-478c8f9ffd5f0a6ab9e470e1a80fff5e
#RPM_RELEASETAG represents only version from CRABServer GH repo without the hash, i.e. py3.211215
# if the rpm build and upload have been successfull, RPM_RELEASETAG should be
# equal to RELEASE_TAG
RPM_RELEASETAG_HASH=$(curl -s http://cmsrep.cern.ch/cmssw/repos/comp.crab_${BRANCH}/slc7_amd64_gcc630/latest/RPMS.json | grep -oP '(?<=crabtaskworker\+)(.*)(?=":)' | head -1)
RPM_RELEASETAG=$(echo ${RPM_RELEASETAG_HASH} | awk -F"-" '{print $1}') # assuming that there is no '-' in the version tag
echo "RPM_RELEASETAG_HASH=${RPM_RELEASETAG_HASH}" >> $WORKSPACE/properties_file

echo "(DEBUG) new env variable defined:"
echo "(DEBUG)   \- RPM_RELEASETAG: $RPM_RELEASETAG"
echo "(DEBUG)   \- RPM_RELEASETAG_HASH: $RPM_RELEASETAG_HASH"
echo "(DEBUG) end"

if [ "${RELEASE_TAG}" == "${RPM_RELEASETAG}" ]; then
	echo "RPMs have been successfully uploaded to repository"
else
   echo "ERROR: RMP build/upload failed"
fi

# we use the inputs to build a unique docker image tag, so that we only pass
# this value to the downstream job, not all the inputs.
IMAGE_TAG=""
if [ ${CRABSERVER_REPO} == "dmwm" ]; then
  IMAGE_TAG=${RELEASE_TAG}
else
  IMAGE_TAG="crab_"${CRABSERVER_REPO}"_"${RELEASE_TAG}
fi
if [ ${WMCORE_REPO} != "dmwm" ]; then
  IMAGE_TAG=${IMAGE_TAG}"-wmc_"${WMCORE_REPO}"_"${WMCORE_TAG}
fi
echo "IMAGE_TAG=${IMAGE_TAG}" >> $WORKSPACE/properties_file

echo "(DEBUG) new env variable:"
echo "(DEBUG)   \- IMAGE_TAG: $IMAGE_TAG"
echo "(DEBUG) end"
