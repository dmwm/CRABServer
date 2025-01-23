#! /usr/bin/env bash
# How to use script:
#  - bumping to latest version > ./cicd/scripts/bumpWMCoreVersion.sh
#  - bumping to specific version > ./cicd/scripts/bumpWMCoreVersion.sh 2.3.7.6

if [ -z "$1" ]
  then
    WMCORE_RELEASE_TAG=$(curl https://api.github.com/repos/dmwm/wmcore/releases/latest -s | jq .tag_name -r)
    echo -e "The latest WMCore stable release is $WMCORE_RELEASE_TAG\n"
else
    WMCORE_RELEASE_TAG=$1
    echo -e "Bumping WMCore version to $WMCORE_RELEASE_TAG\n"
fi

if [[ "$PWD" =~ CRABServer ]]; then
  [[ $PWD =~ (.*)/CRABServer.* ]]
  BASE_WDIR=${BASH_REMATCH[1]}
  WDIR=${BASE_WDIR}/CRABServer

  sed -i -e "s/\(.*\/dmwm\/WMCore\/blob\/\)\(.*\)\(\/requirements.txt$\)/\1$WMCORE_RELEASE_TAG\3/" ${WDIR}/cicd/crabserver_pypi/requirements.txt
  sed -i -e "s/\(.*\/dmwm\/WMCore\/blob\/\)\(.*\)\(\/requirements.txt$\)/\1$WMCORE_RELEASE_TAG\3/" ${WDIR}/cicd/crabtaskworker_pypi/requirements.txt
  sed -i -e "s/\(https:\/\/github.com\/dmwm\/WMCore \)\(.*\)/\1$WMCORE_RELEASE_TAG/" ${WDIR}/cicd/crabserver_pypi/wmcore_requirements.txt
  git diff ${WDIR}/cicd/crabserver_pypi/requirements.txt \
    ${WDIR}/cicd/crabtaskworker_pypi/requirements.txt \
    ${WDIR}/cicd/crabserver_pypi/wmcore_requirements.txt
  echo -e "\n ^^^ If change made above looks ok then commit changes and bump crabserver tag to force rebuilding image, otherwise:
  > git restore cicd/crabserver_pypi/requirements.txt cicd/crabserver_pypi/wmcore_requirements.txt cicd/crabtaskworker_pypi/requirements.txt "
else
  echo -e "Please run this script inside CRABServer codebase directory."
fi

