#! /usr/bin/env bash
# Usage:
#  - to bump to the latest WMCore version > ./cicd/scripts/bumpWMCoreVersion.sh
#  - to bump to a specific WMCore version > ./cicd/scripts/bumpWMCoreVersion.sh <version_number>
#  Example: ./cicd/scripts/bumpWMCoreVersion.sh 2.3.7.6

# Check if specific version is provided as an argument
if [ -z "$1" ]
  then
    # if no argument passed, fetch the latest stable WMCore release tag
    WMCORE_RELEASE_TAG=$(curl https://api.github.com/repos/dmwm/wmcore/releases/latest -s | jq .tag_name -r)
    echo -e "The latest WMCore stable release is $WMCORE_RELEASE_TAG\n"
else
    # Use the provided version argument
    WMCORE_RELEASE_TAG=$1
    echo -e "Bumping WMCore version to $WMCORE_RELEASE_TAG\n"
fi


# Ensure the script is executed within the CRABServer codebase directory
if [[ "$PWD" =~ CRABServer ]]; then
  [[ $PWD =~ (.*)/CRABServer.* ]]
  BASE_WDIR=${BASH_REMATCH[1]}
  WDIR=${BASE_WDIR}/CRABServer

  # Update WMCore version in relevant files
  sed -i -e "s/\(.*\/dmwm\/WMCore\/blob\/\)\(.*\)\(\/requirements.txt$\)/\1$WMCORE_RELEASE_TAG\3/" ${WDIR}/cicd/crabserver_pypi/requirements.txt
  sed -i -e "s/\(.*\/dmwm\/WMCore\/blob\/\)\(.*\)\(\/requirements.txt$\)/\1$WMCORE_RELEASE_TAG\3/" ${WDIR}/cicd/crabtaskworker_pypi/requirements.txt
  sed -i -e "s/\(https:\/\/github.com\/dmwm\/WMCore \)\(.*\)/\1$WMCORE_RELEASE_TAG/" ${WDIR}/cicd/crabserver_pypi/wmcore_requirements.txt

  # Show a diff of the changes made for review
  git diff ${WDIR}/cicd/crabserver_pypi/requirements.txt \
    ${WDIR}/cicd/crabtaskworker_pypi/requirements.txt \
    ${WDIR}/cicd/crabserver_pypi/wmcore_requirements.txt
  echo -e "\n ^^^ If change made above looks ok then commit changes and bump crabserver tag to force rebuilding image, otherwise:
  > git restore cicd/crabserver_pypi/requirements.txt cicd/crabserver_pypi/wmcore_requirements.txt cicd/crabtaskworker_pypi/requirements.txt "
else
  echo -e "Please run this script inside CRABServer codebase directory."
fi

