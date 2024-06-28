#! /bin/bash

set -euo pipefail

#0. ensure variable is set
echo "(DEBUG) Service: ${Service}"
echo "(DEBUG) Image: ${Image}"
echo "(DEBUG) Environment: ${Environment}"

WORK_DIR=$PWD

if [ "X${Service}" == "XTaskWorker" ] ; then
	processName=crab-taskworker
else
	processName=RunPublisher
fi


#1. Deploying
ssh -i $SSH_KEY -o StrictHostKeyChecking=no crab3@${Environment}.cern.ch """
docker exec ${Service} bash -c './stop.sh'; \
docker stop ${Service}; \
docker rm ${Service}; \
~/runContainer.sh -r registry.cern.ch/cmscrab -v ${Image} -s ${Service} -c /data/run.sh
"""

#2. check if ${Service} is running
ssh -i $SSH_KEY -o StrictHostKeyChecking=no crab3@${Environment}.cern.ch \
"docker exec ${Service} bash -c 'ps exfww | grep $processName | grep -v grep | head -1' || true" > isServiceRunning.log
cat isServiceRunning.log
if [ $(cat isServiceRunning.log |wc -l) -ne 1 ] ; then
	echo "${Service} image in ${Environment} update did not succeed. ${Service} is not running. Please investigate manually." > $WORK_DIR/logFile.txt
	ERR=true
else
	echo "${Service} image in ${Environment} was updated to registry.cern.ch/cmscrab/crabtaskworker:${Image} image tag."  > $WORK_DIR/logFile.txt
    ERR=false
fi


#3. Print running containers
echo -e "\nRunning containers: " >> $WORK_DIR/logFile.txt
ssh -i $SSH_KEY -o StrictHostKeyChecking=no crab3@${Environment}.cern.ch  "docker ps" >> $WORK_DIR/logFile.txt

cat $WORK_DIR/logFile.txt

if [[ "${ERR}" == "true" ]] ; then
	exit 1
fi
