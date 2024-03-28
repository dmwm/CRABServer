#!/bin/bash

## bash ./cicd/build/monit-taskworker/monit_build.sh v3.240111

TAG=v3.240325

docker build \
  --build-arg "TAG=$TAG" \
  --network=host \
  -t registry.cern.ch/cmscrab/crabtaskworker:$TAG.monittw \
  ./ \
  -f ./cicd/build/monit-taskworker/Dockerfile

