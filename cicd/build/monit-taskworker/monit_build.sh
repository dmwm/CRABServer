#!/bin/bash

## bash ./cicd/build/monit-taskworker/monit_build.sh

TAG=pypi-test2-1716833532

docker build \
  --build-arg "TAG=$TAG" \
  --network=host \
  -t registry.cern.ch/cmscrab/crabtaskworker:$TAG-monittw \
  ./ \
  -f ./cicd/build/monit-taskworker/Dockerfile

