#!/bin/bash

## bash ./cicd/build/monit-python/monit_build.sh

docker build \
  --network=host \
  -t registry.cern.ch/cmscrab/crabtaskworker:20240422-monitpython \
  ./ \
  -f ./cicd/build/monit-python/Dockerfile
