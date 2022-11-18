#! /bin/bash

# run ./build_and_push.sh <baseimage>
# example: bash build_and_push.sh registry.cern.ch/cmsweb/crabserver:crabserver_61adebf0.wmcore_59890ba1
docker build -t $1.solution2 --build-arg BASEIMAGE=$1 .
docker push $1.solution2
