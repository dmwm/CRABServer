#!/bin/bash -ex
name="$1"
repo="$2"
thisdir=$(dirname $0)
rm -rf tmp; mkdir -p tmp
cp -r ${thisdir}/../init  tmp/$name
pushd tmp/$name
  echo "LABEL org.opencontainers.image.source=\"https://github.com/cms-sw/${repo}\"" >> Dockerfile
  docker build . -t ghcr.io/cms-sw/cmssw/${name}:init
  docker push       ghcr.io/cms-sw/cmssw/${name}:init
  docker rmi        ghcr.io/cms-sw/cmssw/${name}:init
popd
rm -rf tmp
