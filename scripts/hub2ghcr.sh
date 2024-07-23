#!/bin/bash -ex
src="cmssw/${1}:${2}"
des="ghcr.io/cms-sw/cmssw/${1}:${2}"
docker pull ${src}
docker rmi  ${des} || true
docker tag  ${src} ${des}
docker push ${des}
docker rmi  ${des}
docker rmi  ${src}
