#!/bin/bash
# add GitHub repositories for CRABServer and WMCore
# Original script from https://github.com/dmwm/CMSKubernetes/blob/a6612ad45ea6ca79915e71e343cc29a97418973a/docker/crabserver/addGH.sh
# In the original script, we detect tag from installed path inside container.
# However, this new image will install everything inside `/data/srv/current/lib/python/site-packages`.
# And I (Wa) have not yet decided how we are passing version information to the container.

set -euo pipefail

# 1. clone
mkdir /data/repos
pushd /data/repos
git clone https://github.com/dmwm/CRABServer.git
git clone https://github.com/dmwm/WMCore.git

# 2. add remote
pushd CRABServer
git remote add stefano https://github.com/belforte/CRABServer.git
git remote add dario https://github.com/mapellidario/CRABServer.git
git remote add wa https://github.com/novicecpp/CRABServer.git
git remote add vchakrav https://github.com/aspiringmind-code/CRABServer.git

popd
pushd WMCore
git remote add stefano https://github.com/belforte/WMCore.git
git remote add dario https://github.com/mapellidario/WMCore.git
git remote add wa https://github.com/novicecpp/WMCore.git
popd

# add dummy global names to git for git stash to work
git config --global user.email dummy@nowhere
git config --global user.name dummy

# add links to make UI work in GH mode when REST runs with /data/repos as root
mkdir /data/repos/data
ln -s /data/repos/CRABServer/src/html /data/repos/data/html
ln -s /data/repos/CRABServer/src/script /data/repos/data/script
ln -s /data/repos/CRABServer/src/css /data/repos/data/css

# all done, reset cwd and exit
popd
