#!/bin/bash
# add GitHub repositories for CRABServer and WMCore

# TW_VERSION is set by Dockerfile
CRABServerTag=$TW_VERSION

# 0. locate the crabserver directory created by install.sh
CRABServerDir=`realpath /data/srv/TaskManager/current/*/cms/crabtaskworker`

# 1. find out which WMCore tag was installed
export PYTHONPATH=$CRABServerDir/$CRABServerTag/lib/python2.7/site-packages
WMCoreTag=`python -c "from WMCore import __version__; print __version__"`

# 2. create directories for repositories and clone 
mkdir /data/repos
pushd /data/repos
git clone https://github.com/dmwm/CRABServer.git
git clone https://github.com/dmwm/WMCore.git

# 3. checkout the installed tags and add ptrs to developers repos
cd CRABServer
git checkout $CRABServerTag
git remote add stefano https://github.com/belforte/CRABServer.git
git remote add daina https://github.com/ddaina/CRABServer.git
cd ..
cd WMCore
git checkout $WMCoreTag
git remote add stefano https://github.com/belforte/WMCore.git
git remote add daina https://github.com/ddaina/WMCore.git

# 4. all done, reset cwd and exit
popd
