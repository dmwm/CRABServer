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
cd /data/repos
git clone https://github.com/dmwm/CRABServer.git
git clone https://github.com/dmwm/WMCore.git

# 3. checkout the installed tags
cd CRABServer
git checkout $CRABServerTag
cd ..
cd WMCore
git checkout $WMCoreTag
