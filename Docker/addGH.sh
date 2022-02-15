#!/bin/bash
# add GitHub repositories for CRABServer and WMCore

# 0. locate the crabserver directory created by install.sh
# something like: /data/srv/TaskManager/py3.220105-efe7bf851559ee417e352b26e70998cd/slc7_amd64_gcc630/cms/crabtaskworker
CRABServerDir=`realpath /data/srv/TaskManager/current/*/cms/crabtaskworker`

# 1. find out which WMCore tag was installed
export PYTHONPATH=$CRABServerDir/$RPM_RELEASETAG_HASH/lib/python3.8/site-packages
WMCoreGHTag=$(grep __version__ $CRABServerDir/$RPM_RELEASETAG_HASH/lib/python*/site-packages/WMCore/__init__.py | cut -f2 -d"=" |  tr -d " '\"")

# 2. create directories for repositories and clone 
mkdir /data/repos
pushd /data/repos
git clone https://github.com/dmwm/CRABServer.git
git clone https://github.com/dmwm/WMCore.git

# 3. checkout the installed tags and add ptrs to developers repos
cd CRABServer
git checkout $RELEASE_TAG
git remote add stefano https://github.com/belforte/CRABServer.git
git remote add daina https://github.com/ddaina/CRABServer.git
git remote add mapellidario https://github.com/mapellidario/CRABServer.git
git remote add diego https://github.com/dciangot/CRABServer.git
git remote add wa https://github.com/novicecpp/CRABServer.git
cd ..
cd WMCore
git checkout $WMCoreGHTag
git remote add stefano https://github.com/belforte/WMCore.git
git remote add daina https://github.com/ddaina/WMCore.git
git remote add mapellidario https://github.com/mapellidario/WMCore.git
git remote add diego https://github.com/dciangot/WMCore.git
git remote add wa https://github.com/novicecpp/CRABServer.git

# 4. create .gitconfig
cat > ~/.gitconfig << EOF
[user]
        email = nobody@nowhere
        name = crab
[core]
        editor = vim
EOF

# 5. all done, reset cwd and exit
popd
