#!/bin/bash

RCFILE=$PWD/paramsrc-filecheck.sh
CRABAUTH=/data/srv/current/auth/crabserver/CRABServerAuth.py
CRABINIT=/data/crabserver.sh

source $RCFILE

git clone git://github.com/dmwm/deployment.git /data/cfg
cd /data/cfg
if [ "string" == "string${HGVER}"]
then
HGVER=`git tag -l 'HG*'|tail -1`
fi

git reset --hard $HGVER
REPO="-r comp=$COMPREPO" A=/data/cfg/admin
cd /data
$A/InstallDev -R comp@$HGVER -A $ARCH -s image -v $HGVER $REPO -p "admin/devtools frontend crabserver crabcache"

echo "Setting data.extconfigurl to $GISTEXTURL"
sed -i "s|data.extconfigurl.*|data.extconfigurl = '$GISTEXTURL'|" /data/srv/current/config/crabserver/config.py

echo "Creating CRABServerAuth.py"

sudo /bin/cat > /tmp/CRABServerAuth.py <<EOF
import cx_Oracle as DB
import socket
fqdn = socket.getfqdn().lower()
dbconfig = {'dev': {'.title': 'Pre-production',
                    '.order': 1,
                    '*': {'clientid': 'cmsweb-dev@%s' % (fqdn),
                          'dsn': 'devdb11',
                          'liveness': 'select sysdate from dual',
                          'password': '$ORACLEPASS',
                          'schema': '$ORACLEUSER',
                          'timeout': 300,
                          'trace': True,
                          'type': DB,
                          'user': '$ORACLEUSER'}
                   }
           }
EOF
sudo mv /tmp/CRABServerAuth.py $CRABAUTH
sudo chmod 440 $CRABAUTH
sudo chown  _sw:_config $CRABAUTH

if [ ! -f /data/srv/current/auth/crabserver/dmwm-service-cert.pem ]
then
    echo "Creating service certificate"    
    voms-proxy-init --valid 168:00
    sudo cp /tmp/x509up_u$UID /data/srv/current/auth/crabserver/dmwm-service-cert.pem
    sudo cp /tmp/x509up_u$UID /data/srv/current/auth/crabserver/dmwm-service-key.pem
fi

if [ ! -d /data/user/CRABServer ]
then
    cd /data/user/
    git clone https://github.com/$GITUSER/CRABServer
    cd CRABServer
    git remote add upstream https://github.com/dmwm/CRABServer
fi

if [ ! -d /data/user/WMCore ]
then
    cd /data/user/
    git clone https://github.com/$GITUSER/WMCore
    cd WMCore
    git remote add upstream https://github.com/dmwm/WMCore
fi

CRABVERSION=`ls /data/srv/current/sw.pre/$ARCH/cms/crabserver`
INITFILE=/data/srv/current/sw.pre/$ARCH/cms/crabserver/$CRABVERSION/etc/profile.d/init.sh
sed -i 's/\(.*PYTHON_LIB.*\)/#\1/' $INITFILE
echo 'export PYTHONPATH=/data/user/CRABServer/src/python/:/data/user/WMCore/src/python/:$PYTHONPATH' >> $INITFILE
sudo yum -y install python-sqlalchemy

if [ ! -f /data/dbconfig.py ]
then
    echo "Creating dbconfig.py"
    cat > /data/dbconfig.py <<EOF
from WMCore.Configuration import Configuration
config = Configuration()
config.section_('CoreDatabase')
config.CoreDatabase.connectUrl = 'oracle://$ORACLEUSER:$ORACLEPASS@devdb11'
EOF
fi

source /data/srv/current/sw.pre/$ARCH/cms/crabserver/$CRABVERSION/etc/profile.d/init.sh 
source /afs/cern.ch/project/oracle/script/setoraenv.sh -s prod
if [ "string$INITDB" != 'string' ]
then
    /data/user/WMCore/bin/wmcore-db-init --config /data/dbconfig.py --create --modules=Databases.TaskDB,Databases.FileMetaDataDB
fi
cat > $CRABINIT <<EOF
numargs=\${#}
if [ \$numargs -eq 0 ]; then
  echo "Please specify the action (start, stop or status) to take on local CRAB server."
elif [ \$numargs -eq 1 ]; then
  action=\${1}
  cwd=\$(pwd)
  echo "Sourcing environment from /data/srv"
  source $INITFILE
  echo "Executing cd /data; /data/cfg/admin/InstallDev -d /data/srv -s \$action; cd \$cwd; stty sane"
  cd /data; /data/cfg/admin/InstallDev -d /data/srv -s \$action; cd \$cwd; stty sane
else
  echo "This script takes only 1 argument (\$numargs arguments were given)."
fi
EOF

chmod +x $CRABINIT

source $CRABINIT start
