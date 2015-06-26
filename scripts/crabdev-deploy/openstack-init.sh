#!/bin/bash

if [ ! -f ~/.openrc ]
then
    cat > ~/.openrc <<EOF
export OS_AUTH_URL=https://openstack.cern.ch:5000/v2.0
export OS_USERNAME=`id -un`
export OS_TENANT_NAME="Personal \$OS_USERNAME"
# With Keystone you pass the keystone password.
if [ -z \$OS_PASSWORD ]
then
  echo "Please enter your OpenStack Password (or save in OS_PASSWORD env variable to not type it everytime): "
  read -s OS_PASSWORD_INPUT
  export OS_PASSWORD=\$OS_PASSWORD_INPUT
fi
EOF
else
echo 'File ~/.openrc already present. Exiting...'
fi
