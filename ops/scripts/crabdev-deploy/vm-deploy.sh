#!/bin/bash
# from http://clouddocs.web.cern.ch/clouddocs/tutorial/create_your_openstack_profile.html

if [ ! -f ~/.openrc ]
   then    
   echo	"~/.openrc not present. create it with openstack-init.sh"
   exit 1
fi

source ~/.openrc

if [ $# -ne 2 ]
then
echo "Usage: $0 <vm-name> <image>"
echo "Please use one of the following images:"
openstack image list -f csv -c Name|grep "SLC6 CERN Server - x86_64"
exit 1
fi

VMNAME=$1
IMAGE=$2

openstack server list  | grep $VMNAME > /dev/null 

if [ $? -eq 0 ] 
then
    echo "$VMNAME already present in your openstack instances. This is a rebuild"
    openstack server rebuild --image "$IMAGE" $VMNAME
else
echo "Building $VMNAME with image $IMAGE"
openstack server create --flavor m1.large --image "$IMAGE" $VMNAME
fi
