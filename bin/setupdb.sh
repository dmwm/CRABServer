#!/bin/sh
release=$1
dbconfig=$2
dbcommand="wmcore-db-init"
if ! type "$dbcommand" > /dev/null; then
    echo "$dbcommand not found"
    exit 1
fi

mkdir $release 
cd $release
git clone http://git.cern.ch/pub/CAFUtilities
cd CAFUtilities
git checkout $release
export PYTHONPATH=`pwd`/src/python/:$PYTHONPATH
wmcore-db-init --config $dbconfig --create --modules=Databases.TaskDB,Databases.FileMetaDataDB
cd ..
rm -rf CAFUtilities
