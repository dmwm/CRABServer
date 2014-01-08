#!/bin/bash


###### CHECKING THE PARAMETERS ######

if [ $# -ne 2 ] && [ $# -ne 3 ]; then
    echo "Usage: ${0##*/} WMCoreTAG CAFUtilitiesTAG [OutnameSuffix]"
    exit
fi

WMCoreTAG=$1
CAFUtilitiesTAG=$2
#The output filename suffix is used to create for example the Dev transformation
Suffix=$3

if [ -f CMSRunAnaly$Suffix.sh ] || [ -f CMSRunAnaly$Suffix.tgz ] || [ -f CMSRunMCProd$Suffix.sh ] || [ -f CMSRunMCProd$Suffix.tgz ]; then
    echo "I don't want to overwrite already existing files (CMSRun$Suffix*). Please delete them and relaunch."
    exit
fi

###### GETTING THE SOURCE CODE ######

mkdir buildtrf-workdir
cd buildtrf-workdir

echo "Getting required tags"

wget --no-check-certificate "https://github.com/dmwm/WMCore/archive/$WMCoreTAG.tar.gz"
if [ $? -ne 0 ]; then
    echo "Problem downloading WMCore archive"
    exit
fi
tar xzf $WMCoreTAG
if [ $? -ne 0 ]; then
    echo "Cannot untar WMCore archive"
    exit
fi

git clone "http://git.cern.ch/pub/CAFUtilities"
if [ $? -ne 0 ]; then
    echo "Problem cloning CAFUtilities repository"
    exit
fi
cd CAFUtilities
git checkout $CAFUtilitiesTAG
if [ $? -ne 0 ]; then
    echo "Cannot find tag $CAFUtilitiesTAG inside CAFUtilities repository"
    exit
fi
cd ..


###### BUILDING TRANSFORMATION ARCHIVE ######

echo "Building transformation archive"
#go into WMCore, apply the patch, create the zipfile
cd WMCore-$WMCoreTAG
cp ~/repos/CAFUtilities/etc/wmcore-transformation.patch ../CAFUtilities/etc/
patch -p1 < ../CAFUtilities/etc/wmcore-transformation.patch
if [ $? -ne 0 ]; then
    echo "Cannot apply the wmcore-transformation.patch"
    exit
fi

cd src/python/
zip -r ../../../WMCore.zip WMCore/ PSetTweaks/
if [ $? -ne 0 ]; then
    echo "Cannot zip WMCore library"
    exit
fi
cd ../../../

cp CAFUtilities/src/python/transformation/TweakPSet.py CAFUtilities/src/python/transformation/CMSRunAnaly/CMSRunAnaly.* CAFUtilities/src/python/transformation/CMSRunMCProd/CMSRunMCProd.* .
tar cvzf CMSRunAnaly$Suffix.tgz TweakPSet.py WMCore.zip CMSRunAnaly.py
tar cvzf CMSRunMCProd$Suffix.tgz TweakPSet.py WMCore.zip CMSRunMCProd.py
mv CMSRunMCProd$Suffix.tgz CMSRunAnaly$Suffix.tgz ..
mv CMSRunAnaly.sh ../CMSRunAnaly$Suffix.sh
mv CMSRunMCProd.sh ../CMSRunMCProd$Suffix.sh
cd ..
rm -rf buildtrf-workdir
