source ../validation-args.sh
#TAG='HG1605b'
#VERSION='4'

# get only the production release of CMSSW and also filter out all slc5
# since CRAB3 does not support it.
CMSSW_RELEASES=`wget -O- https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML 2>/dev/null`
CMSSW_ARCHS=`echo "$CMSSW_RELEASES" | grep name | sed -n 's|.*name="\(.*\)".*|\1|p'`
CMSSW_VERSIONS=`for arch in $CMSSW_ARCHS;do echo "$CMSSW_RELEASES" | sed -n "/$arch/,/<\/architecture/p" | sed -n 's|.*label="\(.*\)" type.*|\1|p' | egrep 'CMSSW_[0-9]*_[0-9]*_[0-9]*$' | xargs -I'{}' echo "{}:$arch" | grep -v slc5;done | sort -t':'`

# only run over an given CMSSSW series
#CMSSW_VERSIONS=`for arch in $CMSSW_ARCHS;do echo "$CMSSW_RELEASES" | sed -n "/$arch/,/<\/architecture/p" | sed -n 's|.*label="\(.*\)" type.*|\1|p' | egrep 'CMSSW_[0-9]*_[0-9]*_[0-9]*$' | xargs -I'{}' echo "{}:$arch" | grep -v slc5 | egrep 'CMSSW_5.3|CMSSW_8';done | sort -t':'`

#CMSSW_VERSIONS=`echo -ne "CMSSW_8_0_3:slc6_amd64_gcc530\nCMSSW_8_0_4:slc6_amd64_gcc530"`

echo "==================================================================================================="
echo ${CMSSW_VERSIONS}
echo "==================================================================================================="

# run the tests using a wrapper script to have an clean environment using
# env -i command
echo "${CMSSW_VERSIONS}" | awk -F':' '{print $1,$2}' | ( while read CMSSW_VERSION SCRAMARCH; do
  echo
  echo "Testing $CMSSW_VERSION with $SCRAMARCH..."
  echo "============================"
  if [[ $CMSSW_VERSION == *"8_0_"* ]];then
     echo -ne "\e[1m\e[93m==============================================\e[0m\n"
     echo -ne "\e[1m\e[93m      *** WARNING ***\e[0m \n\tCMSSW 8_0_* has a know issue with pycurl\n"
     echo -ne "\e[1m\e[93m==============================================\e[0m\n"
  fi
  env -i $MAIN_DIR/CMSSW_VALIDATION/submit.sh $TAG $VERSION $CMSSW_VERSION $SCRAMARCH $WORK_DIR $MAIN_DIR $CRAB_SOURCE_SCRIPT $CLIENT $STORAGE_SITE $INSTANCE
  echo
done
)


