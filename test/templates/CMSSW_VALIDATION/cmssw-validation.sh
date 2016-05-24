TAG='HG1605b'
VERSION='7'

# get only the production release of CMSSW and also filter out all slc5
# since CRAB3 does not support it.
CMSSW_RELEASES=`wget -O- https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML 2>/dev/null`
CMSSW_ARCHS=`echo "$CMSSW_RELEASES" | grep name | sed -n 's|.*name="\(.*\)".*|\1|p'`
CMSSW_VERSIONS=`for arch in $CMSSW_ARCHS;do echo "$CMSSW_RELEASES" | sed -n "/$arch/,/<\/architecture/p" | sed -n 's|.*label="\(.*\)" type.*|\1|p' | egrep 'CMSSW_[0-9]*_[0-9]*_[0-9]*$' | xargs -I'{}' echo "{}:$arch" | grep -v slc5;done | sort -t':'`

WORK_DIR="/afs/cern.ch/user/j/jmsilva/workspace/crabValidation/$TAG"
MAIN_DIR='/afs/cern.ch/user/j/jmsilva/workspace/crabValidation/scripts'
CRAB_SOURCE_SCRIPT='/cvmfs/cms.cern.ch/crab3/crab_pre.sh'

# use a modified copy of light client until we have it on cvmfs
CLIENT="$PWD/crab_light.sh" 

STORAGE_SITE='T2_CH_CERN'
INSTANCE='preprod'

# run the tests using a wrapper script to have an clean environment using
# env -i command
echo "${CMSSW_VERSIONS}" | awk -F':' '{print $1,$2}' | ( while read CMSSW_VERSION SCRAMARCH; do  
   env -i $MAIN_DIR/CMSSW_VALIDATION/submit.sh $TAG $VERSION $CMSSW_VERSION $SCRAMARCH $WORK_DIR $MAIN_DIR $CRAB_SOURCE_SCRIPT $CLIENT $STORAGE_SITE $INSTANCE
done
)


