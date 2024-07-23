#!/bin/bash -ex
CMSREP="cmsrep.cern.ch"
ADD_PKGS=""
RUN_TESTS="false"
BUILDTIME="true"
if [ "$2" != "" ] ; then CMSREP="$2" ; fi
if [ "$3" != "" ] ; then ADD_PKGS="$3" ; fi
if [ "$4" = "true" ] ; then RUN_TESTS="true" ; fi
if [ "$5" != "" ] ; then BUILDTIME="$5" ; fi
RELEASE_INST_DIR=/cvmfs/cms-ib.cern.ch
INVALID_ARCHS='slc6_amd64_gcc461 slc6_amd64_gcc810 slc7_aarch64_gcc493 slc7_aarch64_gcc530'
export CMSSW_GIT_REFERENCE=/cvmfs/cms.cern.ch/cmssw.git.daily
ARCHS="$1"

if [ "$WORKSPACE" = "" ] ; then export WORKSPACE=$(/bin/pwd) ; fi
cd $WORKSPACE
rm -rf inst; mkdir inst; cd inst

ls /cvmfs/cms-ib.cern.ch >/dev/null 2>&1
ls /cvmfs/cms.cern.ch >/dev/null 2>&1

GET_CMD="curl -s -k -L -o"
if wget --help >/dev/null 2>&1 ; then GET_CMD="wget -q -O" ; fi
$GET_CMD bootstrap.sh http://${CMSREP}/cmssw/bootstrap.sh

if [ "${ARCHS}" = "" ] ; then
  $GET_CMD archs http://${CMSREP}/cgi-bin/repos/cms
  $GET_CMD cmsos https://raw.githubusercontent.com/cms-sw/cms-common/master/common/cmsos
  chmod +x cmsos
  HOST_CMS_ARCH=$(./cmsos 2>/dev/null)
  ARCHS=$(grep ">${HOST_CMS_ARCH}_" archs |  sed "s|.*>${HOST_CMS_ARCH}_|${HOST_CMS_ARCH}_|;s|<.*||")
  rm -f cmsos archs
fi

run_the_matrix () {
  echo "Architecture: $SCRAM_ARCH"
  echo "CMSSW Version: $cmssw_ver ($CMSSW_BASE)"

  RES="ERR"
  [ -d ${WORKSPACE}/cms-bot ] || (pushd $WORKSPACE; git clone --depth 1 https://github.com/cms-sw/cms-bot; popd)
  mkdir -p $WORKSPACE/upload/${SCRAM_ARCH}/${cmssw_ver}
  pushd $WORKSPACE/upload/${SCRAM_ARCH}/${cmssw_ver}
    RELEASE_FORMAT=$cmssw_ver ARCHITECTURE=$SCRAM_ARCH ${WORKSPACE}/cms-bot/run-ib-testbase.sh > run.sh
    ALL_WFS=$(runTheMatrix.py -s -n | grep -v ' workflows ' | grep '^[1-9][0-9]*\(.[0-9][0-9]*\|\)\s' | sed 's| .*||' | tr '\n' ',' | sed 's|,$||')
    echo "${WORKSPACE}/cms-bot/run-ib-relval.py -i 1of1 -f -n -l '${ALL_WFS}'" >> run.sh
    chmod +x run.sh
    (CMS_RELVALS_USER_COMMAND_OPTS='-n 2' ./run.sh 2>&1 | tee -a matrix.log) || true
    mv $CMSSW_BASE/pyRelval .
    find pyRelval -name '*' -type f | grep -v '\.log$' | grep -v '\.py$' | xargs --no-run-if-empty rm -rf
    cat pyRelval/*/workflow.log > relval-out.log || true
    if grep ' tests passed' relval-out.log ; then
      NUM_FAILED_WFS=$(grep ' tests passed' relval-out.log | sed 's|.*tests passed||' | tr ' ' '\n' | grep '^[1-9]' | wc -l)
      if [ ${NUM_FAILED_WFS} -eq 0 ] ; then
        RES="OK"
      else
        echo "Checking known errors..."
        #Check Relval failures only for IBS
        if [ $(echo ${cmssw_ver} | grep "_X_" | wc -l) -gt 0 ] ; then
          RELVAL_RES=ib-relvals.txt
          $WORKSPACE/cms-bot/get-relval-failures.py ${cmssw_ver} ${SCRAM_ARCH} > ${RELVAL_RES} || true
          cat $RELVAL_RES
          cat relval-out.log | grep "FAILED" | while read line ; do
            echo "Processing $line ..."
            relval=$(echo $line | cut -d_ -f1)
            let step=$(echo $line | grep -o -i "Step[0-9][0-9]*-FAILED"  | sed 's|^Step||i;s|-FAILED$||i')+1
            ecode=$(echo $line | sed 's|.* exit: ||' | tr ' ' '\n' | grep '^[1-9]')
            echo "Found Relval ${relval}:step${step}:${ecode}"
            if grep -q "^WF:${relval}:step${step}:${ecode}$" $RELVAL_RES ; then
              echo "Known error"
              echo "${SCRAM_ARCH}.${cmssw_ver}.RELVAL.${relval}.${step}.KNOWN" >> $WORKSPACE/res.txt
            else
              echo "Real error"
              echo "${SCRAM_ARCH}.${cmssw_ver}.RELVAL.${relval}.${step}.ERR" >> $WORKSPACE/res.txt
            fi
          done
          if [ $(cat $WORKSPACE/res.txt | grep "RELVAL" | grep "ERR" |  wc -l) -eq 0 ] ; then RES="OK" ; fi
        else
          #For Releases if #Failures as <50 then ignore the errors
          NUM_WFS=$(echo ${ALL_WFS} | tr ',' '\n' | wc -l)
          let PERCENT_FAILURE=${NUM_FAILED_WFS}*100/${NUM_WFS} || true
          if [ ${PERCENT_FAILURE} -lt 50 ] ; then RES="OK" ; fi
        fi
      fi
    fi
  popd
}

run_addons () {
  echo "Architecture: $SCRAM_ARCH"
  echo "CMSSW Version: $cmssw_ver"

  RES_ADDONS="ERR"
  mkdir -p $WORKSPACE/upload/${SCRAM_ARCH}/${cmssw_ver}
  pushd $WORKSPACE/upload/${SCRAM_ARCH}/${cmssw_ver}
    ((timeout 14400 addOnTests.py -j $(nproc)) 2>&1 | tee -a addons.log) || true
    if grep ' tests passed' addons.log ; then
      if [ $(grep ' tests passed' addons.log | sed 's|.*tests passed||' | tr ' ' '\n' | grep '^[1-9]' | wc -l) -eq 0 ] ; then
        RES_ADDONS="OK"
      fi
    fi
  popd
}

parch=""
touch $WORKSPACE/res.txt
export CMS_PATH=/cvmfs/cms-ib.cern.ch
export SITECONFIG_PATH=/cvmfs/cms-ib.cern.ch/SITECONF/local
for arch in ${ARCHS} ; do
  export SCRAM_ARCH=$arch
  touch $WORKSPACE/cmssw.rel
  release_cycle=$(curl https://cmssdt.cern.ch/SDT/BaselineDevRelease)
  (source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1; scram -a $SCRAM_ARCH list -c CMSSW_ | grep '[0-9]_X_' ; scram -a $SCRAM_ARCH list -c ${release_cycle}) | grep ${RELEASE_INST_DIR}/ | grep -v '/cmssw-patch/' | awk '{print $3}' | tac > $WORKSPACE/cmssw.rel || true
  cat $WORKSPACE/cmssw.rel
  #If tests enabled then find a release for which relvals are fully run
  if $RUN_TESTS ; then
    touch $WORKSPACE/cmssw.rel.filtered
    for v in $(cat $WORKSPACE/cmssw.rel); do
      #if [ -e $v/build-errors ] ; then continue ; fi
      ver=$(basename $v)
      rel_queue=$(echo $ver | sed 's|_X_.*|_X|')
      if [ ! -e ${rel_queue}.json ] ; then
        curl -L https://raw.githubusercontent.com/cms-sw/cms-sw.github.io/master/data/${rel_queue}.json > ${rel_queue}.json
      fi
      if [ $(grep -A 3 -B 3 "/${SCRAM_ARCH}/.*/${ver}/pyRelValMatrixLogs/run/" ${rel_queue}.json | grep '"done": *true' |wc -l) -gt 0 ] ; then
        echo "$v" >> $WORKSPACE/cmssw.rel.filtered
      fi
    done
    mv $WORKSPACE/cmssw.rel.filtered $WORKSPACE/cmssw.rel
  fi
  if $BUILDTIME ; then
    cd $WORKSPACE/inst
    echo ${SCRAM_ARCH} >> $WORKSPACE/res.txt
    if [ $(echo ${INVALID_ARCHS} | tr ' ' '\n' | grep "^${arch}$" | wc -l) -gt 0 ] ; then
      echo ${SCRAM_ARCH}.SKIP >> $WORKSPACE/res.txt
      continue
    fi
    [ "${parch}" != "" ] && rm -rf ${parch}
    parch="${arch}"
    rm -rf ./$SCRAM_ARCH ; mkdir -p ./$SCRAM_ARCH
    cd ./$SCRAM_ARCH
    cat $WORKSPACE/cmssw.rel
    cmssw_ver=""
    boot_repo="cms"
    for v in $(cat $WORKSPACE/cmssw.rel) ; do
      #if [ -e $v/build-errors ] ; then continue ; fi
      cmssw_ver=$(basename $v)
      boot_repo=cms.$(echo $v | cut -d/ -f4)
      if [ "${boot_repo}" = "cms.sw" ]; then boot_repo=cms.$(echo $v | cut -d/ -f6); fi
      break
    done
    CMSSW_IB=true
    if [ "${cmssw_ver}" = "" ] ; then
      cmssw_ver=$(source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1; scram -a $SCRAM_ARCH list -c CMSSW | grep /cvmfs/cms.cern.ch/ | grep -v '/cmssw-patch/' | tail -1 | awk '{print $2}')
      CMSSW_IB=false
    fi
    if ! sh -ex $WORKSPACE/inst/bootstrap.sh -server ${CMSREP} -r ${boot_repo} -a $SCRAM_ARCH setup ; then
      echo ${SCRAM_ARCH}.BOOT.ERR >> $WORKSPACE/res.txt
      continue
    fi
    echo ${SCRAM_ARCH}.BOOT.OK >> $WORKSPACE/res.txt
    if [ "${cmssw_ver}" = "" ] ; then
      echo "Warnings: No CMSSW version available for $SCRAM_ARCH"
      continue
    fi
    echo "Found release: ${cmssw_ver}"
    INST_OPTS="-a $SCRAM_ARCH --debug install --ignore-size --jobs 2 -y"
    $WORKSPACE/inst/$SCRAM_ARCH/common/cmspkg $INST_OPTS cms+cmssw+${cmssw_ver}
    INSTALL_PACKAGES=""
    for pkg in gcc-fixincludes cms+dasgoclient ; do
      pkg=$(($WORKSPACE/inst/$SCRAM_ARCH/common/cmspkg -a $SCRAM_ARCH search "$pkg" | grep 'CMS Experiment package' | tail -1 | sed 's| .*||') || true)
      if [ "${pkg}" != "" ] ; then
        INSTALL_PACKAGES="${INSTALL_PACKAGES} $pkg"
      fi
    done
    if [ "${INSTALL_PACKAGES}" != "" ] ; then
      $WORKSPACE/inst/$SCRAM_ARCH/common/cmspkg $INST_OPTS ${INSTALL_PACKAGES}
    fi
     export cmssw_ver
    (
      export BUILD_ARCH=$(echo ${SCRAM_ARCH} | cut -d_ -f1,2)
      source $WORKSPACE/inst/$SCRAM_ARCH/cmsset_default.sh >/dev/null 2>&1
      scram -a $SCRAM_ARCH project ${cmssw_ver}
      cd ${cmssw_ver}
      eval `scram run -sh` >/dev/null 2>&1
      USE_GIT=false
      if git cms-addpkg FWCore/Version >/dev/null 2>&1 ; then USE_GIT=true ; fi
      for p in Calibration/EcalCalibAlgos FWCore/PrescaleService FWCore/SharedMemory FWCore/Framework DataFormats/Common DataFormats/StdDictionaries CondFormats/HIObjects ${ADD_PKGS} ; do
        [ -e $CMSSW_RELEASE_BASE/src/$p ] || continue
        if $USE_GIT  ; then
          git cms-addpkg $p
        else
          mkdir -p $CMSSW_BASE/src/$p
          rsync -a $CMSSW_RELEASE_BASE/src/$p/ $CMSSW_BASE/src/$p/
        fi
      done
      if scram build -j $(nproc) ; then
        echo ${SCRAM_ARCH}.${cmssw_ver}.BUILD.OK >> $WORKSPACE/res.txt
      else
        echo ${SCRAM_ARCH}.${cmssw_ver}.BUILD.ERR >> $WORKSPACE/res.txt
        RUN_TESTS=false
      fi
      RES="SKIP"
      RES_ADDONS="SKIP"
      if $RUN_TESTS ; then
        #Run relvals if cmssw is an IB
        if $CMSSW_IB ; then run_the_matrix ; fi
        run_addons
        echo "RESULT: $RES"
      fi
      echo "${SCRAM_ARCH}.${cmssw_ver}.TEST.${RES}" >> $WORKSPACE/res.txt
      echo "${SCRAM_ARCH}.${cmssw_ver}.TEST_ADDONS.${RES_ADDONS}" >> $WORKSPACE/res.txt
    )
    rm -rf $SCRAM_ARCH
  else
    RES="SKIP"
    RES_ADDONS="SKIP"
    cmssw_ver=$(head -1 $WORKSPACE/cmssw.rel | sed 's|.*/||' || true)
    if [ "${cmssw_ver}" = "" ] ; then
      echo "Warnings: No CMSSW version available for $SCRAM_ARCH"
      echo "Skipping tests ..."
      continue
    else
      echo "Getting CMSSW area from /cvmfs: $cmssw_ver"
      export BUILD_ARCH=$(echo ${SCRAM_ARCH} | cut -d_ -f1,2)
      source /cvmfs/cms.cern.ch/cmsset_default.sh
      scram -a $SCRAM_ARCH project ${cmssw_ver}
      cd ${cmssw_ver}
      eval `scram run -sh` >/dev/null 2>&1
      if $RUN_TESTS ; then
        #Run relvals if cmssw is an IB
        if $CMSSW_IB ; then run_the_matrix ; fi
        run_addons
      fi
    fi
    echo "${SCRAM_ARCH}.${cmssw_ver}.TEST.${RES}" >> $WORKSPACE/res.txt
    echo "${SCRAM_ARCH}.${cmssw_ver}.TEST_ADDONS.${RES_ADDONS}" >> $WORKSPACE/res.txt
  fi
done
[ "${parch}" != "" ] && rm -rf ${parch}
cd $WORKSPACE
rm -rf inst
cat $WORKSPACE/res.txt
if [ $(grep '\.ERR$' $WORKSPACE/res.txt | wc -l) -gt 0 ] ; then exit 1 ; fi
echo "ALL_OK"
