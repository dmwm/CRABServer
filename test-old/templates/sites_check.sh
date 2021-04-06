#
# Script will submit jobs to each site setting it as whitelist
#
#
#Parameters required to change !
#------------------------------
source ./validation-args.sh


#Get specified CMSSW
if [ ! -d "$WORK_DIR" ]; then
  mkdir -p $WORK_DIR
fi
cd $WORK_DIR
if [ ! -d "$CMSSW" ]; then
  cmsrel $CMSSW
fi
cd $WORK_DIR/$CMSSW/src/


#Setup CMS environment
cmsenv

#Source crab client
#This is required until it will be solved and added crabclient in CMSSW
source $CLIENT

cp -R $MAIN_DIR/config/* .

file_name=./MinBias_PrivateMC_EventBased_Sites.py

# Change this and get full list from SiteDB
counter=0
for site_name in T1_DE_KIT T1_ES_PIC T1_FR_CCIN2P3 T1_IT_CNAF T1_RU_JINR T1_RU_JINR_Disk T1_UK_RAL T1_UK_RAL_Disk T1_US_FNAL T1_US_FNAL_Disk T2_AT_Vienna T2_BE_IIHE T2_BE_UCL T2_BR_SPRACE T2_BR_UERJ T2_CH_CERN T2_CH_CSCS T2_CN_Beijing T2_DE_DESY T2_DE_RWTH T2_EE_Estonia T2_ES_CIEMAT T2_ES_IFCA T2_FI_HIP T2_FR_CCIN2P3 T2_FR_GRIF_IRFU T2_FR_GRIF_LLR T2_FR_IPHC T2_GR_Ioannina T2_HU_Budapest T2_IN_TIFR T2_IT_Bari T2_IT_Legnaro T2_IT_Pisa T2_IT_Rome T2_KR_KNU T2_MY_UPM_BIRUNI T2_PK_NCP T2_PL_Swierk T2_PL_Warsaw T2_PT_NCG_Lisbon T2_RU_IHEP T2_RU_INR T2_RU_ITEP T2_RU_JINR T2_RU_PNPI T2_RU_RRC_KI T2_RU_SINP T2_TH_CUNSTDA T2_TR_METU T2_UA_KIPT T2_UK_London_Brunel T2_UK_London_IC T2_UK_SGrid_Bristol T2_UK_SGrid_RALPP T2_US_Caltech T2_US_Florida T2_US_MIT T2_US_Nebraska T2_US_Purdue T2_US_UCSD T2_US_Vanderbilt T2_US_Wisconsin T3_AS_Parrot T3_BY_NCPHEP T3_CH_PSI T3_CN_PKU T3_CO_Uniandes T3_ES_Oviedo T3_EU_Parrot T3_FR_IPNL T3_GR_Demokritos T3_GR_IASA T3_HR_IRB T3_HU_Debrecen T3_IN_PUHEP T3_IR_IPM T3_IT_Bologna T3_IT_Firenze T3_IT_MIB T3_IT_Napoli T3_IT_Perugia T3_IT_Trieste T3_KR_KISTI T3_KR_KNU T3_KR_UOS T3_MX_Cinvestav T3_NZ_UOA T3_RU_FIAN T3_TW_NCU T3_TW_NTU_HEP T3_UK_GridPP_Cloud T3_UK_London_QMUL T3_UK_London_RHUL T3_UK_London_UCL T3_UK_SGrid_Oxford T3_UK_ScotGrid_ECDF T3_UK_ScotGrid_GLA T3_US_Baylor T3_US_Brown T3_US_Colorado T3_US_Cornell T3_US_FIT T3_US_FIU T3_US_FNALLPC T3_US_FNALXEN T3_US_FSU T3_US_JHU T3_US_Kansas T3_US_MIT T3_US_Minnesota T3_US_NEU T3_US_NU T3_US_NotreDame T3_US_OSU T3_US_Omaha T3_US_Parrot T3_US_ParrotTest T3_US_Princeton T3_US_Princeton_ICSE T3_US_PuertoRico T3_US_Rice T3_US_Rutgers T3_US_SDSC T3_US_TAMU T3_US_TTU T3_US_UB T3_US_UCD T3_US_UCR T3_US_UIowa T3_US_UMD T3_US_UMiss T3_US_UTENN T3_US_UVA T3_US_Vanderbilt_EC2;
do
    echo $file_name;
    file_name_temp=${file_name:2:(${#file_name})-5};

#Generate new name
    #new_name='dso-issue-UK-'$TAG-$VERSION-$file_name_temp-$site_name
    new_name=$TAG-$VERSION-$file_name_temp-$site_name-$counter
    publish_name=$new_name-`date +%s`
    echo $new_name
#General part
    sed --in-place "s|\.General\.requestName = .*|\.General\.requestName = '$new_name'|" $file_name
    sed --in-place "s|\.General\.workArea = .*|\.General\.workArea = '$TAG-$VERSION-dso-issue-UK' |" $file_name

    #Site part
    sed --in-place "s|\.Site\.storageSite = .*|\.Site\.storageSite = '$STORAGE_SITE' |" $file_name
    sed --in-place "s|\.whitelist = .*|\.whitelist = \['$site_name'\]|" $file_name

    echo crab submit -c $file_name
    counter=$((counter+1))
done
