#source .env
export X509_USER_PROXY=/tmp/x509up_u150799
export CRABClient_version=prod  # from .env
export REST_Instance=test1  # from .env
export CMSSW_release=CMSSW_13_0_2
export Client_Validation_Suite=true
bash -x cicd/gitlab/clientValidation.sh
