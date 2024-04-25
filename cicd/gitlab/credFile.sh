#! /bin/bash

##H Usage: credFile.sh FILEPATH TYPE
##H
##H Copy file from CI Variable and change the owner of file to current runner
##H user and mode `0600`. Note that new file path is hardcode and must print to
##H stdout, all other output must redirect to stdderr.
##H
##H FILEPATH: Path to file. Usually value of CI variable is path when expose
##H             it as file.
##H TYPE:     Credential type. If TYPE is `x509`, it will try to generate a
##H             proxy from the cert installed in the runner machine (~/.globus/)
##H              with `voms-proxy-init`
##H
##H Example:
##H   export X509_USER_PROXY=$(cicd/gitlab/credFile.sh $X509_USER_PROXY x509)

set -euo pipefail

helpFunction() {
    echo;
    cat $0 | grep "^##H" | sed -r "s/##H(| )//g" >&2
}


CREDS_FILE=$1
CREDS_TYPE=$2

case "${CREDS_TYPE}" in
    x509)
        rc=0
        >&2 openssl x509 -checkend 0 -noout -in "${CREDS_FILE}" || rc=$?
        if [[ "${rc}" -ne 0 ]]; then
            >&2 echo "Proxy file has expired. Generating new one from local cert..."
            >&2 voms-proxy-init --rfc --voms cms -valid 196:00
            CREDS_FILE="$(voms-proxy-info -path)"
        fi
        ;;
    ssh)
        : # noop
        ;;
    *)
      >&2 echo "ERROR: Unknown CREDS_TYPE: ${CREDS_TYPE}"
      helpFunction
      exit 1
      ;;
esac

CREDS_COPIED=$(basename ${CREDS_FILE})_copy
cp ${CREDS_FILE} ${CREDS_COPIED}
chown $(id -u):$(id -g) "${CREDS_COPIED}"
chmod 0600 "${CREDS_COPIED}"
realpath "${CREDS_COPIED}"
