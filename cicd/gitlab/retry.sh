#! /bin/bash
# retry command (script) passed as argument if it exits with code=4

set -euo pipefail

export RETRY_MAX=${RETRY_MAX:-10}
export RETRY_SLEEP_SECONDS=${RETRY_SLEEP_SECONDS:-900}

RETRY=1
while true; do
    echo "${RETRY}/${RETRY_MAX} attempt."
    export RETRY RETRY_MAX
    rc=0
    "$@" || rc=$?
    if [[ $rc != 0 ]]; then
        echo "Script attempt ${RETRY}/${RETRY_MAX} completed with exit code ${rc}. Try again ? "
        if [[ $rc == 4 ]]; then
            if [[ $RETRY -eq $RETRY_MAX ]]; then
                echo "Reach max retry count: $RETRY. End test"
                exit 1
            fi
            echo -n "Sleep for ${RETRY_SLEEP_SECONDS} seconds before retry."
	          echo " Until " `date -d "now + ${RETRY_SLEEP_SECONDS} seconds" +"%H:%M %Z"`
            sleep "${RETRY_SLEEP_SECONDS}"
            RETRY=$((RETRY + 1))
            continue
        else
            echo "Unexpected error. Exit code: $rc"
            exit 1
        fi

    else
        echo "Script successful after attempt ${RETRY}/${RETRY_MAX}"
        break
    fi
done
