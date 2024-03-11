#! /bin/bash
# retry

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
        echo "Command fail with exit code ${rc} (${RETRY}/${RETRY_MAX} attempted)"
        if [[ $rc == 4 ]]; then
            if [[ $RETRY -eq $RETRY_MAX ]]; then
                echo "Reach max retry count: $RETRY"
                exit 1
            fi
            echo "Sleep for ${RETRY_SLEEP_SECONDS} seconds."
            sleep "${RETRY_SLEEP_SECONDS}"
            RETRY=$((RETRY + 1))
            continue
        else
            echo "Unexpected error. Exit code: $rc"
            exit 1
        fi

    else
        break
    fi
done
