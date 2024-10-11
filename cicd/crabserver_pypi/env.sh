#!/bin/bash
# This script is for sourcing only!
# The script wrap `./manage.py env -c` which only print the export command using
# eval. Simply run `. env.sh` and your shell will have the same environment
# variables as the services that run with command `./manange.py start -c -s <service>`.
#
# Please look loo manage.sh -> env_eval() to see which env is exposed.
#
# if "-g" provide as first arg, run `./manage.py env -g` instead

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [[ ${1} == '-g' ]]; then
    out="$("${SCRIPT_DIR}"/manage.py env -g)"
else
    out="$("${SCRIPT_DIR}"/manage.py env -c)"
fi
echo "will source these variables: "
echo "${out}"
eval "${out}"
