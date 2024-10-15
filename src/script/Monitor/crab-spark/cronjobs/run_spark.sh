#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# work directory
pushd "${SCRIPT_DIR}"

# source the environment for spark submit
source ../workdir/bootstrap.sh

# submit $1 to spark, where $1 supposes to be a data pulling file (.py)
spark-submit --master yarn --packages org.apache.spark:spark-avro_2.12:3.5.0 $@

popd
