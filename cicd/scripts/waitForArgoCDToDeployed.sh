set -euo pipefail
NEW_IMAGE_TAG="${NEW_IMAGE_TAG:?NEW_IMAGE_TAG is required}"
DESIRED_IMAGE="${DESIRED_IMAGE:=registry.cern.ch/cmscrab/crabserver:$NEW_IMAGE_TAG}"
TIME_OUT=$(( $(date +%s) + 600 ))

JSONPATH="{.spec.template.spec.containers[?(@.name=='crabserver')].image}"
while :; do
  CURRENT_IMAGE="$(kubectl -n crab get deployment/crabserver -o jsonpath="$JSONPATH")" || CURRENT_IMAGE=""
  [ "$CURRENT_IMAGE" = "$DESIRED_IMAGE" ] && break
  [ "$(date +%s)" -ge "$TIME_OUT" ] && { echo "Timed out waiting for $DESIRED_IMAGE"; exit 1; }
  sleep 2
done
