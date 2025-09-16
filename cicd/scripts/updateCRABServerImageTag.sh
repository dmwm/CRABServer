set -euo pipefail
DEPLOY_ENV="${DEPLOY_ENV:?DEPLOY_ENV is required}"
NEW_IMAGE_TAG="${NEW_IMAGE_TAG:?NEW_IMAGE_TAG is required}"
DESIRED_IMAGE="${DESIRED_IMAGE:=registry.cern.ch/cmscrab/crabserver:$NEW_IMAGE_TAG}"

UPSTREAM_IAC_REPO_URL="${UPSTREAM_IAC_REPO_URL:=https://github.com/sinonkt/CMSKubernetes}"
UPSTREAM_IAC_BRANCH="${UPSTREAM_IAC_BRANCH:=migrate-to-argocd}"
GITLAB_SIDECAR_REPO_URL="${GITLAB_SIDECAR_REPO_URL:=gitlab.cern.ch/crab3/crab-k8s-overlays}"
GITLAB_SIDECAR_REPO_BRANCH="${GITLAB_SIDECAR_REPO_BRANCH:=master}"
GITLAB_SIDECAR_REPO_PAT="${GITLAB_SIDECAR_REPO_PAT:?GITLAB_SIDECAR_REPO_PAT is required}"
CI_BOT_USER_NAME="${CI_BOT_USER_NAME:=crabserver-ci-bot}"
CI_BOT_USER_EMAIL="${CI_BOT_USER_NAME:=krittin.phornsiricharoenphant@cern.ch}"

LATEST_COMMIT_SHA=$(git ls-remote "$UPSTREAM_IAC_REPO_URL" "$UPSTREAM_IAC_BRANCH" | awk '{print $1}')

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

git clone --branch ${GITLAB_SIDECAR_REPO_BRANCH} \
  "https://oauth2:${GITLAB_SIDECAR_REPO_PAT}@${GITLAB_SIDECAR_REPO_URL}" \
  $WORKDIR/crab-k8s-overlays

# Workaround, automatically bump chart via vendoring until we have clear helm chart registry. then we bumped chart version instead.
yq -y -i --arg LATEST_COMMIT_SHA "$LATEST_COMMIT_SHA" '.directories[0].contents[0].git.ref = $LATEST_COMMIT_SHA' $WORKDIR/crab-k8s-overlays/vendir.yml

# Automatically bump image regarding DevOperator desired image.
yq -i -y --arg DESIRED_IMAGE "$DESIRED_IMAGE" '.image.tag = $DESIRED_IMAGE' $WORKDIR/crab-k8s-overlays/helm/crabserver/values-$DEPLOY_ENV.yaml

pushd $WORKDIR/crab-k8s-overlays
vendir sync
git config user.name  "$CI_BOT_USER_NAME"
git config user.email "$CI_BOT_USER_EMAIL"
git commit -am "bumped ${DEPLOY_ENV}'s crabserver:${NEW_IMAGE_TAG}, chart:${LATEST_COMMIT_SHA}"
git push -u origin $GITLAB_SIDECAR_REPO_BRANCH
popd
