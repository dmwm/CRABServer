set -euo pipefail
DEPLOY_ENV="${DEPLOY_ENV:?DEPLOY_ENV is required}"
NEW_IMAGE_TAG="${NEW_IMAGE_TAG:?NEW_IMAGE_TAG is required}"
DESIRED_IMAGE="${DESIRED_IMAGE:=registry.cern.ch/cmscrab/crabserver:$NEW_IMAGE_TAG}"

UPSTREAM_IAC_REPO_URL="${UPSTREAM_IAC_REPO_URL:=https://github.com/sinonkt/CMSKubernetes}"
UPSTREAM_IAC_BRANCH="${UPSTREAM_IAC_BRANCH:=migrate-to-argocd}"
GITLAB_SIDECAR_REPO_URL="${GITLAB_SIDECAR_REPO_URL:=gitlab.cern.ch/kphornsi/crab-iac}"
GITLAB_SIDECAR_REPO_BRANCH="${GITLAB_SIDECAR_REPO_BRANCH:=master}"
GITLAB_SIDECAR_REPO_PAT="${GITLAB_SIDECAR_REPO_PAT:?GITLAB_SIDECAR_REPO_PAT is required}"
CI_BOT_USER_NAME="${CI_BOT_USER_NAME:=crabserver-ci-bot}"
CI_BOT_USER_EMAIL="${CI_BOT_USER_NAME:=krittin.phornsiricharoenphant@cern.ch}"

LATEST_COMMIT_SHA=$(git ls-remote "$UPSTREAM_IAC_REPO_URL" "$UPSTREAM_IAC_BRANCH" | awk '{print $1}')

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

git clone --branch ${GITLAB_SIDECAR_REPO_BRANCH} \
  "https://oauth2:${GITLAB_SIDECAR_REPO_PAT}@${GITLAB_SIDECAR_REPO_URL}" \
  $WORKDIR/crab-iac

yq -i -y --arg DESIRED_IMAGE "$DESIRED_IMAGE" '
  .spec.template.spec.containers |=
    map(if .name=="crabserver"
        then .image = $DESIRED_IMAGE
        else . end)
' $WORKDIR/crab-iac/argocd/apps/crab/crabserver/overlays/$DEPLOY_ENV/patches/image.yaml

pushd $WORKDIR/crab-iac
git config user.name  "$CI_BOT_USER_NAME"
git config user.email "$CI_BOT_USER_EMAIL"
git commit -am "bump ${DEPLOY_ENV} crabserver image -> ${NEW_IMAGE_TAG}, bump upstream base -> ${LATEST_COMMIT_SHA}"
git push -u origin $GITLAB_SIDECAR_REPO_BRANCH
popd
