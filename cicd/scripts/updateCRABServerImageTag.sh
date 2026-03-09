set -euo pipefail
DEPLOY_ENV="${DEPLOY_ENV:?DEPLOY_ENV is required}"
NEW_IMAGE_TAG="${NEW_IMAGE_TAG:?NEW_IMAGE_TAG is required}"

UPSTREAM_IAC_REPO_URL="${UPSTREAM_IAC_REPO_URL:=github.com/nausikt/CMSKubernetes}"
UPSTREAM_IAC_BRANCH="${UPSTREAM_IAC_BRANCH:=crab}"
UPSTREAM_IAC_REPO_PAT="${UPSTREAM_IAC_REPO_PAT:?UPSTREAM_IAC_REPO_PAT is required}"
CI_BOT_USER_NAME="${CI_BOT_USER_NAME:=crab-gitlab-ci}"
CI_BOT_USER_EMAIL="${CI_BOT_USER_NAME:=krittin.phornsiricharoenphant@cern.ch}"
CI_COMMIT_AUTHOR="${CI_COMMIT_AUTHOR:=crab-gitlab-ci}"

LATEST_COMMIT_SHA=$(git ls-remote "$UPSTREAM_IAC_REPO_URL" "$UPSTREAM_IAC_BRANCH" | awk '{print $1}')

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

git clone --branch ${UPSTREAM_IAC_BRANCH} \
  "https://${UPSTREAM_IAC_REPO_PAT}@${UPSTREAM_IAC_REPO_URL}" \
  $WORKDIR/CMSKubernetes

# Automatically bump image regarding DevOperator desired image.
yq -i -y --arg NEW_IMAGE_TAG "$NEW_IMAGE_TAG" '.image.tag = $NEW_IMAGE_TAG' $WORKDIR/CMSKubernetes/helm/crabserver/values-$DEPLOY_ENV.yaml

pushd $WORKDIR/CMSKubernetes
git config user.name  "$CI_BOT_USER_NAME"
git config user.email "$CI_BOT_USER_EMAIL"
git commit -am "deploy(${DEPLOY_ENV}): crabserver=${NEW_IMAGE_TAG} chart=${LATEST_COMMIT_SHA:0:7} by ${CI_COMMIT_AUTHOR}"
git push -u origin $UPSTREAM_IAC_BRANCH
popd
