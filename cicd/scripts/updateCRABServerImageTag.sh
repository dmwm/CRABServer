set -euo pipefail
DEPLOY_ENV="${DEPLOY_ENV:?DEPLOY_ENV is required}"
NEW_IMAGE_TAG="${NEW_IMAGE_TAG:?NEW_IMAGE_TAG is required}"

UPSTREAM_IAC_REPO="${UPSTREAM_IAC_REPO:=nausikt/CMSKubernetes}"
UPSTREAM_IAC_BRANCH="${UPSTREAM_IAC_BRANCH:=crab}"
CI_BOT_USER_NAME="${CI_BOT_USER_NAME:=crab-gitlab-ci}"
CI_BOT_USER_EMAIL="${CI_BOT_USER_EMAIL:=cms-service-crab-operators@cern.ch}"
CI_COMMIT_AUTHOR="${CI_COMMIT_AUTHOR:=crab-gitlab-ci}"

# Setup SSH deploy key
mkdir -p ~/.ssh
KEY_PATH="$HOME/.ssh/crab-gitlab-ci.deploy"
echo "${UPSTREAM_IAC_REPO_DEPLOY_PRIVATE_PKI_BASE64}" | base64 -d | tr -d '\r' > "$KEY_PATH"
chmod 600 "$KEY_PATH"
ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null
export GIT_SSH_COMMAND="ssh -i $KEY_PATH -o IdentitiesOnly=yes"

# Get latest SHA
LATEST_COMMIT_SHA=$(git ls-remote \
  "git@github.com:${UPSTREAM_IAC_REPO}.git" \
  "$UPSTREAM_IAC_BRANCH" | awk '{print $1}')

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR" "$KEY_PATH"' EXIT     # cleanup on exit

git clone --branch ${UPSTREAM_IAC_BRANCH} \
  "git@github.com:${UPSTREAM_IAC_REPO}" \
  $WORKDIR/CMSKubernetes

# Automatically bump image regarding DevOperator desired image.
yq -i -y --arg NEW_IMAGE_TAG "$NEW_IMAGE_TAG" \
  '.image.tag = $NEW_IMAGE_TAG' \
  "$WORKDIR/CMSKubernetes/helm/crabserver/values-${DEPLOY_ENV}.yaml"

pushd "$WORKDIR/CMSKubernetes"
git config user.name  "$CI_BOT_USER_NAME"
git config user.email "$CI_BOT_USER_EMAIL"
git commit -am "deploy(${DEPLOY_ENV}): crabserver=${NEW_IMAGE_TAG} commit=${LATEST_COMMIT_SHA:0:7} by ${CI_COMMIT_AUTHOR}"
git push -u origin $UPSTREAM_IAC_BRANCH
popd
