TAG=pypi-devthree-$(date +"%s")
git tag $TAG
git push gitlab $TAG -o ci.variable="MANUAL_CI_PIPELINE_ID=7174671" -o ci.variable="SKIP_DEPLOY=t" -o ci.variable="SKIP_BUILD=t"
