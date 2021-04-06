### CRAB CI/CD flow
##### 1. Building RPMs
* Each new release created in CRABServer repository triggers [Jenkins job](https://cmssdt.cern.ch/dmwm-jenkins/job/CRABServer_BuildOnRelease) that builds RPMs and docker image for crabcache, crabserver, crabtaskworker. 
* In `master` branch agreed naming convention for tagging new releases should follow this logic: `v3.YYMMDD`. Note: **dash is not allowed in branch or tag name.** 
* Built RPMs are stored in `comp.crab_[branch]` repository every time overwriting old RPMs files. `[branch]` stands for the branch name which was used to create new release, e.g. `comp.crab_master`.

##### 2. Building Docker images

* If RPMs build job was successful, [Jenkins job](https://cmssdt.cern.ch/dmwm-jenkins/job/CRABServer_BuildImage/) that builds Docker images is triggered.  
* Built images are stored in `cmssw/[crabcache|crabserver|crabtaskworker]` Docker Hub repository.
* After build is done, email notification is sent confirming that overall build process is done with indication whether build failed or succeed. Log is attached to the email for investigation.

##### 3. Deploying crabserver
* crabserver can be deployed to given environment by entering needed parameters in this [Jenkins job](https://cmssdt.cern.ch/dmwm-jenkins/job/CRABServer_Deploy_Server/build?delay=0sec).
* Two parameters should be provided before starting the build job: `DOCKER_IMAGE_TAG` and `ENVIRONMENT`.
* `DOCKER_IMAGE_TAG` requires to enter image tag that should be deployed.
* `ENVIRONMENT` requires to enter `k8s` host name where provided tag should be deployed. Default value points to `cmsweb-testbed.cern.ch`.
* After deployment of crabserver is done, email notification is sent confirming that deployment process is done with indication whether it failed or succeed. Log is attached to the email for investigation. 
