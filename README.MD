# **NEW** learn CRABServer via [DeepWiki](https://deepwiki.com/dmwm/CRABServer)

### CRAB CI/CD flow with [Jenkins](https://cmssdt.cern.ch/dmwm-jenkins/)
##### 1. Building RPMs
* Each release created in CRABServer repository triggers [Jenkins job](https://cmssdt.cern.ch/dmwm-jenkins/job/CRABServer_BuildOnRelease) that builds RPMs for crabserver and crabtaskworker. 
* In `master` branch agreed naming convention for tagging new releases should follow this logic: `v3.YYMMDD`. Note: **dash is not allowed in branch or tag name as RPM build will fail.** 
* Built RPMs are stored in `comp.crab_[branch]` repository every time overwriting old RPMs files. `[branch]` stands for the branch name which was used to create new release, e.g. `comp.crab_master`.

##### 2. Building Docker images
* If RPMs build job was successful, [Jenkins job](https://cmssdt.cern.ch/dmwm-jenkins/job/CRABServer_BuildImage/) that builds Docker images is triggered.  
* Built images are stored in `cmssw/[crabserver|crabtaskworker]` Docker Hub repository while crabserver is also stored in [CERN Registry](https://registry.cern.ch/harbor/projects/1771/repositories/crabserver).
* After build is done, email notification is sent confirming that overall build process is done with indication whether build failed or succeeded. Log file is attached to the email for investigation.

##### 3. Deploying crabserver
* crabserver can be deployed to given environment by entering needed parameters in this [Jenkins job](https://cmssdt.cern.ch/dmwm-jenkins/job/CRABServer_Deploy_Server/build?delay=0sec).
* Two parameters should be provided before starting the build job: `CRABServer_image` and `REST_Instance`.
* `CRABServer_image` requires to enter image tag that should be deployed.
* `REST_Instance` requires to enter `k8s` host name where provided tag should be deployed. Default value points to `cmsweb-test2.cern.ch`.
* After deployment of crabserver is done, email notification is sent confirming that deployment process is done with indication whether it failed or succeeded. Log file is attached to the email for investigation. 

##### 4. Deploying TaskWorker/Publisher
* TaskWorker/Publisher can be deployed to given environment by entering needed parameters in this [Jenkins job](https://cmssdt.cern.ch/dmwm-jenkins/job/CRABServer_Deploy_TW/build?delay=0sec).
* Three parameters should be provided before starting the build job: `Service`, `Image` and `Environment`.
* `Service` requires to select which service should be deployed: TaskWorker or Publisher.
* `Image` requires to enter image tag that should be deployed.
* `Environment` requires to enter host name where provided tag should be deployed. Default value points to `crab-dev-tw01`.
* After deployment of TaskWorker/Publisher is done, email notification is sent confirming that deployment process is done with indication whether it failed or succeeded. Log file is attached to the email for investigation. 

##### 5. Running testing with Jenkins
* One can start running testing suits in this [Jenkins job](https://cmssdt.cern.ch/dmwm-jenkins/job/CRABServer_ExecuteTests/).
* 3 kinds of testings can be started: `Client_Validation_Suite`, `Task_Submission_Status_Tracking` and `Client_Configuration_Validation`.
* Different kinds of testings and needed to provide parameters are explained in Jenkins jobs description.

