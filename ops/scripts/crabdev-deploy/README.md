# Crab Server Installation Scripts

This repo contains scripts to build a CRAB 3 development testbed, assuming you're working on CERN lxplus system and interacting with the Openstack facility.

## Usage

The typical workflow of the first time you use this script collection is:
  * `cd CRABServer/scripts/crabdev-deploy/`
  * `cp paramsrc.sample paramsrc`
  * customize `paramsrc` (read below)
  * put a host certificate and key in the `certs` directory. the file have to be named `my-host-certificate.pem` and `my-host-key.pem`
  * `./vm-deploy.sh my-crab-server 'SLC6 CERN Server - x86_64 [2015-02-10]' # consider that if the vm already exists it will be reinstalled from scratch`
  *  wait until you can ssh to the VM
  * `ssh my-crab-server`
  * `cd path/to/crabdev-deploy`
  * `./crabserver-install-part1.sh # at the end will reboot automatically`
  * `ssh my-crab-server`
  * `./crabserver-install-part2.sh`

## Configuration

During `crabserver-install-part2.sh` the configuration file `paramsrc` is used where shell-variable have to be defined. The paramaters are:

  * `ORACLEUSER`: your oracle user
  * `ORACLEPASS`: your oracle password
  * `GITUSER`: your github user account
  * `INITDB`: if defined the oracle database will be initialized. Necessary only the first time
  * `HGVER`: version of the crab server. It is an optional parameter. If it is not configured, the last one will be used (you can obtain it with `git tag -l 'HG*'|tail -1` in the git repository `git://github.com/dmwm/deployment.git`)
  * `REST_HOSTNAME`: hostname of the RESTful CRAB3 interface, aka Crab Server
  * `TW_HOSTNAME`: hostname of the Task Worker. if not defined REST_HOSTNAME will be used instead
  * `GISTEXTURL`: (eg. `https://gist.githubusercontent.com/talamoig/a46f05a991df431febb2/raw/gistfile1.txt`) If you don't provide it, a suggestion
of its content will be printed, according to the other parameters

Consider that you should have:

  * a oracle account;
  *  `CRABServer` and `WMCore` repositories forked on your account.


## Content of the directory

The files are:

  * `openstack-init.sh`: creates the `~/.openrc` file to interact with CERN Openstack from the command line;
  * `vm-deploy.sh`: to build (and rebuild) CERN Openstack VMs from the command line;
  * `crabserver-install-part1.sh`: to do the first part of the installation of a crab server (steps 1 and 2 of https://cms-http-group.web.cern.ch/cms-http-group/tutorials/environ/vm-setup.html);
  * `crabserver-install-part2.sh`: to do the second part of the installation (from "Machine Preparation" until the end in https://twiki.cern.ch/twiki/bin/view/CMSPublic/CMSCrabRESTInterface)
  * `paramsrc.sample`: a sample file with parameters necessary for the installation (rename it to `paramsrc`);
  * `paramsrc-filecheck.sh`: script for checking that all needed parameters are present in `paramsrc`.
