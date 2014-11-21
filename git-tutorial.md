---
title: CRABServer
layout: default
related:
 - { name: Project, link: "https://github.com/dmwm/CRABServer" }
 - { name: Feedback, link: "https://github.com/dmwm/CRABServer/issues/new" }
 - { name: Documentation, link: "https://twiki.cern.ch/twiki/bin/viewauth/CMSPublic/SWGuideCrab" }
---

This is the documentation page for CRABServer.

## Tag structure

Each release series should have a tag called:
    3.X.X.rcXX or 3.X.X.preXX

## Default branch

All changes on official repo must be done to default branch. The default branch of CRABServer is `master`

## Newcomer setup

Fork the CRABServer repository by [clicking
here](https://github.com/dmwm/CRABServer/fork). Note down the url of your
repository, you'll need it to propose your changes.

* `<forked_repo>` Your repository url.

### For every new feature / bug fix you want to propose.

0) Prepare your working directory

      WORK_DIR=/data/user/

1) Create build area and prepare CRABServer for modify:

      mkdir -p $WORK_DIR
      cd $WORK_DIR
      git clone https://github.com/dmwm/CRABServer.git

2) Create branch and make your changes on it.

      cd $WORK_DIR/CRABServer
      git branch <branch_name>
      git checkout <branch_name>
      vi <any_file>

where:

* `<branch_name>` your custom branch name (Name it in one or two words about changes)
* `<any_file> Edit files which you would like to merge later`

3) Once you are satisfied with the above push your changes to your repository. If needed, squash your commits into one, so we can keep a bit of sanity in CRABServer:

      eval `ssh-agent`
      ssh-add
      cd $WORK_DIR/CRABServer
      git add <file_name>
      git commit -m '<commit_message>' -s
      git remote add $USER <forked_repo>
      git push $USER <branch_name>

where

* `<file_name>` - files which you edited and want to add to pull request. You can see edited files using `git status`.
* `<commit_message>` - Commit message. For sanity, please describe what it fixes. If you are also fixing issue, add `fixes #<issue_id>`.
* `<issue_id>` - You can see issue id [here](https://github.com/dmwm/CRABServer/issues) on selected issue. It starts with #XXXX.
* `<forked_rep>` - Your forked repository url.
* `<branch_name>` - Your branch name.

4) [Create a Pull Request from your repository to the official one](https://github.com/juztas/CRABServer/compare/).

## Other useful info

### Keep your forked repo up to date

      cd $WORK_DIR/CRABServer
      git checkout master
      git fetch
      git status
      git pull origin
      git push $USER master

### TODO

* How to squash
* How to fix conflicts
* Else...

