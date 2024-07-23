#!/usr/bin/env python3

from docker_utils import (get_repos, get_teams, get_permissions, get_members, create_repo, create_team,
                          add_permissions, add_member, delete_repo, delete_team, delete_permissions, delete_member)
from argparse import ArgumentParser
import yaml
import sys
from os.path import join, dirname, abspath

parser = ArgumentParser(description='Synchronize Docker HUB with yaml configuration file')
parser.add_argument('-u', '--username', dest='username', help="Provide Docker Hub username for synchronization", type=str, default='cmssw')
parser.add_argument('-n', '--dry-run', dest='dryrun', help="Dry Run mode enabled by default. Disable it to make changes to docker hub", action="store_true", default=False)
args = parser.parse_args()
changes_applied = False
if not args.dryrun:
  print('==== DRY RUN MODE DISABLED ====')

def update_dockerhub(config_file, docker_hub, username = args.username, team_name = None, repo = None, 
                     team_id = None, yaml_permissions = None, what_to_sync = None, dryrun=args.dryrun):
  dryrun_message = 'Dry run mode enabled, no changes to Docker Hub applied'
  global changes_applied
  diff_list = [item for item in config_file + docker_hub if item not in config_file or item not in docker_hub]
  for list_item in diff_list:
    if list_item in config_file and list_item not in docker_hub:
      changes_applied = True
      if what_to_sync == 'repos':
        print('###### Adding repository: "%s"' % list_item)
        print(dryrun_message) if dryrun else print(create_repo(username, list_item)[0])
      elif what_to_sync == 'teams':
        print('###### Creating team: "%s"' % list_item)
        print(dryrun_message) if dryrun else print(create_team(username, list_item)[0])
      elif what_to_sync == 'permissions':
        print('###### Adding "%s" permission to "%s" repository for "%s" team:' % (yaml_permissions[list_item], list_item, team_name))
        print(dryrun_message) if dryrun else print(add_permissions(username, list_item, team_id, yaml_permissions[list_item])[0])
      elif what_to_sync == 'members':
        print('###### Adding member "%s" to "%s" team:' % (list_item, team_name))
        print(dryrun_message) if dryrun else print(add_member(username, team_name, list_item)[0])
    if list_item in docker_hub and list_item not in config_file:
      changes_applied = True
      if what_to_sync == 'repos':
        print('###### Deleting repository: "%s"' % list_item)
        if dryrun: print(dryrun_message)
        else:
          delete_status = delete_repo(username, list_item)
          if not delete_status:
            print('Error: %s repository is not empty. Could not be removed' % list_item)
            sys.exit(1)
          elif not delete_status[0]:
            print(delete_status)
            sys.exit(1)
          else: print(delete_status[0])
      elif what_to_sync == 'teams':
        print('###### Deleting team: "%s"' % list_item)
        if dryrun: print(dryrun_message)
        else:
          delete_status = delete_team(username, list_item)
          if not delete_status:
            print('Error: %s team is not empty. Could not be removed' % list_item)
            sys.exit(1)
          elif not delete_status[0]:
            print(delete_status)
            sys.exit(1)
          else: print(delete_status[0])
      elif what_to_sync == 'permissions':
        print('###### Deleting permission for "%s" repository from "%s" team:' % (list_item, team_name))
        print(dryrun_message) if dryrun else print(delete_permissions(username, list_item, team_id)[0])
      elif what_to_sync == 'members':
        print('###### Deleting member "%s" from "%s" team:' % (list_item, team_name))
        print(dryrun_message) if dryrun else print(delete_member(username, team_name, list_item)[0])

yaml_location = join(dirname(dirname(abspath(__file__))), "docker-config.yaml")
with open(yaml_location) as file:
  try:
    from yaml import FullLoader
    yaml_file = yaml.load(file, Loader=FullLoader)
  except ImportError as e:
    yaml_file = yaml.load(file)
repos_config_dict = yaml_file['repositories']
yaml_repos = repos_config_dict.keys()
yaml_teams = yaml_file['teams'].keys()
# UPDATE REPOSITORIES:
print('\nSynchronizing repositories for "%s":' % args.username)
hub_repos = get_repos(args.username)
if not hub_repos[0]: print(hub_repos[1]) & sys.exit(1)
if hub_repos[1] == []:
  print('##### Error: No repositories found. Check Docker Hub username')
  sys.exit(1)
update_dockerhub(list(yaml_repos), list(hub_repos[1]), what_to_sync='repos')
# UPDATE TEAMS:
print('\nSynchronizing teams for "%s":' % args.username)
hub_teams = get_teams(args.username)
if not hub_teams[0]: print(hub_teams[1]) & sys.exit(1)
update_dockerhub(list(yaml_teams), list(hub_teams[1].keys()), what_to_sync='teams') 
team_acces_map = {}
hub_teams = get_teams(args.username)[1]
for team_name in hub_teams:
  if team_name != 'owners':
  # UPDATE PERMISSIONS:
    print('\nSynchronizing permissions for "%s" team:' % team_name)
    hub_permissions = get_permissions(args.username, team_name)
    yaml_permissions = {}
    if not hub_permissions[0]: print(hub_permissions[1]) & sys.exit(1)
    for repo in repos_config_dict.keys():
      try:
        yaml_permissions[repo] = repos_config_dict[repo][team_name]
      except Exception:
        continue
    if not yaml_permissions:
      print('No repository permissions found in yaml config file for "%s"' % team_name)
      yaml_access_list = []
    else:
      yaml_access_list = yaml_permissions.keys()
    permissions_to_add = {}
    for item in hub_permissions[1].items():
      repository, permission = item[0], item[1]
      try:
        if yaml_permissions and yaml_permissions[repository] != permission:
          continue
      except KeyError as error:
        print('###### Error: "%s" repository does not exist on Docker Hub' % repository)
        continue
      permissions_to_add[repository] = permission
    docker_access_list = []
    docker_access_list = permissions_to_add.keys()
    team_id = hub_teams[team_name]
    update_dockerhub(list(yaml_access_list), list(docker_access_list), team_name=team_name, 
    team_id=team_id, yaml_permissions=yaml_permissions, what_to_sync = 'permissions')
    # UPDATE MEMBERS:
    print('\nSynchronizing members for "%s" team:' % team_name)
    hub_team_members = get_members(args.username, team_name)
    if team_name not in yaml_teams:
      continue
    if not hub_team_members[0]: print(hub_team_members[1]) & sys.exit(1)
    members_in_yaml = yaml_file['teams'][team_name]
    if members_in_yaml is None: 
      print('No members found in yaml config file for "%s"' % team_name)
      members_in_yaml = []
    update_dockerhub(members_in_yaml, hub_team_members[1], team_name=team_name, what_to_sync='members')
if args.dryrun and changes_applied:
  print('\nDOCKER HUB CONFIGURATION CHANGED\n')
  sys.exit(1)
