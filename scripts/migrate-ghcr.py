#!/usr/bin/env python3
import sys, yaml, re
from argparse import ArgumentParser
from subprocess import getstatusoutput as run_cmd
from os.path import join, dirname, abspath, expanduser
from docker_utils import get_tags, get_repos, hub_request
from time import sleep
topdir = dirname(dirname(abspath(__file__)))
sys.path.insert(0,join(topdir,'..', "cms-bot"))
from github_utils import get_org_packages, get_org_package_versions, get_org_package_version

token_file = expanduser("~/.ghcr_token")

parser = ArgumentParser()
parser.add_argument("-r", "--fullrun")
args = parser.parse_args()
full_run = args.fullrun

def get_manifest(image):
  """Retrieve SHA256 digest of Docker image manifest."""
  e , sha = run_cmd("docker manifest inspect %s | grep '\"digest\"' | head -1 | sed 's|.*sha256:||;s|\".*||'" % image)
  return sha.strip()

def create_repo (name, repo):
  """Create a new repository in GitHub with the given name and remote location."""
  print("  Creating ",name, repo)
  e, o = run_cmd("%s/bin/create-gh-package.sh %s %s" % (topdir, name, repo))
  if e:
    print(o)
    return False
  return True

def push_tag(name, tag):
  """Push a Docker image tag to the GitHub container registry."""
  print("  --> Pushing tag cmssw/%s:%s" % (name, tag))
  e, o = run_cmd("%s/bin/hub2ghcr.sh %s %s" % (topdir, name, tag))
  if e:
    print(o)
    return False
  return True

def add_tags(name):
  """Push Docker image tags to the GitHub container registry.

    Compares the tags for a given package from GitHub
    and DockerHub, and pushes any new tags to GitHub using the `push_tag` function.
  """
  rname = "cmssw%%2F%s" % name
  res, hub_tags = get_tags(rname, full=True)

  if not res: return res
  gh_tags = get_org_package_versions("cms-sw", package=rname, token_file=token_file)
  gh_sha = {}
  hub_sha = {}
  # Fill gh_sha dict with GitHub tag names as keys and their corresponding sha as values
  for tag in gh_tags:
    for tag_name in tag['metadata']['container']['tags']:
      # tag_last_update = datetime.datetime.strptime(tag['updated_at'], '%Y-%m-%dT%H:%M:%SZ').date()
      gh_sha[tag_name] = tag['name']  # In GitHub, tag['name'] contains the sha value

  moving_tags = ["^latest$"]
  moving_tags.append("^(x86_64|aarch64|ppc64le)(-grid|-runtime|-bootstrap|)$")
  moving_tags.append("^(rhel[0-9]+)(-itb|)(-x86_64|-aarch64|-ppc64le|)$")
  
  # Check #1: Check if tag already exists in GitHub based on the tag name
  for tag in hub_tags:
    try:
      sha=tag['digest']
    except:
      try:
        sha = tag['images'][-1]['digest']
      except:
        print("  ERROR: No digest ", tag['name'], tag)
        continue
    tag_name = tag['name']  # In DockerHub, tag['name'] contains the tag name
    #tag_last_update = datetime.datetime.strptime(tag['tag_last_pushed'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
    if (not [exp for exp in moving_tags if re.match(exp, tag_name)]) and (tag_name in gh_sha):
      if full_run == "true":  ## 
        print("  Tag exists: ", tag_name)
        continue
      else:  ## if full_run is false, we break once the tag exists (they are ordered by date)
        break
    # Fill hub_sha dict with changing DockerHub tag names as keys and their corresponding sha as values
    if not tag_name.startswith("tmp-"):
      hub_sha[tag_name] = sha

  # Check #2: Check if tag exists based on sha or image manifest, if tag name changes over time
  for tag_name in hub_sha:
    if (tag_name in gh_sha) and (hub_sha[tag_name]==gh_sha[tag_name]):
      print("  Tag exists: ", tag_name)
      continue
    img = "cmssw/%s:%s" % (name, tag_name)
    if get_manifest(img) == get_manifest("ghcr.io/cms-sw/"+img):
      print("  Tag exists: ", tag_name)
    else:
      # Push non-existing tags
      if not push_tag(name, tag_name):
        return False
  return True

name_repo_map = {
  'cmssw' : 'cms-docker',
  'dmwm'  : 'dmwm-containers',
}

repo_conf_file = join(topdir, "docker-config.yaml")
repo_conf = {}
with open(join(dirname(dirname(abspath(__file__))), "docker-config.yaml")) as ref:
  try:
    from yaml import FullLoader
    repo_conf = yaml.load(ref, Loader=FullLoader)
  except ImportError as e:
    repo_conf = yaml.load(ref)

gh_repos = get_org_packages("cms-sw", token_file=token_file)
print("%s GH Repos found:\n  %s" % (len(gh_repos), "\n  ".join([r['name'] for r in gh_repos])))
for name in repo_conf['repositories']:
  fname = "cmssw/%s" % name
  repo_obj = repo_conf['repositories'][name]
  found=False
  print("Working on repository ",fname)
  for team in name_repo_map:
    if team in repo_obj:
      if not [True for r in gh_repos if r['name']==fname]:
        create_repo (name, name_repo_map[team])
      else:
        print("  OK already exists.")
      if not add_tags(name):
        sys.exit(1)
      found=True
      break
  if not found:
    print("ERROR: Unknown team: ",team,repo_obj)
