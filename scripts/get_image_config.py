#!/usr/bin/env python3
from __future__ import print_function
import yaml
import sys, re
from os.path import exists, join, dirname, abspath
from docker_utils import get_digest
from datetime import datetime
import hashlib
now = datetime.now()

regex_var = re.compile('^(.*?)([$]{1,2})\{([^}]+)\}(.*)$')

def push_info(setup, data, variables=False):
  data.append({})
  for k in setup:
    if k in ['tags', 'groups']: continue
    if type(setup[k])==dict:
      push_info(setup[k], data, variables or k=="variables")
    else:
      sval = "" if setup[k] is None else str(setup[k])
      if variables:
        if not ".variables" in data[0]:
          data[0][".variables"]={}
        data[0][".variables"][k] = sval
      data[-1][k]=sval

def pop_info(data, cnt):
  while len(data)>cnt: data.pop()
  return

def get_key(key, data):
  for item in data[::-1]:
    if key in item: return str(item[key])
  return ""

def expand_var(var, data):
  while True:
    m = regex_var.match(var)
    if not m: break
    val = ""
    if m.group(2)=='$$': val = eval(m.group(3))
    else: val = get_key(m.group(3), data)
    var = "%s%s%s" % (m.group(1), val, m.group(4))
  return var

def expand(data):
  nbuilds = []
  for item in data:
    nbuilds.append({})
    for k in item:
      if k=='.variables': continue
      v = item[k]
      k = expand_var(k, data)
      nbuilds[-1][k] = expand_var(str(v), data)
  return nbuilds

def process_tags(setup, data, images):
  if 'tags' not in setup: return
  for tag in setup['tags']:
    cnt = len(data)
    if setup['tags'][tag]:
      push_info(setup['tags'][tag], data)
    data[-1]['tag']=tag
    img_data = expand(data)
    pop_info(data, cnt)
    image_name = get_key('container', img_data) + ":"+get_key('tag', img_data)
    if get_key('disabled', img_data)=="True":
      print("Ignoring",image_name,"as it is marked disabled")
      continue
    arch = get_key('architecture', img_data)
    print("\n** Working on",get_key('from', img_data),"for arch",arch)
    res, from_manifest = get_digest(get_key('from', img_data), arch, debug=True)
    print("Base Image: ",get_key('from', img_data),arch,res,from_manifest)
    if not res:
      print("Base image ",get_key('from', img_data),arch,"not available yet.")
      continue
    watch_img = get_key('watch', img_data)
    watch_manifest = ""
    if watch_img:
      res, watch_manifest = get_digest(watch_img, arch, debug=True)
      if not res:
        print("Watch image ",watch_img,arch,"not available yet.")
        continue
    override = get_key('override', img_data).lower()
    if override != 'true':
      res , manifest = get_digest(image_name, arch)
      print("Existing image",res , manifest)
      if manifest: continue
      override = "false"

    images.append({})
    images[-1]['OVERRIDE_TAG']=override
    images[-1]['DOCKER_REPOSITORY']=get_key('repository', img_data)
    images[-1]['DOCKER_NAME']=get_key('name', img_data)
    images[-1]['DOCKER_CONTAINER']=get_key('container', img_data)
    images[-1]['IMAGE_NAME']=image_name
    images[-1]['IMAGE_TAG']=get_key('tag', img_data)
    images[-1]['IMAGE_TAG_ALIAS']=get_key('alias', img_data)
    images[-1]['MULTI_ARCH']=get_key('multi_arch', img_data)
    images[-1]['BASE_UPSTREAM_CHECKSUM']=from_manifest

    base_image = get_key('from', img_data)
    if not '/' in base_image: base_image="library/"+base_image
    if not ':' in base_image: base_image=base_image+":latest"
    images[-1]['BASE_DOCKER_REPOSITORY']= base_image.split("/")[0]
    images[-1]['BASE_DOCKER_NAME']=base_image.split(":")[0].split("/")[1]
    images[-1]['BASE_DOCKER_CONTAINER']=base_image.split(":")[0]
    images[-1]['BASE_IMAGE_NAME']=base_image
    images[-1]['BASE_IMAGE_TAG']=base_image.split(":")[1]

    images[-1]['IMAGE_BUILD_ARGS']=get_key('args', img_data)
    images[-1]['IMAGE_PUSH']=get_key('push', img_data)
    images[-1]['DOCKER_FILE']=get_key('docker', img_data)
    images[-1]['TEST_SCRIPT']=get_key('script', img_data)
    images[-1]['TEST_NODE']=get_key('node', img_data)
    images[-1]['ARCHITECTURE']=arch
    images[-1]['BUILD_CONTEXT']="."
    images[-1]['NOTIFY_TO']="all"
    for xkey in ['delete_pattern', 'expires_days', 'build_context']:
      val = get_key(xkey, img_data)
      if val:
        images[-1][xkey.upper()]=val
    chkdata = [from_manifest]
    if watch_manifest:
      chkdata.append(watch_manifest)
    if ".variables" in data[0]:
      for v in data[0][".variables"]:
        images[-1][v] = get_key(v, img_data)
        if (not v in ['SKIP_TESTS', 'CVMFS_UNPACKED', 'BUILD_DATE', 'NOTIFY_TO', 'CMS_COMPATIBLE_OS', 'CI_TESTS', 'BUILD_CONTEXT']) and images[-1][v]:
          chkdata.append("%s=%s" % (v, images[-1][v]))

    config_dir = get_key('config_dir', img_data)
    scripts_dir = config_dir + "/" + images[-1]['BUILD_CONTEXT']
    docFile = join(config_dir, images[-1]['DOCKER_FILE'])
    print("base man:",from_manifest)
    if watch_manifest: print("watch man:",watch_manifest)
    print("tag:",image_name)
    with open(docFile, encoding="utf-8") as ref:
        chkdata.append(hashlib.md5(ref.read().encode()).hexdigest())
    print("chksum:", docFile, chkdata[-1])
    with open(docFile, encoding="utf-8") as ref:
      for line in ref.readlines():
          items = [i for i in line.split(" ") if i]
          if (items[0] not in ["ADD", "COPY"]) or (":" in items[1]):
            continue
          xfile = join(scripts_dir, items[1])
          with open(xfile, encoding="utf-8") as xref:
            chkdata.append(hashlib.md5(xref.read().encode()).hexdigest())
          print("chksum:", xfile, chkdata[-1])
    print("Full checksum",chkdata)
    images[-1]['BUILD_CHECKSUM'] = hashlib.md5(("\n".join(chkdata)).encode()).hexdigest()
  return

def process_groups(setup, data, images):
  if 'groups' not in setup: return
  prev_group = get_key("group", data)
  gcount = int(get_key("group_count", data)) + 1
  for group in setup['groups']:
    cnt = len(data)
    data.append({})
    if prev_group == "":
      data[-1]['group'] = group
    else:
      data[-1]['group'] = "%s-%s" % (prev_group, group)
    data[-1]['group_count'] = gcount
    data[-1]['group%s' % gcount] = group
    push_info(setup['groups'][group], data)
    process_tags(setup['groups'][group], data, images)
    process_groups(setup['groups'][group], data, images)
    pop_info(data, cnt)

def get_docker_images(name, repository='cmssw'):
  images = []
  setup_file = join(dirname(dirname(abspath(__file__))), name, "config.yaml")
  if not exists(setup_file):
    print("Warnings: No such file %s" % setup_file)
    return images
  with open(setup_file) as file:
    try:
      from yaml import FullLoader
      setup = yaml.load(file, Loader=FullLoader)
    except ImportError as e:
      setup = yaml.load(file)
  data = [{}]
  data[-1]['docker'] = "Dockerfile"
  data[-1]['config_dir'] = dirname(setup_file)
  data[-1]['repository'] = repository
  data[-1]['name'] = name
  data[-1]['container'] = join(repository, name)
  data[-1]['group'] = ""
  data[-1]['group_count'] = -1
  data[-1]['disabled'] = False
  push_info(setup, data)
  if not 'groups' in setup:
    setup['groups'] = {'default' : {'tags': dict(setup['tags'])}}
    del setup['tags']
  process_groups(setup, data, images)
  return images

if __name__ == "__main__":
  for name in sys.argv[1:]:
    for img in get_docker_images(name):
      print (img)
