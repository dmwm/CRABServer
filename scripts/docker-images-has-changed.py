#!/usr/bin/env python3
from __future__ import print_function
from argparse import ArgumentParser
from get_image_config import get_docker_images
from docker_utils import get_labels
import glob, os
from os.path import dirname
import json

def create_file(img):
  print('Writing to file ....')
  file_name = (img.get('IMAGE_NAME').replace('/', '_').replace(':', '_') + '.txt')
  with open(file_name,'w+') as file:
    for key, value in img.items():
      line = str(key) + '=' + str(value) + '\n'
      file.write(line)
  file.close()
  print(file_name,' file created')

def find_image_deps(image, cache):
  if image not in cache: return []
  return [cache[image]] + find_image_deps(cache[image], cache)

parser = ArgumentParser(description='Check if docker image based on the same layers as the parent image')
parser.add_argument('-r', dest='repo', type=str, help="Provide specific repository for docker images.")
parser.add_argument("-f", "--force",    dest="force",   action="store_true", help="Force re-build", default=False)
parser.add_argument('-t', '--tags',     dest='tags', help="Comma separated list of tags to process", type=str, default = '')
args = parser.parse_args()
repos = []
if args.repo:
  repos.append(args.repo)
else:
  dir = os.path.dirname(dirname(os.path.abspath(__file__)))
  for file in glob.glob(dir + '/**/*.yaml*'):
    repos.append(os.path.basename(dirname(file)))

tags = [ t for t in  args.tags.replace(' ','').split(",") if t]
imgs = {}
image_deps = {}
for reponame in repos:
  images_list = get_docker_images(reponame)

  # Generate a dictionary of base image checksum
  base_dict = {}
  for element in images_list:
    name = element["BASE_DOCKER_REPOSITORY"]
    arch = element["ARCHITECTURE"]
    if (name != "cmssw") or (element['BASE_IMAGE_NAME'] in ['cmssw/cc7:aarch64-d20240110', 'cmssw/cc7:x86_64-d20240110']):
        base_dict[arch] = element["BASE_UPSTREAM_CHECKSUM"]

  for img in images_list:
    if tags and (not img['IMAGE_TAG'] in tags): continue
    print("Working on ",img)
    buildimg = args.force
    image = img['IMAGE_NAME']
    base = img['BASE_IMAGE_NAME']
    image_deps[image] = base
    labels = get_labels(image)
    img['BASE_UPSTREAM_CHECKSUM'] = base_dict[img['ARCHITECTURE']]
    if not buildimg:
      buildimg = ('build-checksum' not in labels) or (labels['build-checksum'] != img['BUILD_CHECKSUM'])
      print("===>",image, base, buildimg, img['BUILD_CHECKSUM'])
    if buildimg:
      imgs[image] = img

print("\n")
build_imgs = []
for ximg in imgs:
  img = imgs[ximg]
  bimgs = find_image_deps(ximg, image_deps)
  print("FOUND: %s which depends on %s" % (ximg, bimgs))
  dimgs = [i for i in bimgs if i in imgs]
  if dimgs:
    print("SKIPPING: %s requires %s to be build first." % (ximg, dimgs))
  else:
    build_imgs.append(img)

all_dependency_imgs = list(image_deps.values())
for img in build_imgs:
  img_name = img.get('IMAGE_NAME')
  if img_name in all_dependency_imgs:
    img["RECHECK_TAGS_ON_SUCCESS"]=True
  print("BUILD:", img_name)
  create_file(img)
