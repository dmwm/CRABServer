#!/usr/bin/env python3
from __future__ import print_function
from argparse import ArgumentParser
from docker_utils import get_digest
import sys

parser = ArgumentParser(description='')
parser.add_argument('-r', dest='repository',  type=str, help="Docker container repository e.g. cmssw/cms", default="")
parser.add_argument('-s', dest='source',      type=str, help="Existing docker image tag e.g rhel6-itb")
parser.add_argument('-d', dest='destination', type=str, help="New docker image tag e.g rhel6")
args = parser.parse_args()
if not (args.source and args.destination):
  parser.error("Missing arguments.")

src_tag = args.source       if ':' in args.source      else "%s:%s" % (args.repository, args.source)
des_tag = args.destination  if ':' in args.destination else "%s:%s" % (args.repository, args.destination)
src = get_digest(src_tag, "")
if not src[0]:
  print("ERROR: Unable to find source image %s" % src_tag)
  sys.exit(0)

des = get_digest(des_tag, "")
print("%s vs %s" % (src[1], des[1]), file=sys.stderr)
if src[1] != des[1]:
  print("SOURCE_IMAGE=%s" % src_tag)
  print("DESTINATION_IMAGE=%s" % des_tag)
