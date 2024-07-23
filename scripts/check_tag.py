#!/usr/bin/env python3
import sys
from docker_utils import get_tags

res, data=get_tags(sys.argv[1])
if res and (sys.argv[2] in data):
  print("FOUND:%s" % sys.argv[2])
