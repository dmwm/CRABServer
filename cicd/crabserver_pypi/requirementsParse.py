#! /usr/bin/env python3
# Parsing pip repo/version from wmcore_requirements.txt
# Print repo url and version, separate by space as an output.
# For example:
#   wmcore @ git+https://github.com/dmwm/WMCore@2.2.4rc6
# Output:
#   shell> ./requirementsParse.py -f wmcore_requirements.txt
#   https://github.com/dmwm/WMCore 2.2.4rc6
#   shell>

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-f', '--filename', default='wmcore_requirements.txt')
args = parser.parse_args()
with open(args.filename, encoding='utf-8') as r:
    wmcoreRequirementStr = ''
    for line in r:
        line = line.partition('#')[0]
        line = line.strip()
        if line.startswith('wmcore'):
            wmcoreRequirementStr = line
            break
_, tmpRepo, tmpCommit = wmcoreRequirementStr.split('@')
commit = tmpCommit.strip()
repo = tmpRepo.split('+')[1].strip()
print(repo, commit)
