""" This worker can be used for testing purposes. Just run:
         "python SequentialPublisher.py --config /path/to/config"
    and have fun!
    If you want to immediately start a pdb session
         "python SequentialPublisher.py --config /path/to/config --debug"

    More details: it instantiates the Publisher Worker with the TEST flag True. This makes the Worker
    sequential (it does not instantiate new threads) and the logging is done at the console and not on
    a file.
"""
from __future__ import division

import logging
import os
import argparse
import time

from Publisher.PublisherMaster import Master

logging.getLogger().setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument('--config', help='Publisher config file', default='PublisherConfig.py')
parser.add_argument('--debug', help='start with pbd', action='store_true')
args = parser.parse_args()
# need to pass the configuration file path to the slaves
configurationFile = os.path.abspath(args.config)
usePdb = args.debug

# ask for verbose logging in test mode
quiet = False
debug = True
testMode  = True

if usePdb:
   import pdb
   pdb.set_trace()

master = Master(configurationFile, quiet, debug, testMode)

while (True):
   master.algorithm()
   time.sleep(master.pollInterval())
   

