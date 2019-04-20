""" This worker can be used for testing purposes. Just run:
         "python SequentialPublisher.py /path/to/config"
    and have fun!
    If you want to immedately start a pdb session add a 2nd argument (any word will do)
         "python SequentialPublisher.py /path/to/config d"

    More details: it instantiates the Publisher Worker with the TEST flag True. This makes the Worker
    sequential (it does not instantiate new threads) and the logging is done at the console and not on
    a file.
"""
from __future__ import division

from WMCore.Configuration import Configuration
from Publisher.PublisherMaster import Master

import logging
import sys

logging.getLogger().setLevel(logging.DEBUG)

from WMCore.Configuration import loadConfigurationFile, Configuration
config = loadConfigurationFile( sys.argv[1] )

# ask for verbose logging in test mode
quiet = False
debug = True
test  = True

usePdb = ( len(sys.argv) == 3 )
if usePdb:
   import pdb
   pdb.set_trace()

master = Master(config, quiet, debug, test)
master.algorithm()
