""" This worker can be used for testing purposes. Just run:
         "python SequentialPublisher.py /path/to/config"
    and have fun!

    More details: it instantiates the MasterWorker with the TEST flag True. This makes the MasterWorker
    sequential (it does not instantiate new threads) and the logging is done at the console and not on
    a file.
"""

from WMCore.Configuration import Configuration
from Publisher.Worker import Worker

import logging
import sys

logging.getLogger().setLevel(logging.DEBUG)

from WMCore.Configuration import loadConfigurationFile, Configuration
config = loadConfigurationFile( sys.argv[1] )

# ask for verbose logging in test mode
quiet = False
debug = True
test  = True

worker = Worker(config, quiet, debug, test)
worker.algorithm()
