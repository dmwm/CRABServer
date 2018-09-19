""" This worker can be used for testing purposes. Just run:
         "python SequentialWorker.py /path/to/config"
    and have fun!
    If you want to immedately start a pdb session add a 2nd argument (any word will do)
         "python SequentialWorker.py /path/to/config d"

    More details: it instantiates the MasterWorker with the TEST flag True. This makes the MasterWorker
    sequential (it does not instantiate new threads) and the logging is done at the console and not on
    a file.
"""

from WMCore.Configuration import loadConfigurationFile
from TaskWorker.MasterWorker import MasterWorker, validateConfig

import logging
import sys

logging.getLogger().setLevel(logging.DEBUG)

config = loadConfigurationFile( sys.argv[1] )
config.TaskWorker.nslaves = 1

validateConfig(config)

usePdb = ( len(sys.argv) == 3 )
if usePdb:
    import pdb
    pdb.set_trace()

mc = MasterWorker(config=config, logWarning=False, logDebug=True, sequential=True, console=True)
mc.algorithm()
