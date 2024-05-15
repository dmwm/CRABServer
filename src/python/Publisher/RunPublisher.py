"""
  Unified script to start Publisher in normal or debug (sequential) mode
  and to select Publisher_rucio vs Publisher_schedd
  This worker can be used for testing purposes. Just run:
         "python Publisher.py --config /path/to/config --service Publisher_rucio "
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

logging.getLogger().setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument('--config', help='Publisher config file', default='PublisherConfig.py')
parser.add_argument('--service', help='Publisher_schedd or Publisher_rucio', default='Publisher_schedd')
parser.add_argument('--testMode', help='run in sequential mode (no subprocesses)', action='store_true')
parser.add_argument('--debug', help='start with pdb', action='store_true')
args = parser.parse_args()
# need to pass the configuration file path to the slaves
configurationFile = os.path.abspath(args.config)
service = args.service
testMode = args.testMode
debug = args.debug

# the following if surely is "bad". In the end we can not maintain two PublisheMaster with 80% code overlap
# but since we did not touch PublisherMaster.py (the _schedd) one in one year, maybe we can limp a along
if service == 'Publisher_schedd':
    from Publisher.PublisherMaster import Master
elif service == 'Publisher_rucio':
    from Publisher.PublisherMasterRucio import Master
else:
    raise Exception(f"Unknown/Unspecified service= {service}")

QUIET = False  # there is actually no need for this in PublisherMaster*py, Some cleanup could be done
if debug:
    import pdb
    pdb.set_trace()  # pylint: disable=forgotten-debug-statement

master = Master(configurationFile, quiet=QUIET, debug=debug, testMode=testMode)

while True:
    master.algorithm()
    time.sleep(master.pollInterval())
