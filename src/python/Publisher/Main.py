"""
  Unified script to start Publisher in normal or debug (sequential) mode
  and to select Publisher_rucio vs Publisher_schedd
  This worker can be used for testing purposes. Just run:
         "crab-publisher --config /path/to/config --service Publisher_rucio --pdb"
    with "--pdb", loglevel is set to debug, and print to , the worker will
      - initiate pdb
      - run in sequential
      - log level set to debug and print to console

"""
from __future__ import division

import os
import argparse
import time

def main():
    """
    parse args and run.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        help="Publisher config file",
                        default="PublisherConfig.py")
    parser.add_argument("--service",
                        help="Publisher_schedd or Publisher_rucio",
                        default="Publisher_schedd")
    parser.add_argument("--sequential",
                        action="store_true",
                        default=False,
                        help="run in sequential (no subprocesses) mode")
    parser.add_argument("--pdb",
                        action="store_true",
                        default=False,
                        help="Enter pdb mode. Set up TW to run sequential mode and invoke pdb.")
    parser.add_argument("--logDebug",
                        action="store_true",
                        default=False,
                        help="print log level debug to stdout")
    parser.add_argument("--console",
                        action="store_true",
                        default=False,
                        help="log to console")

    args = parser.parse_args()
    # the following if surely is "bad". In the end we can not maintain two PublisheMaster with 80% code overlap
    # but since we did not touch PublisherMaster.py (the _schedd) one in one year, maybe we can limp a along
    if args.service == 'Publisher_schedd':
        from Publisher.PublisherMaster import Master #pylint: disable=import-outside-toplevel
    elif args.service == 'Publisher_rucio':
        from Publisher.PublisherMasterRucio import Master #pylint: disable=import-outside-toplevel
    else:
        raise Exception(f"Unknown/Unspecified service= {args.service}")

    if args.pdb:
        args.console = True
        args.sequential = True
        args.logDebug = True
        import pdb #pylint: disable=import-outside-toplevel
        pdb.set_trace()  # pylint: disable=forgotten-debug-statement

    # need to pass the configuration file path to the slaves
    configurationFile = os.path.abspath(args.config)

    # main
    master = Master(configurationFile, sequential=args.sequential, logDebug=args.logDebug, console=args.console)
    while True:
        master.algorithm()
        time.sleep(master.pollInterval())
