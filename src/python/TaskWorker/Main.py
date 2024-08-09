"""
Top driver for TaskWorker service
"""
from optparse import OptionParser  # pylint: disable=deprecated-module
import signal
import os
import logging
import sys

from WMCore.Configuration import loadConfigurationFile
from TaskWorker.WorkerExceptions import ConfigException

# NOTE: While importing the list of exceptions must be done before code starts
# and it is safe, CRAB related code can not imported here. We need first
# to load the TW configuration to know if to import htcondor/classad
# or htcondor2/classad2. So it is done below after config. validation


def validateConfig(config):
    """Verify that the input configuration contains all needed info

    :arg WMCore.Configuration config: input configuration
    :return bool, string: flag for validation result and a message."""
    if getattr(config, 'TaskWorker', None) is None:
        return False, "Configuration problem: Task worker section is missing. "
    return True, 'Ok'


def main():
    """
    parse args and run.
    """
    usage = "usage: %prog [options] [args]"
    parser = OptionParser(usage=usage)

    parser.add_option("-d", "--logDebug",
                      action="store_true",
                      dest="logDebug",
                      default=False,
                      help="print extra messages to stdout")
    parser.add_option("-w", "--logWarning",
                      action="store_true",
                      dest="logWarning",
                      default=False,
                      help="don't print any messages to stdout")
    parser.add_option("-s", "--sequential",
                      action="store_true",
                      dest="sequential",
                      default=False,
                      help="run in sequential (no subprocesses) mode")
    parser.add_option("-c", "--console",
                      action="store_true",
                      dest="console",
                      default=False,
                      help="log to console")
    parser.add_option("--config",
                      dest="config",
                      default=None,
                      metavar="FILE",
                      help="configuration file path")
    parser.add_option("--pdb",
                      action="store_true",
                      dest="pdb",
                      default=False,
                      help="Enter pdb mode. Set up TW to run sequential mode and invoke pdb.")

    (options, args) = parser.parse_args()  # pylint: disable=unused-variable


    if not options.config:
        raise ConfigException("Configuration not found")

    configuration = loadConfigurationFile(os.path.abspath(options.config))
    status, msg = validateConfig(configuration)
    if not status:
        raise ConfigException(msg)

    # Only after loacConfiguratiob has set useHtcV2 in the environment
    # we can importing HTCondorLocator and TaskWorker so that in all files we can use
    # the env.vat. to decide if to import htcondor or htcondor2
    if getattr(configuration.TaskWorker, 'useHtcV2', None):
        print("Configuration says to use HTC Bindings V2")
    else:
        print("Configuration says to use HTC Bindings V1")
    from TaskWorker.MasterWorker import MasterWorker

    if options.pdb:
        # override root loglevel to debug
        logging.getLogger().setLevel(logging.DEBUG)
        # need to force a single thread
        if not hasattr(configuration, 'TaskWorker'):
            configuration.section_('TaskWorker')
            configuration.TaskWorker.nslaves = 1
        if not hasattr(configuration, 'FeatureFlags'):
            configuration.section_('FeatureFlags')
            configuration.FeatureFlags.childWorker = False
        # start with pdb
        import pdb #pylint: disable=import-outside-toplevel
        pdb.set_trace() #pylint: disable=forgotten-debug-statement
        mc = MasterWorker(config=configuration, logWarning=False, logDebug=True, sequential=True, console=True)
        mc.algorithm()
        # exit program
        sys.exit(0)

    # main
    mw = None
    try:
        mw = MasterWorker(configuration, logWarning=options.logWarning, logDebug=options.logDebug, sequential=options.sequential, console=options.console)
        signal.signal(signal.SIGINT, mw.quit_)
        signal.signal(signal.SIGTERM, mw.quit_)
        mw.algorithm()
    except:
        if mw:
            mw.logger.exception("Unexpected and fatal error. Exiting task worker")
        #don't really wanna miss this, propagating the exception and exiting really bad
        raise
    finally:
        #there can be an exception before slaves are created, e.g. in the __init__
        if hasattr(mw, 'slaves'):
            mw.slaves.end()
