import logging

from argparse import ArgumentParser

from ASO.Rucio.RunTransfer import RunTransfer
import ASO.Rucio.config as config
from python.ASO.Rucio.exception import RucioTransferException


class RucioTransferMain:
    """
    This class is entrypoint of RUCIO_Transfers.py It setup logs
    handlers and called RunTransfer to execute RUCIO_Transfers.py
    algorithm.
    """
    def __init__(self):
        self._initLogger()
        self.logger = logging.getLogger('RucioTransfer.RucioTransferMain')

    def run(self):
        """
        Execute RunTransfer.algorithm.
        Exception handling here is for debugging purpose. If
        RucioTransferException was raise, it mean some condition is
        not meet and we want to fail (fast) this process.  But if
        Exception is raise mean something gone wrong with our code and
        need to investigate.
        """
        self.logger.info("executing RunTransfer")
        try:
            self.logger.info('executing RunTransfer')
            run = RunTransfer()
            run.algorithm()
        except RucioTransferException as ex:
            raise ex
        except Exception as ex:
            raise Exception("Unexpected error during main  %s") from ex
        self.logger.info('transfer completed')

    def _initLogger(self):
        logger = logging.getLogger('RucioTransfer')
        logger.setLevel(logging.DEBUG)
        hldr = logging.StreamHandler()
        formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
        hldr.setFormatter(formatter)
        logger.addHandler(hldr)


def main():
    """
    This main function is mean to called by RUCIO_Transfers.py script.
    Arguments are process here and only for run integration test or
    run process directly from this file.
    """
    opt = ArgumentParser(usage=__doc__)
    opt.add_argument("--force-publishname", dest="force_publishname", default=None,
                     help="use provided output dataset name instead of output")
    opts = opt.parse_args()

    # Wa: I personally does not know how to mock this in unittest. I manually
    # instantiate new one in test function before run one.
    # Will switch to WMCore.Configuration.ConfigurationEx later

    config.config = opts
    rucioTransfer = RucioTransferMain()

    rucioTransfer.run()


if __name__ == "__main__":
    main()
