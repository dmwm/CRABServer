"""
Main module.
"""

import logging
from argparse import ArgumentParser

import ASO.Rucio.config as config # pylint: disable=consider-using-from-import
from ASO.Rucio.RunTransfer import RunTransfer
from ASO.Rucio.exception import RucioTransferException

def initLogger():
    """
    Initialize root logger
    """
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
    opt.add_argument("--force-publishname", dest="force_publishname", default=None, type=str,
                     help="use provided output dataset name instead of output")
    opt.add_argument("--force-last-line", dest="force_last_line", default=None, type=int,
                     help="")
    opt.add_argument("--force-total-files", dest="force_total_files", default=None, type=int,
                     help="")
    # default here must change because theses current value is too low (chunk=2/max=5)
    opt.add_argument("--replicas-chunk-size", dest="replicas_chunk_size", default=5, type=int,
                     help="")
    opt.add_argument("--max-file-per-dataset", dest="max_file_per_dataset", default=20, type=int,
                     help="")
    opt.add_argument("--last-line-path", dest="last_line_path",
                     default='task_process/transfers/last_transfer.txt',
                     help="")
    opt.add_argument("--transfer-txt-path", dest="transfers_txt_path",
                     default='task_process/transfers.txt',
                     help="")
    opt.add_argument("--rest-info-path", dest="rest_info_path",
                     default='task_process/RestInfoForFileTransfers.json',
                     help="")
    opt.add_argument("--container-ruleid-path", dest="container_ruleid_path",
                     default='task_process/transfers/container_ruleid.json',
                     help="")
    opt.add_argument("--transfer-ok-path", dest="transfer_ok_path",
                     default='task_process/transfers/transfer_ok.txt',
                     help="")
    opt.add_argument("--ignore-transfer-ok", dest="ignore_transfer_ok",
                     action='store_true',
                     help="")
    opt.add_argument("--bookkeeping-block-complete-path", dest="bookkeeping_block_complete_path",
                     default='task_process/transfers/block_complete.txt',
                     help="")
    opt.add_argument("--ignore-bookkeeping-block-complete", dest="ignore_bookkeeping_block_complete",
                     action='store_true',
                     help="")
    opt.add_argument("--open-dataset-timeout", dest="open_dataset_timeout", default=1*60*60, type=int, # 1 hours
                     help="Open dataset timeout in seconds")
    opts = opt.parse_args()

    # Put args to config module to share variable across process.
    # NOTE: For unittest, manually instantiate new one with argparse.Namespace
    # class before execute test.
    config.args = opts

    # init logger
    initLogger()
    logger = logging.getLogger('RucioTransfer')


    # Execute RunTransfer.algorithm().
    # Exception handling here is for debugging purpose. If
    # RucioTransferException was raise, it mean some condition is
    # not meet and we want to fail (fast) this process.  But if
    # Exception is raise mean something gone wrong with our code and
    # need to investigate.
    try:
        logger.info('executing RunTransfer')
        run = RunTransfer()
        run.algorithm()
    except RucioTransferException as ex:
        raise ex
    except Exception as ex:
        raise Exception("Unexpected error during main") from ex
    logger.info('transfer completed')

if __name__ == "__main__":
    main()
