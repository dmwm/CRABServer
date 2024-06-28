"""
Fork process to run a function inside child process
This to prevent the worker process get stuck or die without master notice.
Leverage concurrent.futures.ProcessPoolExecutor to fork process with single
process, return value back or propargate exception from child process
to caller.

The startChildWorker() can handle coredump, timeout, and generic exception.

Original issue: https://github.com/dmwm/CRABServer/issues/8428

Note about `logger` object. This works out-of-the-box because:
- We spawn child process with `fork`
- Worker process stop and do nothing, wait until `work()` finish.
This makes child-worker and worker processes have the same log file fd, but only
one write the logs to the file at a time. Not sure if there is any risk of
deadlock. Need more test on production.

See more: https://github.com/python/cpython/issues/84559
Possible solution (but need a lot of code change): https://stackoverflow.com/a/32065395
"""

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
import multiprocessing as mp
import signal
from TaskWorker.WorkerExceptions import ChildUnexpectedExitException, ChildTimeoutException


def startChildWorker(config, work, workArgs, logger):
    """
    Public function to run any function in child-worker.

    :param config: crab configuration object
    :type config: WMCore.Configuration.ConfigurationEx
    :param work: a function that need to run in child process
    :type work: function
    :param workArgs: tuple of arguments of `work()`
    :type workArgs: tuple
    :param logger: log object
    :param logger: logging.Logger

    :returns: return value from `work()`
    :rtype: any
    """
    procTimeout = config.FeatureFlags.childWorkerTimeout
    # Force start method to 'fork' to inherit logging setting. Otherwise logs
    # from child-worker will not go to log files in process/tasks or propagate
    # back to MasterWorker process.
    with ProcessPoolExecutor(max_workers=1, mp_context=mp.get_context('fork')) as executor:
        future = executor.submit(_runChildWorker, work, workArgs, procTimeout, logger)
        try:
            outputs = future.result(timeout=procTimeout+1)
        except BrokenProcessPool as e:
            raise ChildUnexpectedExitException('Child process exited unexpectedly.') from e
        except TimeoutError as e:
            raise ChildTimeoutException(f'Child process timeout reached (timeout {procTimeout} seconds).') from e
        except Exception as e:
            raise e
    return outputs

def _signalHandler(signum, frame):
    """
    Simply raise timeout exception and let ProcessPoolExecutor propagate error
    back to parent process.
    Boilerplate come from https://docs.python.org/3/library/signal.html#examples
    """
    raise TimeoutError("The process reached timeout.")

def _runChildWorker(work, workArgs, timeout, logger):
    """
    The wrapper function to start running `work()` on the child-worker. It
    install SIGALARM with `timeout` to stop processing current work and raise
    TimeoutError when timeout is reach.

    :param work: a function that need to run in child process
    :type work: function
    :param workArgs: tuple of arguments of `work()`
    :type workArgs: tuple
    :param timeout: function call timeout in seconds
    :type timeout: int
    :param loggerConfig: logger configuration
    :param loggerConfig: dict

    :returns: return value from `work()`
    :rtype: any
    """

    # main
    logger.debug(f'Installing SIGALARM with timeout {timeout} seconds.')
    signal.signal(signal.SIGALRM, _signalHandler)
    signal.alarm(timeout)
    outputs = work(*workArgs)
    logger.debug('Uninstalling SIGALARM.')
    signal.alarm(0)
    return outputs
