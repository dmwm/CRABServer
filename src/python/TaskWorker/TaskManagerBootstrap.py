"""
 bootstrap one TW action which requires a separate process
"""
import sys

from TaskWorker.Actions import PreDAG, PreJob, PostJob


def bootstrap():
    """ bootstrap one TW action which requires a separate process """
    print(f"Entering TaskManagerBootstrap with args: {sys.argv}")
    command = sys.argv[1]
    if command == "POSTJOB":
        return PostJob.PostJob().execute(*sys.argv[2:])
    if command == "PREJOB":
        return PreJob.PreJob().execute(*sys.argv[2:])
    if command == "PREDAG":
        return PreDAG.PreDAG().execute(*sys.argv[2:])
    raise Exception(f"Unknown command {sys.argv[1]} passed to TaskMangerBootstrap.py")


if __name__ == '__main__':
    try:
        retval = bootstrap()
        print(f"Ended TaskManagerBootstrap with code {retval}")
        sys.exit(retval)
    except Exception as e:
        print(f"Got a fatal exception: {e}")
        raise
