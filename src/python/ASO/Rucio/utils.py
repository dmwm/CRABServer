import shutil
from contextlib import contextmanager

from ServerUtilities import encodeRequest

@contextmanager
def writePath(path):
    """
    Prevent bookkeeping file corruption by simply write to temp file and replace
    original file.

    This simple contextmanager provide new io object for file write operation
    to `path` with `_tmp` suffix. At the end of `with` statement it will
    replace original path with temp file.

    :param path: path to write.
    :yield: `_io.TextIOWrapper` object for write operation.
    """
    tmpPath = f'{path}_tmp'
    with open(tmpPath, 'w', encoding='utf-8') as w:
        yield w
    shutil.move(tmpPath, path)
