"""
generic?general utilities
"""

import tarfile
from ServerUtilities import executeCommand

def addToGZippedTarfile(fileList=None, tarFile=None):
    """
    add file names in fileListd to a gzipped tarFile
    it is assumed that tarFile name has the format: xxx.tar.gz
    this requires unzipping the tarfile, adding, zipping again
    """
    if not fileList or not tarFile:
        return
    cmd = f"gunzip {tarFile}"
    out, err, exitcode = executeCommand(cmd)
    if exitcode:
        raise Exception(f"Failed to gunzip {tarFile}: exitcode {exitcode}\n stdout: {out}\n stderr:{err}")
    unzippedTarFile = tarFile.replace(".gz", "")
    with tarfile.open(unzippedTarFile, mode='a') as tf:
        for f in fileList:
            tf.add(f)
    cmd = f"gzip {unzippedTarFile}"
    out, err, exitcode = executeCommand(cmd)
    if exitcode:
        raise Exception(f"Failed to gzip {unzippedTarFile}: exitcode {exitcode}\n stdout: {out}\n stderr:{err}")
