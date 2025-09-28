"""
generic?general utilities
"""

import os, tarfile

def addToGZippedTarfile(fileList=None, tarFile=None):
    """
    add file names in fileListd to a gzipped tarFile
    it is assumed that tarFile name has the format: xxx.tar.gz
    since tar operated as a stream and only allow appending
    there is no concept of replacing, archive will grow by duplicated key records
    thus this function will rebuild/clone tarInfo of all non duplicated members
    from src tarFile into the new tar.gz one and add new fresh files
    as overwritten/replacement for duplicated arc names in archive.
    """
    if not fileList or not tarFile:
        return

    tmp = tarFile + ".tmp"
    to_replaces = [(os.path.basename(file_path), file_path) for file_path in fileList]
    arc_names = [arc for arc, _ in to_replaces]
    with tarfile.open(tarFile, "r:gz") as src, tarfile.open(tmp, "w:gz") as dst:
        # copy all members except the ones we will replace
        for member in src:
            if member.name not in arc_names:
                if member.isfile():
                    fobj = src.extractfile(member)
                    try:
                        dst.addfile(member, fobj)
                    finally:
                        if fobj is not None:
                            fobj.close()
                else: # in case of dirs/symlinks
                    dst.addfile(member) 
        # ones we replace 
        for arc, file_path in to_replaces:
            dst.add(file_path, arcname=arc)

    # non-atomic replace but compatible with both py2/py3.
    os.remove(tarFile)
    os.rename(tmp, tarFile)
