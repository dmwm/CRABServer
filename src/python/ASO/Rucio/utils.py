"""
The utility function of Rucio ASO.
"""
import shutil
import re
import os
from contextlib import contextmanager
import itertools

from ServerUtilities import encodeRequest
from ASO.Rucio.exception import RucioTransferException

@contextmanager
def writePath(path):
    """
    Prevent bookkeeping file corruption by writing to temp path and replacing
    original path with the file in temp path when exiting contextmanager.

    This guarantee file in `path` will not be touched until return from caller.

    :param path: path to write.
    :type path:
    :yield: return object from `open()` function for write operation.
    :ytype: _io.TextIOWrapper
    """
    tmpPath = f'{path}_tmp'
    with open(tmpPath, 'w', encoding='utf-8') as w:
        yield w
    shutil.move(tmpPath, path)

def chunks(l, n=1):
    """
    Yield successive n-sized chunks from l.

    :param l: list to split
    :type l: list
    :param n: chunk size
    :type n: int
    :return: yield the next chunk list
    :rtype: generator
    """
    if isinstance(l, list):
        for i in range(0, len(l), n):
            yield l[i:i + n]
    elif isinstance(l, dict):
        l = list(l.items())
        for i in range(0, len(l), n):
            yield l[i:i + n]
    else:
        while True:
            newList = list(itertools.islice(l, n))
            if newList:
                yield newList
            else:
                break

def updateToREST(client, api, subresource, fileDoc):
    """
    Upload fileDoc to REST

    :param client: CRAB REST client.
    :type client: RESTInteractions.CRABRest
    :param api: API name
    :type api: string
    :param subresource: API subresource
    :type subresource: string
    :param fileDoc: fileDoc to upload to REST
    :type fileDoc: dict
    """
    fileDoc['subresource'] = subresource
    client.post(
        api=api,
        data=encodeRequest(fileDoc)
    )

def tfcLFN2PFN(lfn, tfc, proto, depth=0):
    """
    Just for crosschecking with FTS algo we use in `getSourcePFN()`
    Will remove it later.
    """
    # Hardcode
    MAX_CHAIN_DEPTH = 5
    if depth > MAX_CHAIN_DEPTH:
        raise RucioTransferException(f"Max depth reached matching lfn {lfn} and protocol {proto} with tfc {tfc}")
    for rule in tfc:
        if rule['proto'] == proto:
            if 'chain' in rule:
                lfn = tfcLFN2PFN(lfn, tfc, rule['chain'], depth + 1)
            regex = re.compile(rule['path'])
            if regex.match(lfn):
                return regex.sub(rule['out'].replace('$', '\\'), lfn)
    if depth > 0:
        return lfn
    raise ValueError(f"lfn {lfn} with proto {proto} cannot be matched by tfc {tfc}")


def LFNToPFNFromPFN(lfn, pfn):
    """
    Simple function to convert from LFP to PFN by extract prefix from example
    PFN.

    :param lfn: LFN
    :type lfn: string
    :param pfn: PFN example to extract it prefix
    :type pfn: string

    :return: PFN of LFN param
    :rtype: string
    """
    pfnPrefix = '/'.join(pfn.split("/")[:-2])
    if lfn.split("/")[-2] == 'log' :
        fileid = '/'.join(lfn.split("/")[-3:])
    else:
        fileid = '/'.join(lfn.split("/")[-2:])
    return f'{pfnPrefix}/{fileid}'

def addSuffixToDatasetName(dataset, txt):
    tmp = dataset.split('/')
    tmp[2] += txt
    return '/'.join(tmp)


def parseFileNameFromLFN(lfn):
    # cmsRun_3.log.tar.gz => cmsRun.log.tar.gz
    # output_3.root => output.root
    filename = os.path.basename(lfn)
    regex = re.compile(r'^cmsRun_[0-9]+\.log\.tar\.gz$')
    if regex.match(filename):
        return 'cmsRun.log.tar.gz'
    leftPiece, jobid_fileext = filename.rsplit("_", 1)
    origFileName = leftPiece
    if "." in jobid_fileext:
        fileExt = jobid_fileext.rsplit(".", 1)[-1]
        origFileName = leftPiece + "." + fileExt
    return origFileName


def parseFileNameFromDatasetName(dataset):
    tmp = dataset.split('/')
    return tmp[2].split('__')[-1]
