import shutil
import re
from contextlib import contextmanager
import itertools

from ServerUtilities import encodeRequest
from ASO.Rucio.exception import RucioTransferException

@contextmanager
def writePath(path):
    """
    Prevent bookkeeping file corruption by simply write to temp file and replace
    original file.

    This simple contextmanager provide new io object for file write operation
    to `path` with `_tmp` suffix. At the end of `with` statement it will
    replace original path with temp file.

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
    :return: yield the next list chunk
    :rtype: generator
    """
    if isinstance(l, list):
        for i in range(0, len(l), n):
            yield l[i:i + n]
    else:
        while True:
            newList = list(itertools.islice(l, n))
            if newList:
                yield newList
            else:
                break



def updateDB(client, api, subresource, fileDoc, logger=None):
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
                import pdb; pdb.set_trace()
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
