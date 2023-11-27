"""
The utility function of Rucio ASO.
"""
import shutil
import re
import os
import itertools
import subprocess
from contextlib import contextmanager

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


def addSuffixToProcessedDataset(dataset, strsuffix):
    """
    Adding suffix to ProcessedDataset name.

    :param dataset: DBS dataset name
    :type dataset: str
    :param strsuffix: string suffix to add
    :type strsuffix: str

    :return: new DBS dataset name
    :rtype: str

    >>> addSuffixToDatasetName('/GenericTTbar/cmsbot-mypublishdbsname-1/USER', '_output.root')
    '/GenericTTbar/cmsbot-mypublishdbsname-1_output.root/USER'

    """
    tmp = dataset.split('/')
    tmp[2] += strsuffix
    return '/'.join(tmp)


def parseFileNameFromLFN(lfn):
    """
    Parsing file name from LFN.

    For the job's output files, we append `_{job_id}` before the last file
    extension. But for the log file, we use the format
    `cmsRun_{job_id}.log.tar.gz` instead. So, the log file will be hardcoded to
    always return 'cmsRun.log.tar.gz'.

    See https://github.com/dmwm/CRABServer/blob/f5fa82078ff858fd35cf11773020f258abd2c3c7/src/python/TaskWorker/Actions/DagmanCreator.py#L621-L626
    and https://github.com/dmwm/CRABServer/blob/f5fa82078ff858fd35cf11773020f258abd2c3c7/src/python/TaskWorker/Actions/DagmanCreator.py#L644
    on how file name is created.

    :param lfn: LFN
    :type lfn: str

    :return: file name
    :rtype: str

    >>>parseFileNameFromLFN('/store/user/rucio/cmsbot/somedir/output_3.root')
    'output.root'

    >>>parseFileNameFromLFN('/store/user/rucio/cmsbot/somedir/file.with.long.ext_3.txt')
    'file.with.long.ext.txt'

    >>>parseFileNameFromLFN('/store/user/rucio/cmsbot/somedir/cmsRun_3.log.tar.gz')
    'cmsRun.log.tar.gz'

    """
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


def callGfalRm(pfns, proxy, logPath):
    """
    Calling `gfal-rm` to delete files by passing list PFNs as arguments.
    Ignore thhe exit code and pipe all logs to `logPath`.

    :param pfs: list of pfn need to delete
    :type pfs: list
    :param proxy: X509 proxy path
    :type proxy: str
    :param logPath: logs path to append the logs
    :type logPath: str
    """
    # checking gfal-rm command,
    subprocess.check_call('command -v gfal-rm', shell=True)
    timeout = len(pfns) * 3 * 60 # 3 minutes per file
    pfnsArg = ' '.join(pfns)
    command = f'set -x; X509_USER_PROXY={proxy} timeout {timeout} gfal-rm -v -t 180 {pfnsArg} >> {logPath} 2>&1 &'
    subprocess.call(command, shell=True)
