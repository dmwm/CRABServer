"""
takes as input a file name with a list of DIDs and checks for bad Rucio replicas
For each file:
 - check locations
 - find size and checksum from Rucio
 - check size via gfal
 - if needed check alder2 checksum
 - propose a set o replicas to be marked as bad

Writes a log to stdout (including recommanded action) and a summary table
"""
import subprocess
import tempfile
import sys
import time
import pandas as pd

from RucioUtils import Client

def main():
    debug = len(sys.argv) == 3
    # initialize Rucio
    rucio = Client()
    rucio.whoami()

    # read file list
    filelist = sys.argv[1]
    with open(filelist, 'r', encoding='utf-8') as fh:
        didList = fh.readlines()
    # prepare a list of dictionaries
    globalList = []
    for did in didList:
        if debug:
            print(did)
        did = did.rstrip()  # readlines leaves a \n at the end
        fileDict = prepareEmptyDict()
        fileDict['kind'] = 'DID'
        fileDict['did'] = did
        scope = did.split(':')[0]
        name = did.split(':')[1]
        fileId = name.split('/')[-1].split('.')[0]
        fileDict['fileId'] = fileId
        fileDict['name'] = name
        fileDict['onTape'] = False
        fileDict['#disk'] = 0
        try:
            replicaGen = rucio.list_replicas([{'scope':scope, 'name':name}])
        except Exception:  # sometimes there's a bad DID
            continue
        replica = next(replicaGen)  # since we passed a lit of one DID as arg, there's only on replica object
        referenceSize = replica['bytes']
        referenceAdler2 = replica['adler32']
        fileReplicas = []
        globalList.append(fileDict)
        for rse, pfn in replica['rses'].items():
            if 'Tape' in rse:
                fileDict['onTape'] = True
                continue
            fileDict['#disk'] += 1
        for rse, pfn in replica['rses'].items():
            if 'Tape' in rse:
                continue
            replicaDict = prepareEmptyDict()
            replicaDict['kind'] = 'Rep'
            replicaDict['fileId'] = fileId
            replicaDict['site'] = rse
            if debug:
                print(rse)
            replicaDict['sizeOK'] = checkReplicaSize(pfn[0], referenceSize, debug)
            replicaDict['OK'] = replicaDict['sizeOK']
            if replicaDict['OK'] in ['FileNotFound', 'NA'] :
                globalList.append(replicaDict)
                continue
            if replicaDict['sizeOK']:
                replicaDict['adlerOK'] = checkReplicaAdler32(pfn[0], referenceAdler2, debug)
                if replicaDict['adlerOK'] == 'NA':
                    replicaDict['OK'] = replicaDict['sizeOK']
                else:
                    replicaDict['OK'] &= replicaDict['adlerOK']
            replicaDict['markBAD'] = 'YES' if fileDict['onTape'] and not replicaDict['OK'] else 'NO'
            fileReplicas.append(replicaDict)
            globalList.append(replicaDict)

    myf=pd.DataFrame(globalList)
    table=myf[['kind','fileId','onTape', '#disk',  'site', 'sizeOK', 'adlerOK', 'OK', 'markBAD', 'did']].to_html()
    writeOutHtmlTable(table, filelist)


def writeOutHtmlTable(table, fileName):

    beginningOfDoc = '<!DOCTYPE html>\n<html>\n'
    header = htmlHeader()
    now = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    title = f"\n<center><b>Status of Files in {fileName} and Action to take - {now}</b></center><hr>\n"
    endOfDoc = '\n</html>'
    with open('FileStatusAndAction.html', 'w', encoding='utf-8') as fh:
        fh.write(beginningOfDoc)
        fh.write(header)
        fh.write(title)
        fh.write(table)
        fh.write(endOfDoc)


def prepareEmptyDict():
    aDict = {}
    keys = ['fileId', 'did', 'kind', 'name', 'onTape', '#disk', 'site',
            'sizeOK', 'adlerOK', 'markBAD', 'OK']
    for k in keys:
        aDict[k] = ''
    return aDict

def checkReplicaSize(pfn, referenceSize, debug):
    cmd = f"gfal-ls -l {pfn}"
    out, err, ec = executeCommand(cmd)
    if ec:
        if 'File not found' in err or 'No such file' in err:
            return 'FileNotFound'
        if 'HTTP 500' in err:
            return 'NA'
    try:
        replicaSize = int(out.split()[4])
    except Exception:
        return 'NA'
    check = replicaSize == referenceSize
    return check


def checkReplicaAdler32(pfn, referenceAdler32, debug):
    """
    compute Adler32 on a remote PFN
    returns True, False or 'NA' if operation failes
    """
    f = tempfile.NamedTemporaryFile(delete=False, prefix='testFile_')
    fname = f.name
    executeCommand(f"rm -f {fname}")
    if debug:
        print(f"checking Adler32 for {pfn}")
    cmd = f"gfal-copy {pfn} {fname}"
    out, err, ec = executeCommand(cmd)
    if ec:
        return 'NA'
    cmd = f"gfal-sum {fname} ADLER32"
    out, err, ec = executeCommand(cmd)
    if ec:
        return 'NA'
    adler32 = out.split()[1]
    check = adler32 == referenceAdler32
    if debug:
        print(f"{adler32:s} {referenceAdler32:s} {check}")
    executeCommand(f"rm -f {fname}")
    return check


def htmlHeader():
    """
    something to make a prettier HTML table, stolen from Ceyhun's
    https://cmsdatapop.web.cern.ch/cmsdatapop/eos-path-size/size.html
    and trimmed down a lot
    """
    head = """<head>
    <!-- prepared using https://datatables.net/download/ -->
    <link rel="stylesheet" type="text/css"
     href="https://cdn.datatables.net/v/dt/jq-3.6.0/jszip-2.5.0/dt-1.12.1/b-2.2.3/b-colvis-2.2.3/b-html5-2.2.3/b-print-2.2.3/cr-1.5.6/date-1.1.2/kt-2.7.0/rr-1.2.8/sc-2.0.6/sb-1.3.3/sp-2.0.1/sl-1.4.0/sr-1.1.1/datatables.min.css"/>

    <!--  Please do not delete below CSSes, important for pretty view -->
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/v/dt/dt-1.12.1/datatables.min.css"/>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.11.4/css/dataTables.bootstrap.min.css">

    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {
            font-family: 'Trebuchet MS', sans-serif;
            font-size: 12px;
        }
        table td {
            word-break: break-all;
        }
        /* First row bold */
        table td:nth-child(1) {
            font-weight: bold;
        }
        /* Header rows, total and titles, align left */
        table th:nth-child(n+2) {
            text-align: left !important;
            color:  #990000 !important;
        }
        /* First column color */
        table th:nth-child(1) {
            color: #990000;
        }
        /* No carriage return for values, no break lines */
        table tr td {
          white-space: nowrap;
        }
    </style>
</head>"""
    return head


def executeCommand(command):
    """ Execute passed bash command. There is no check for command success or failure. Who`s calling
        this command, has to check exitcode, out and err
    """
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    exitcode = process.returncode
    # suprocess returns bytes, but we like strings better
    stdout = out.decode(encoding='UTF-8') if out else ''
    stderr = err.decode(encoding='UTF-8') if err else ''
    return stdout, stderr, exitcode


if __name__ == '__main__':
    main()
