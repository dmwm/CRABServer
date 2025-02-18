#!/usr/bin/env python3
# coding: utf-8
from __future__ import division
from __future__  import print_function
import os
import json
import subprocess
import argparse

from dbs.apis.dbsClient import DbsApi

def main():
    # get a validate file name from args

    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='file containing the dump of the block', default=None, required=True)
    args = parser.parse_args()
    fileName = args.file
    #fileName = 'failed-block-at-1611258668.34.txt' # just an example

    failedBlocksDir = '/data/srv/Publisher_files/FailedBlocks/'
    filePath = failedBlocksDir + fileName
    if not os.path.isfile(filePath):
        print("File %s not found in %s" % (fileName, failedBlocksDir))
        return

    # initialize DBS access
    # if X509 vars are not defined, use default Publisher location
    userProxy = os.getenv('X509_USER_PROXY')
    if userProxy:
        x509Cert = userProxy
        x509Key = userProxy
    else:
        x509Cert = os.getenv('X509_USER_CERT') if os.getenv('X509_USER_CERT') else '/data/certs/robotcert.pem'
        x509Key = os.getenv('X509_USER_KEY') if os.getenv('X509_USER_KEY') else '/data/certs/robotkey.pem'
    os.environ['X509_USER_CERT'] = x509Cert
    os.environ['X509_USER_KEY'] = x509Key
    #migUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/phys03/DBSMigrate'
    phy3Url = 'https://cmsweb-prod.cern.ch/dbs/prod/phys03/DBSReader'
    #globUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'
    destUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/phys03/DBSWriter'
    #apiG = DbsApi(url=globUrl, debug=True)
    apiP3 = DbsApi(url=phy3Url, debug=True)
    #apiMig = DbsApi(url=migUrl, debug=True)
    apiDest = DbsApi(url=destUrl, debug=True)

    with open(filePath, 'r', encoding='utf8') as fp:
        blockData = fp.read()
    if '//' in blockData:
        print('New DBS does not like "//" in LFN, replacing with single "/"')
        blockData = blockData.replace('//', '/')
    # from pprint.pprint format to a dictionary (slow, unsafe, but handy)
    block = eval(blockData)  # pylint: disable=eval-used

    targetDataset = block['dataset']['dataset']

    print('Block is meant to be added to dataset\n%s' % targetDataset)

    # look for files already present in DBS phys03
    alreadyPresentFile = False
    lfns = [f['logical_file_name'] for f in block['files']]
    print('Block contains %d files' % len(lfns))
    numPresent = 0
    sameDSet = 0
    otherDSet = 0
    otherDSlist = set()
    for lfn in lfns:
        ret = apiP3.listFiles(logical_file_name=lfn)
        if ret:
            alreadyPresentFile = True
            numPresent += 1
            if numPresent < 5:
                print('file %s found in DBS' % lfn)
            if numPresent == 5:
                print('more files found ...')
            #details = apiP3.listFiles(logical_file_name=lfn, detail=True)
            #print(details)
            lfnDSet = apiP3.listDatasets(logical_file_name=lfn)[0]['dataset']
            if lfnDSet == targetDataset:
                sameDSet += 1
                if sameDSet < 5:
                    print('this lfn is already in target dataset')
            else:
                otherDSet += 1
                if otherDSet < 5:
                    print('this lfn belongs to another dataset:\n%s' % lfnDSet)
                if not lfnDSet in otherDSlist:
                    otherDSlist.add(lfnDSet)

            #lfnBlock = apiP3.listBlocks(logical_file_name=lfn)
            #print('in block:\n%s' % lfnBlock[0]['block_name'])

    if alreadyPresentFile:
        print('%d/%d file(s) from input blocks are already in DBS/phys03. Publication will fail' %
              (numPresent, len(lfns)))
        print('files already present in target dataset: %d' % sameDSet)
        print('files present in DBS in another dataset: %d' % otherDSet)
        if otherDSet:
            print('other datasets containing files from this block:\n%s' % otherDSlist)
        return

    print('No obvious reason for Publication failure found, try to insert again')
    try:
        apiDest.insertBulkBlock(block)
    except Exception as ex:
        print("Publication failed with exception:\n%s" % str(ex))
        print('Make one more try using direct curl POST')
        jsonFile = '/tmp/block.json'
        with open(jsonFile, 'w', encoding='utf8') as fp:
            json.dump(block, fp)
        print('block dumped to %s' % jsonFile)
        # if json is big, zip it
        gzip = os.stat(jsonFile).st_size > 1024*1024  # 1 MByte
        if gzip:
            cmd = 'gzip %s' % jsonFile
            subprocess.call(cmd, shell=True)
            print('json file was over 1MB, zipped')
        cmd = 'curl -sS --cert %s  --key %s -H "Content-type: application/json" ' % (x509Cert, x509Key)
        if gzip:
            cmd += ' -H "Content-Encoding: gzip" --data-binary  @%s  ' % (jsonFile + '.gz')
        else:
            cmd += ' -d@%s  ' % jsonFile
        cmd += destUrl + '/bulkblocks'
        print('will execute:\n%s' % cmd)
        ret = subprocess.call(cmd, shell=True)
        if ret == 0:
            print("\nBlock publication done OK")
        return
    print("Block publication done OK")

    return

if __name__ == '__main__':

    main()
