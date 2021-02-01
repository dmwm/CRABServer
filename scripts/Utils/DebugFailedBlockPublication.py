#!/usr/bin/env python
# coding: utf-8
from __future__ import division
from __future__  import print_function
import os
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
    #migUrl = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSMigrate'
    phy3Url = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'
    #globUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    destUrl = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'
    #apiG = DbsApi(url=globUrl)
    apiP3 = DbsApi(url=phy3Url)
    #apiMig = DbsApi(url=migUrl)
    apiDest = DbsApi(url=destUrl)

    with open(filePath) as fp:
        blockData = fp.read()
    # from pprint.pprint format to a dictionary (slow, unsafe, but handy)
    block = eval(blockData)  # pylint: disable=eval-used

    targetDataset = block['dataset']['dataset']

    print('Block is meant to be added to dataset\n%s' % targetDataset)

    # look for files already present in DBS phys03
    alreadyPresentFile = False
    lfns = [file['logical_file_name'] for file in block['files']]
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
    print("Block publication done OK")

    return

if __name__ == '__main__':
    main()
