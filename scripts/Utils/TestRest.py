#!/usr/bin/env python3
# pylint: disable=line-too-long
"""
needs to be run in a TW container or some other way to have the
CRABServer environment

SETUP sequence:
voms-proxy-init -voms cms etc. etc.
export X509_USER_PROXY etc.
source /data/srv/TaskManager/env.sh (or equivalent)
"""
from __future__ import division
from __future__ import print_function
import os
import time
import argparse

from RESTInteractions import CRABRest
from ServerUtilities import encodeRequest


#instance = preprod
#hostname = cmsweb-testbed.cern.ch
#hostname = cmsweb-test2.cern.ch
#hostname = cmsweb.cern.ch

proxy = os.environ['X509_USER_PROXY']
print("Proxy: %s" % proxy)
asoworker = 'schedd'


all_ids = [
    '272aa4b24a18834c8ce9b47f70ed0c71f446d073ea0d3af6db8b6c40cc',
    '279c1d1b97d2c7a5418b6af6e15931f2272d2506935cd080f50dc05bea',
    '27bdb06ca40995999bf5881a2bf561e79d0a8989aabc3dec504ab7c1a5',
    '275333b8c3893b5451742cf1ff35a83fd9d174bb11e1184024abee13dc',
    '273b0a494a27455d4af2c9d52aa24002c27e4949133024e9070e4b71b3',
    '2786217938c0e27c1a875ca69427173b031ada45ca11ec9e5c3491cf27',
    '278b35f31e4f729fbb01a76c0e17d3da12889072fefd27990390338fc5',
    '2778f91660386324b3ef6dbc0ec688b6cfa24af47bca2ac227c048773d',
    '279467c05bdb3d27b6e26508eec0e4efe063f10c458554803ba3952d66',
    '27b62670c0c0325103a08dceab376e9a4c22cd65bc42b6fada53e04692',
    '27a99fe24ae4195c1c8a3d671aff86110feffa2adb776d920e257bbcc3',
    '27d654781eb7d2013f8186609cb65e4d9a6b5f49b5e01c5c099f2a6531',
    '27afe12a7cdec488643b505633d308a007846b3708a9155334c6f1c139',
    '27a55051fb800140dec4020b31570b6e683694bd7ec5d95ec0734b51b5',
]

def mark_transferred(ids, server):
    """
    Mark the list of files as tranferred
    :param ids: list of Oracle file ids to update
    :return: 0 success, 1 failure
    """
    try:

        print("Marking done %d files" % len(ids))

        data = dict()
        data['asoworker'] = asoworker
        data['subresource'] = 'updateTransfers'
        data['list_of_ids'] = ids
        data['list_of_transfer_state'] = ["DONE" for _ in ids]

        t1 = time.time()
        server.post(api='filetransfers', data=encodeRequest(data))
        t2 = time.time()
        print("Marked good")
        elapsed = int(t2 - t1)
    except Exception as ex:
        t2 = time.time()
        elapsed = int(t2 - t1)
        print("Error updating documents:\n %s" % str(ex))
    return elapsed

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--r', help='REST host name (e.g. cmsweb-testbed)', required=True)
    parser.add_argument('--i', help='DB instance (prod/preprod/dev)', default='preprod')
    parser.add_argument('--n', help='# of ids to update (1-10)', default=1)

    args = parser.parse_args()
    n = int(args.n)
    hostname = args.r
    instance = args.i
    ids = all_ids[0:n]

    print("test: %s on db %s" % (hostname, instance))
    print("test with %d ids" % n)
    crabserver = CRABRest(hostname=hostname, localcert=proxy, localkey=proxy, userAgent='CRABtestSB')
    crabserver.setDbInstance(dbInstance=instance)
    elapsed = mark_transferred(ids, crabserver)
    print("elapsed time: %d sec" % elapsed)
    return


if __name__ == "__main__":
    main()
