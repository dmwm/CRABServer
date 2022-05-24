"""
run with:

# enter a publisher container
source env.sh
python3 StressTest_REST_POST.py -c 10 -l 2

# if you do not want to use `test1` rest or `dev` db, just use
python3 StressTest_REST_POST.py --concurrent 10 --loops 2 --instance preprod --db-instance preprod

# if you want to use a different lfn template:
python3 StressTest_REST_POST.py -c 10 -l 2 \
--lfntemplate "/store/temp/user/${username}.${random}/${dataset1}/${dataset2}/../${timestamp}/0000/output_%d.root" \
--filesintemplate 100
"""

import os
import sys
import logging
import pprint
import asyncio
import time
import argparse
import statistics
import functools

from RESTInteractions import CRABRest
from ServerUtilities import getHashLfn, encodeRequest

logger = logging.getLogger()
logformat = '%(asctime)s %(levelname)-10s: %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.WARNING, format=logformat)

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-c", "--concurrent",
  help="number concurrent requests",
  type=int,
  default=5)
parser.add_argument("-l", "--loops",
  help="number of times we run --concurrent requests",
  type=int,
  default=2)
parser.add_argument("-i", "--instance",
  help="which CRAB REST instance you want to test",
  type=str,
  choices=["prod","preprod","test1", "test2"],
  default='test1')
parser.add_argument("-d", "--db-instance",
  help="which CRAB database you want to connect the REST to",
  type=str,
  choices=["prod","preprod","dev"],
  default='dev')
parser.add_argument("-t", "--lfntemplate",
  help="which lfnTemplate you want to use",
  type=str,
  default='/store/temp/user/dmapelli.08aeb7b050bc579abba57f9ea54d7bc13447d65b/GenericTTbar/autotest-dmapelli-1645459633/220221_160717/0000/output_%d.root')
parser.add_argument("-f", "--filesintemplate",
  help="number of files in the template",
  type=int,
  default=10)

args = parser.parse_args()

# if X509 vars are not defined, use default Publisher location
userProxy = os.getenv('X509_USER_PROXY')
if userProxy:
    os.environ['X509_USER_CERT'] = userProxy
    os.environ['X509_USER_KEY'] = userProxy
if not os.getenv('X509_USER_CERT'):
    os.environ['X509_USER_CERT'] = '/data/certs/servicecert.pem'
if not os.getenv('X509_USER_KEY'):
    os.environ['X509_USER_KEY'] = '/data/certs/servicekey.pem'

hostCert = os.getenv('X509_USER_CERT')
hostKey = os.getenv('X509_USER_KEY')

# CRAB REST API's
restHost='cmsweb-{}.cern.ch'.format(args.instance)
crabServer = CRABRest(hostname=restHost, localcert=hostCert,
                      localkey=hostKey, retry=0,
                      userAgent='CRABStressTest')
dbInst=args.db_instance
crabServer.setDbInstance(dbInstance=dbInst)

def mark_good(files, crabServer, logger):
    """
    Mark the list of files as tranferred
    """

    msg = "Marking %s file(s) as published." % len(files)
    logger.info(msg)
    nMarked = 0
    for lfn in files:
        data = {}
        source_lfn = lfn
        docId = getHashLfn(source_lfn)
        data['asoworker'] = 'schedd'
        data['subresource'] = 'updatePublication'
        data['list_of_ids'] = [docId]
        data['list_of_publication_state'] = ['DONE']
        data['list_of_retry_value'] = [1]
        data['list_of_failure_reason'] = ['']

        try:
            result = crabServer.post(api='filetransfers', data=encodeRequest(data))
            logger.debug("updated DocumentId: %s lfn: %s Result %s", docId, source_lfn, result)
        except Exception as ex:
            logger.error("Error updating status for DocumentId: %s lfn: %s", docId, source_lfn)
            logger.error("Error reason: %s", ex)

        nMarked += 1
        if nMarked % 10 == 0:
            logger.info('marked %d files', nMarked)

# prepare a list of 10 files
lfnTemplate = args.lfntemplate 
files=[]
for i in range(1,11):
    files.append(lfnTemplate % i)
#pprint.pprint(files)

loop = asyncio.get_event_loop()

async def mark_good_async():
    logger.debug("start")
    start_time = time.time()
    await loop.run_in_executor(None, functools.partial(mark_good, files,crabServer,logger))
    single_time = time.time() - start_time
    logger.debug("end")
    return single_time

async def main():
    tasks = []
    for _ in range(args.concurrent):
        tasks.append(mark_good_async())
    exec_times = await asyncio.gather(*tasks)
    logger.warning("quantiles(times): %s", statistics.quantiles(exec_times, n=10))
    logger.warning("max(times): %s", max(exec_times))
    return exec_times

if __name__ == "__main__":
    for _ in range(args.loops):
        loop.run_until_complete(main())
