"""
Utility functions for spark scripts
"""
# pylint: disable=protected-access

import concurrent.futures

from datetime import timedelta
from osearch import get_es_client, OpenSearchInterface

def get_candidate_files(start_date, end_date, spark, base, day_delta=1):
    """
    Returns a list of hdfs folders that can contain data for the given dates.
    Copy from CMSMONIT CMSSpark:
    https://github.com/dmwm/CMSSpark/blob/b8efa0ac5cb57b617ee8d1ea9bb26d53fb0443b0/src/python/CMSSpark/spark_utils.py#L768
    """
    st_date = start_date - timedelta(days=day_delta)
    ed_date = end_date + timedelta(days=day_delta)
    days = (ed_date - st_date).days

    sc = spark.sparkContext
    # The candidate files are the folders to the specific dates,
    # but if we are looking at recent days the compaction procedure could
    # have not run yet, so we will consider also the .tmp folders.

    candidate_files = [
        f"{base}/{(st_date + timedelta(days=i)).strftime('%Y/%m/%d')}{{,.tmp}}"
        for i in range(0, days)
    ]
    fsystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    uri = sc._gateway.jvm.java.net.URI
    path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = fsystem.get(uri("hdfs:///"), sc._jsc.hadoopConfiguration())
    candidate_files = [url for url in candidate_files if fs.globStatus(path(url))]
    return candidate_files


def send_os(docs, index_name, schema, secretpath, timestamp, batch_size=10000, printsummary=True):
    """
    Convenient one-liner function to send data to opensearch using osearch lib

    :param docs: documents to send to opensearch
    :type docs: dict
    :param index_name: opensearch index name
    :type index_name: str
    :param schema: opensearch index schema
    :type schema: str
    :param secretpath: path to secret file which contains "<user>:<password>"
    :type secretpath: str
    :param timestamp: timestamp in second to build
    :type timestamp: str
    :param batch_size: how many docs we send to os in a single request
    :type batch_size: int
    :param printsummary: if yes, print summary text
    :type printsummary: bool

    :return: number of total docs and number of fail-to-send docs.
    :rtype: (int, int)
    """
    client = get_es_client("os-cms.cern.ch/os", secretpath, schema)
    idx = client.get_or_create_index(timestamp=timestamp, index_template=index_name, index_mod="M")
    no_of_fail_saved = client.send(idx, docs, metadata=None, batch_size=batch_size, drop_nulls=False)
    if printsummary:
        print("========================================================================"
              , "FINISHED : "
              , len(docs), "ROWS ARE SENT"
              , no_of_fail_saved, "ROWS ARE FAILED"
              , "========================================================================", sep='\n')
    return len(docs), no_of_fail_saved

def send_os_parallel(docs, index_name, schema, secretpath, timestamp, batch_size=10000):
    """
    Convenient one-liner function to send data to opensearch using osearch lib,
    in parallel.

    Note that it has the same params as send_os() except `printsummary`, and
    return None
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        futures = []
        for chunk in OpenSearchInterface.to_chunks(docs, batch_size):
            future = executor.submit(send_os, chunk, index_name, schema, secretpath, timestamp, batch_size+1, False)
            futures.append(future)
        total_docs = 0
        total_fails = 0
        for f in futures:
            ndocs, nfails = f.result()
            total_docs += ndocs
            total_fails += nfails
        print("========================================================================"
              , "FINISHED : "
              , total_docs, "ROWS ARE SENT"
              , total_fails, "ROWS ARE FAILED"
              , "========================================================================", sep='\n')
