#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File        : osearch.py
Author      : Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : OpenSearch client to send data to CERN IT OpenSearch clusters

Requirements:
    - opensearch-py~=2.1
    - OpenSearch cluster host name.
    - Secret file one line of 'username:password' of the OpenSearch. Ask to CMS Monitoring team.
    - Index mapping schema and settings, see get_index_schema() below.
    - Index template starting with 'test-'.

How to use:
    # ------------------------------- Simple detailed -------------------------------------
    # Example documents to send
    docs = [{"timestamp": epoch_seconds, "field1": "t1 short", "field2": "t2 long text", "count": 1}, {...}, {...}, ...]

    # Define your index mapping and settings:
    def get_index_schema():
        return {
            "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
            "mappings": {
                "properties": {
                    "timestamp": {"format": "epoch_second", "type": "date"},
                    "field1": {"ignore_above": 1024, "type": "keyword"},
                    "field2": {"type": "text"},
                    "count": {"type": "long"},
                }
            }
        }

    _index_template = 'test-foo'
    client = osearch.get_es_client("es-cms1.cern.ch/es", 'secret_opensearch.txt', get_index_schema())

    # index_mod="": 'test-foo', index_mod="Y": 'test-foo-YYYY', index_mod="M": 'test-foo-YYYY-MM', index_mod="D": 'test-foo-YYYY-MM-DD',
    idx = client.get_or_create_index(timestamp=time.time(), index_template=_index_template, index_mod="")

    client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)

    # ------------------------------- Big Spark dataframe -------------------------------------
    _index_template = 'test-foo'
    client = osearch.get_es_client("es-cms1.cern.ch/es", 'secret_opensearch.txt', get_index_schema())
    for part in df.rdd.mapPartitions().toLocalIterator():
        print(f"Length of partition: {len(part)}")
        # You can define below calls in a function for reusability
        idx = client.get_or_create_index(timestamp=part[0]['timestamp'], index_template=_index_template, index_mod="D") # sends to daily index
        client.send(idx, part, metadata=None, batch_size=10000, drop_nulls=False)

    # ------------------------------- Small Spark dataframe -------------------------------------
    docs = df.toPandas().to_dict('records') # and go with 'Simple detailed` example
"""

import json
import logging
import time
import concurrent.futures
from collections import Counter as collectionsCounter
from datetime import datetime

from opensearchpy import OpenSearch

# Global OpenSearch connection client
_opensearch_client = None

# Global index cache, keep tracks of indices that are already created with mapping in the OpenSearch instance
_index_cache = set()


def get_es_client(host, secret_file, index_mapping_and_settings):
    """Creates OpenSearch client

    Uses a global OpenSearch client and return it if connection still holds, otherwise creates a new connection.
    """
    global _opensearch_client
    if not _opensearch_client:
        # reinitialize
        _opensearch_client = OpenSearchInterface(host, secret_file, index_mapping_and_settings)
    return _opensearch_client


class OpenSearchInterface(object):
    """Robust interface to OpenSearch cluster

    secret_es.txt: "username:password"
    """

    def __init__(self, host, secret_file, index_mapping_and_settings):
        try:
            logging.info("OpenSearch instance is initializing")
            self.host = host
            self.secret_file = secret_file
            self.index_mapping_and_settings = index_mapping_and_settings
            username, password = open(secret_file).readline().split(':')
            username, password = username.strip(), password.strip()
            url = 'https://' + username + ':' + password + '@' + host
            self.handle = OpenSearch(
                [url],
                http_compress=True,
                verify_certs=True,
                use_ssl=True,
                ca_certs='/etc/pki/tls/certs/ca-bundle.trust.crt',
            )
        except Exception as e:
            logging.error("OpenSearchInterface initialization failed: " + str(e))

    def make_mapping(self, idx):
        """Creates mapping of the index

        idx: Full index name test-foo(no date format), test-foo-YYYY-MM-DD(index_mod=D)
        """
        body = json.dumps(self.index_mapping_and_settings)
        # Make mappings for OpenSearch index
        result = self.handle.indices.create(index=idx, body=body, ignore=400)
        if result.get("status") != 400:
            logging.warning("Creation of index %s: %s" % (idx, str(result)))
        elif "already exists" not in result.get("error", "").get("reason", ""):
            logging.error("Creation of index %s failed: %s" % (idx, str(result.get("error", ""))))

    def get_or_create_index(self, timestamp, index_template, index_mod=""):
        """Creates index with mappings and settings if not exist


        timestamp      : epoch seconds
        index_template : index base name
        index_mode     : one of 'Y': "index_template"-YYYY, 'M': "index_template"-YYYY-MM, 'D': "index_template"-YYYY-MM-DD,
                           empty string uses single index as "index_template"

        Returns yearly/monthly/daily index string depending on the index_mode and creates it if it does not exist.
        - It checks if index mapping is already created by checking _index_cache set.
        - And returns from _index_cache set if index exists
        - Else, it creates the index with mapping which happens in the first batch of the month ideally.
        """
        global _index_cache

        timestamp = int(timestamp)
        if index_mod.upper() == "Y":
            idx = time.strftime("%s-%%Y" % index_template, datetime.utcfromtimestamp(timestamp).timetuple())
        elif index_mod.upper() == "M":
            idx = time.strftime("%s-%%Y-%%m" % index_template, datetime.utcfromtimestamp(timestamp).timetuple())
        elif index_mod.upper() == "D":
            idx = time.strftime("%s-%%Y-%%m-%%d" % index_template, datetime.utcfromtimestamp(timestamp).timetuple())
        else:
            idx = index_template

        if idx in _index_cache:
            return idx
        get_es_client(self.host, self.secret_file, self.index_mapping_and_settings).make_mapping(idx=idx)
        _index_cache.add(idx)
        return idx

    @staticmethod
    def parse_errors(result):
        """Parses bulk send result and finds errors to log
        """
        reasons = [d.get("index", {}).get("error", {}).get("reason", None) for d in result["items"]]
        counts = collectionsCounter([_f for _f in reasons if _f])
        n_failed = sum(counts.values())
        logging.error("Failed to index %d documents to OpenSearch: %s" % (n_failed, str(counts.most_common(3))))
        return n_failed

    @staticmethod
    def drop_nulls_in_dict(d):  # d: dict
        """Drops the dict key if the value is None

        OpenSearch mapping does not allow None values and drops the document completely.
        """
        return {k: v for k, v in d.items() if v is not None}  # dict

    @staticmethod
    def to_chunks(data, samples=10000):
        """Yields chunks of data"""
        length = len(data)
        for i in range(0, length, samples):
            yield data[i:i + samples]

    @staticmethod
    def make_es_body(bulk_list, metadata=None):
        """Prepares documents for bulk send by adding metadata part and separating with new line
        """
        metadata = metadata or {}
        body = ""
        for data in bulk_list:
            if metadata:
                data.setdefault("metadata", {}).update(metadata)
            body += json.dumps({"index": {}}) + "\n"
            body += json.dumps(data) + "\n"
        return body

    def send(self, idx, data, metadata=None, batch_size=10000, drop_nulls=False):
        """Send data in bulks to OpenSearch instance, batching is implemented by default.

        Args:
            idx: full index name, can be index_template or index_template-YYYY-..
            data: can be a single document or list of documents to send
            metadata: metadata that will be sent with each document
            batch_size: batching sample size
            drop_nulls: in Grafana, null strings cause trouble in aggregations. If it is true, it drops None fields from the documents.
        """
        global _opensearch_client
        _opensearch_client = get_es_client(self.host, self.secret_file, self.index_mapping_and_settings)

        # If one document as dict, make it list
        if not isinstance(data, list):
            data = [data]

        result_n_failed = 0
        for chunk in self.to_chunks(data, batch_size):
            if drop_nulls:
                chunk = [self.drop_nulls_in_dict(_x) for _x in chunk]
            body = self.make_es_body(chunk, metadata)
            res = _opensearch_client.handle.bulk(body=body, index=idx, request_timeout=300)
            if res.get("errors"):
                result_n_failed += self.parse_errors(res)
        if result_n_failed > 0:
            logging.error("OpenSearch send failed count: ", result_n_failed)
        logging.debug("OpenSearch send", len(data) - result_n_failed, "documents successfully")
        return result_n_failed

def send_os(docs, index_name, schema, secretpath, timestamp, batch_size=10000, printsummary=True):

    client = get_es_client("os-cms.cern.ch/os", secretpath, schema)
    idx = client.get_or_create_index(timestamp=timestamp, index_template=index_name, index_mod="M")
    no_of_fail_saved = client.send(idx, docs, metadata=None, batch_size=batch_size, drop_nulls=False)
    if printsummary:
        print("========================================================================"
              , "FINISHED : "
              , len(docs), "ROWS ARE SENT"
              , no_of_fail_saved, "ROWS ARE FAILED"
              , "========================================================================", sep='\n')
    else:
        return len(docs), no_of_fail_saved

def send_os_parallel(docs, index_name, schema, secretpath, timestamp, batch_size=10000):
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
