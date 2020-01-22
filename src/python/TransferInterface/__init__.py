#! /bin/env python

from __future__ import absolute_import, division, print_function
import logging
import os
# import re
# from argparse import ArgumentParser

from CMSRucio import CMSRucio
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Storage.TrivialFileCatalog import readTFC
# import fts3.rest.client.easy as fts3
# from datetime import timedelta
from ServerUtilities import encodeRequest


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    :param l: list to splitt in chunks
    :param n: chunk size
    :return: yield the next list chunk
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def mark_transferred(ids, oracleDB):
    """
    Mark the list of files as tranferred
    :param ids: list of Oracle file ids to update
    :return: 0 success, 1 failure
    """
    os.environ["X509_CERT_DIR"] = os.getcwd()

    already_list = []
    if os.path.exists("task_process/transfers/transferred_files.txt"):
        with open("task_process/transfers/transferred_files.txt", "r") as list_file:
            for _data in list_file.readlines():
                already_list.append(_data.split("\n")[0])

    ids = [x for x in ids if x not in already_list]

    if len(ids) > 0:
        try:
            logging.debug("Marking done %s" % ids)

            data = dict()
            data['asoworker'] = 'rucio'
            data['subresource'] = 'updateTransfers'
            data['list_of_ids'] = ids
            data['list_of_transfer_state'] = ["DONE" for _ in ids]

            oracleDB.post('/filetransfers',
                          data=encodeRequest(data))
            logging.info("Marked good %s" % ids)
            with open("task_process/transfers/transferred_files.txt", "a+") as list_file:
                for id_ in ids:
                    list_file.write("%s\n" % id_)
        except Exception:
            logging.exception("Error updating documents")
            return 1
    else:
        logging.info("Nothing to update (Done)")
    return 0


def mark_failed(ids, failures_reasons, oracleDB):
    """
    Mark the list of files as failed
    :param ids: list of Oracle file ids to update
    :param failures_reasons: list of strings with transfer failure messages
    :return: 0 success, 1 failure
    """
    os.environ["X509_CERT_DIR"] = os.getcwd()

    if len(ids) > 0:

        try:
            data = dict()
            data['asoworker'] = 'rucio'
            data['subresource'] = 'updateTransfers'
            data['list_of_ids'] = ids
            data['list_of_transfer_state'] = ["FAILED" for _ in ids]
            data['list_of_failure_reason'] = failures_reasons
            data['list_of_retry_value'] = [0 for _ in ids]

            oracleDB.post('/filetransfers',
                          data=encodeRequest(data))
            logging.info("Marked failed %s" % ids)
        except Exception:
            logging.exception("Error updating documents")
            return None
    else:
        logging.info("Nothing to update (Failed)")

    return ids


class CRABDataInjector(CMSRucio):
    """
    General Class for injecting a cms dataset in rucio
    """

    def __init__(self, dataset, site, rse=None, scope='cms',
                 uuid=None, check=True, lifetime=None, dry_run=None, account=None, auth_type=None, creds=None):

        super(CRABDataInjector, self).__init__(account=account, auth_type=auth_type, scope=scope, dry_run=dry_run, creds=creds)
        self.dataset = dataset
        self.site = site
        if rse is None:
            rse = site
        self.rse = rse
        self.uuid = uuid
        self.check = check
        self.lifetime = lifetime

    def add_dataset(self):
        """[summary]

        """

        logging.info(self.site)
        self.register_dataset(self.dataset, '', self.lifetime)

        self.cli.add_replication_rule(dids=[{'scope': self.scope, 'name': "/"+self.dataset}],
                                      copies=1,
                                      rse_expression="{0}=True".format(self.rse),
                                      comment="")
