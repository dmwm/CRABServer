#! /bin/env python

from __future__ import absolute_import, division, print_function

import json
import math
import re
import time
import logging

from itertools import islice
from subprocess import PIPE, Popen
import requests
from requests.exceptions import ReadTimeout

import rucio.rse.rsemanager as rsemgr
from rucio.client.client import Client
from rucio.common.exception import (DataIdentifierAlreadyExists, FileAlreadyExists, RucioException,
                                    AccessDenied)
DEBUG_FLAG = False
DEFAULT_DASGOCLIENT = '/usr/bin/dasgoclient'

DEFAULT_PHEDEX_INST = 'prod'
DEFAULT_DATASVC_URL = 'https://cmsweb.cern.ch/phedex/datasvc/json'
DATASVC_MAX_RETRY = 3
DATASVC_RETRY_SLEEP = 10

class CMSRucio(object):
    """
    Interface for Rucio with the CMS data model

    CMS         Rucio
    File/LFN    File
    Block       Dataset
    Dataset     Container

    We try to use the correct terminology on for variable and parameter names
    where the CMS facing code use File/Block/Dataset and the Rucio facing code
    uses File/Dataset/Container
    """

    def __init__(self, account, auth_type, creds=None, scope='cms', dry_run=False,
                 das_go_path=DEFAULT_DASGOCLIENT, check=False):
        self.account = account
        self.auth_type = auth_type
        self.creds = creds
        self.scope = scope
        self.dry_run = dry_run
        self.dasgoclient = das_go_path
        self.check = check

        self.cli = Client(account=self.account, auth_type=self.auth_type, creds=self.creds)


    def get_file_url(self, lfn, rse):
        """
        Return the rucio url of a file.
        """
        return self.get_global_url(rse) + '/' + lfn

    def test(self):

        print("Ciao")

    def get_global_url(self, rse):
        """
        Return the base path of the rucio url
        """
        print("Getting parameters for rse %s" % rse)

        # rse = rsemgr.get_rse_info(rse)
        # proto = rse['protocols'][0]
        protos = self.cli.get_protocols(rse)
        proto = protos[0]

        schema = proto['scheme']
        prefix = proto['prefix'] + '/' + self.scope.replace('.', '/')
        if schema == 'srm':
            prefix = proto['extended_attributes']['web_service_path'] + prefix
        url = schema + '://' + proto['hostname']
        if proto['port'] != 0:
            url = url + ':' + str(proto['port'])
        url = url + prefix
        print("Determined base url %s" % url)
        return url


    def cms_blocks_in_container(self, container, scope='cms'):
        """
        getting the cms_blocks (rucio datasets) in a rucio container
        """

        block_names = []
        response = self.cli.get_did(scope=scope, name=container)
        if response['type'].upper() != 'CONTAINER':
            return block_names

        response = self.cli.list_content(scope=scope, name=container)
        for item in response:
            if item['type'].upper() == 'DATASET':
                block_names.append(item['name'])

        return block_names

    def get_replica_info_for_blocks(self, scope='cms', dataset=None, block=None, node=None):

        """
        This mimics the API of a CMS PhEDEx function. Be careful changing it

        _blockreplicas_
        Get replicas for given blocks

        dataset        dataset name, can be multiple (*)
        block          block name, can be multiple (*)
        node           node name, can be multiple (*)
        se             storage element name, can be multiple (*)
        update_since  unix timestamp, only return replicas updated since this
                time
        create_since   unix timestamp, only return replicas created since this
                time
        complete       y or n, whether or not to require complete or incomplete
                blocks. Default is to return either
        subscribed     y or n, filter for subscription. default is to return either.
        custodial      y or n. filter for custodial responsibility.  default is
                to return either.
        group          group name.  default is to return replicas for any group.
        """

        block_names = []
        result = {'block': []}

        if isinstance(block, (list, set)):
            block_names = block
        elif block:
            block_names = [block]

        if isinstance(dataset, (list, set)):
            for dataset_name in dataset:
                block_names.extend(self.cms_blocks_in_container(dataset_name, scope=scope))
        elif dataset:
            block_names.extend(self.cms_blocks_in_container(dataset, scope=scope))

        for block_name in block_names:
            dids = [{'scope': scope, 'name': block_name} for block_name in block_names]

            response = self.cli.list_replicas(dids=dids)
            nodes = set()
            for item in response:
                for node, state in item['states'].items():
                    if state.upper() == 'AVAILABLE':
                        nodes.add(node)
            result['block'].append({block_name: list(nodes)})
        return result

    def dataset_summary(self, scope='cms', dataset=None):
        """
        Summary of a dataset metadata
        """
        response = self.cli.list_files(scope=scope, name=dataset)
        summary = {'files': {}, 'dataset': dataset}
        dataset_bytes = 0
        dataset_events = 0
        dataset_files = 0
        files = []
        for fileobj in response:
            dataset_files += 1
            summary['files'].update({fileobj['name']: {
                'bytes': fileobj['bytes'],
                'events': fileobj['events'],
            }})
            files.append({'scope': scope, 'name': fileobj['name']})
            if fileobj['bytes']:
                dataset_bytes += fileobj['bytes']

            if fileobj['events']:
                dataset_events += fileobj['events']
        summary.update({'bytes': dataset_bytes, 'events': dataset_events,
                        'file_count': dataset_files})
        summary.update({'size': self.convert_size_si(dataset_bytes)})

        site_summary = {}

        for chunk in self.grouper(files, 1000):
            response = self.cli.list_replicas(dids=chunk)
            for item in response:
                lfn = item['name']
                for node, state in item['states'].items():
                    if state.upper() == 'AVAILABLE':
                        if node not in site_summary:
                            site_summary[node] = {'file_count': 0, 'bytes': 0, 'events': 0}
                        site_summary[node]['file_count'] += 1
                        if summary['files'][lfn]['bytes']:
                            site_summary[node]['bytes'] += summary['files'][lfn]['bytes']
                        if summary['files'][lfn]['events']:
                            site_summary[node]['events'] += summary['files'][lfn]['events']

        for node in site_summary:
            site_summary[node]['size'] = self.convert_size_si(site_summary[node]['bytes'])

        summary['sites'] = site_summary

        return summary

    def register_crab_replicas(self, rse, lfns, sizes, checksums):
        """
        Register file replicas
        """
        if self.dry_run:
            print(' Dry run only. Not registering files.')
            return
        if checksums:
            replicas = [{'scope': self.scope, 'pfn': pfn,'name': lfn, 'bytes': size, 'adler32': checksum, 'state': 'A'} for lfn, pfn, size, checksum in zip(lfns, pfns, sizes, checksums)]
        else:
            replicas = [{'scope': self.scope, 'name': lfn, 'bytes': size, 'state': 'A'} for lfn, size in zip(lfns, sizes)]
        print(replicas)
        # TODO: check if did already exists and choose between the following 2
        try:
            self.cli.add_replicas(rse=rse, files=replicas)
        except Exception as ex:
            logging.exception("Failed to add replicas")
            raise ex

        try:
            self.cli.update_replicas_states(rse=rse, files=replicas)
        except Exception as ex:
            logging.exception("Failed to update states")
            raise ex


    def register_temp_replicas(self, rse, lfns, pfns, sizes, checksums):
        """
        Register file replicas
        """

        if self.dry_run:
            print(' Dry run only. Not registering files.')
            return
        if checksums:
            replicas = [{'scope': self.scope, 'pfn': pfn,'name': lfn, 'bytes': size, 'adler32': checksum} for lfn, pfn, size, checksum in zip(lfns, pfns, sizes, checksums)]
        else:
            replicas = [{'scope': self.scope, 'pfn': pfn,'name': lfn, 'bytes': size} for lfn, pfn, size in zip(lfns, pfns, sizes)]
        #print(replicas)
        try:
            if self.cli.add_replicas(rse=rse, files=replicas):
                return True
        except Exception as ex:
            raise ex

    def delete_replicas(self, rse, replicas):
        """
        Delete replicas from the current RSE.
        """
        if not replicas:
            return

        print("Deleting files from %s in Rucio: %s" %
              (rse, ", ".join([filemd['name'] for filemd in replicas])))

        if self.dry_run:
            print(" Dry run only.  Not deleting replicas.")
            return

        logging.debug('%s: %s' % (rse, [{'scope': self.scope,
                                                      'name': filemd['name'],
                                                     } for filemd in replicas]))
        try:
            self.cli.delete_replicas(rse=rse, files=[{'scope': self.scope,
                                                      'name': filemd['name'],
                                                     } for filemd in replicas])
        except AccessDenied:
            print("Permission denied in deleting replicas: %s" %
                  ", ".join([filemd['name'] for filemd in replicas]))
        except Exception:
            logging.exception("Failed to remove file")

    def register_dataset(self, block, dataset, lifetime=None):
        """
        Create the rucio dataset corresponding to a CMS block and
        attach it to the container (CMS dataset)
        """

        if self.dry_run:
            print(' Dry run only. Not creating dataset (CMS block %s).' % block)
            return

        try:
            self.cli.add_dataset(scope=self.scope, name=block, lifetime=lifetime)
        except DataIdentifierAlreadyExists:
            pass

        try:
            self.cli.attach_dids(scope=self.scope, name=dataset,
                                 dids=[{'scope': self.scope, 'name': block}])
        except RucioException:
            pass

    def register_container(self, dataset, lifetime):
        """
        Create a container (CMS Dataset)
        """

        if self.dry_run:
            print(' Dry run only. Not creating container (CMS dataset %s).' % dataset)
            return

        try:
            self.cli.add_container(scope=self.scope, name=dataset, lifetime=lifetime)
        except DataIdentifierAlreadyExists:
            pass

    def attach_files(self, lfns, block):
        """
        Attach the file to the container
        """
        if not lfns:
            return

        if self.dry_run:
            print(' Dry run only. Not attaching files to %s.' % block)
            return

        try:
            if self.cli.attach_dids(scope=self.scope, name=block,
                                 dids=[{'scope': self.scope, 'name': lfn} for lfn in lfns]):
                return True
        except FileAlreadyExists:
            raise FileAlreadyExists
            #pass
        except Exception as ex:
            raise ex

    def get_phedex_metadata(self, dataset, pnn):
        """
        Gets the list of blocks at a PhEDEx site, their files and their metadata
        """
        print("Initializing... getting the list of blocks and files")
        return_blocks = {}
        blocks = das_go_client("block dataset=%s site=%s system=phedex"
                               % (dataset, pnn), self.dasgoclient)
        for item in blocks:
            block_summary = {}
            block_name = item['block'][0]['name']
            files = das_go_client("file block=%s site=%s system=phedex"
                                  % (block_name, pnn), self.dasgoclient)
            for item2 in files:

                # sometimes dasgoclient does not return the checksum attribute for a file
                # re-fetching data fix the problem
                try:
                    item2['file'][0]['checksum']
                except KeyError:
                    print("file %s misses checksum attribute, try to refetch from das",
                          item2['file'][0]['name'])
                    time.sleep(5)
                    dummy = das_go_client("file file=%s system=phedex" % item2['file'][0]['name'])
                    item2['file'][0] = dummy['file'][0]

                try:
                    cksum = re.match(r"\S*adler32:([^,]+)",
                                     item2['file'][0]['checksum']).group(1)
                except AttributeError:
                    raise AttributeError("file %s has non parsable checksum entry %s"\
                                         % (item2['file'][0]['name'], item2['file'][0]['checksum']))

                cksum = "{0:0{1}x}".format(int(cksum, 16), 8)
                block_summary[item2['file'][0]['name']] = {
                    'name': item2['file'][0]['name'],
                    'checksum': cksum,
                    'size': item2['file'][0]['size']
                }
            return_blocks[block_name] = block_summary
        print("PhEDEx initalization done.")

        return return_blocks

    def add_rule(self, names, rse_exp, comment, copies=1):
        """
        Just wrapping the add_replication_rule method of the ruleclient
        """

        dids = [{'scope': self.scope, 'name': name} for name in names]

        if self.dry_run:
            print("Dry run, no rule added.")
            return

        self.cli.add_replication_rule(dids=dids,
                                      copies=copies,
                                      rse_expression=rse_exp,
                                      comment=comment)



    def del_rule(self, rid):
        """
        Just wrapping the delete_replication_rule method of ruleclient
        """

        if self.dry_run:
            print("Dry run, rule %s not deleted." % rid)
            return

        try:
            self.cli.delete_replication_rule(rid, purge_replicas=False)
        except AccessDenied:
            print("Premission denied in removing rule (rid: %s)" % rid)
            raise AccessDenied

    def update_rule(self, rid, options):
        """
        Just wrapping the update_replication_rule method of ruleclient
        """

        if self.dry_run:
            print("Dry run, rule %s not modified." % rid)
            return

        self.cli.update_replication_rule(rid, options)

    @staticmethod
    # FIXME: Pull this from WMCore/Utils/IteratorTools when we migrate
    def grouper(iterable, csize):
        """
        :param iterable: List of other iterable to slice
        :type: iterable
        :param csize: Chunk size for resulting lists
        :type: int
        :return: iterator of the sliced list
        Source: http://stackoverflow.com/questions/3992735/
           python-generator-that-groups-another-iterable-into-groups-of-n
        """
        iterable = iter(iterable)
        return iter(lambda: list(islice(iterable, csize)), [])

    @staticmethod
    def convert_size(size_bytes):
        """
        Convert size in bytes into human readable.
        Base 1024.
        """
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        ival = int(math.floor(math.log(size_bytes, 1024)))
        power = math.pow(1024, ival)
        size = round(size_bytes / power, 2)
        return "%s %s" % (size, size_name[ival])

    @staticmethod
    def convert_size_si(size_bytes):
        """
        Convert size in bytes into human readable.
        Base 1000.
        """
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        ival = int(math.floor(math.log(size_bytes, 1000)))
        power = math.pow(1000, ival)
        size = round(size_bytes / power, 2)
        return "%s %s" % (size, size_name[ival])


def get_subscriptions(pnn, dataset=None, since=None, debug=DEBUG_FLAG):
    """
    Get a dictionary of "per block" phedex subscriptions
    :pnn:    phedex node name
    :dataset: dataset containing the blocks (default null, all datasets)
    :since: datasets created since (default null, all datasets)
    """

    subs = {}
    req = {'node': pnn, 'collapse': 'n', 'percent_min': '100'}
    if dataset is not None:
        req['block'] = dataset + '%23*'
        print("Getting fileblock subscriptions for phedex dataset %s" % dataset)
    if since is not None:
        req['create_since'] = since
        print("Getting fileblock subscriptions created since %s" % since)

    phedex_subs = datasvc_client('subscriptions', req, debug=debug)

    if len(phedex_subs['phedex']['dataset']) == 0:
        if debug:
            print("Subscription list is empty.")
        return {}

    for dataset in phedex_subs['phedex']['dataset']:
        for block in dataset['block']:
            if block['is_open'] == 'y':
                if debug:
                    print("Block %s is open, skipping" % block['name'])
                continue
            subs[block['name']] = block['subscription'][0]
    return subs


def datasvc_client(call, options, instance=DEFAULT_PHEDEX_INST,
                   url=DEFAULT_DATASVC_URL, debug=DEBUG_FLAG):
    """
    just wrapping a call to datasvc apis
    """
    url = DEFAULT_DATASVC_URL + '/' + instance
    url += '/' + call + '?'
    url += '&'.join([opt + '=' + val for opt, val in options.items()])

    done = False
    tries = 0

    if debug:
        print('DEBUG:' + url)

    while tries < DATASVC_MAX_RETRY and (not done):
        try:
            done = True
            req = requests.get(url, allow_redirects=False, verify=False)
        except ReadTimeout:
            done = False
            time.sleep(DATASVC_RETRY_SLEEP)
            tries += 1

    #ReadTimeout

    if debug:
        print('DEBUG:' + str(req.status_code))
        print('DEBUG:' + req.text)

    if req.status_code != 200:
        raise Exception('Request Failed')

    return json.loads(req.text)

def das_go_client(query, dasgoclient=DEFAULT_DASGOCLIENT, debug=DEBUG_FLAG):
    """
    just wrapping the dasgoclient command line
    """
    proc = Popen([dasgoclient, '-query=%s' % query, '-json'], stdout=PIPE)
    output = proc.communicate()[0]
    if debug:
        print('DEBUG:' + output)
    return json.loads(output)

def get_phedex_tfc(pnn):
    """
    Get the TFC of a PhEDEx node.
    """
    req = datasvc_client('tfc', {'node': pnn})
    return req['phedex']['storage-mapping']['array']
