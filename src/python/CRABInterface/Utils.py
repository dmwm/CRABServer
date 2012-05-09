import logging
import copy
import traceback
from collections import namedtuple

import WMCore
from WMCore.REST.Error import *
from WMCore.Database.CMSCouch import CouchServer, CouchError

"""
The module contains some utility functions used by the various modules of the CRAB REST interface
"""

CouchDBConn = namedtuple("CouchDBConn", ["db", "name", "conn"])

def conn_couch(databases):
    """
    Decorator to be used among REST resources to optimize connections to CouchDB

    arg str list databases: list of string telling which database connections
                            should be started; currently availables are 
                            'monitor' and 'asomonitor'.
    """
    def wrap(func):
        def wrapped_func(*args):
            if 'monitor' in databases and not args[0].monitordb.conn:
                args[0].monitordb = args[0].monitordb._replace(conn=args[0].monitordb.db.connectDatabase(args[0].monitordb.name, create=False))
            if 'asomonitor' in databases and not args[0].asodb.conn:
                args[0].asodb = args[0].asodb._replace(conn=args[0].asodb.db.connectDatabase(args[0].asodb.name, create=False))
            return func(*args)
        return wrapped_func
    return wrap

def setProcessingVersion(request, reqmgrurl, reqmgrname):
    """
    If no ProcessingVersion is specified, go to couch to figure out next one.
    """
    logger = logging.getLogger("CRABLogger.Utils")

    if request.get('ProcessingVersion', None):
        return

    try:
        logger.debug("Connecting to database %s using the couch instance at %s: " % (reqmgrname, reqmgrurl))
        couchdb = CouchServer(reqmgrurl)
        database = couchdb.connectDatabase(reqmgrname, create=False)
    except CouchError, ex:
        raise ExecutionError("Error connecting to couch database", errobj = ex, trace=traceback.format_exc())

    startkey = [request['Requestor'], request['PublishDataName'], request['InputDataset']]
    endkey = copy.copy(startkey)
    endkey.append({})
    options = {"startkey" : startkey, "endkey" : endkey}

    requests = database.loadView("ReqMgr", "requestsByUser", options)
    logger.debug("Found %d rows in the requests database." % requests["total_rows"])

    versions = []
    for row in requests['rows']:
        oldVersion = row['value']['version']
        try:
            versions.append(int(oldVersion.replace('v', '')))
        except ValueError: # Not an int, so we ignore it
            pass

    logger.debug("Existing versions for workflow are: %s" % versions)
    newVersion = 1
    if versions:
        newVersion = max(versions) + 1

    logger.debug("New version is: %s" % newVersion)
    return 'v%d' % newVersion
