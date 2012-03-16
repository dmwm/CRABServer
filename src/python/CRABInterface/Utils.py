import logging
import copy

import WMCore
from WMCore.REST.Error import *
from WMCore.Database.CMSCouch import CouchServer, CouchError

"""
The module contains some utility functions used by the various modules of the CRAB REST interface
"""

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
        database = couchdb.connectDatabase(reqmgrname)
    except CouchError, ex:
        logger.exception(ex)
        raise ExecutionError("Error connecting to couch database", errobj = ex)

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
