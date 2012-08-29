import logging
import copy
import traceback
from collections import namedtuple
from time import mktime, gmtime

from WMCore.REST.Error import ExecutionError
from WMCore.Database.CMSCouch import CouchServer, CouchError
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import addSiteWildcards
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

"""
The module contains some utility functions used by the various modules of the CRAB REST interface
"""

CouchDBConn = namedtuple("CouchDBConn", ["db", "name", "conn"])
CMSSitesCache = namedtuple("CMSSitesCache", ["cachetime", "sites"])

def conn_handler(services):
    """
    Decorator to be used among REST resources to optimize connections to other services
    as CouchDB and SiteDB, PhEDEx, WMStats monitoring

    arg str list services: list of string telling which service connections
                           should be started; currently availables are
                           'monitor' and 'asomonitor'.
    """
    def wrap(func):
        def wrapped_func(*args, **kwargs):
            if 'monitor' in services and not args[0].monitordb.conn:
                args[0].monitordb = args[0].monitordb._replace(conn=args[0].monitordb.db.connectDatabase(args[0].monitordb.name, create=False))
            if 'asomonitor' in services and not args[0].asodb.conn:
                args[0].asodb = args[0].asodb._replace(conn=args[0].asodb.db.connectDatabase(args[0].asodb.name, create=False))
            if 'sitedb' in services and (not args[0].allCMSNames.sites or (args[0].allCMSNames.cachetime+1800 < mktime(gmtime()))):
                args[0].allCMSNames = CMSSitesCache(sites=SiteDBJSON().getAllCMSNames(), cachetime=mktime(gmtime()))
                if hasattr(args[0], 'wildcardKeys') and hasattr(args[0], 'wildcardSites'):
                    addSiteWildcards(args[0].wildcardKeys, args[0].allCMSNames.sites, args[0].wildcardSites)
            if 'wmstats' in services and not args[0].wmstats:
                args[0].wmstats = WMStatsWriter(args[0].wmstatsurl)
            if 'phedex' in services and not args[0].phedex:
                args[0].phedex = PhEDEx(responseType='xml', dict=args[0].phedexargs)
            return func(*args, **kwargs)
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

    inputDataset = request.get("InputDataset", None)
    startkey = [request['Requestor'], request['PublishDataName'], inputDataset]
    endkey = copy.copy(startkey)
    endkey.append({})
    options = {"startkey" : startkey, "endkey" : endkey}

    requests = database.loadView("ReqMgr", "requestsByUser", options)
    logger.debug("Found %d rows in the requests database." % requests["total_rows"])

    versions = []
    for row in requests['rows']:
        oldVersion = row['value']['version']
        try:
            versions.append(int(oldVersion))
        except ValueError: # Not an int, so we ignore it
            pass

    logger.debug("Existing versions for workflow are: %s" % versions)
    newVersion = 1
    if versions:
        newVersion = max(versions) + 1

    logger.debug("New version is: %s" % newVersion)
    return newVersion
