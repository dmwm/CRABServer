"""
This module aims to contain the method specific to the REST interface.
These are extensions which are not directly contained in WMCore.REST module
and it shouldn't have any other dependencies a part of that and cherrypy.

Currently authz_owner_match uses a WMCore.Database.CMSCouch method
but in next versions it should be dropped, as from the CRABInterface.
"""
from WMCore.REST.Error import MissingObject

import cherrypy
import traceback


def authz_operator(username = None):
    """ Check if the the user who is trying to access this resource (i.e.: cherrypy.request.user['login'], the cert username) is the
        same as username. If not check if the user is a CRAB3 operator. {... 'operator': {'group': set(['crab3']) ... in the cherrypy roles}
        If the username is not passed just check role
    """
    if cherrypy.request.user['login'] != username and\
       'crab3' not in cherrypy.request.user.get('roles', {}).get('operator', {}).get('group', set()):
        raise cherrypy.HTTPError(403, "You are not allowed to access this resource. You need to be a CRAB3 operator in sitedb to perform this action")


def authz_owner_match(dbapi, workflows, Task):
    """Match user against authorisation requirements to modify an existing resource.
       Allows to cache couchdb fetched documents if the caller needs them.

       :arg WMCore.CMSCouch.Database database: database connection to retrieve the docs
       :arg str list workflows: a list of workflows unique name as positive check
       :return: in case retrieve_docs is not false the list of couchdb documents."""
    user = cherrypy.request.user
    log = cherrypy.log

    alldocs = []

    for wf in workflows:
        wfrow = None
        try:
            wfrow = dbapi.query(None, None, Task.GetUserFromID_sql, taskname = wf).next()
        except Exception as ex:
            excauthz = RuntimeError("The document '%s' is not retrievable '%s'" % (wf, str(ex)))
            raise MissingObject("The resource requested does not exist", trace=traceback.format_exc(), errobj = excauthz)

        if wfrow[0] == cherrypy.request.user['login']:
            alldocs.append(wfrow)
            continue
        log("ERROR: authz denied for user '%s' to the resource '%s. Resource belong to %s'" % (user, wf, wfrow[0]))
        raise cherrypy.HTTPError(403, "You are not allowed to access this resource.")

    if len(workflows) == len(alldocs):
        log("DEBUG: authz user %s to access resources %s" % (user, workflows))
        return

    log("ERROR: authz denied for user '%s' to resource '%s'" % (user, str(workflows)))
    raise cherrypy.HTTPError(403, "You are not allowed to access this resource.")


def authz_login_valid():
    if not cherrypy.request.user['login']:
        raise cherrypy.HTTPError(403, "You are not allowed to access this resource. Please check: https://twiki.cern.ch/twiki/bin/viewauth/CMS/SiteDBForCRAB")
