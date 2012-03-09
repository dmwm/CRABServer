"""
This module aims to contain the method specific to the REST interface.
These are extensions which are not directly contained in WMCore.REST module
and it shouldn't have any other dependencies a part of that and cherrypy.

Currently authz_owner_match uses a WMCore.Database.CMSCouch method
but in next versions it should be dropped, as from the CRABInterface.
"""

import cherrypy

def authz_owner_match(database, workflows, retrieve_docs=True):
    """Match user against authorisation requirements to modify an existing resource.
       Allows to cache couchdb fetched documents if the caller needs them.

       :arg WMCore.CMSCouch.Database database: database connection to retrieve the docs
       :arg str list workflows: a list of workflows unique name as positive check
       :return: in case retrieve_docs is not false the list of couchdb documents."""

    user = cherrypy.request.user
    log = cherrypy.log

    # this means document are cached in memory
    alldocs = []

    for wf in workflows:
        wfdoc = None
        try:
            wfdoc = database.document( id = wf )
        except:
            log("ERROR: could not retrieve the requested document %s" % wf)
            raise cherrypy.HTTPError(404, "The resource requested does not exist")
        if wfdoc and 'Requestor' in wfdoc:
            if wfdoc['Requestor'] == cherrypy.request.user['login']:
                alldocs.append(wfdoc)
                continue
        log("ERROR: authz denied for user %s to resources %s" % (user, str(workflows)))
        raise cherrypy.HTTPError(403, "You are not allowed to access this resource.")

    if len(workflows) == len(alldocs):
        log("DEBUG: authz user %s to access resource %s")
        if retrieve_docs:
            return alldocs
        return

    log("ERROR: authz denied for user %s to resources %s" % (user, str(workflows)))
    raise cherrypy.HTTPError(403, "You are not allowed to access this resource.")
