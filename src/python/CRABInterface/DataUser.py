import logging

from WMCore.RequestManager.RequestDB.Interface.User import Registration
from WMCore.RequestManager.RequestDB.Interface.Group import Information
from WMCore.RequestManager.RequestDB.Interface.Admin import GroupManagement, ProdManagement
import WMCore.RequestManager.RequestDB.Connection as DBConnect
from WMCore.REST.Error import InvalidParameter


class DataUser(object):
    """
    The class is intended as a container of utility functions. The CRAB REST interface uses it for handling
    users in the request manager.
    """
    def addNewUser(self, userDN, userHN, email='nomail@cern.ch', group="Analysis", team="Analysis"):
        """
        The only function of the module currently available. It receives the userDN
        and the user email and insert the user if not already present in the database.

        The client must pass the user DN.
        The server get the username from sitedb (using userDN)
        The user DN must be propagated to WMBS wmbs_user.name
        Email is not actually required fo the Analysis use case but it cannot be empty in the db
        """
        logger = logging.getLogger("CRABLogger.DataUser")

        if not userDN:
            raise InvalidParameter("Bad input userDN not defined")
        if not userHN:
            raise InvalidParameter("Bad input userHN not defined")
        if not email:
            raise InvalidParameter("Bad input user email not defined")
        if not team:
            raise InvalidParameter("Bad input Team not defined")
        if not group:
            raise InvalidParameter("Bad input Group not defined")

        if not Registration.isRegistered(userHN):
            logger.info("Registering user %s with email %s and userDN %s)" % (userHN, email, userDN))
            Registration.registerUser(userHN, email, userDN)
        if group and not Information.groupExists(group):
            logger.info("Registering group %s" % group)
            GroupManagement.addGroup(group)
        logger.debug("Registering group %s" % group)
        factory = DBConnect.getConnection()
        idDAO = factory(classname = "Requestor.ID")
        userId = idDAO.execute(userHN)
        assocDAO = factory(classname = "Requestor.GetAssociation")
        assoc = assocDAO.execute(userId)
        if not assoc:
            logger.debug("Adding user %s to group %s" % (userHN, group))
            GroupManagement.addUserToGroup(userHN, group)

        if not ProdManagement.getTeamID(team):
            logger.info("Registering team %s" % team)
            ProdManagement.addTeam(team)
