# WMCore dependecies here
from WMCore.REST.Error import InvalidParameter
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_strlist, validate_num

# CRABServer dependecies here
from CRABInterface.DataCampaign import DataCampaign
from CRABInterface.RESTExtensions import authz_owner_match, authz_login_valid
from CRABInterface.Regexps import RX_CAMPAIGN, RX_WORKFLOW, RX_SUBRESTAT

# external dependecies here
import cherrypy


class RESTCampaign(RESTEntity):
    """REST entity for campaigns allows to handle a set of workflows all together"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.campaignmgr = DataCampaign(config)

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()

        if method in ['PUT']:
            validate_str("campaign", param, safe, RX_CAMPAIGN, optional=False)
            validate_strlist("workflow", param, safe, RX_WORKFLOW)

        elif method in ['POST']:
            validate_str("campaign", param, safe, RX_CAMPAIGN, optional=False)

        elif method in ['GET']:
            validate_str("campaign", param, safe, RX_CAMPAIGN, optional=True)
            validate_num('age', param, safe, optional=True)
            validate_str('subresource', param, safe, RX_SUBRESTAT, optional=True)
            validate_num('limit', param, safe, optional=True)
            if not safe.kwargs['campaign'] and not safe.kwargs['age']:
                raise InvalidParameter("Invalid input parameters")
            if not safe.kwargs['campaign'] and safe.kwargs['subresource']:
                raise InvalidParameter("Invalid input parameters")

        elif method in ['DELETE']:
            validate_str("campaign", param, safe, RX_CAMPAIGN, optional=False)
            validate_num("force", param, safe, optional=True)

    @restcall
    def put(self, campaign, workflow):
        """Insert a new campaign  and eventually adds a set of workflows to the campaign.
           The caller needs to be a CMS user with a valid CMS x509 cert/proxy.

           :arg str campaign: campaign name requested by the user;
           :arg str list workflow: workflow names requested by the user;
           :return: the campaign name with the relative workflow names."""

        raise NotImplementedError
        result = []
        uniquecampaign = self.campaignmgr.create(campaign)
        if workflow:
            result = self.campaignmgr.injectWorkflow(campaign, workflow)
        return [{'campaign': uniquecampaign, 'workflows': result}]

    @restcall
    def post(self, campaign):
        """Resubmit an existing campaign. The caller needs to be a CMS user owner of the campaign.

           :arg str campaign: unique name identifier of the campaign;
           :returns: the list of modified field"""

        raise NotImplementedError

    @restcall
    def get(self, campaign, age, subresource, limit):
        """Retrieves the campaigns information, like a status summary, in case the campaign unique name is specified.
           Otherwise returns all campaigns since (now - age) for which the user is the owner.
           The caller needs to be a CMS user owner of the campaign.

           :arg str campaign: list of unique name identifiers of campaigns;
           :arg int age: max campaigns age in days;
           :arg str subresource: the campaign sub-resource to retrieve
           :arg int limit: limit of sub-resource elements to retrieve
           :retrun: the list of campaigns with the relative status summary or (the list of) sub-resource(s)"""

        raise NotImplementedError
        result = []
        if campaign:
            if not subresource:
                result = self.campaignmgr.campaignSummary(campaign)
            elif subresource == 'logs':
                result = self.campaignmgr.logs(campaign, limit)
            elif subresource == 'data':
                result = self.campaignmgr.output(campaign, limit)
            else:
                raise NotImplementedError
        elif age:
            # retrieve the information about latest campaigns for that user
            # age can have a default: 1 week ?
            result = self.campaignmgr.retrieveRecent(cherrypy.request.user['login'], age)

        return result

    @restcall
    def delete(self, campaign, force):
        """Aborts a campaign. The user needs to be a CMS owner of the campaign.

           :arg str list campaign: list of unique name identifiers of campaigns;
           :arg int force: force to delete the campaigns in any case; 0 no, everything else yes;
           :return: nothing?"""

        raise NotImplementedError
