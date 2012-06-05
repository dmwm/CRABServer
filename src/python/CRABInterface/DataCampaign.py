import logging

# WMCore dependecies here
from WMCore.Database.CMSCouch import CouchServer
from WMCore.REST.Error import MissingObject

# CRABServer dependencies here
from CRABInterface.DataUserWorkflow import DataUserWorkflow
from CRABInterface.Utils import CouchDBConn, conn_handler

class DataCampaign(object):
    """Entity that allows to operate on campaign resources.
       DataCampaign is a container on a set of workflows which have not strict relation
       a part of being part of the same campaign container.
       An operations on a campaign implies to act in all the workflows
       it contains.

       CAMPAIGN-UNIQUE-NAME
         |
         |_workflow-1.0__workflow-1.1__....
         |
         |_workflow-2.0__workflow-2.1__workflow-2.2__....
         |
         |_workflow-3.0__....

       This class does not provide any aggregation of the workflows in the campaign,
       and it returns a list of workflows. Aggregation of workflows which have a hierarchical
       relationship (e.g. resubmission) is left to the DataUserWorkflow class.
       This means that it shows information of the first column of the schema above,
       where the first column aggregates the row information (thanks to DataUserWorkflow)."""

    @staticmethod
    def globalinit(monurl, monname):
        DataCampaign.monitordb = CouchDBConn(db=CouchServer(monurl), name=monname, conn=None)

    def __init__(self, config):
        self.logger = logging.getLogger('CRABLogger.DataCampaign')
        self.userworkflow = DataUserWorkflow()

    def create(self, campaign):
        """Create a campaign object with an unique name.
           This can be a simple call to ReqMgr python API.

           :arg str campaign: the campaign name received from the client
           :return: unique campaign name"""

        raise NotImplementedError
        return {}

    def injectWorkflow(self, campaign, wfs):
        """Insert a workflow into the ReqMgr. It uses the `DataUserWorkflow` object.
           To create the campaign and to get the unique campaign name refer to
           `create` method.

           :arg str campaign: the unique campaign name
           :arg str list wfs: the list of workflows to create
           :return: the workflows unique names."""

        for workflow in wfs:
            yield self.userworkflow.inject(workflow)

    def resubmit(self, campaign, workflows=None):
        """Resubmit all the unfinished workflows in the campaign.

           :arg str campaign: the unique campaign name
           :arg list str workflows: the list of workflows part of the campaign
           :return: the workflows unique names."""

        workflows = self.getCampaignWorkflows(campaign)
        if not workflows:
            raise MissingObject("Cannot find workflows in campaign")

        for workflow in workflows:
            yield self.userworkflow.resubmit(workflow)

    def kill(self, campaign, force, workflows=None):
        """Cancel all the unfinished workflows in the campaign.

           :arg str campaign: the unique campaign name
           :arg int force: in case the workflow has to be forced when deleted
           :arg list str workflows: the list of workflows part of the campaign
           :return: potentially nothing"""

        workflows = self.getCampaignWorkflows(campaign)
        if not workflows:
            raise MissingObject("Cannot find workflows in campaign")

        for workflow in workflows:
            yield self.userworkflow.kill(workflow)

    @conn_handler(services=['monitor'])
    def getCampaignWorkflows(self, campaign):
        """Retrieve the list of workflow names part of the campaign.
           This considers only top level workflows and not their resubmissions.

           :arg str campaign: the unique campaign name
           :return: list of workflows"""

        ## TODO: this needs to be changed, avoiding to pick resubmitted workflows.
        raise NotImplementedError

    def campaignSummary(self, campaign, workflows=None):
        """Provide the campaign status summary. This method iterates on all the workflows
           part of the campaign starting from the oldest one, and counts the number of
           jobs in each state. Then it returns a list where the first element is the
           campaign name, the second element is the campaign status, the third is a dict
           where keys are the 'main' states and values are number of jobs, the fourth is
           the breakdown of the 'main' states, and finally a dict containing the same
           information grouped by site

           :arg str campaign: the unique campaign name
           :arg list str workflows: the list of workflows part of the campaign
           :return: the campaign status summary"""

        workflows = self.getCampaignWorkflows(campaign)
        if not workflows:
            raise MissingObject("Cannot find workflows in campaign")

        for workflow in workflows:
            yield self.userworkflow.status(workflow)

    def retrieveRecent(self, user, since):
        """Query the monitoring db to retrieve the latest user campaigns.
           It can include the campaign status.

           :arg str user: the user hn login name (or the DN?)
           :arg int since: timestamp in second to not retrieve campaigns older then this
           :return: the list of campaign-staus objects."""

        raise NotImplementedError
        return []

    def output(self, campaign, limit):
        """Returns the campaign output PFN. It takes care of the LFN - PFN conversion too.

           :arg str campaign: a campaign name
           :arg int limit: the limit on the number of PFN to return
           :return: an object with the list of output pfns"""
        workflows = self.getCampaignWorkflows(campaign)
        if not workflows:
            raise MissingObject("Cannot find workflows in campaign")

        for workflow in workflows:
            yield self.userworkflow.output(workflow)

    def logs(self, campaign, limit):
        """Returns the campaign log archive PFN. It takes care of the LFN - PFN conversion too.

           :arg str campaign: a campaign name
           :arg int limit: the limit on the number of PFN to return
           :return: an object with the list of log pfns"""
        workflows = self.getCampaignWorkflows(campaign)
        if not workflows:
            raise MissingObject("Cannot find workflows in campaign")

        yield self.userworkflow.log(workflows, limit)
