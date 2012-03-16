import logging

# WMCore dependecies here
from WMCore.Database.CMSCouch import CouchServer, CouchError, Database, CouchNotFoundError
from WMCore.REST.Error import ExecutionError, MissingObject

# CRABServer dependencies here
from CRABInterface.DataWorkflow import DataWorkflow


class DataCampaign(object):
    """Entity that allows to operate on campaign resources.
       DataCampaign is a container on a set of workflows.
       An operations on a campaign implies to act in all the workflows
       it contains"""

    @staticmethod
    def globalinit(monurl, monname):
        DataCampaign.couchdb = CouchServer(monurl)
        DataCampaign.database = DataCampaign.couchdb.connectDatabase(monname)

    def __init__(self, config):
        self.logger = logging.getLogger('CRABLogger.DataCampaign')
        self.workflow = DataWorkflow()

    def create(self, campaign):
        """Create a campaign object with an unique name.
           This can be a simple call to ReqMgr python API.

           :arg str campaign: the campaign name received from the client
           :return: unique campaign name"""

        raise NotImplementedError
        return {}

    def injectWorkflow(self, campaign, wfs):
        """Insert a workflow into the ReqMgr. It uses the `DataWorkflow` object.
           To create the campaign and to get the unique campaign name refer to
           `create` method.

           :arg str campaign: the unique campaign name
           :arg str list wfs: the list of workflows to create
           :return: the workflows unique names."""

        for worfkflow in wfs:
            yield self.workflow.inject(workflow)

    def resubmit(self, campaign, workflows=None):
        """Resubmit all the unfinished workflows in the campaign.

           :arg str campaign: the unique campaign name
           :arg list str workflows: the list of workflows part of the campaign
           :return: the workflows unique names."""

        if not workflows:
            workflows = self.getCampaignWorkflows(campaign)
        for workflow in workflows:
            yield self.workflow.resubmit(workflow)

    def kill(self, campaign, force, workflows=None):
        """Cancel all the unfinished workflows in the campaign.

           :arg str campaign: the unique campaign name
           :arg int force: in case the workflow has to be forced when deleted
           :arg list str workflows: the list of workflows part of the campaign
           :return: potentially nothing"""

        if not workflows:
            workflows = self.getCampaignWorkflows(campaign)
        for workflow in workflows:
            yield self.workflow.kill(workflow)

    def _aggregateCampaign(self, wfDocs):
        """A resubmission keeps succeded jobs, and, after a kill,
           sends a new workflow on the not/analyzed part of the
           dataset (parts where jobs are running/scheduled/failed/etc).
           In the aggregation we iterates on 'old' workflows and sum
           the number of succeded jobs. The last resubmission is kept
           as it is.

           :arg dict list: a list with the workflow document summary
           :return: two dicts, one with the high-level aggregation per
                    state and another one with the details for each
                    state."""

        jobsPerState = {}
        detailsPerState = {}
        if not wfDocs:
            return jobsPerState, detailsPerState

        #jobs in success state are taken from all the workflows
        jobsPerState['success'] = sum( [ doc['success'] for doc in wfDocs if doc.get('success', None) ] )

        lastDoc = wfDocs[-1]
        for state in lastDoc:
            if state != 'inWMBS':
                if type(lastDoc[state])==int:
                    jobsPerState[state] = lastDoc[state]
                else:
                    jobsPerState[state] = sum( lastDoc[state].values() )
                    detailsPerState[state] = lastDoc[state]
                    #count back retry and first from the total
                    if state == 'submitted' and 'submitted' in lastDoc:
                        jobsPerState['submitted'] -= lastDoc['submitted'].get('first', 0)
                        jobsPerState['submitted'] -= lastDoc['submitted'].get('retry', 0)

        return jobsPerState, detailsPerState

    def getCampaignWorkflows(self, campaign):
        """Retrieve the list of workflow names part of the campaign

           :arg str campaign: the unique campaign name
           :return: list of workflows"""
        #Get all the 'reqmgr_request' documents related to the campaign (one for each wf)
        options = {"startkey": [campaign,0], "endkey": [campaign,{}]}
        try:
            campaignDocs = self.database.loadView("WMStats", "campaign-request", options)
        except CouchNotFoundError, ce:
            raise ExecutionError("Impossible to load campaign-request view from WMStats")
        if not campaignDocs['rows']:
            raise MissingObject("Cannot find requested campaign")

        return [doc['value']['id'] for doc in campaignDocs['rows']]

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

        if not workflows:
            workflows = self.getCampaignWorkflows(campaign)
        wfDocs = [self.workflow.getWorkflow(wf) for wf in workflows]
        #checking that we have some workflow documents
        if not wfDocs:
            raise MissingObject("Cannot find any workflow in the campaign")

        jobsPerState, detailsPerState = self._aggregateCampaign([doc['status'] for doc in wfDocs if doc.get('status', None)])
        #From the first 'agent' document retrieve the total number of jobs
        if 'status' in wfDocs[0] and 'inWMBS' in wfDocs[0]['status']:
            jobsPerState['total'] = wfDocs[0]['status']['inWMBS']
        else:
            jobsPerState['total'] = 0

        detailsPerSite = {}
        #iterate on all the workflows and from them get a list with all the sites
        siteList = reduce(lambda x,y: x+y, [doc['sites'].keys() for doc in wfDocs if doc.get('sites', None)], [])
        #for each site get the aggregation
        for site in siteList:
            detailsPerSite[site] = self._aggregateCampaign([doc['sites'][site] for doc in wfDocs if doc.get('sites', None) and doc['sites'].get(site, None)])

        #get the status of the campaign: sort by update_time the request_stauts list of the most recent workflow
        campaignStatus = sorted([x for x in wfDocs[-1]["request_status"]], key=lambda y: y['update_time'] )[-1]['status']

        return [wfDocs[-1]["campaign"], campaignStatus, jobsPerState, detailsPerState, detailsPerSite]

    def retrieveRecent(self, user, since):
        """Query the monitoring db to retrieve the latest user campaigns.
           It can include the campaign status.

           :arg str user: the user hn login name (or the DN?)
           :arg int since: timestamp in second to not retrieve campaigns older then this
           :return: the list of campaign-staus objects."""

        raise NotImplementedError
        return []
