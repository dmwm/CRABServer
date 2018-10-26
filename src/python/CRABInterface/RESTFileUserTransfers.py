""" Add me
"""
from __future__ import print_function
# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_num, validate_strlist
from WMCore.REST.Error import InvalidParameter, UnsupportedMethod

from CRABInterface.Utils import getDBinstance
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_USERNAME, RX_TASKNAME, RX_SUBGETUSERTRANSFER, RX_SUBPOSTUSERTRANSFER, RX_JOBID, RX_ANYTHING

from ServerUtilities import TRANSFERDB_STATUSES, PUBLICATIONDB_STATUSES
# external dependecies here
import time
import logging


class RESTFileUserTransfers(RESTEntity):
    """REST entity to handle interactions between CAFTaskWorker and TaskManager database"""

    #Disabling unused argument since those are all required by the framework
    def __init__(self, app, api, config, mount): #pylint: disable=unused-argument
        RESTEntity.__init__(self, app, api, config, mount)
        self.transferDB = getDBinstance(config, 'FileTransfersDB', 'FileTransfers')
        self.logger = logging.getLogger("CRABLogger.FileTransfers")

    #Disabling again unused argument since those are all required by the framework
    def validate(self, apiobj, method, api, param, safe): #pylint: disable=unused-argument
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        # TODO, use authz_user_action() which will allow only user to maintain and modify its own
        # documents
        authz_login_valid()
        if method in ['PUT']:
            # Do we want to validate everything?
            # Now what is put in CouchDB is not validated
            # And all put are prepared by us in JOB wrappers, so it should already be correct.
            # P.S. Validation is done in function and it double check if all required keys are available
            print (param, safe)
            validate_str("id", param, safe, RX_ANYTHING, optional=False)
            validate_str("username", param, safe, RX_ANYTHING, optional=False)
            validate_str("taskname", param, safe, RX_ANYTHING, optional=False)
            validate_str("destination", param, safe, RX_ANYTHING, optional=False)
            validate_str("destination_lfn", param, safe, RX_ANYTHING, optional=False)
            validate_str("source", param, safe, RX_ANYTHING, optional=False)
            validate_str("source_lfn", param, safe, RX_ANYTHING, optional=False)
            validate_num("filesize", param, safe, optional=False)
            validate_num("publish", param, safe, optional=False)
            validate_str("job_id", param, safe, RX_JOBID, optional=False)
            validate_num("job_retry_count", param, safe, optional=False)
            validate_str("type", param, safe, RX_ANYTHING, optional=False)
            validate_str("asoworker", param, safe, RX_ANYTHING, optional=True)
            validate_num("transfer_retry_count", param, safe, optional=True)
            validate_num("transfer_max_retry_count", param, safe, optional=True)
            validate_num("publication_retry_count", param, safe, optional=True)
            validate_num("publication_max_retry_count", param, safe, optional=True)
            validate_num("start_time", param, safe, optional=False)
            validate_str("rest_host", param, safe, RX_ANYTHING, optional=False)
            validate_str("rest_uri", param, safe, RX_ANYTHING, optional=False)
            validate_str("transfer_state", param, safe, RX_ANYTHING, optional=False)
            validate_str("publication_state", param, safe, RX_ANYTHING, optional=False)
            validate_str("fts_id", param, safe, RX_ANYTHING, optional=True)
            validate_str("fts_instance", param, safe, RX_ANYTHING, optional=True)
        if method in ['POST']:
            # POST is for update, so we should allow anyone anything?
            # Of Course no, but there are multiple combinations, so we are not validating here
            # and all validation is in post function
            validate_str("subresource", param, safe, RX_SUBPOSTUSERTRANSFER, optional=False)
            validate_str("id", param, safe, RX_ANYTHING, optional=True)
            validate_str("username", param, safe, RX_USERNAME, optional=True)
            validate_str("taskname", param, safe, RX_TASKNAME, optional=True)
            validate_num("start_time", param, safe, optional=True)
            validate_str("source", param, safe, RX_ANYTHING, optional=True)
            validate_str("source_lfn", param, safe, RX_ANYTHING, optional=True)
            validate_num("filesize", param, safe, optional=True)
            validate_str("transfer_state", param, safe, RX_ANYTHING, optional=True)
            validate_num("transfer_retry_count", param, safe, optional=True)
            validate_num("publish", param, safe, optional=False)
            validate_str("publication_state", param, safe, RX_ANYTHING, optional=True)
            validate_str("job_id", param, safe, RX_JOBID, optional=True)
            validate_num("job_retry_count", param, safe, optional=True)
            validate_strlist("listOfIds", param, safe, RX_ANYTHING)  # Interesting... TODO. Have optional in strlist
        elif method in ['GET']:
            validate_str("subresource", param, safe, RX_SUBGETUSERTRANSFER, optional=False)
            validate_str("id", param, safe, RX_TASKNAME, optional=True)
            validate_str("username", param, safe, RX_USERNAME, optional=True)
            validate_str("taskname", param, safe, RX_TASKNAME, optional=True)
        elif method in ['DELETE']:
            raise UnsupportedMethod('This method is not supported in this API!')

    @restcall
    def put(self, **kwargs):
        """ Insert a new file transfer in database. It raises an error if you try to upload
            document with same id."""
        ###############################################
        # PUT call
        # ---------------------------------------------
        # Description:
        # CRAB3 job wrapper or PostJob calls this API and provides all required variables
        # for new file transfers.
        # ---------------------------------------------
        # Always required variables:
        # id: unique id
        # user: username
        # taskname: taskname to which this file belongs
        # destination: destination site to which to transfer file
        # destination_lfn: destination logical file name location
        # source: source site from which to transfer file
        # source_lfn: source logical file name location
        # size: file size in bytes
        # publish: Publish flag: 0 - False, 1 - True
        # transfer_state: This file transfer status. If not transferred directly it has to be NEW, otherwise DONE
        # publication_state: This file publication status. It will be always NEW, and will not be published if publish flag is False
        # job_id: Job ID
        # job_retry_count: Job run retry count
        # type: Job output type
        # rest_host: rest host in which look for filemetadata
        # rest_uri: rest uri in which look for filemetadata
        ###############################################
        # Also we need to ensure that specific variables are defined which are needed to store
        binds = {}
        for key in ['id', 'username', 'taskname', 'destination', 'destination_lfn',
                    'source', 'source_lfn', 'filesize', 'publish', 'start_time',
                    'job_id', 'job_retry_count', 'type', 'rest_host', 'rest_uri']:
            binds[key] = [kwargs[key]]
            del kwargs[key]
        # Make a change to a number for TRANSFER_STATE and PUBLICATION_STATE
        binds['publication_state'] = [PUBLICATIONDB_STATUSES[kwargs['publication_state']]]
        binds['transfer_state'] = [TRANSFERDB_STATUSES[kwargs['transfer_state']]]
        # Optional Keys. If they are not set by document submitter, it will use default 2
        for key in ['transfer_max_retry_count', 'publication_max_retry_count']:
            if key in kwargs and kwargs[key]:
                binds[key] = [kwargs[key]]
            else:
                binds[key] = [2]
        # TODO for future, we could also allow custom FTS instance, which is one of the things Andrew asked.
        # I foresee to be it nice feature.
        # Also this could be moved into rest configuration as it would be controlled by crab and not ASO.
        # ASO would do only its jobs by submitting and making some clever decisions on transfers
        # Also we add last update key to current timestamp.
        binds['last_update'] = [int(time.time())]
        self.api.modify(self.transferDB.AddNewFileTransfer_sql, **binds)
        return []


    @restcall
    def post(self, subresource, id, username, taskname, start_time, source, source_lfn, filesize,
             transfer_state, transfer_retry_count, publish, publication_state, job_id, job_retry_count, listOfIds):
        """This is used for user to allow kill transfers for specific task, retryPublication or retryTransfers.
            So far we do not allow retryPublications or retryTransfers for themselfs."""
        binds = {}
        binds['last_update'] = [int(time.time())]
        if subresource == 'updateDoc':
            binds['id'] = [id]
            binds['taskname'] = [taskname]
            binds['username'] = [username]
            binds['start_time'] = [start_time]
            binds['source'] = [source]
            binds['source_lfn'] = [source_lfn]
            binds['filesize'] = [filesize]
            binds['transfer_state'] = [TRANSFERDB_STATUSES[transfer_state]]
            binds['publish'] = [publish]
            binds['publication_state'] = [PUBLICATIONDB_STATUSES[publication_state]]
            binds['transfer_retry_count'] = [transfer_retry_count]
            binds['job_id'] = [job_id]
            binds['job_retry_count'] = [job_retry_count]
            return self.api.modify(self.transferDB.UpdateUserTransfersById_sql, **binds)
        elif subresource == 'killTransfers':
            ###############################################
            # killTransfers API
            # ---------------------------------------------
            # Description:
            # Users can call this API and kill all transfers which are in NEW state.
            # NEW state indicates that transfers there so far not acquired by ASO.
            # ---------------------------------------------
            # Always required variables:
            # user: username
            # taskname: taskname to which this file belongs
            ###############################################
            binds['username'] = [username]
            binds['taskname'] = [taskname]
            binds['transfer_state'] = [TRANSFERDB_STATUSES['NEW']]
            binds['new_transfer_state'] = [TRANSFERDB_STATUSES['KILL']]
            return self.api.modifynocheck(self.transferDB.KillUserTransfers_sql, **binds)
        elif subresource == 'killTransfersById':
            # This one is a bit problematic! If PostJob timeouts after 24 and transfer is in
            # any state. It will send to kill these transfers. Now if state is ACQUIRED
            # and transfer is submitted to FTS, it should kill specific file transfer in FTS
            # if it is available in it and also remove file from source and destination.
            # TODO. For future whenever ASO is at good level, we can work on killing all any other
            # Which are ACQUIRED, SUBMITTED
            killStatus = {'killed': [], 'failedKill': []}
            binds['transfer_state'] = [TRANSFERDB_STATUSES['NEW']]
            binds['new_transfer_state'] = [TRANSFERDB_STATUSES['KILL']]
            binds['username'] = [username]
            for item in listOfIds:
                binds['id'] = [item]
                try:
                    self.api.modify(self.transferDB.KillUserTransfersById_sql, **binds)
                except:
                    killStatus['failedKill'].append(item)
                    continue
                killStatus['killed'].append(item)
            return [killStatus]
        elif subresource == 'retryTransfers':
            raise NotImplementedError
            ###############################################
            # retryTransfers API
            # ---------------------------------------------
            # Description:
            # Users can call this API and retry transfers which are in FAILED state.
            # NEW state indicates that transfers there so far not acquired by ASO.
            # P.S So far this is not turned ON!!!! For the future to allow users to retry Transfers
            # ---------------------------------------------
            # Always required variables:
            # user: username
            # taskname: taskname to which this file belongs
            ###############################################
            # binds['transfer_state'] = TRANSFERDB_STATUSES['FAILED']
            # binds['new_transfer_state'] = TRANSFERDB_STATUSES['NEW']
            # self.api.modify(self.transferDB.RetryUserTransfers_sql, **binds)
            # For the future to allow users to retry Transfers

    @restcall
    def get(self, subresource, id, username, taskname):
        """ Retrieve all columns for a specified task or
            """
        binds = {}
        if subresource == 'getById':
            ###############################################
            # getById API
            # ---------------------------------------------
            # Description:
            # Users & Also PostJob can query this API to get info about specific document
            # in database.
            # ---------------------------------------------
            # Always required variables:
            # id: id
            ###############################################
            binds['id'] = id
            return self.api.query(None, None, self.transferDB.GetById_sql, **binds)
        if not username:
            raise InvalidParameter('Username is not defined')
        if not taskname:
            raise InvalidParameter('TaskName is not defined')
        binds['username'] = username
        binds['taskname'] = taskname
        print (binds)
        if subresource == 'getTransferStatus':
            ###############################################
            # getTransferStatus API
            # ---------------------------------------------
            # Description:
            # Get Transfer status for specific task which belongs to user
            # ---------------------------------------------
            # Always required variables:
            # username: username
            # taskname: taskname
            ###############################################
            return self.api.query(None, None, self.transferDB.GetTaskStatusForTransfers_sql, **binds)
        elif subresource == 'getPublicationStatus':
            ###############################################
            # getPublicationStatus API
            # ---------------------------------------------
            # Description:
            # Get Transfer status for specific task which belongs to user
            # ---------------------------------------------
            # Always required variables:
            # username: username
            # taskname: taskname
            ###############################################
            return self.api.query(None, None, self.transferDB.GetTaskStatusForPublication_sql, **binds)
        return {}

    @restcall
    def delete(self):
        """ Delete a doc from the DB """
        raise NotImplementedError
