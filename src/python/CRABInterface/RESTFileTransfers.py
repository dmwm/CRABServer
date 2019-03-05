from __future__ import print_function
# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_num, validate_strlist
from WMCore.REST.Error import InvalidParameter, UnsupportedMethod

from CRABInterface.Utils import getDBinstance
from CRABInterface.RESTExtensions import authz_login_valid, authz_operator_without_raise
from CRABInterface.Regexps import RX_USERNAME, RX_VOPARAMS, RX_TASKNAME, RX_SUBGETTRANSFER, RX_SUBPOSTTRANSFER, \
                                  RX_USERGROUP, RX_USERROLE, RX_CMSSITE, RX_ASO_WORKERNAME, RX_ANYTHING

from ServerUtilities import TRANSFERDB_STATUSES, PUBLICATIONDB_STATUSES
# external dependecies here
import time, logging
import cherrypy


class RESTFileTransfers(RESTEntity):
    """REST entity to handle interactions between CAFTaskWorker and TaskManager database"""

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.transferDB = getDBinstance(config, 'FileTransfersDB', 'FileTransfers')
        self.logger = logging.getLogger("CRABLogger.FileTransfers")

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if not (authz_operator_without_raise('aso', 'operator') or
                authz_operator_without_raise('crab3', 'operator')):
            # TODO: do not forget to uncomment this
            # so far just print a warning
            print ('WARNING. NOT AUTHORIZED')
            #raise ForbiddenAccess('Access is restricted to this API')
        if method in ['PUT']:
            raise UnsupportedMethod('This method is not supported in this API!')
        elif method in ['POST']:
            # POST is for update, so we should allow anyone anything?
            # Of Course no, but there are multiple combinations, so we are not validating here
            # and all validation is in post function
            validate_str("subresource", param, safe, RX_SUBPOSTTRANSFER, optional=False)
            validate_str("asoworker", param, safe, RX_ASO_WORKERNAME, optional=False)
            validate_str("username", param, safe, RX_USERNAME, optional=True)
            validate_str("list_of_ids", param, safe, RX_ANYTHING, optional=True)
            validate_str("list_of_transfer_state", param, safe, RX_ANYTHING, optional=True)
            validate_str("list_of_failure_reason", param, safe, RX_ANYTHING, optional=True)
            validate_str("list_of_retry_value", param, safe, RX_ANYTHING, optional=True)
            validate_str("list_of_fts_instance", param, safe, RX_ANYTHING, optional=True)
            validate_str("list_of_fts_id", param, safe, RX_ANYTHING, optional=True)
            validate_str("list_of_publication_state", param, safe, RX_ANYTHING, optional=True)
            validate_num("time_to", param, safe, optional=True)
            validate_num("publish_flag", param, safe, optional=True)

        elif method in ['GET']:
            validate_str("subresource", param, safe, RX_SUBGETTRANSFER, optional=False)
            validate_str("username", param, safe, RX_USERNAME, optional=True)
            validate_str("vogroup", param, safe, RX_VOPARAMS, optional=True)
            validate_str("vorole", param, safe, RX_VOPARAMS, optional=True)
            validate_str("taskname", param, safe, RX_TASKNAME, optional=True)
            validate_str("destination", param, safe, RX_CMSSITE, optional=True)
            validate_str("source", param, safe, RX_CMSSITE, optional=True)
            validate_str("asoworker", param, safe, RX_ASO_WORKERNAME, optional=True)
            validate_num("grouping", param, safe, optional=False)
            validate_num("limit", param, safe, optional=True)
        elif method in ['DELETE']:
            # This one I don`t really like to have implemented
            # There is some security concerns
            # But I still have one idea what we should clean up and where.
            # So leave it for the future.
            pass

    @restcall
    def post(self, **kwargs):
        """This is used by ASO/CRAB3 Operator to acquire Transfers/Publication or update/retry/kill them.
           Find a description next to each call"""
        subresource = kwargs['subresource']
        binds = {}
        timeNow = int(time.time())
        binds['last_update'] = [timeNow]
        #binds['asoworker'] = [kwargs['asoworker']]

        if subresource == 'acquireTransfers':
            ###############################################
            # acquireTransfers API
            # ---------------------------------------------
            # Description:
            # ASO calls this view and CRABServer will mark  all docs, which do not
            # have asoworker assigned from this transfer_state to new_transfer_state
            # ---------------------------------------------
            # Always required variables:
            # (str) asoworker: ASO Worker name for which acquire Transfers.
            ###############################################
            binds['limit'] = [1000]
            binds['transfer_state'] = [TRANSFERDB_STATUSES['NEW']]
            binds['new_transfer_state'] = [TRANSFERDB_STATUSES['ACQUIRED']]
            binds['username'] = [kwargs['username']]
            return self.api.modifynocheck(self.transferDB.AcquireTransfers_sql, **binds)

        elif subresource == 'acquirePublication':
            ###############################################
            # acquirePublication API
            # ---------------------------------------------
            # Description:
            # ASO calls this view and CRABServer will mark all docs, which file transfer
            # status is done, will update asoworker to new asoworker status. also will
            # look for this specific publication state and will change it to new_publication_state
            # :transfer_done AND publish = :publish_new
            # ---------------------------------------------
            # Always required variables:
            # (str) asoworker: ASO Worker name for which acquire Publication.
            ###############################################
            # TODO: Maybe we want to acquire publication per ASO machine
            # by meaning if we have 2 ASO concurrent running, each of them can acquire multiple files
            # from same task. Maybe is good to enforce acquire only for tasks which do not have
            # asoworker defined. This would enforce to group files per and lower a bit load on dbs.
            binds['publish_flag'] = [1]
            binds['transfer_state'] = [TRANSFERDB_STATUSES['DONE']]
            binds['publication_state'] = [PUBLICATIONDB_STATUSES['NEW']]
            binds['new_publication_state'] = [PUBLICATIONDB_STATUSES['ACQUIRED']]
            return self.api.modifynocheck(self.transferDB.AcquirePublication_sql, **binds)

        elif subresource == 'updateTransfers':
            ###############################################
            # updateTransfers API
            # ---------------------------------------------
            # Description:
            # ASO calls this view and CRABServer will mark all docs, which have asoworker
            # in row and will make all posted modification.
            # If there are passed N (2 or 4) items, it will do corresponding action in update
            # :::::::
            # 2 items, Required keys are:
            # ([str, str]) list_of_ids: list of ids which are in database.
            # ([str, str]) list_of_transfer_state: transfer_state which is one of: ['FAILED', 'DONE', 'RETRY', 'SUBMITTED', 'KILLED']
            # optional: ([str, str]) list_of_failure_reason
            # :::::::
            # 4 items, Required keys are:
            # ([str, str]) list_of_ids: list of ids which are in database.
            # ([str, str]) list_of_transfer_state: transfer_state which is one of: ['FAILED', 'DONE', 'RETRY', 'SUBMITTED', 'KILLED']
            # ([str, str]) list_of_fts_instance: list of fts instance to which transfer was submitted.
            # ([str, str]) list_of_fts_id: list of fts id for this specific document transfer
            # ---------------------------------------------
            # Always required variables:
            # (str) asoworker: ASO Worker name for which these transfers there acquired.
            ###############################################
            binds['last_update'] = [timeNow]
            # TODO: fix case: if 'fts_instance' in kwargs
            if kwargs['list_of_fts_instance']:
                #del errorMsg
                ids = (kwargs['list_of_ids'].translate(None,"[ ]'")).split(",")
                states = (kwargs['list_of_transfer_state'].translate(None,"[ ]'")).split(",")
                retry = [0 for x in states]
                reasons = ["" for x in states]
                instances = (str(kwargs['list_of_fts_instance'].translate(None,"[ ]'"))).split(",")
                fts_id = (str(kwargs['list_of_fts_id'].translate(None,"[ ]'"))).split(",")

                for num in range(len(ids)):
                    binds['id'] = [ids[num]]
                    binds['transfer_state'] = [TRANSFERDB_STATUSES[states[num]]] 
                    binds['fts_instance'] = [str(instances[num])] 
                    binds['fts_id'] = [str(fts_id[num])]
                    binds['fail_reason'] = [reasons[num]]
                    binds['retry_value'] = [int(retry[num])]
                    self.api.modifynocheck(self.transferDB.UpdateTransfers_sql, **binds)
            else:
                ids = (kwargs['list_of_ids'].translate(None,"[ ]'")).split(",")
                states = (kwargs['list_of_transfer_state'].translate(None,"[ ]'")).split(",")
                retry = [0 for x in states]
                reasons = ["" for x in states]
                if kwargs['list_of_retry_value'] is not None:
                    reasons = (kwargs['list_of_failure_reason'].translate(None,"[]'")).split(",")
                    retry = (kwargs['list_of_retry_value'].translate(None,"[ ]'")).split(",")
                for num in range(len(ids)):
                    binds['id'] = [ids[num]]
                    binds['transfer_state'] = [TRANSFERDB_STATUSES[states[num]]]
                    binds['fts_instance'] = [None]
                    binds['fts_id'] = [None]
                    binds['fail_reason'] = [reasons[num]]
                    binds['retry_value'] = [int(retry[num])]
                    self.api.modifynocheck(self.transferDB.UpdateTransfers_sql, **binds)

        elif subresource == 'updatePublication':
            ###############################################
            # updatePublication API
            # ---------------------------------------------
            # Description:
            # ASO calls this view and CRABServer will mark all docs, which have asoworker
            # in row and will make publication status for all list of ids new status
            # list of ids are keys which need to be updated.
            # 2 items are required in list_of_ids. Keys:
            # (str) id: Document id which is in database.
            # (str) publication_state: publication_state which is one of: ['FAILED', 'DONE', 'RETRY']
            # ---------------------------------------------
            # Always required variables:
            # (str) asoworker: ASO Worker name for which acquire Publication.
            ###############################################
            binds['last_update'] = [timeNow]
            #compareOut, errorMsg = self.compareLen(kwargs, ['list_of_ids', 'list_of_publication_state'])
            #if compareOut:
            #    del errorMsg
            ids = (kwargs['list_of_ids'].translate(None,"[ ]'")).split(",")
            states = (kwargs['list_of_publication_state'].translate(None,"[ ]'")).split(",")
            retry = [0 for x in states]
            reasons = ["" for x in states]
            if kwargs['list_of_retry_value'] is not None:
                reasons = (kwargs['list_of_failure_reason'].translate(None,"[]'")).split(",")
                retry = (kwargs['list_of_retry_value'].translate(None,"[ ]'")).split(",")
            for num in range(len(ids)):
                binds['publication_state'] = [PUBLICATIONDB_STATUSES[states[num]]]
                binds['id'] = [ids[num]]
                binds['fail_reason'] = [reasons[num]]
                binds['retry_value'] = [int(retry[num])]
                binds['publish'] = [kwargs["publish_flag"] or -1]
                self.api.modify(self.transferDB.UpdatePublication_sql, **binds)

        elif subresource == 'retryPublication':
            ###############################################
            # retryPublication API
            # ---------------------------------------------
            # Description:
            # ASO calls this view and CRABServer will mark all docs with publication_state NEW, which have asoworker
            # assigned and it did not reached max retries count and last_update is lower
            # than specified time_to.
            # ---------------------------------------------
            # Always required variables:
            # (str) asoworker: ASO Worker name for which acquire Publication.
            # (str) time_to: timespan until which time
            ###############################################
            if 'time_to' not in kwargs:
                raise InvalidParameter()
            binds['time_to'] = kwargs['time_to']
            binds['new_publication_state'] = PUBLICATIONDB_STATUSES['NEW']
            binds['publication_state'] = PUBLICATIONDB_STATUSES['RETRY']
            self.api.modify(self.transferDB.RetryPublication_sql, **binds)

        elif subresource == 'retryTransfers':
            ###############################################
            # retryTransfers API
            # ---------------------------------------------
            # Description:
            # ASO calls this view and CRABServer will mark all docs with transfer_state NEW, which have asoworker
            # assigned and it did not reached max retries count and last_update is lower
            # than specified time_to.
            # ---------------------------------------------
            # Always required variables:
            # (str) asoworker: ASO Worker name for which retry transfers.
            # (str) time_to: timespan until which time
            ###############################################
            if 'time_to' not in kwargs:
                raise InvalidParameter()
            binds['time_to'] = [kwargs['time_to']]
            binds['new_transfer_state'] = [TRANSFERDB_STATUSES['NEW']]
            binds['transfer_state'] = [TRANSFERDB_STATUSES['RETRY']]
            return self.api.modifynocheck(self.transferDB.RetryTransfers_sql, **binds)

        elif subresource == 'killTransfers':
            ###############################################
            # killTransfers API
            # ---------------------------------------------
            # Description:
            # ASO calls this view and CRABServer will mark all docs with transfer_state KILL for specific list_of_ids
            # which have asoworker assigned and their TRANSFER_STATE is NEW
            # than specified time_to.
            # list_of_ids is a list which contains string of ids
            # ---------------------------------------------
            # Always required variables:
            # (str) asoworker: ASO Worker name for which acquire Publication.
            ###############################################
            for oneId in kwargs['list_of_ids']:
                binds['id'] = [oneId]
                binds['transfer_state'] = [TRANSFERDB_STATUSES['NEW']]
                binds['new_transfer_state'] = [TRANSFERDB_STATUSES['KILL']]
                self.api.modify(self.transferDB.KillTransfers_sql, **binds)

    @restcall
    def get(self, subresource, username, vogroup, vorole, taskname, destination, source, asoworker, grouping, limit):
        """ Retrieve all docs from DB for specific parameters.
            """
        binds = {}
        if subresource == 'getVOMSAttributesForTask':
            ###############################################
            # getVOMSAttributesForTask
            # ---------------------------------------------
            # Required variables:
            # (str) taskname: taskname
            # ---------------------------------------------
            # TODO: This is already saved in taskdb table and we are just reusing it in
            # different tables. ASO could query directly taskdb and get VOMS attributes.
            # This would lower filetransfersDB table size.
            if not taskname:
                raise InvalidParameter('TaskName is not defined')
            binds['taskname'] = taskname
            rows = self.api.query(None, None, self.transferDB.GetVOMSAttr_sql, **binds)
            return rows

        if subresource in ['acquiredTransfers', 'acquiredPublication']:
            ###############################################
            # acquiredTransfers||acquiredPublication API
            # ---------------------------------------------
            # Always required variables:
            # (int) grouping: 0, 1, 2, 3, 4 (variable keys represent needed variables for queries. Defaults variables always must be defined)
            # (str) asoworker: ASO Worker name which acquired Transfers/Publication. This is always used in matching query
            # (str) username: UserName
            # (int) limit: If not defined, it will set max 1000. Maximum allowed is 1000
            # (str) state: Transfer state which is by default ACQUIRED. Not allowed to change through API
            # ---------------------------------------------
            if not asoworker:
                raise InvalidParameter("Required asoworker parameter is not set.")
            if not limit:
                limit = 1000
            binds['limit'] = limit
            #binds['asoworker'] = asoworker
            sqlQuery = ""
            if subresource == 'acquiredTransfers':
                if grouping > 3:
                    raise InvalidParameter('This grouping level is not implemented')
                binds['state'] = TRANSFERDB_STATUSES['ACQUIRED']
                if grouping == 0:
                    # ---------------------------------------------
                    # Return: Docs, which match these conditions: asoworker, state = ACQUIRED
                    # ---------------------------------------------
                    sqlQuery = self.transferDB.GetDocsTransfer0_sql
                elif grouping == 1:
                    # ---------------------------------------------
                    # (str) taskname: taskname
                    # Return: Docs, which match these conditions: asoworker, state = ACQUIRED, username
                    # ---------------------------------------------
                    if not username:
                        raise InvalidParameter('Username is not defined')
                    #Oracle treats NULL diffrently in query, you have to specifically do param is NULL., param = NULL does not work.
                    #Replacing with 'None' here and using NVL in the query fixes this issue
                    binds['username'] = username
                    binds['vogroup'] = vogroup if vogroup is not None else 'None'
                    binds['vorole'] = vorole if vorole is not None else 'None'
                    sqlQuery = self.transferDB.GetDocsTransfer1_sql
                elif grouping == 2:
                    # ---------------------------------------------
                    # (str) source: source site
                    # Return: Docs, which match these conditions: asoworker, state = ACQUIRED, username, source
                    # ---------------------------------------------
                    if not source:
                        raise InvalidParameter('Source is not defined')
                    binds['source'] = source
                    sqlQuery = self.transferDB.GetDocsTransfer2_sql
                elif grouping == 3:
                    # ---------------------------------------------
                    # (str) source: source site.
                    # (str) destination: destination site
                    # Return: Docs, which match these conditions: asoworker, state = ACQUIRED, username, source, destination
                    if not source or not destination:
                        raise InvalidParameter('Either source or destination is not defined')
                    binds['source'] = source
                    binds['destination'] = destination
                    sqlQuery = self.transferDB.GetDocsTransfer3_sql
                rows = self.api.query(None, None, sqlQuery, **binds)
                return rows
            elif subresource == 'acquiredPublication':
                if grouping > 1:
                    raise InvalidParameter('This grouping level is not implemented')
                binds['state'] = PUBLICATIONDB_STATUSES['ACQUIRED']
                binds['transfer_state'] = TRANSFERDB_STATUSES['DONE']
                if grouping == 0:
                    # ---------------------------------------------
                    # Return: Docs, which match these conditions: asoworker, state = ACQUIRED, username
                    # ---------------------------------------------
                    sqlQuery = self.transferDB.GetDocsPublication0_sql
                elif grouping == 1:
                    # ---------------------------------------------
                    # (str) taskname: taskname
                    # Return: Docs, which match these conditions: asoworker, state = ACQUIRED, username, taskname
                    # ---------------------------------------------
                    if not username:
                        raise InvalidParameter('Username is not defined')
                    binds['username'] = username
                    sqlQuery = self.transferDB.GetDocsPublication1_sql
                rows = self.api.query(None, None, sqlQuery, **binds)
                return rows

        if subresource == 'getTransfersToKill':
            # ---------------------------------------------
            # (str) taskname: taskname
            # Return: Docs, which match these conditions: asoworker, state = KILL
            # ---------------------------------------------
            if grouping == 0:
                if not asoworker:
                    raise InvalidParameter("Required asoworker parameter is not set.")
                if not limit:
                    limit = 5000
                binds['limit'] = limit if limit < 5000 else 5000
                #binds['asoworker'] = asoworker
                binds['state'] = TRANSFERDB_STATUSES['KILL']
                sqlQuery = self.transferDB.GetDocsTransfer0_sql
                rows = self.api.query(None, None, sqlQuery, **binds)
            return rows

        elif subresource == 'groupedTransferStatistics':
            if grouping > 5:
                raise InvalidParameter('This grouping level is not implemented')
            sqlQuery, binds = self.getGroupedTransferQuery(grouping, username, taskname, source, destination, asoworker)
            rows = self.api.query(None, None, sqlQuery, **binds)
            return rows
        elif subresource == 'groupedPublishStatistics':
            if grouping > 2:
                raise InvalidParameter('This grouping level is not implemented')
            sqlQuery, binds = self.getGroupedPublicationQuery(grouping, username, taskname, asoworker)
            rows = self.api.query(None, None, sqlQuery, **binds)
            return rows
        elif subresource == 'activeUsers':
            rows = self.api.query(None, None, self.transferDB.GetActiveUsers_sql, asoworker=asoworker)
            return rows
        elif subresource == 'activeUserPublications':
            rows = self.api.query(None, None, self.transferDB.GetActiveUserPublication_sql, asoworker=asoworker)
            return rows


    @restcall
    def delete(self):
        """ Delete a doc from the DB """
        raise NotImplementedError

# TODO document all grouped transfers
    def getGroupedTransferQuery(self, grouping, username, taskname, source, destination, asoworker):
        """get query and bind parameters for it"""
        binds = {}
        if grouping == 0:
            # Return grouped status of transfer for each asoworker
            return self.transferDB.GetGroupedTransferStatistics0_sql, binds
        elif grouping == 1:
            # Return grouped status of transfers for user
            if not username:
                raise InvalidParameter('Username is not defined')
            binds['username'] = username
            if asoworker:
                binds['asoworker'] = asoworker
                return self.transferDB.GetGroupedTransferStatistics1_sql, binds
            else:
                return self.transferDB.GetGroupedTransferStatistics1a_sql, binds
        elif grouping == 2:
            # Return grouped status of transfer for user and taskname
            if not username or not taskname:
                raise InvalidParameter('Required variable is not defined')
            binds['username'] = username
            binds['taskname'] = taskname
            if asoworker:
                binds['asoworker'] = asoworker
                return self.transferDB.GetGroupedTransferStatistics2_sql, binds
            else:
                return self.transferDB.GetGroupedTransferStatistics2a_sql, binds
        elif grouping == 3:
            # Return grouped status of transfer for user and source
            if not username or not source:
                raise InvalidParameter('Required variable is not defined')
            binds['username'] = username
            binds['source'] = source
            if asoworker:
                binds['asoworker'] = asoworker
                return self.transferDB.GetGroupedTransferStatistics3_sql, binds
            else:
                return self.transferDB.GetGroupedTransferStatistics3a_sql, binds
        elif grouping == 4:
            # Return grouped status of transfers for user and source and destination
            if not username or not source or not destination:
                raise InvalidParameter('Required variable is not defined')
            binds['username'] = username
            binds['source'] = source
            binds['destination'] = destination
            if asoworker:
                binds['asoworker'] = asoworker
                return self.transferDB.GetGroupedTransferStatistics4_sql, binds
            else:
                return self.transferDB.GetGroupedTransferStatistics4a_sql, binds
        elif grouping == 5:
            # Return grouped status of transfers for user and source and destination
            if not username or not source or not destination or not taskname:
                raise InvalidParameter('Required variable is not defined')
            binds['username'] = username
            binds['taskname'] = taskname
            binds['source'] = source
            binds['destination'] = destination
            if asoworker:
                binds['asoworker'] = asoworker
                return self.transferDB.GetGroupedTransferStatistics5_sql, binds
            else:
                return self.transferDB.GetGroupedTransferStatistics5a_sql, binds

    def getGroupedPublicationQuery(self, grouping, username, taskname, asoworker):
        """Get query and bind parameters for query"""
        binds = {}
        if grouping == 0:
            # Return grouped status of publication for each asoworker
            return self.transferDB.GetGroupedPublicationStatistics0_sql, binds
        elif grouping == 1:
            # Return grouped status of publication for user
            if not username:
                raise InvalidParameter('Username is not defined')
            binds['username'] = username
            if asoworker:
                binds['asoworker'] = asoworker
                return self.transferDB.GetGroupedPublicationStatistics1_sql, binds
            else:
                return self.transferDB.GetGroupedPublicationStatistics1a_sql, binds
        elif grouping == 2:
            # Return grouped status of publication for user and taskname
            if not username or not taskname:
                raise InvalidParameter('Required variable is not defined')
            binds['username'] = username
            binds['taskname'] = taskname
            if asoworker:
                binds['asoworker'] = asoworker
                return self.transferDB.GetGroupedPublicationStatistics2_sql, binds
            else:
                return self.transferDB.GetGroupedPublicationStatistics2a_sql, binds

    def compareLen(self, inputDict, keys):
        """for all available keys it checks if this key exists and len of all
           lists are the same."""
        listLen = -1
        for key in keys:
            if key not in inputDict:
                return False, "Key %s does not exist in input" % key
            if listLen != len(inputDict[key]) and listLen != -1:
                return False, "Len of input keys are different. Expected %s, Got %s " % (listLen, len(inputDict[key]))
            listLen = len(inputDict[key])
        return True, ""


    def checkKeys(self, keysToCheck, jsonDoc):
        for key in keysToCheck:
            if key not in jsonDoc:
                raise InvalidParameter("BADJOB")

    def assignKeys(self, keys, jsonDoc, binds):
        self.checkKeys(keys, jsonDoc)
        for key in keys:
            binds[key] = jsonDoc[key]
        return binds
