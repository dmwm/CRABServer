from __future__ import division

import json
from RESTInteractions import HTTPRequests
from httplib import HTTPException
from TaskWorker.WorkerExceptions import TaskWorkerException

def blocksRequest(blocks, ddmServer, cert, key, verbose=False):
    site = "T2*" # will let Dynamo choose which T2 to stage the blocks to
    return serverCall(ddmServer, cert, key, verbose, 'post', 'copy', data=json.dumps({"item": blocks, "site": site}))

def statusRequest(ddmReqid, ddmServer, cert, key, verbose=False):
    return serverCall(ddmServer, cert, key, verbose, 'get', 'pollcopy', data={'request_id': ddmReqid})

def serverCall(ddmServer, cert, key, verbose, call, api, data):
    server = HTTPRequests(url=ddmServer, localcert=cert, localkey=key, verbose=verbose)
    commonAPI = '/registry/request'
    try:
        ddmRequest = getattr(server, call)(commonAPI+'/'+api, data=data)
    except HTTPException as hte:
        msg = "HTTP Error while contacting the DDM server %s:\n%s" % (ddmServer, str(hte))
        msg += "\nHTTP Headers are: %s" % hte.headers
        raise TaskWorkerException(msg, retry=True)

    return ddmRequest[0]
