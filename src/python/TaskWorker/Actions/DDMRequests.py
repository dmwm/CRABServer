from __future__ import division

import json
from RESTInteractions import HTTPRequests

def blocksRequest(blocks, ddmServer, cert, key, verbose=False):
    site = "T2*" # will let Dynamo choose which T2 to stage the blocks to
    return serverCall(ddmServer, cert, key, verbose, 'post', 'copy', data=json.dumps({"item": blocks, "site": site}))

def statusRequest(ddmReqid, ddmServer, cert, key, verbose=False):
    return serverCall(ddmServer, cert, key, verbose, 'get', 'pollcopy', data={'request_id': ddmReqid})

def serverCall(ddmServer, cert, key, verbose, call, api, data):
    server = HTTPRequests(url=ddmServer, localcert=cert, localkey=key, verbose=verbose)
    commonAPI = '/registry/request'
    ddmRequest = getattr(server, call)(commonAPI+'/'+api, data=data)
    return ddmRequest[0]
