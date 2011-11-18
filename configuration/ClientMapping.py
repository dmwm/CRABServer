#!/usr/bin/env python
"""
_ClientMapping_

This allows to have an agnostic client.
For each client command it is possible to define
the path of the REST request, the map between
the client configuration and the final request
to send to the server. It includes type of the parameter
to have the client doing a basic sanity check on
the input data type.
"""

import time

defaulturi = {
    'submit' :  { 'uri': '/crabinterface/crab/task/',
                  'map':  {
                            "RequestType"       : {"default": "Analysis",       "config": None,                     "type": "StringType",  "required": True },
                            "Group"             : {"default": "Analysis",       "config": 'User.group',             "type": "StringType",  "required": True },
                            "Team"              : {"default": "Analysis",       "config": 'User.team',              "type": "StringType",  "required": True },
                            "Requestor"         : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "Username"          : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "RequestName"       : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "RequestorDN"       : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "SaveLogs"          : {"default": False,            "config": 'General.saveLogs',       "type": "BooleanType", "required": True },
                            "asyncDest"         : {"default": None,             "config": 'Site.storageSite',       "type": "StringType",  "required": True },
                            "PublishDataName"   : {"default": str(int(time.time())), "config": 'Data.publishDataName',   "type": "StringType",  "required": True },
                            "ProcessingVersion" : {"default": "v1",             "config": 'Data.processingVersion', "type": "StringType",  "required": True },
                            "DbsUrl"            : {"default": "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet", "config": 'Data.dbsUrl', "type": "StringType",  "required": True },
                            "SiteWhitelist"     : {"default": None,             "config": 'Site.whitelist',         "type": "ListType",    "required": False},
                            "SiteBlacklist"     : {"default": None,             "config": 'Site.blacklist',         "type": "ListType",    "required": False},
                            "RunWhitelist"      : {"default": None,             "config": 'Data.runWhitelist',      "type": "StringType",    "required": False},
                            "RunBlacklist"      : {"default": None,             "config": 'Data.runBlacklist',      "type": "StringType",    "required": False},
                            "BlockWhitelist"    : {"default": None,             "config": 'Data.blockWhitelist',    "type": "ListType",    "required": False},
                            "BlockBlacklist"    : {"default": None,             "config": 'Data.blockBlacklist',    "type": "ListType",    "required": False},
                            "JobSplitAlgo"      : {"default": None,             "config": 'Data.splitting',         "type": "StringType",  "required": False},
                            "VoRole"            : {"default": None,             "config": 'User.voRole',            "type": "StringType",  "required": False},
                            "VoGroup"           : {"default": None,             "config": 'User.voGroup',           "type": "StringType",  "required": False}
                            #"JobSplitArgs"      : {"default": None,             "config": 'Data.filesPerJob',       "type": IntType,    "required": False},
                            #"JobSplitArgs"      : {"default": None,             "config": 'Data.eventPerJob',       "type": IntType,    "required": False},
                          },

                  'other-config-params' : ["General.serverUrl", "General.requestName", "JobType.pluginName", "JobType.externalPluginFile", "Data.unitsPerJob", "Data.splitting", \
                                               "JobType.psetName", "JobType.inputFiles", "Data.inputDataset", "User.email", "Data.lumiMask", "General.workArea"]
                },
    'get-log' :  {'uri': '/crabinterface/crab/log/'},
    'get-output'   : {'uri': '/crabinterface/crab/data/'},
    'reg_user'    : {'uri': '/crabinterface/crab/user/'},
    'server_info' : {'uri': '/crabinterface/crab/info/'},
    'status' : {'uri': '/crabinterface/crab/task/'},
    'upload' : {'uri': '/crabinterface/crab/uploadUserSandbox'},
    'get-error': {'uri': '/crabinterface/crab/jobErrors/'},
    'report': {'uri': '/crabinterface/crab/goodLumis/'},
    'kill':   {'uri': '/crabinterface/crab/task/'},
    'resubmit': {'uri': '/crabinterface/crab/reprocessTask/'},
}
