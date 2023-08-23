from __future__ import division

import socket

import cx_Oracle as DB

fqdn = socket.getfqdn().lower()
s3 = {'access_key':'FIXME',
      'secret_key':'FIXME'}
dbconfig = {'preprod': {'.title': 'Pre-production',
                        '.order': 1,
                        '*': {'clientid': 'cmsweb-preprod@%s' %(fqdn),
                              'dsn': 'int2r',
                              'liveness': 'select sysdate from dual',
                              'password': 'FIXME',
                              'schema': 'cmsweb_analysis_preprod',
                              'timeout': 300,
                              'trace': True,
                              'type': DB,
                              'user': 'cmsweb_analysis_preprod'}},
            'prod': {'.title': 'Production',
                    '.order': 0,
                        'GET': {'clientid': 'cmsweb-prod-r@%s' %(fqdn),
                             'dsn': 'cmsr',
                             'liveness': 'select sysdate from dual',
                             'password': 'FIXME',
                             'schema': 'cms_analysis_reqmgr_r',
                             'timeout': 300,
                             'trace': True,
                             'type': DB,
                             'user': 'cms_analysis_reqmgr_r'},
                        '*':  {'clientid': 'cmsweb-prod-w@%s' %(fqdn),
                            'dsn': 'cmsr',
                            'liveness': 'select sysdate from dual',
                            'password': 'FIXME',
                            'schema': 'cms_analysis_reqmgr_w',
                            'timeout': 300,
                            'trace': True,
                            'type': DB,
                            'user': 'cms_analysis_reqmgr_w'}},
            'dev': {'.title': 'Development',
                        '.order': 2,
                        '*': {'clientid': 'cmsweb-dev@%s' %(fqdn),
                              'dsn': 'int2r',
                              'liveness': 'select sysdate from dual',
                              'password': 'FIXME',
                              'schema': 'cmsweb_analysis_dev',
                              'timeout': 300,
                              'trace': True,
                              'type': DB,
                              'user': 'cmsweb_analysis_dev'}}
            }
