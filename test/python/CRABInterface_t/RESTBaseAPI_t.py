from cherrypy.test import test, webtest, helper
from cherrypy import expose, response, config as cpconfig
import os
import string, random

from WMCore.REST.Test import setup_test_server, fake_authz_headers
import WMCore.REST.Test as RT

from CRABInterface import RESTBaseAPI


COUCH_URL = os.getenv("COUCHURL")
REQMGR_DB = 'test_reqmgrdb'
MON_DB = 'test_wmstat'
CC_DB = 'test_configcache'
MONASO_DB = 'test_asomon'
DBSURL = 'http://localhost/fake_dbs_url/servlet/DBSServlet'
ACDCDB = 'test_wmagent_acdc'

SUBMIT_BODY = "workflow=pippo&jobtype=Cmssw&jobsw=CMSSW_4_2_5&jobarch=slc5_amd64_gcc434&splitalgo=FileBased&algoargs=args&userisburl=https://cfg&savelogsflag=0&publishname=ciao&asyncdest=T2_IT_Bari&inputdata=/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO&configfile=itslocatedhere"

class RESTBaseAPI_t(RESTBaseAPI.RESTBaseAPI):
    """The CRABServer REST API unit test modules"""

    def __init__(self, app, config, mount):
        #print config
        config.monurl = COUCH_URL
        config.monname = MON_DB
        config.asomonurl = COUCH_URL
        config.asomonname = MONASO_DB
        config.configcacheurl = COUCH_URL
        config.configcachename = CC_DB
        config.reqmgrurl = COUCH_URL
        config.reqmgrname = REQMGR_DB
        config.phedexurl = 'https://some.url.notvalid.cern.ch/phedex/'
        connectUrl='oracle://u:p@oradb'
        #config.CoreDatabase.connectUrl = connectUrl
        config.connectUrl = connectUrl
        config.dbsurl = DBSURL
        config.acdcurl = COUCH_URL
        config.acdcdb = ACDCDB
        config.delegatedn =  []
        config.defaultBlacklist=  []
        config.loggingLevel = 10
        config.loggingFile = 'CRAB.log'
        RESTBaseAPI.RESTBaseAPI.__init__(self, app, config, mount)

class Tester(helper.CPWebCase):

    def test_workflow_submit(self, fmt = 'application/json', page = "/test/workflow",
                                   inbody = SUBMIT_BODY):
        h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
        # empty of input submit failing with 400
        self.getPage(page, headers=h, method="PUT", body=None)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # fully working submit
        self.getPage(page, headers=h, method="PUT", body=inbody)
        # TODO assertions!
        # self.assertStatus("400 Bad Request")
        # self.assertInBody("RequestName")


    def test_workflow_get(self, fmt = 'application/json', page = "/test/workflow", inbody=None):
        h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
        # no workflow specified
        self.getPage(page + '?subresource=data', headers=h, method="GET", body=inbody)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # invalid input parameter specified
        self.getPage(page + '?workflow=pippo&invalid_param=xxx', headers=h, method="GET", body=inbody)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # too long workflow name
        randomlongname=''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(200))
        self.getPage(page + '?workflow='+randomlongname, headers=h, method="GET", body=inbody)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")



def setup_server():
    srcfile = RESTBaseAPI_t.__name__
    setup_test_server(srcfile, srcfile.split(".")[-1], app_name = 'test')

if __name__ == '__main__':
    setup_server()
    helper.testmain()
