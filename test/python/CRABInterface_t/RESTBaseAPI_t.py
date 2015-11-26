helper = None
# breaks in CP 3.2
try:
    from cherrypy.test import test, webtest, helper
except:
    pass
if helper:
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

    SUBMIT_BODY="jobarch=slc5_amd64_gcc434&configdoc=0f1dfe2c1a9608367727d1d7e9f57e5e&workflow=pippo&splitalgo=FileBased&publishdbsurl=https%3A%2F%2Fcmsdbsprod.cern.ch%3A8443%2Fcms_dbs_ph_analysis_02_writer%2Fservlet%2FDBSServlet&publishname=1349346832&savelogsflag=0&userisburl=https%3A%2F%2Fcmsweb.cern.ch%2Fcrabcache%2Ffile%3Fhashkey%3Dcc8c74c376a6b5c693dc2246d635dc6f252af940b9ec844af4c873ae3efe6ea5&jobsw=CMSSW_4_2_5&asyncdest=T2_IT_Pisa&algoargs=250&inputdata=%2FRelValProdTTbar%2FJobRobot-MC_3XY_V24_JobRobot-v1%2FGEN-SIM-DIGI-RECO&jobtype=Analysis&blacklistT1=1"
    class RESTBaseAPI_t(RESTBaseAPI.RESTBaseAPI):
        """The CRABServer REST API unit test modules"""
        broken = 1
        def __init__(self, app, config, mount):
            config.monurl = COUCH_URL
            config.monname = MON_DB
            config.asomonurl = COUCH_URL
            config.asomonname = MONASO_DB
            config.configcacheurl = COUCH_URL
            config.configcachename = CC_DB
            config.reqmgrurl = COUCH_URL
            config.reqmgrname = REQMGR_DB
            config.phedexurl = 'https://some.url.notvalid.cern.ch/phedex/'
            connectUrl='oracle://u:p@devdb11'
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
        broken = 1
        def test_workflow_empty_submit(self, fmt = 'application/json', page = "/test/workflow",inbody = None):
            """Try to submit workflow without any parameters"""

            h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
            self.getPage(page, headers=h, method="PUT", body=inbody)
            self.assertStatus("400 Bad Request")
            self.assertHeader("X-Error-Http", "400")
            self.assertHeader("X-Error-Detail", "Invalid input parameter")

        def test_workflow_non_null_submit(self, fmt = 'application/json', page = "/test/workflow",inbody = SUBMIT_BODY):
            """Try submitting job with all the parameters """
            h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
            self.getPage(page, headers=h, method="PUT", body=inbody)
            # TODO assertions!
            #self.assertStatus("40 Bad Request")
            #self.assertInBody("RequestName")

        def test_workflow_get_wo_workflow(self, fmt = 'application/json', page = "/test/workflow", inbody=None):
            """Try getting workflow without workflow parameter provided"""
            
            h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
            self.getPage(page + '?subresource=data', headers=h, method="GET", body=inbody)
            self.assertStatus("400 Bad Request")
            self.assertHeader("X-Error-Http", "400")
            self.assertHeader("X-Error-Detail", "Invalid input parameter")

        def test_workflow_get_invalid_workflow_param(self, fmt = 'application/json', page = "/test/workflow", inbody=None):
            """Try getting workflow without workflow parameter specified """
            
            h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
            self.getPage(page + '?workflow', headers=h, method="GET", body=inbody)
            self.assertStatus("400 Bad Request")
            self.assertHeader("X-Error-Http", "400")
            self.assertHeader("X-Error-Detail", "Invalid input parameter")

        def test_worklow_invalid_param(self, fmt = 'application/json', page = "/test/workflow", inbody=None):
            """Try getting workflow with some extra invalid parameter provided"""

            h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
            self.getPage(page + '?workflow=pippo&invalid_param=xxx', headers=h, method="GET", body=inbody)
            self.assertStatus("400 Bad Request")
            self.assertHeader("X-Error-Http", "400")
            self.assertHeader("X-Error-Detail", "Invalid input parameter")

        def test_workflow_too_long_param(self, fmt = 'application/json', page = "/test/workflow", inbody=None):
            """Try getting workflow with random and too long parameter provided """
            
            h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
            randomlongname=''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(200))
            self.getPage(page + '?workflow='+randomlongname, headers=h, method="GET", body=inbody)
            self.assertStatus("400 Bad Request")
            self.assertHeader("X-Error-Http", "400")
            self.assertHeader("X-Error-Detail", "Invalid input parameter")

        def test_workflow_invalid_param_only(self,fmt = 'application/json', page = "/test/workflow", inbody=SUBMIT_BODY):
            """Try submitting job with invalid parameter ONLY """

            h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
            self.getPage(page + '?invalid=x', headers=h, method="GET", body=inbody)
            self.assertStatus("400 Bad Request")
            self.assertHeader("X-Error-Http", "400")
            self.assertHeader("X-Error-Detail", "Invalid input parameter")


    def setup_server():
        srcfile = RESTBaseAPI_t.__name__
        setup_test_server(srcfile, srcfile.split(".")[-1], app_name = 'test')

    if __name__ == '__main__':
        setup_server()
        helper.testmain()
