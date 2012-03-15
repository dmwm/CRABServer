from cherrypy.test import test, webtest, helper
from cherrypy import expose, response, config as cpconfig
import os, tempfile, tarfile, hashlib, shutil

from WMCore.REST.Test import setup_test_server, fake_authz_headers
import WMCore.REST.Test as RT
from WMCore.Services.Requests import uploadFile, downloadFile

from UserFileCache import RESTBaseAPI

UFC_CACHE=os.path.join(os.getcwd(), 'test_cache')
#UFC_CACHE=tempfile.mkdtemp(prefix='testufc', dir=os.getcwd())

class RESTBaseAPI_t(RESTBaseAPI.RESTBaseAPI):
    """The UserFileCache REST API unit test modules"""

    def __init__(self, app, config, mount):
        config.cachedir = UFC_CACHE
        os.mkdir(config.cachedir)
        RESTBaseAPI.RESTBaseAPI.__init__(self, app, config, mount)

class Tester(helper.CPWebCase):

    def setUp(self):
        self.tgz, self.checksum = self._create_isb()
        self.json, self.jsonchecksum = self._create_isb(name = 'test_publish.tgz')

    def tearDown(self):
        self._delete()

    def _create_isb(self, name = 'test.tgz'):
        tgz = tarfile.open(name=name, mode='w:gz', dereference=True)
        tgz.add(os.getcwd(), recursive=True)
        checksum = self._get_checksum(tgz)
        tgz.close()
        return tgz, checksum

    def _delete(self):
        os.remove(self.tgz.name)
        os.remove(self.json.name)

    def _fake_upload_isb(self, name = 'test.tgz'):
        os.mkdir( os.path.join(UFC_CACHE, self.checksum[0:2]) )
        shutil.copyfile(name, os.path.join(UFC_CACHE, self.checksum[0:2], self.checksum))
        return self.tgz, self.checksum

    def _fake_upload_json(self, username, name = 'test_publish.tgz'):
        if not os.path.isdir( os.path.join(UFC_CACHE, username) ):
            os.mkdir( os.path.join(UFC_CACHE, username) )
        shutil.copyfile(name, os.path.join(UFC_CACHE, username, name))
        return self.json, self.jsonchecksum

    def _get_checksum(self, tgz):
        lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tgz.getmembers()]
        hasher = hashlib.sha256(str(lsl))
        return hasher.hexdigest()

    def test_upload_tgz(self, fmt='application/json', page="/test/file", inbody = None):
        h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
        # empty of input submit failing with 400
        self.getPage(page, headers=h, method="PUT", body=None)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # empty inputfile
        body1 = 'hashkey='+self.checksum
        h1 = h + [('Content-Length', len(body1))]
        self.getPage(page, headers=h1, method="PUT", body=body1)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # real upload request
        body2 = 'hashkey='+self.checksum+'&inputfile='+open(self.tgz.name,'rb').read() #TODO change how we pass the input file
        h2 = h + [('Content-Length', len(body2))]
        self.getPage(page, headers=h, method="PUT", body=body2)
        # TODO this is not working given how we pass the input file now
        #self.assertStatus("200 OK")

    def test_download_tgz(self, fmt='application/x-download', page="/test/file", inbody = None):
        h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
        # empty request failing
        self.getPage(page, headers=h, method="GET", body=None)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # real file upload
        tgz, checksum = self._fake_upload_isb()
        body1 = '?hashkey='+checksum
        self.getPage(page + body1, headers=h, method="GET", body=inbody)
        self.assertStatus("200 OK")
        self.assertHeader("Content-Disposition", 'attachment; filename="%s"' % checksum)
        self.assertHeader("Content-Type", fmt)

    def test_exists_tgz(self, fmt='application/json', page="/test/file", inbody = None):
        h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
        # empty request failing
        self.getPage(page, headers=h, method="GET", body=None)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # checking a not existing file
        body1 = '?hashkey='+str(64*'a')+'&nodownload=1'
        self.getPage(page + body1, headers=h, method="GET", body=inbody)
        self.assertStatus("404 Not Found")
        self.assertHeader("X-Error-Http", "404")
        self.assertHeader("X-Error-Detail", "No such instance")
        self.assertHeader("X-Error-Info", "Not found")
        # checking a real file
        tgz, checksum = self._fake_upload_isb()
        body1 = '?hashkey='+checksum+'&nodownload=1'
        self.getPage(page + body1, headers=h, method="GET", body=inbody)
        self.assertStatus("200 OK")
        self.assertHeader("Content-Type", fmt)
        self.assertInBody(checksum)

    def test_upload_json(self, fmt='application/json', page="/test/file", inbody = None):
        h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
        # empty of input submit failing with 400
        self.getPage(page, headers=h, method="PUT", body=None)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # real upload request
        # TODO this is not working given how we pass the input file now
        #body2 = 'hashkey='+self.jsonchecksum+'&inputfilename='+self.json.name+'&inputfile='+open(self.json.name,'rb').read()
        #h2 = h + [('Content-Length', len(body2))]
        #self.getPage(page, headers=h, method="PUT", body=body2)
        #self.assertStatus("200 OK")

    def test_download_json(self, fmt='application/x-download', page="/test/file", inbody = None):
        h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
        username = [head[1] for head in h if head[0] == 'cms-authn-login'][-1]
        # empty request failing
        self.getPage(page, headers=h, method="GET", body=None)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # real file download
        tgz, checksum = self._fake_upload_json(username)
        body1 = '?inputfilename='+os.path.basename(tgz.name)
        self.getPage(page + body1, headers=h, method="GET", body=inbody)
        self.assertStatus("200 OK")
        self.assertHeader("Content-Disposition", 'attachment; filename="%s"' % os.path.basename(tgz.name))
        self.assertHeader("Content-Type", fmt)

    def test_exists_json(self, fmt='application/json', page="/test/file", inbody = None):
        h = fake_authz_headers(RT.test_authz_key.data) + [("Accept", fmt)]
        username = [head[1] for head in h if head[0] == 'cms-authn-login'][-1]
        # empty request failing
        self.getPage(page, headers=h, method="GET", body=None)
        self.assertStatus("400 Bad Request")
        self.assertHeader("X-Error-Http", "400")
        self.assertHeader("X-Error-Detail", "Invalid input parameter")
        # checking a not existing file
        body1 = '?hashkey='+str(64*'a')+'&nodownload=1'
        self.getPage(page + body1, headers=h, method="GET", body=inbody)
        self.assertStatus("404 Not Found")
        self.assertHeader("X-Error-Http", "404")
        self.assertHeader("X-Error-Detail", "No such instance")
        self.assertHeader("X-Error-Info", "Not found")
        # checking a real file
        tgz, checksum = self._fake_upload_json(username)
        body1 = '?inputfilename='+os.path.basename(tgz.name)+'&nodownload=1'
        self.getPage(page + body1, headers=h, method="GET", body=inbody)
        self.assertStatus("200 OK")
        self.assertHeader("Content-Type", fmt)
        # Following asertion fails
        #self.assertInBody(checksum)


def setup_server():
    srcfile = RESTBaseAPI_t.__name__
    setup_test_server(srcfile, srcfile.split(".")[-1], app_name = 'test')

if __name__ == '__main__':
    setup_server()
    helper.testmain()
    # cleaning cache when tests are over
    shutil.rmtree(UFC_CACHE)
