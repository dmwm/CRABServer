import os
import re
import sys
import time
import stat
import types
import random
import urllib
import struct
import commands
import cPickle as pickle
import xml.dom.minidom
import socket
import tempfile

from hashlib import sha1

#from taskbuffer.JobSpec import JobSpec
#from taskbuffer.FileSpec import FileSpec

baseURL = 'http://voatlas220.cern.ch:25080/server/panda'
baseURLSSL = 'https://voatlas220.cern.ch:25443/server/panda'
baseURLCSRVSSL = "https://voatlas178.cern.ch:25443/server/panda"
baseURLSUB     = "http://pandaserver.cern.ch:25080/trf/user"

baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'
baseURLCSRVSSL = "https://pandaserver.cern.ch:25443/server/panda"
baseURLSUB     = "http://pandaserver.cern.ch:25080/trf/user"

# exit code
EC_Failed = 255

globalTmpDir = ''

#import CRABInterface
#basepath = CRABInterface.__path__[0]
#sys.path = [basepath]+sys.path


def userCertFile(userDN, vo, group, role):
    x509 = "/data/state/crabserver/proxy/%s" % sha1( userDN + vo + group + role ).hexdigest()
    if os.access(x509,os.R_OK):
        return x509

    msg = "No valid grid proxy certificate found"
    print "Looking for proxy certificate in %s for user %s" % (x509, userDN)
    raise Exception(msg)

# look for a CA certificate directory
def _x509_CApath():
    # use X509_CERT_DIR
    try:
        return os.environ['X509_CERT_DIR']
    except:
        pass
    # get X509_CERT_DIR
    gridSrc = _getGridSrc()
    com = "%s echo $X509_CERT_DIR" % gridSrc
    tmpOut = commands.getoutput(com)
    return tmpOut.split('\n')[-1]

class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl" '
        # verification of the host certificate
        self.verifyHost = False
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # verbose
        self.verbose = False

    # GET method
    def get(self,url,data,rucioAccount=False):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # add rucio account info
        if rucioAccount:
            if os.environ.has_key('RUCIO_ACCOUNT'):
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if os.environ.has_key('RUCIO_APPID'):
                data['appid'] = os.environ['RUCIO_APPID']
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print strData[:-1]
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret

    def post(self,url,data,rucioAccount=False):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            #com += ' --cert %s' % '/tmp/mycredentialtest'
        if self.sslKey != '':
            com += ' --key %s' % self.sslCert
            #com += ' --key %s' % '/tmp/mycredentialtest'
        # add rucio account info
        if rucioAccount:
            if os.environ.has_key('RUCIO_ACCOUNT'):
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if os.environ.has_key('RUCIO_APPID'):
                data['appid'] = os.environ['RUCIO_APPID']
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print strData[:-1]
        print "###### curl command: %s  #########" % com
        s,o = commands.getstatusoutput(com)
        #print s,o
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            #com += ' --cert %s' % '/data/certs/prova'
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # emulate PUT
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        if self.verbose:
            print com
        # execute
        ret = commands.getstatusoutput(com)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # convert return
    def convRet(self,ret):
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        # add messages to silent errors
        if ret[0] == 35:
            ret = (ret[0],'SSL connect error. The SSL handshaking failed. Check grid certificate/proxy.')
        elif ret[0] == 7:
            ret = (ret[0],'Failed to connect to host.')
        elif ret[0] == 55:
            ret = (ret[0],'Failed sending network data.')
        elif ret[0] == 56:
            ret = (ret[0],'Failure in receiving network data.')
        return ret




class PandaBooking():

    def __init__(self, user, vo, group, role):
        self.user = user
        self.vo = vo
        self.group = group
        self.role = role

    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,timeRange,dn=None,verbose=False):
        # instantiate curl
        curl = _Curl()
        #curl.sslCert = _x509()
        #curl.sslKey  = _x509()
        curl.sslCert = userCertFile(self.user, self.vo, self.group, self.role)
        curl.sslKey  = userCertFile(self.user, self.vo, self.group, self.role)

        curl.verbose = verbose
        # execute
        url = baseURLSSL + '/getJobIDsInTimeRange'
        data = {'timeRange':timeRange}
        if dn != None:
            data['dn'] = dn
        status,output = curl.post(url,data)
        if status!=0:
            print output
            return status,None
        try:
            return status,pickle.loads(output)
        except:
            type, value, traceBack = sys.exc_info()
            print "ERROR getJobIDsInTimeRange : %s %s" % (type,value)
            return EC_Failed,None


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,jobID,dn=None,nJobs=0,verbose=False):
        # instantiate curl
        curl = _Curl()
        #curl.sslCert = _x509()
        #curl.sslKey  = _x509()
        curl.sslCert = userCertFile(self.user, self.vo, self.group, self.role)
        curl.sslKey  = userCertFile(self.user, self.vo, self.group, self.role)

        curl.verbose = verbose
        # execute
        url = baseURLSSL + '/getPandIDsWithJobID'
        data = {'jobID':jobID, 'nJobs':nJobs}
        if dn != None:
            data['dn'] = dn
        status,output = curl.post(url,data)
        if status!=0:
            print output
            return status,None
        try:
            return status,pickle.loads(output)
        except:
            type, value, traceBack = sys.exc_info()
            print "ERROR getPandIDsWithJobID : %s %s" % (type,value)
            return EC_Failed,None



    # get full job status
    def getFullJobStatus(self,ids,verbose):
        # serialize
        strIDs = pickle.dumps(ids)
        # instantiate curl
        curl = _Curl()
        curl.sslCert = userCertFile(self.user, self.vo, self.group, self.role)
        curl.sslKey  = userCertFile(self.user, self.vo, self.group, self.role)
        curl.verbose = verbose
        # execute
        url = baseURLSSL + '/getFullJobStatus'
        data = {'ids':strIDs}
        status,output = curl.post(url,data)
        #print output
        #bho = pickle.loads(output)
        try:
            return status,pickle.loads(output)
        except:
            type, value, traceBack = sys.exc_info()
            print "ERROR getFullJobStatus : %s %s" % (type,value)


    # kill jobs
    def killJobs(self, ids, code=None, verbose=False, useMailAsID=False):
        # serialize
        strIDs = pickle.dumps(ids)
        # instantiate curl
        curl = _Curl()
        curl.sslCert = userCertFile(self.user, self.vo, self.group, self.role)
        curl.sslKey  = userCertFile(self.user, self.vo, self.group, self.role)
        curl.verbose = verbose
        # execute
        url = baseURLSSL + '/killJobs'
        data = {'ids':strIDs,'code':code,'useMailAsID':useMailAsID}
        status,output = curl.post(url,data)
        try:
            return status,pickle.loads(output)
        except:
            type, value, traceBack = sys.exc_info()
            errStr = "ERROR killJobs : %s %s" % (type,value)
            print errStr
            return EC_Failed,output+'\n'+errStr
