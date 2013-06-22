import logging, sys
from hashlib import sha1
from WMCore.Credential.SimpleMyProxy import SimpleMyProxy

if len(sys.argv) is not 5:
    print "Usage example: "
    print 'python logon_openssl.py "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mcinquil/CN=660800/CN=Mattia Cinquilli" /data/certs/hostkey.pem /data/certs/hostcert.pem "/DC=ch/DC=cern/OU=computers/CN=mattia-dev01.cern.ch"'
    sys.exit(1)

logger = logging.getLogger("OpenSSL MyProxy test")
myproxyserver = "myproxy.cern.ch"
userdn = sys.argv[1]
defaultDelegation = {'logger': logger,
                     'proxyValidity' : '192:00',
                     'min_time_left' : 36000,
                     'server_key': sys.argv[2],
                     'server_cert': sys.argv[3],}
timeleftthreshold = 60 * 60 * 24
mypclient = SimpleMyProxy(defaultDelegation)
userproxy = mypclient.logonRenewMyProxy(username=sha1(sys.argv[4]+userdn).hexdigest(), myproxyserver=myproxyserver, myproxyport=7512)
print "Proxy Retrieved with len ", len(userproxy)
