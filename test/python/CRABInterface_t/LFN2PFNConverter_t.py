'''
Created on Oct 20, 2011
'''
import unittest
import socket
from xml.parsers.expat import ExpatError
from CRABInterface.LFN2PFNConverter import LFN2PFNConverter


class LFN2PFNConverterTest(unittest.TestCase):
    siteName = 'T2_BE_UCL'
    lfn = '/store/temp/user/jmmaes/RelValProdTTbar/crab303test-jmmaes-004/v1/0000/861F86B0-1EF0-E011-A7A4-00226406A18A.root'
    expPFN = 'srm://ingrid-se02.cism.ucl.ac.be:8444/srm/managerv2?SFN=/storage/data/cms/store/temp/user/jmmaes/RelValProdTTbar/crab303test-jmmaes-004/v1/0000/861F86B0-1EF0-E011-A7A4-00226406A18A.root'

    def testConnectionOk(self):
        converter = LFN2PFNConverter()
        res = converter.lfn2pfn(self.siteName, self.lfn)
        self.assertEqual(self.expPFN, res)

    def testConnectionDown(self):
        #assume phedex is down. Simulate this setting a non-existing endpoint
        converter = LFN2PFNConverter(dict={'endpoint' : 'https://iamnotthere.com/ciaobello'})
        self.assertRaises(socket.error, converter.lfn2pfn, *(self.siteName, self.lfn))

    def testWrongAsnwer(self):
        #assume we got a non-xml answer. Simulate this connecting to google
        converter = LFN2PFNConverter(dict={'endpoint' : 'https://www.google.com'})
        self.assertRaises(ExpatError, converter.lfn2pfn, *(self.siteName, self.lfn))

    def testNotExistingSite(self):
        converter = LFN2PFNConverter()
        res = converter.lfn2pfn('XXX', self.lfn)
        self.assertEqual(None, res)


if __name__ == "__main__":
    unittest.main()
