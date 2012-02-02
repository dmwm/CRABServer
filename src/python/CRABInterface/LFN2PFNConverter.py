'''
Created on Oct 19, 2011

@author: mmasche
'''
from WMCore.Storage.TrivialFileCatalog import readTFC
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Lexicon import cmsname

class LFN2PFNConverter:
    '''
    The class handles the TFC cache of each site, and allow to perform the lfn2pfn
    conversion without connecting to phedex every time
    '''

    def __init__(self, dict=None):
        '''
        Constructor: create
        '''
        self.phedex = PhEDEx(responseType='xml', dict=dict)
        self.sitedb = SiteDBJSON()

    def lfn2pfn(self, siteName, lfn):
        #Default: cache expires in 0.5 hours in Service
        try:
            cmsname(siteName)
        except:
            siteName = self.sitedb.seToCMSName( siteName )
        self.phedex.getNodeTFC(siteName)
        tfcCacheFile = self.phedex.cacheFileName('tfc', inputdata={'node': siteName})
        tfc = readTFC(tfcCacheFile)

        return tfc.matchLFN('srmv2', lfn)
