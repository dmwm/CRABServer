import os
import logging
from diskcache import Cache
from WMCore.Services.CRIC.CRIC import CRIC
from ServerUtilities import tempSetLogLevel

TW_CACHE_DIR = os.getenv("TW_CACHE_DIR", default="/tmp/tw-cache")
CRIC_TTL = int(os.getenv("CRIC_TTL", default=60 * 60 * 24))

cache = Cache(TW_CACHE_DIR)

class CachedCRICService(CRIC):

    def __init__(self, *args, **kwargs):
        with tempSetLogLevel(logger=kwargs['logger'], level=logging.ERROR):
            super().__init__(*args, **kwargs)

    @cache.memoize(ignore={0}, expire=CRIC_TTL)
    def getAllPhEDExNodeNames(self, *args, **kwargs):
        return super().getAllPhEDExNodeNames(*args, **kwargs)

    @cache.memoize(ignore={0}, expire=CRIC_TTL)
    def getAllPSNs(self, *args, **kwargs):
        return super().getAllPSNs(*args, **kwargs)

    @cache.memoize(ignore={0}, expire=CRIC_TTL)
    def PNNstoPSNs(self, *args, **kwargs):
        return super().PNNstoPSNs(*args, **kwargs)


if __name__ == '__main__':
    ###
    # Usage: (Inside TaskWorker container)
    # > export PYTHONPATH="$PYTHONPATH:/data/srv/current/lib/python/site-packages"
    # Cleaning the cache before run this test.
    # > rm -rf /tmp/tw-cache
    # > CRIC_TTL=5 python3 ExternalService.py
    # Example:
    # To see how the cache works over a longer time span while also helping us supress flooded debug messages.
    # > CRIC_TTL=120 python3 ExternalService.py
    ###

    from time import sleep
    from WMCore.Configuration import loadConfigurationFile
    from ServerUtilities import newX509env
    config = loadConfigurationFile('/data/srv/TaskManager/cfg/TaskWorkerConfig.py')
    envForCMSWEB = newX509env(X509_USER_CERT=config.TaskWorker.cmscert, X509_USER_KEY=config.TaskWorker.cmskey)
    logging.basicConfig(level=logging.DEBUG)

    with envForCMSWEB:
        CRICService = CachedCRICService(logger=logging.getLogger(),
                                        configDict={"cacheduration": 1, "pycurl": True})
        (start_hits, start_misses) = cache.stats()
        print('===== Test::Begining with %d cache keys and [%d/%d](hits/misses) =====' % (len(list(cache.iterkeys())), *cache.stats()))
        for i in range(4):
            CRICService.getAllPSNs()
            CRICService.getAllPhEDExNodeNames()
            CRICService.PNNstoPSNs([])
        print('===== Test::CacheKeys::%s ====='%list(cache.iterkeys()))
        (end_hits, end_misses) = cache.stats()
        total_hits = end_hits - start_hits
        total_misses = end_misses - start_misses
        assert len(list(cache.iterkeys())) == 3, "There should be 3 keys in the cache"
        assert total_hits >= 9, 'From total of 12 function calls, there should be at least 9 hits over a CRIC_TTL=%s seconds. but got (%d/%d)' % (CRIC_TTL, *cache.stats())
        assert total_misses <= 3, 'From total of 12 function calls, there should be either 0 misses or 3 misses (when cache was expired and need refresh). but got (%d/%d)' % cache.stats()
        print('===== Test::Success::CachedCRICService works as expected with Total/Hits/Misses == (12/%d/%d) =====' % (total_hits, total_misses))
        print('===== Test::Success::with accumulated Cache Stats:: (Hits/Misses) == (%d/%d) =====' % cache.stats())