import os
from diskcache import Cache
from WMCore.Services.CRIC.CRIC import CRIC

TW_CACHE_DIR = os.getenv("TW_CACHE_DIR", default="/tmp/tw-cache")
CRIC_TTL = int(os.getenv("CRIC_TTL", default=60 * 60 * 24))

cache = Cache(TW_CACHE_DIR)

class CachedCRICService(CRIC):

    def __init__(self, *args, **kwargs):
       super().__init__(*args, **kwargs)

    @cache.memoize(ignore={0}, expire=CRIC_TTL)
    def _CRICSiteQuery(self, callname):
        return super()._CRICSiteQuery(callname)
