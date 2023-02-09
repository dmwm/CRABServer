from __future__ import print_function
from __future__ import division

from TaskWorker.WorkerExceptions import TaskWorkerException

def getNativeRucioClient(config=None, logger=None):
    """
    instantiates a Rucio python Client for use in CRAB TaskWorker
    :param config: a TaskWorker configuration object in which
                    at least the variables used below are defined
    :param logger: a valid logger instance
    :return: a Rucio Client object
    """
    logger.info("Initializing native Rucio client")
    from rucio.client import Client

    rucio_cert = getattr(config.Services, "Rucio_cert", config.TaskWorker.cmscert)
    rucio_key = getattr(config.Services, "Rucio_key", config.TaskWorker.cmskey)
    logger.debug("Using cert [%s]\n and key [%s] for rucio client.", rucio_cert, rucio_key)
    nativeClient = Client(
        rucio_host=config.Services.Rucio_host,
        auth_host=config.Services.Rucio_authUrl,
        ca_cert=config.Services.Rucio_caPath,
        account=config.Services.Rucio_account,
        creds={"client_cert": rucio_cert, "client_key": rucio_key},
        auth_type='x509'
    )
    ret = nativeClient.ping()
    logger.info("Rucio server v.%s contacted", ret['version'])
    ret = nativeClient.whoami()
    logger.info("Rucio client initialized for %s in status %s", ret['account'], ret['status'])

    return nativeClient

def getWritePFN(rucioClient=None, siteName='', lfn='',
                operations=['third_party_copy_write', 'write'], logger=None):
    """
    convert a single LFN into a PFN which can be used for Writing via Rucio
    Rucio supports the possibility that at some point in the future sites may
    require different protocols or hosts for read or write operations
    :param rucioClient: Rucio python client, e.g. the object returned by getNativeRucioClient above
    :param siteName: e.g. 'T2_CH_CERN'
    :param lfn: a CMS-style LFN
    :param logger: a valid logger instance
    :return: a CMS-style PFN
    """

    # add a scope to turn LFN into Rucio DID syntax
    did = 'cms:' + lfn
    # we prefer to do ASO via FTS which uses 3rd party copy, fall back to protocols defined
    # for other operations in case that fails, order matters here !
    # "third_party_copy_write": provides the PFN to be used with FTS
    # "write": provides the PFN to be used with gfal
    # 2022-08: dario checked with felipe that every sane RSE has non-zero value
    # for the third_party_copy_write column, which means that it is available.
    for operation in operations:
        try:
            logger.warning('Try Rucio lfn2pn with operation %s', operation)
            didDict = rucioClient.lfns2pfns(siteName, [did], operation=operation)
            break
        except Exception as ex:
            msg = 'Rucio lfn2pfn resolution for %s failed with:\n%s\nTry next one.'
            logger.warning(msg, operation, str(ex))
            didDict = None
    if not didDict:
        msg = 'lfn2pfn resolution with Rucio failed for site: %s  LFN: %s' % (siteName, lfn)
        msg += ' with exception :\n%s' % str(ex)
        raise TaskWorkerException(msg)

    # lfns2pfns returns a dictionary with did as key and pfn as value:
    #  https://rucio.readthedocs.io/en/latest/api/rse.html
    # {u'cms:/store/user/rucio': u'gsiftp://eoscmsftp.cern.ch:2811/eos/cms/store/user/rucio'}
    pfn = didDict[did]
    logger.info('Will use %s as stageout location', pfn)

    return pfn
