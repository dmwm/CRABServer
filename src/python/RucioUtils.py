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

    nativeClient = Client(
        rucio_host=config.Services.Rucio_host,
        auth_host=config.Services.Rucio_authUrl,
        ca_cert=config.Services.Rucio_caPath,
        account=config.Services.Rucio_account,
        creds={"client_cert": config.TaskWorker.cmscert, "client_key": config.TaskWorker.cmskey},
        auth_type='x509'
    )
    ret = nativeClient.ping()
    logger.info("Rucio server v.%s contacted", ret['version'])
    ret = nativeClient.whoami()
    logger.info("Rucio client initialized for %s in status %s", ret['account'], ret['status'])

    return nativeClient

def getWritePFN(rucioClient=None, siteName='', lfn='', logger=None):
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
    try:
        didDict = rucioClient.lfns2pfns(siteName, [did], operation='write')
    except Exception as ex:
        logger.warning('Rucio lfn2pfn resolution for Write failed with:\n%s', ex)
        logger.warning("Will try with operation='read'")
        try:
            didDict = rucioClient.lfns2pfns(siteName, [did], operation='read')
        except Exception as ex:
            msg = 'lfn2pfn resolution with Rucio failed for site: %s  LFN: %s' % (siteName, lfn)
            msg += ' with exception :\n%s' % str(ex)
            raise TaskWorkerException(msg)

    # lfns2pfns returns a dictionary with did as key and pfn as value:
    #  https://rucio.readthedocs.io/en/latest/api/rse.html
    # {u'cms:/store/user/rucio': u'gsiftp://eoscmsftp.cern.ch:2811/eos/cms/store/user/rucio'}
    pfn = didDict[did]

    return pfn
