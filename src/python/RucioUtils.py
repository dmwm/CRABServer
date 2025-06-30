""" a small set of utilities to work with Rucio used in various places """
import logging
import time

from TaskWorker.WorkerExceptions import TaskWorkerException
from rucio.client import Client as NativeClient
from rucio.common.exception import RSENotFound, RuleNotFound

class Client:
    def __init__(self, *args, retries=3, delay=180, logger=None, **kwargs):
        self._client = NativeClient(*args, **kwargs)
        self._retries = retries
        self._delay = delay
        self._logger = logger or logging.getLogger(__name__)

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                attempt = 0
                while attempt < self._retries:
                    try:
                        return attr(*args, **kwargs)
                    except Exception as e:
                        self._logger.warning(
                            "Failed to execute '%s' (attempt %d/%d): %s",
                            name, attempt + 1, self._retries, str(e)
                        )
                        attempt += 1
                        if attempt < self._retries:
                            time.sleep(self._delay)
                        else:
                            self._logger.error(
                                "Operation '%s' failed after %d attempts", name, self._retries
                            )
                            raise
            return wrapper
        else:
            return attr

def getNativeRucioClient(config=None, logger=None):
    """
    instantiates a Rucio python Client for use in CRAB TaskWorker
    :param config: a TaskWorker configuration object in which
                    at least the variables used below are defined
    :param logger: a valid logger instance
    :return: a Rucio Client object
    """
    logger.info("Initializing native Rucio client")

    rucioLogger = logging.getLogger('RucioClient')
    rucioLogger.setLevel(logging.INFO)

    # silence a few noisy components used by rucio
    ul = logging.getLogger('urllib3')
    ul.setLevel(logging.ERROR)
    dl = logging.getLogger('dogpile')
    dl.setLevel(logging.ERROR)
    cl = logging.getLogger('charset_normalizer')
    cl.setLevel(logging.ERROR)

    # allow for both old and new configuration style

    if getattr(config, 'Services', None):
        rucioConfig = config.Services
    else:
        rucioConfig = config

    rucioCert = getattr(rucioConfig, "Rucio_cert")
    rucioKey = getattr(rucioConfig, "Rucio_key")
    logger.debug("Using cert [%s]\n and key [%s] for rucio client.", rucioCert, rucioKey)
    client = Client(
        rucio_host=rucioConfig.Rucio_host,
        auth_host=rucioConfig.Rucio_authUrl,
        ca_cert=rucioConfig.Rucio_caPath,
        account=rucioConfig.Rucio_account,
        creds={"client_cert": rucioCert, "client_key": rucioKey},
        auth_type='x509',
        logger=rucioLogger,
        retries=3,
        delay=180
    )

    # Initial check: these calls now retry automatically
    ret = client.ping()
    logger.info("Rucio server v.%s contacted", ret['version'])
    ret = client.whoami()
    logger.info("Rucio client initialized for %s in status %s", ret['account'], ret['status'])

    return client


def getWritePFN(rucioClient=None, siteName='', lfn='',  # pylint: disable=dangerous-default-value
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
    exceptionString = ""
    didDict = None
    for operation in operations:
        try:
            logger.warning('Try Rucio lfn2pn with operation %s', operation)
            didDict = rucioClient.lfns2pfns(siteName, [did], operation=operation)
            break
        except RSENotFound:
            msg = f"Site {siteName} not found in CMS site list"
            raise TaskWorkerException(msg) from RSENotFound
        except Exception as ex:  # pylint: disable=broad-except
            msg = 'Rucio lfn2pfn resolution for %s failed with:\n%s\nTry next one.'
            logger.warning(msg, operation, str(ex))
            exceptionString += f"operation: {operation}, exception: {ex}\n"
    if not didDict:
        msg = f"lfn2pfn resolution with Rucio failed for site: {siteName}  LFN: {lfn}"
        msg += f" with exception(s) :\n{exceptionString}"
        raise TaskWorkerException(msg)

    # lfns2pfns returns a dictionary with did as key and pfn as value:
    #  https://rucio.readthedocs.io/en/latest/api/rse.html
    # {u'cms:/store/user/rucio': u'gsiftp://eoscmsftp.cern.ch:2811/eos/cms/store/user/rucio'}
    pfn = didDict[did]
    logger.info(f"Will use {pfn} as stageout location")

    return pfn


def getRuleQuota(rucioClient=None, ruleId=None):
    """ return quota needed by this rule in Bytes """
    size = 0
    try:
        rule = rucioClient.get_replication_rule(ruleId)
    except RuleNotFound:
        return 0
    files = rucioClient.list_files(scope=rule['scope'], name= rule['name'])
    size = sum(file['bytes'] for file in files)
    return size

def getRucioUsage(rucioClient=None, account=None, activity =None):
    """ size of Rucio usage for this account (if provided) or by activity """
    if activity is None:
        if account is None:
            totalusage = 0
            raise ValueError("Error: Account and Activity both unspecified")
        else:
            usageGenerator = rucioClient.get_local_account_usage(account=account)
            totalBytes = 0
            for usage in usageGenerator:
                used = usage['bytes']
                totalBytes += used
            totalusage = totalBytes
    else:
        filters = {'activity': activity}
        if account is not None:
            filters['account'] = account
        rules = rucioClient.list_replication_rules(filters=filters)
        if activity == 'Analysis Input':
            valid_states = ['OK', 'REPLICATING', 'STUCK', 'SUSPENDED']
        elif activity == 'Analysis TapeRecall':
            valid_states = ['REPLICATING', 'STUCK', 'SUSPENDED']  # Exclude 'OK' state
        else:
            print("Error: Unknown activity selected in quota report.")
            valid_states = []

        # Calculate usage only if valid_states is set
        # Rucio does not keep track by activity internally, so we need to find all rules and sum all files locked by each rule
        if valid_states:
            totalusage = sum(getRuleQuota(rucioClient, rule['id']) for rule in rules if rule['state'] in valid_states)
        else:
            totalusage = 0

    return totalusage