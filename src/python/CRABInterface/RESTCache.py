from __future__ import division
import os
import uuid
import logging
import cherrypy
import boto3
from botocore.exceptions import ClientError

# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str
from WMCore.REST.Error import MissingParameter, ExecutionError

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid, authz_operator
from CRABInterface.Regexps import RX_SUBRES_CACHE, RX_CACHE_OBJECTTYPE, RX_TASKNAME, RX_USERNAME, RX_TARBALLNAME
from ServerUtilities import getUsernameFromTaskname, MeasureTime

class RESTCache(RESTEntity):
    """
    REST entity for accessing CRAB Cache on S3
    Supports only GET method

    As per the S3 buckt structure in https://github.com/dmwm/CRABServer/wiki/CRABCache-replacement-with-S3
    objects in S3 are always under a <username> prefix. Then there is a <taskname> level and for each
    task we write a unique clientlog, taskworkerlog, debugfiles tarball, which gets overwritten whenever e.g.
    log is updated.
    Instead sandboxes get their own directory and the usual hash as a name, so that they can be
    identified and reused across tasks

    These return a presigned URL for uploading files:
    GET: /crabserver/prod/cache?subresource=upload&objecttype=clientlog&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&objecttype=twlog&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&objecttype=runtimefiles&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&objecttype=debugfiles&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&objecttype=sandbox&tarballname=<hash>

    For objects other than sandbox, the same URL's with subresource=download instead of subresource=upload
    returns a presigned URL to download files, and with subresource=retrieve returns the actual object
    e.g.
    GET: /crabserver/prod/cache?subresource=download&objecttype=twlog&taskname=<task>
    GET: /crabserver/prod/cache?subresource=download&objecttype=runtimefiles&taskname=<task>
    GET: /crabserver/prod/cache?subresource=retrieve&objecttype=clientlog&taskname=<task>

    Behavior is different for sandboxes since those are uploaded before taskname is knwon
    and therefore must be located via username+tarballname.
    GET: /crabserver/prod/cache?subresource=download&objecttype=sandboxg&username=<user>&tarballname=<name>
    Same for subresource=retreive, but using retrieve makes little sense for sandboxes.
    The tarballname is created during crab submit and stored in DB TASKS table in columns
     tm_user_sandbox or tm_debug_files

    These retun information about usage:
    GET: /crabserver/prod/cache?subresource=list&username=<username>&objecttype=<objecttype>
    GET: /crabserver/prod/cache?subresource=used&username=<username>

    AUTHORIZATION
    Users will only be able to get an upload PreSigned URL for their tasks.
    CRAB operators (TaskWorker e.g.!) will be able to upload anything
    Retrieve and donwload will be restricted to owners and CRAB operators.
    In cases where logs or debug files need to be shared, the caller
    can share the PreSignedUrl obtained via the download subresource.
    Information about usage will be available to any authenticated user.
    """

    def __init__(self, app, api, config, mount, extconfig):
        RESTEntity.__init__(self, app, api, config, mount)
        self.logger = logging.getLogger("CRABLogger.RESTCache")
        # get S3 connection secrets from the CRABServerAuth file in the same way
        # as done for DB connection secrets. That file needs to contain an "s3"
        # dictionary with keys: access_key, secret_key
        # and config.py file for crabserver needs to point to it via the line
        # data.s3 = 'CRABServerAuth.s3'
        # following lines are copied from
        # https://github.com/dmwm/WMCore/blob/77a1ae719757a1eef766f8fb0c9f29ce6fcd2275/src/python/WMCore/REST/Server.py#L1735
        modname, item = config.s3.rsplit(".", 1)
        module = __import__(modname, globals(), locals(), [item])
        s3Dict = getattr(module, item)
        access_key = s3Dict['access_key']
        secret_key = s3Dict['secret_key']

        # in order to use S3 based CRABCache the cacheSSL config. param in rest external config
        # must be set to "endpoint/bucket" e.g. https://s3.cern.ch/<bucketname>
        cacheSSL = extconfig.centralconfig['backend-urls']['cacheSSL']
        # make sure any trailing '/' in the cacheSSL url does not end in the bucket name
        cacheSSL = cacheSSL.rstrip('/')
        bucket = cacheSSL.split('/')[-1]
        endpoint = 'https://s3.cern.ch'  # hardcode this. In case it can be moved to the s3Dict in config
        self.s3_bucket = bucket
        self.s3_client = boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=access_key,
                                      aws_secret_access_key=secret_key)

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_CACHE, optional=False)
            validate_str('objecttype', param, safe, RX_CACHE_OBJECTTYPE, optional=True)
            validate_str('taskname', param, safe, RX_TASKNAME, optional=True)
            validate_str('username', param, safe, RX_USERNAME, optional=True)
            validate_str('tarballname', param, safe, RX_TARBALLNAME, optional=True)

    @restcall
    def get(self, subresource, objecttype, taskname, username, tarballname):  # pylint: disable=redefined-builtin
        """
           :arg str subresource: the specific information to be accessed;
        """
        authenticatedUserName = cherrypy.request.user['login']  # the username of who's calling
        # a bit of code common to 3 subresource's: validate args and prepare the s3_objectKey inside the bucket
        if subresource in ['upload', 'retrieve', 'download']:
            if not objecttype:
                raise MissingParameter("objecttype is missing")
            if objecttype == 'sandbox':
                if not tarballname:
                    raise MissingParameter("tarballname is missing")
                ownerName = authenticatedUserName if subresource=='upload' else username
                # sandbox goes in bucket/username/sandboxes/
                objectPath = ownerName + '/sandboxes/' + tarballname
            else:
                if not taskname:
                    raise MissingParameter("takskname is missing")
                ownerName = getUsernameFromTaskname(taskname)
                # task related files go in bucket/username/taskname/
                objectPath = ownerName + '/' + taskname + '/' + objecttype
            s3_objectKey = objectPath

        if subresource == 'upload':
            # returns a dictionary with the information to upload a file with a POST
            # via a "PreSigned URL". It can return  an empty string '' as URL to indicate that
            # a sandbox upload request refers to an existing object with same name
            # WMCore REST does not allow to return None
            authz_operator(username=ownerName, group='crab3', role='operator')
            if objecttype == 'sandbox':
                # we only upload same sandbox once
                alreadyThere = False
                try:
                    with MeasureTime(self.logger, modulename=__name__, label="get.upload.head_object") as _:
                        # from https://stackoverflow.com/a/38376288
                        self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_objectKey)
                    alreadyThere = True
                except ClientError:
                    pass
                if alreadyThere:
                    return ["", {}]  # this tells client not to upload
            expiration = 60 * 60  # 1 hour is good for retries and debugging
            try:
                with MeasureTime(self.logger, modulename=__name__, label="get.upload.generate_presigned_post") as _:
                    response = self.s3_client.generate_presigned_post(
                        self.s3_bucket, s3_objectKey, ExpiresIn=expiration)
                # this returns a dictionary like:
                # {'url': u'https://s3.cern.ch/bucket1',
                # 'fields': {'policy': u'eyJjb ... jEzWiJ9', # policy is a 164-char-long string
                # 'AWSAccessKeyId': u'5d4270f1e022442783646c34cf552d55',
                # 'key': objectPath, 'signature': u'pm58cUqxNQHBZXS1B/Er6P89IhU='}}
                # need to build a single URL string to return
                preSignedUrl = response
            except ClientError as e:
                raise ExecutionError("Connection to s3.cern.ch failed:\n%s" % str(e))
            # somehow it does not work to return preSignedUrl as a single object
            return [preSignedUrl['url'], preSignedUrl['fields']]

        if subresource == 'download':
            authz_operator(username=ownerName, group='crab3', role='operator')
            if subresource=='sandbox' and not username:
                raise MissingParameter("username is missing")
            # returns a PreSignedUrl to download the file within the expiration time
            expiration = 60 * 60  # 1 hour default is good for retries and debugging
            if objecttype in ['debugfiles', 'clientlog', 'twlog']:
                expiration = 60*60 * 24 * 30 # for logs make url valid as long as we keep files (1 month)
            try:
                with MeasureTime(self.logger, modulename=__name__, label="get.download.generate_presigned_post") as _:
                    response = self.s3_client.generate_presigned_url('get_object',
                                            Params={'Bucket': self.s3_bucket, 'Key': s3_objectKey},
                                            ExpiresIn=expiration)
                preSignedUrl = response
            except ClientError as e:
                raise ExecutionError("Connection to s3.cern.ch failed:\n%s" % str(e))
            return preSignedUrl

        if subresource == 'retrieve':
            # download from S3 into a temporary file, read it, and return content to caller
            authz_operator(username=ownerName, group='crab3', role='operator')
            tempFile = '/tmp/boto.' + uuid.uuid4().hex
            try:
                with MeasureTime(self.logger, modulename=__name__, label="get.retrieve.download_file") as _:
                    self.s3_client.download_file(self.s3_bucket, s3_objectKey, tempFile)
            except ClientError as e:
                raise ExecutionError("Connection to s3.cern.ch failed:\n%s" % str(e))
            with open(tempFile) as f:
                txt = f.read()
            os.remove(tempFile)
            return txt

        if subresource == 'list':
            # list all files (aka objects, aka keys in S3 lingo) for a given usermame
            # if arg objecttype is present, returns only the file names for that objecttype
            if not username:
                raise MissingParameter('username is missing')
            # In S3 we always need to retrieve all keys even if some filtering/compression
            # will be applied before reporting, since there is a limit of 1K key per call,
            # multiple calls will be needed, S3 paginators make that easy
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
            # We use S3 prefix to limit retrieved list to a user, since in our buckets
            # file keys always have the form <username>/... see:
            # https://github.com/dmwm/CRABServer/wiki/CRABCache-replacement-with-S3#bucket-organization  and
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
            #
            fileNames = []
            with MeasureTime(self.logger, modulename=__name__, label="get.list.get_paginator") as _:
                paginator = self.s3_client.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': self.s3_bucket,
                                    'Prefix': username}
            page_iterator = paginator.paginate(**operation_parameters)
            for page in page_iterator:
                # strip the initial "username/" from the S3 key name
                namesInPage = [item['Key'].replace(username+'/', '', 1) for item in page['Contents']]
                fileNames += namesInPage
            if objecttype:
                filteredFileNames = [f for f in fileNames if objecttype in f]
                fileNames = filteredFileNames
            return fileNames

        if subresource == 'used':
            # return space used by username, in MBytes (rounded to integer)
            if not username:
                raise MissingParameter('username is missing')
            with MeasureTime(self.logger, modulename=__name__, label="get.used.get_paginator") as _:
                paginator = self.s3_client.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': self.s3_bucket,
                                    'Prefix': username}
            page_iterator = paginator.paginate(**operation_parameters)
            # S3 records object size in bytes, see:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
            usedBytes = 0
            for page in page_iterator:
                for item in page['Contents']:
                    usedBytes += item['Size']
            usedMBytes = usedBytes // 1024 // 1024
            # WMCore REST wants to return lists
            return [usedMBytes]
