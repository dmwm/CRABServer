from __future__ import division
import os
import uuid
import logging
import boto3
from botocore.exceptions import ClientError

# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str
from WMCore.REST.Error import MissingParameter, ExecutionError

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_SUBRES_CACHE, RX_CACHE_OBJECT, RX_TASKNAME, RX_USERNAME, RX_CACHENAME
from ServerUtilities import getUsernameFromTaskname

class RESTCache(RESTEntity):
    """
    REST entity for accessing CRAB Cache on S3
    Supports only GET method

    As per the S3 buckt structure in https://github.com/dmwm/CRABServer/wiki/CRABCache-replacement-with-S3
    objects in S3 are always under a <username> prefix. The there is a <taskname> level and for each
    task we write a unique clientlog, taskworkerlog, debugfiles tarball, which gets overwritten whenever e.g.
    log is updated.
    Instead sandboxes get their own directory and the usual hash as a name, so that they can be
    identified and reused across tasks

    These return a presigned URL for uploading files:
    GET: /crabserver/prod/cache?subresource=upload&object=clientlog&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&object=twlog&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&object=debugfiles&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&object=sandbox&cachename=<hash>
    Same URL's with subresource=retrieve instead of subresource=upload return the actual object
    e.g.
    GET: /crabserver/prod/cache?subresource=retrieve&object=clientlog&taskname=<task>

    These retun information about usage
    GET: /crabserver/prod/cache?subresource=list&username=<username>&object=<object>
    GET: /crabserver/prod/cache?subresource=used&username=<username>

    AUTHORIZATION
    users will only be able to get an upload PreSigned URL for their tasks
    retrieve of sandbox will be restricted to owners
    retrieve of logs and debug files will be free
    CRAB operators will have full access
    """

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.logger = logging.getLogger("CRABLogger:RESTCache")

        # TODO: read these from secret file
        access_key = "5d4270f1e022442783646c34cf552d55"
        secret_key = "310e1af0fe7a43f1a2477fb77e3a5101"
        # TODO take this from rest config json file ? worry about transition from old to new !
        endpoint = 'https://s3.cern.ch'
        self.s3_client = boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=access_key,
                                      aws_secret_access_key=secret_key, verify=False)
        self.s3_bucket = 'bucket1'

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_CACHE, optional=False)
            validate_str('object', param, safe, RX_CACHE_OBJECT, optional=False)
            validate_str('taskname', param, safe, RX_TASKNAME, optional=True)
            validate_str('username', param, safe, RX_USERNAME, optional=True)
            validate_str('cachename',param, safe, RX_CACHENAME, optional=True)

    @restcall
    def get(self, subresource, object, taskname, username, cachename):  # pylint: disable=redefined-builtin
        """
           :arg str subresource: the specific information to be accessed;
        """

        if subresource == 'upload':
            # returns a dictionary with the information to upload a file with a POST
            # via a "PreSigned URL"
            if not object:
                raise MissingParameter("object to upload is missing")
            if not taskname:
                raise MissingParameter("takskname is missing")
            if object == 'sandbox' and not cachename:
                raise MissingParameter("cachename is missing")
            ownerName = getUsernameFromTaskname(taskname)
            # TODO add code here to check that username has authorization
            objectPath = ownerName + '/' + taskname + '/' + object
            if object == 'sandbox':
                objectPath += '/' + cachename
            expiration = 3600  # 1 hour is good for testing
            try:
                response = self.s3_client.generate_presigned_post(
                    self.s3_bucket, objectPath, ExpiresIn=expiration)
                # this returns a dictionary like:
                # {'url': u'https://s3.cern.ch/bucket1',
                # 'fields': {'policy': u'eyJjb ... jEzWiJ9', # policy is a 164-char-long string
                # 'AWSAccessKeyId': u'5d4270f1e022442783646c34cf552d55',
                # 'key': objectPath, 'signature': u'pm58cUqxNQHBZXS1B/Er6P89IhU='}}
                # need to build a single URL string to return
                preSignedUrl = response
            except:
                raise ExecutionError("Connection to s3.cern.ch failed")
            # somehow it does not work to return preSignedUrl as a single object
            return [preSignedUrl['url'], preSignedUrl['fields']]

        if subresource == 'retrieve':
            # downloads a file from S3 to /tmp and serves it to the client
            if not object:
                raise MissingParameter("object to upload is missing")
            if not taskname:
                raise MissingParameter("takskname is missing")
            ownerName = getUsernameFromTaskname(taskname)
            # TODO insert here code to check if username is authorized to read this object
            objectPath = ownerName + '/' + taskname + '/' + object
            # since taskname and object come from WMCore validate_str they are newbytes objects
            # and type(objectPath) is <class 'future.types.newbytes.newbytes'> which breaks
            # python2 S3 transfer code with a keyError
            # here's an awful hack to turn those newbytes into an old-fashioned py2 string
            objectName = ''  # a string
            for i in objectPath:  # subscripting a newbytes string gets tha ASCII value of the char !
                objectName += chr(i)  # from ASCII numerical value to python2 string

            # download from S3 into a temporary file, read it, and return content to caller
            tempFile = '/tmp/boto.' + uuid.uuid4().hex
            try:
                self.s3_client.download_file(self.s3_bucket, objectName, tempFile)
            except ClientError as e:
                raise ExecutionError("Connection to s3.cern.ch failed:\n%s" % str(e))
            with open(tempFile) as f:
                txt = f.read()
            os.remove(tempFile)

            return txt

        if subresource == 'list':
            # list all files (aka objects, aka keys in S3 lingo) for a given usermame
            # if arg object is present, returns only the file names for that object
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
            paginator = self.s3_client.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': self.s3_bucket,
                                    'Prefix': username}
            page_iterator = paginator.paginate(**operation_parameters)
            for page in page_iterator:
                namesInPage = [item['Key'].lstrip(username+'/') for item in page['Contents']]
                fileNames += namesInPage
            if object:
                filteredFileNames = [f for f in fileNames if object in f]
                fileNames = filteredFileNames
            return fileNames

        if subresource == 'used':
            # return spare used by username, in MBytes (rounded to integer)
            if not username:
                raise MissingParameter('username is missing')
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
            usedMBytes = usedBytes / 1024 / 1024
            return usedMBytes
