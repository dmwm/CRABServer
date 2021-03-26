import os
import uuid
import logging
import boto3
from botocore.exceptions import ClientError

# WMCore dependecies here
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str
from WMCore.REST.Error import MissingParameter, ExecutionError, InvalidParameter

# CRABServer dependecies here
from CRABInterface.RESTExtensions import authz_login_valid
from CRABInterface.Regexps import RX_SUBRES_CACHE, RX_CACHE_OBJECT, RX_TASKNAME, RX_USERNAME
from ServerUtilities import getUsernameFromTaskname

class RESTCache(RESTEntity):
    """
    REST entity for accessing CRAB Cache on S3
    Supports only GET method

    These return a presigned URL for uploading files:
    GET: /crabserver/prod/cache?subresource=upload&object=clientlog&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&object=twlog&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&object=sandbox&taskname=<task>
    GET: /crabserver/prod/cache?subresource=upload&object=debugfiles&taskname=<task>
    Same URL's with subresource=retrieve instead of subresource=upload return the actual object
    e.g.
    GET: /crabserver/prod/cache?subresource=retrieve&object=clientlog&taskname=<task>

    These retun information about usage
    GET: /crabserver/prod/cache?subresource=list&username=<username>
    GET: /crabserver/prod/cache?subresource=used&username=<username>

    AUTHORIZATION
    users will only be able to get an upload PS URL for their tasks
    retieve of sandbox will be restricted to owners
    retrieve of logs and debut files will be free
    CRAB operators will have full access
    """

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.logger = logging.getLogger("CRABLogger:RESTCache")

        # TODO: read these from secret file
        access_key="5d4270f1e022442783646c34cf552d55"
        secret_key="310e1af0fe7a43f1a2477fb77e3a5101"
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
            validate_str('object', param, safe, RX_CACHE_OBJECT, optional=True)
            validate_str('taskname', param, safe, RX_TASKNAME, optional=True)
            validate_str('username', param, safe, RX_USERNAME, optional=True)

    @restcall
    def get(self, subresource, object, taskname, username):
        """Retrieves the server information, like delegateDN, filecacheurls ...
           :arg str subresource: the specific server information to be accessed;
        """

        if subresource == 'upload':
            if not object:
                raise MissingParameter("object to upload is missing")
            if not taskname:
                raise MissingParameter("takskname is missing")
            ownerName = getUsernameFromTaskname(taskname)
            # TODO add code here to check that username has authorization
            objectPath = ownerName + '/' + taskname + '/' + object
            expiration = 3600
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
                print("S3 error: %s", str(e))
                raise ExecutionError("Connection to s3.cern.ch failed")
            with open(tempFile) as f:
                txt = f.read()
            os.remove(tempFile)

            return txt

        if subresource == 'list':
            if not username:
                raise MissingParameter('username is missing')
            response = self.s3_client.list_objects(Bucket=self.s3_bucket)
            fullList = [object['Key'] for object in response['Contents']]
            return fullList

        if subresource == 'used':
            if not username:
                raise MissingParameter('username is missing')
            usedSpace = '218 MBytes'
            return usedSpace
