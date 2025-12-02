
"""
The module contains some utility functions used by the various modules of the CRAB REST interface
"""
import os
from contextlib import contextmanager
from collections import namedtuple
from time import mktime, gmtime
import time
import subprocess
import io
import json
import copy
import pycurl
import cherrypy

from WMCore.WMFactory import WMFactory
from WMCore.REST.Error import ExecutionError, InvalidParameter
from WMCore.Services.pycurl_manager import ResponseHeader
from WMCore.REST.Server import RESTArgs

CMSSitesCache = namedtuple("CMSSitesCache", ["cachetime", "sites"])

# These parameters are set in the globalinit (called in RESTBaseAPI)
credServerPath = None


def getDBinstance(config, namespace, name):
    if config.backend.lower() == 'mysql':
        backend = 'MySQL'
    elif config.backend.lower() == 'oracle':
        backend = 'Oracle'

    # factory = WMFactory(name = 'TaskQuery', namespace = 'Databases.TaskDB.%s.Task' % backend)
    factory = WMFactory(name=name, namespace=f"Databases.{namespace}.{backend}.{name}")

    return factory.loadObject(name)


def execute_command(command, logger, timeout):
    """
    _execute_command_
    Function to manage commands.
    """

    stdout, stderr, rc = None, None, 99999
    proc = subprocess.Popen(
        command, shell=True, cwd=os.environ['PWD'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )

    t_beginning = time.time()
    seconds_passed = 0
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error(f"Timeout in {command} execution.")
            return stdout, rc

        time.sleep(0.1)

    stdout, stderr = proc.communicate()
    rc = proc.returncode

    logger.debug(f"Executing : \n command : {command}\n output : {stdout}\n error: {stderr}\n retcode : {rc}")

    return stdout, rc


# This is used in case git is down for more than 30 minutes
# NOTE BY STEFANO B: I do not like this global and the hacky way to
# have a fallback here. IMHO there should be a class which we instantiate
# for each configuration file that we want to retreive, with the fallback as
# a private variable. No need to expose cfgFallback to the outside.
# But at this time I am happy to have something which worke even if ugly.
# Another much needed cleanup would be to avoid using the same extConfCommon
# name for the structure loaded from gitlab and the differently formatted one
# done below, avoid the deletion of dictionary keys etc.
# But we can't spend all time cleaning up old code
#
global cfgFallback  # pylint: disable=global-statement, global-at-module-level
cfgFallback = {}


@contextmanager
def validate_dict(argname, param, safe, maxjsonsize=1024):
    """
    Provide context manager to validate kv of DictType argument.

    validate_dict first checks that if an argument named `argname` is
    JSON-like dict object, check if json-string exceeds `maxjsonsize`
    before deserialize with json.loads()

    Then, as contextmanager, validate_dict yield a tuple of RESTArgs
    (dictParam, dictSafe) and execute the block nested in "with" statement,
    which expected `validate_*` to validate all keys inside json, in the
 same way as
    `param`/`safe` do in DatabaseRESETApi.validate(), but against
    dictParam/dictSafe instead.

    If all keys pass validation, the dict object (not the string) is copied
    into `safe.kwargs` and the original string value is removed from
    `param.kwargs`. If not all keys are validated, it will raise an
    exception.

    Note that validate_dict itself does not support optional argument.

    Example in DatabaseRESTApi.validate() to validate "acceleratorparams"
    optional dict parameter with 1 mandatory and 2 optional key

    if param.kwargs.get("acceleratorparams", None):
        with validate_dict("acceleratorparams", param, safe) as (accParams, accSafe):
            custom_err = "Incorrect '{}' parameter. Parameter is also required when Site.requireAccelerator is True"
            validate_num("GPUMemoryMB", accParams, accSafe, minval=0, custom_err=custom_err.format("GPUMemoryMB"))
            validate_strlist("CUDACapabilities", accParams, accSafe, RX_CUDA_VERSION)
            validate_str("CUDARuntime", accParams, accSafe, RX_CUDA_VERSION, optional=True)
    else:
        safe.kwargs["acceleratorparams"] = None
    """

    val = param.kwargs.get(argname, None)
    if len(val) > maxjsonsize:
        raise InvalidParameter(f"Param is larger than {maxjsonsize} bytes")
    try:
        data = json.loads(val)
    except Exception as e:
        raise InvalidParameter("Param is not valid JSON-like dict object") from e
    if data is None:
        raise InvalidParameter("Param is not defined")
    if not isinstance(data, dict):
        raise InvalidParameter("Param is not a dictionary encoded as JSON object")
    dictParam = RESTArgs([], copy.deepcopy(data))
    dictSafe = RESTArgs([], {})
    yield (dictParam, dictSafe)
    if dictParam.kwargs:
        raise InvalidParameter(f"Excess keyword arguments inside keyword argument, not validated kwargs={{'{argname}': {dictParam.kwargs}}}")
    safe.kwargs[argname] = data
    del param.kwargs[argname]
