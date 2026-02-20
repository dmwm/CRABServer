"""
A standalone script to create the list of users to be assigned
to "highprio" accounting_group when submitted by TaskWorker
and keep it up to date in TW host VM in the local file
 /data/container/TaskWorker/groups/highPriorityUsers
This is designd to be run on a TW VM as a crontab by user crab3
"""

import os
import json
import logging
import sys
import time
import random
import requests


def getLogger():
    """
    create the logger object for this script
    from example in https://docs.python.org/3/howto/logging.html#configuring-logging
    """
    logger = logging.getLogger('GetHighPriorityUsersFromCric')

    ch = logging.StreamHandler()
    # add a hook for extended logging in case of debugging,
    # an overkill here, but.. just for Stefano to have an example handy
    if os.getenv('TRACE', None):
        logger.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.ERROR)
        ch.setLevel(logging.ERROR)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def getHighPrioUsersFromCRIC(logger=None):
    """
    get the list of high priority users from CRIC with retries
    at the moment it simply queries the CMS_CRAB_HighPrioUsers e-group
    If needed it can be extended to query multiple e-groups.
    arguments:
      cert,key : string : absolute path name for PEM file to use for authentication
      logger : a logging.logger object
    returns: a list of usernames
    """

    cricGroup = 'CMS_CRAB_HighPrioUsers'
    cricUrl = f"https://cms-cric.cern.ch//api/accounts/group/query/?json&name={cricGroup}"
    capath = '/etc/grid-security/certificates'
    cert = '/data/certs/robotcert.pem'
    key = '/data/certs/robotkey.pem'

    # make requests less verbose
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    if os.getenv('TRACE', None):
        logging.getLogger("urllib3").setLevel(logging.DEBUG)

    nRetries = 4
    msg = ""
    for i in range(nRetries + 1):
        try:
            logger.debug(f"Query CRIC. Try N. {i+1}")
            # make HTTP GET
            r = requests.get(url=cricUrl, cert=(cert, key), verify=capath, timeout=10)
            if not r.ok:
                exMsg = f"HTTP error from CRIC: code {r.status_code} reason: {r.reason}"
                exMsg += f" content\n {r.text}"
                raise RuntimeError(exMsg)
            # get JSON output and parse it
            highPrioUsers = []
            for user in r.json()[cricGroup]['users']:
                if not user['login']:
                    # in some odd cases it is possible to have an entry w/o username
                    continue
                highPrioUsers.append(user['login'])
        except Exception as ex:  # pylint: disable=broad-except
            if i < nRetries:
                sleeptime = 20 * (i + 1) + random.randint(-10, 10)
                msg += f"\nSleeping {sleeptime} seconds after error. Details:\n{ex}"
                time.sleep(sleeptime)
            else:
                # this was the last retry
                msg += "no more retries will be done. Exit with error"
                raise RuntimeError(msg) from ex
        else:
            break
    return highPrioUsers



def main():
    """ the main function """
    logger = getLogger()
    highPriorityUsersFile = '/data/container/TaskWorker/groups/highPriorityUsers.json'
    try:
        highPrioUsers = getHighPrioUsersFromCRIC(logger=logger)
    except Exception as ex:  # pylint: disable=broad-except
        msg = "Error when getting the high priority users list from CRIC." \
              "\nWill leave current one unchanged." \
              f" Error reason:\n{ex}"
        logger.error(msg)
        sys.exit(1)
    with open(highPriorityUsersFile, 'w', encoding='utf-8') as outfile:
        json.dump(highPrioUsers, outfile)
        logger.info(f"new file written: {highPriorityUsersFile}")


if __name__ == '__main__':
    main()
