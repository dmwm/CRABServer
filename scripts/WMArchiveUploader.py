from __future__ import print_function

import os
import sys
import time
import json
import signal
import logging
from logging.handlers import TimedRotatingFileHandler

from WMCore.Services.WMArchive.WMArchive import WMArchive

#sudo -u cms1921 sh -c 'export LD_LIBRARY_PATH=/data/srv/glidecondor/condor_local/spool/2904/0/cluster6052904.proc0.subproc0/; export PYTHONPATH=../WMCore/src/python/:$PYTHONPATH:/data/srv/glidecondor/condor_local/spool/2904/0/cluster6052904.proc0.subproc0/CRAB3.zip; python /afs/cern.ch/user/m/mmascher/repos/CRABServer/scripts/WMArchiveUploader.py'

BASE_DIR = None
UPLOAD_CERT = None
UPLOAD_KEY = None
WMARCHIVE_URL = None

LOGNAME = "wmarchiveprocess.log"
NEW_FJR_DIR = 'new'
PROCESSED_FJR_DIR = 'processed'
BULK_SIZE = 200
#PROCESSING_FJR_DIR = 'processing'

QUIT = False


class QuietExit(Exception):
    """ Simple exception used as a message from functions to the main. What it means is that the inner function
        already handled the error and the main is not printing any info.
    """
    pass


def loadConf():
    """ This is an example configuration file:
        {
            "BASE_DIR" : "/data/srv/glidecondor/condor_local/spool/2904/0/cluster6052904.proc0.subproc0/wmarchive/",
            "WMARCHIVE_URL" : "https://vocms013.cern.ch/wmarchive"
            "UPLOAD_CERT" : "/data/srv/glidecondor/condor_local/spool/2904/0/cluster6052904.proc0.subproc0/602e17194771641967ee6db7e7b3ffe358a54c59",
            "UPLOAD_KEY" : "/data/srv/glidecondor/condor_local/spool/2904/0/cluster6052904.proc0.subproc0/602e17194771641967ee6db7e7b3ffe358a54c59",
            "BULK_SIZE" : 20
        }
        BASE_DIR is the location where this script is going to chdir to. It will create a file named wmarchiveprocess.log for logging,
            and it will read reports from $BASE_DIR/new (This value is also read by the postjob which will save files there)
        WMARCHIVE_URL is the url used as a target to upload documents in WMArchive
        UPLOAD_CERT and UPLOAD_KEY are the certificates used to talk to cmsweb
        BULK_SIZE how many documents we upload every loop
    """
    global BASE_DIR
    global WMARCHIVE_URL
    global UPLOAD_KEY
    global UPLOAD_CERT
    global BULK_SIZE

    logger = logging.getLogger()
    try:
        with open("/etc/wmarchive.json") as fd:
            conf = json.load(fd)
    except:
        logger.exception("Cannot load configuration file /etc/wmarchive.json")
        logger.info(loadConf.__doc__)
        raise QuietExit
    BASE_DIR = str(conf["BASE_DIR"])
    WMARCHIVE_URL = str(conf["WMARCHIVE_URL"])
    UPLOAD_KEY = str(conf["UPLOAD_KEY"])
    UPLOAD_CERT = str(conf["UPLOAD_CERT"])
    BULK_SIZE = int(conf["BULK_SIZE"])


def logSetup():
    """ Function that set up the log framework. It is called right at the start at the script when
        the working dir is not known and we can't create a logfile, so what it does is creating
        a simple logger that prints things on the screen
    """
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s", level=logging.DEBUG)


def addFileToLog():
    """ Now that we loaded the conf and we know the working directory setup the file to log to
    """
    logger = logging.getLogger()
    logfname = os.path.join(BASE_DIR, LOGNAME)
    handler = TimedRotatingFileHandler(logfname, 'midnight', backupCount=30)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def quit():
    """ Catches kill signals and set up the variable to exit the script
    """
    global QUIT
    QUIT = True


def main():
    """ Main loop
    """

    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)

    loadConf()
    addFileToLog()

    logger = logging.getLogger()
    logger.info("Starting main loop")

    wmarchiver = WMArchive(WMARCHIVE_URL, {'pycurl' : True, "key" : UPLOAD_KEY, "cert" : UPLOAD_KEY})

    while not QUIT:
        reports = os.listdir(os.path.join(BASE_DIR, NEW_FJR_DIR))
        currentReps = sorted(reports[:BULK_SIZE])
        logger.debug("Current reports are %s" % currentReps)
        docs = []

        if docs:
            for rep in currentReps:
                repFullname = os.path.join(BASE_DIR, NEW_FJR_DIR, rep)
                with open(repFullname) as fd:
                    docs.append(json.load(fd))

            response = wmarchiver.archiveData(docs)

            # Partial success is not allowed either all the insert is successful or none is
            if response[0]['status'] == "ok" and len(response[0]['ids']) == len(docs):
                logger.info("Successfully uploaded %d docs", len(docs))
                for rep in currentReps:
                    repFullname = os.path.join(BASE_DIR, NEW_FJR_DIR, rep)
                    repDestName = os.path.join(BASE_DIR, PROCESSED_FJR_DIR, rep)
                    os.rename(repFullname, repDestName)
            else:
                logger.warning("Upload failed and it will be retried in the next cycle: %s: %s.",
                                response[0]['status'], response[0]['reason'])
        else:
            time.sleep(60)


if __name__ == '__main__':
    logSetup()
    try:
        main()
    except QuietExit:
        #Don't do anything since this has been handled by the inner function
        pass
    except:
        # Hopefully this will never happen, but we need to foresee this case and log it properly
        logger = logging.getLogger()
        logger.exception("Unexcpected error in main")
