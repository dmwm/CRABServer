#!/usr/bin/python

from __future__ import print_function, division

import os
import sys
import time
import json
import atexit
import pycurl
import signal
import logging
import subprocess
from httplib import HTTPException
from logging.handlers import TimedRotatingFileHandler

from WMCore.WMException import WMException
from WMCore.Services.WMArchive.WMArchive import WMArchive

from datetime import date

#sudo -u condor sh -c 'export LD_LIBRARY_PATH=/data/srv/SubmissionInfrastructureScripts/; export PYTHONPATH=/data/srv/SubmissionInfrastructureScripts/WMCore/src/python; python /data/srv/SubmissionInfrastructureScripts/WMArchiveUploaderNew.py'

class Daemon(object):
    """
    A generic daemon class.
       
    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
       
    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
       
        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)
       
        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError as e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
       
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
       
        # write pidfile
        atexit.register(self.removePidfile)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)
       
    def removePidfile(self):
        os.remove(self.pidfile)
 
    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
       
        if pid:
            processes = subprocess.Popen(['pgrep', '-f', 'WMArchiveUploader'], stdout=subprocess.PIPE, shell=False)
            response = processes.communicate()[0]
            listOfProcesses = [int(processes) for processes in response.split()]

            if pid in listOfProcesses:
                message = "pidfile %s already exist and process is running.\n"
                sys.stderr.write(message % self.pidfile)
                sys.exit(1)
            else:
                self.removePidfile()           

        # Start the daemon
        self.daemonize()
        try:
            self.run()
        except Exception: #pylint: disable=broad-except
            logger = logging.getLogger()
            logger.exception("Unhandled exception. Exiting.")
 
    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
       
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart
 
        # Try killing the daemon process       
        try:
            while True:
                os.kill(pid, signal.SIGTERM)
                time.sleep(1)
        except OSError as err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(str(err))
                sys.exit(1)
 
    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()
 
    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """


class QuietExit(Exception):
    """ Simple exception used as a message from functions to the main. What it means is that the inner function
        already handled the error and the main is not printing any info.
    """
    pass


class WMArchiveUploader(Daemon):
    
    def __init__(self): #pylint: disable=super-init-not-called
        #  super(WMArchiveUploader, self).__init__("/data/srv/SubmissionInfrastructureScripts/WMArchiveWD/WMArchiveUploader.lock")    
        self.baseDir = None
        self.uploadCert = None
        self.uploadKey = None
        self.wmarchiveURL = None
        self.bulksize = 200
        
        self.logName = "wmarchiveprocess.log"
        self.newFjrDir = 'new'
        self.processedFjrDir = 'processed'
        self.stopFlag = False
        
    def loadConf(self):
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
        logger = logging.getLogger()
        try:
            with open("/etc/wmarchive.json") as fd:
                conf = json.load(fd)
        except:
            logger.exception("Cannot load configuration file /etc/wmarchive.json")
            logger.info(WMArchiveUploader.loadConf.__doc__)
            raise QuietExit
        self.baseDir = str(conf["BASE_DIR"])
        self.wmarchiveURL = str(conf["WMARCHIVE_URL"])
        self.uploadKey = str(conf["UPLOAD_KEY"])
        self.uploadCert = str(conf["UPLOAD_CERT"])
        self.bulksize = int(conf["BULK_SIZE"])

        super(WMArchiveUploader, self).__init__(os.path.join(self.baseDir, "WMArchiveUploader.lock"))
    
    def addFileToLog(self):
        """ Now that we loaded the conf and we know the working directory setup the file to log to
        """
        logger = logging.getLogger()
        logfname = os.path.join(self.baseDir, self.logName)
        handler = TimedRotatingFileHandler(logfname, 'midnight', backupCount=30)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def checkIfFolderExists(self):
        dirName = date.today().strftime('%Y-%m-%d')
        fullPathName = os.path.join(self.baseDir, self.processedFjrDir, dirName)
        if not os.path.isdir(fullPathName):
            os.mkdir(fullPathName)
        return dirName

    def quit(self, dummyCode, dummyTraceback):
        logger = logging.getLogger()
        logger.info("quit called")
        self.stopFlag = True
    
    def run(self):
        """ Main loop
        """
        signal.signal(signal.SIGTERM, self.quit)

        logger = logging.getLogger()
        logger.info("Starting main loop")
    
        wmarchiver = WMArchive(self.wmarchiveURL, {'pycurl' : True, "key" : self.uploadKey, "cert" : self.uploadCert})
        while not self.stopFlag:
            reports = os.listdir(os.path.join(self.baseDir, self.newFjrDir))
            currentReps = sorted(reports[:self.bulksize])
            logger.debug("Current reports are %s", currentReps)
            docs = []
    
            if currentReps:
                for rep in currentReps:
                    repFullname = os.path.join(self.baseDir, self.newFjrDir, rep)
                    with open(repFullname) as fd:
                        #Some params have to be int, see https://github.com/dmwm/CRABServer/issues/5578
                        #TODO Remove the following 4 lines once we are sure old task are not in the system
                        tmpdoc = json.load(fd)
                        for step in tmpdoc["steps"]:
                            for key in ('NumberOfThreads', 'NumberOfStreams'):
                                if key in step["performance"]["cpu"]:
                                    step["performance"]["cpu"][key] = int(float(step["performance"]["cpu"][key]))                        
                            for key in ('TotalInitTime', 'TotalInitCPU'):
                                if key in step["performance"]["cpu"]:
                                    step["performance"]["cpu"][key] = float(step["performance"]["cpu"][key])                        

                        docs.append(tmpdoc)
  
                try:
                    response = wmarchiver.archiveData(docs)
                except (pycurl.error, HTTPException, WMException) as e:
                    logger.error("Error uploading docs: %s", e)
                    time.sleep(60)
                    continue
    
                # Partial success is not allowed either all the insert is successful or none is
                if response and response[0]['status'] == "ok" and len(response[0]['ids']) == len(docs):
                    logger.info("Successfully uploaded %d docs", len(docs))
                    for rep in currentReps:
                        repFullname = os.path.join(self.baseDir, self.newFjrDir, rep)
                        repDestName = os.path.join(self.baseDir, self.processedFjrDir, self.checkIfFolderExists(), rep)
                        os.rename(repFullname, repDestName)
                else:
                    logger.warning("Upload failed and it will be retried in the next cycle: %s: %s.",
                                    response[0]['status'], response[0]['reason'])
                    time.sleep(60)
            else:
                time.sleep(60)
        logger.info("Exiting")


def logSetup():
    """ Function that set up the log framework. It is called right at the start at the script when
        the working dir is not known and we can't create a logfile, so what it does is creating
        a simple logger that prints things on the screen
    """
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s", level=logging.DEBUG)


if __name__ == '__main__':
    logSetup()
    try:
        daemon = WMArchiveUploader()
        daemon.loadConf()
        daemon.addFileToLog()

        if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                daemon.start()
            elif 'stop' == sys.argv[1]:
                daemon.stop()
            elif 'restart' == sys.argv[1]:
                daemon.restart()
            else:
                print("Unknown command")
                sys.exit(2)
            sys.exit(0)
        else:
            print("usage: %s start|stop|restart" % sys.argv[0])
            sys.exit(2)    
    except QuietExit:
        #Don't do anything since this has been handled by the inner function
        pass

