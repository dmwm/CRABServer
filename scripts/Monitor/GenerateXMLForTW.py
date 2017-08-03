from __future__ import print_function
import os
import sys
import time
import json
import urllib
import logging
import classad
import htcondor
import traceback
import subprocess
from datetime import datetime
from httplib import HTTPException
from socket import gethostname
from pprint import pprint

from RESTInteractions import HTTPRequests
from WMCore.Configuration import loadConfigurationFile

from TaskWorker.__init__ import __version__ as __tw__version__

fmt = "%Y-%m-%dT%H:%M:%S%z"


class CRAB3CreateXML(object):

    def __init__(self, resthost, xmllocation, logger=None):
        if not logger:
            self.logger = logging.getLogger(__name__)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logger

        self.xmllocation = xmllocation
        self.resthost = resthost
        self.pool = ''
        self.schedds = []
        self.resthost = "cmsweb.cern.ch"
        # use child collector on port 9620 to get schedd attributes
        collName = "cmsgwms-collector-global.cern.ch:9620,cmssrv221.fnal.gov:9620"
        self.coll = htcondor.Collector(collName)
        #self.restInstance = "prod" # this is to prepare for monitoring other cmsweb instances
        #self.restUri = "/crabserver/%/task"%self.restInstance

    def getCountTasksByStatus(self):
        try:
            resturi = "/crabserver/prod/task"
            # resturi = self.restUri%self.restInstance  # this is to prepare for monitoring other cmsweb instances 
            configreq = { 'minutes': "120", 'subresource': "counttasksbystatus" }
            server = HTTPRequests(self.resthost, "/data/certs/backup-service-certs/servicecert.pem", "/data/certs/backup-service-certs/servicekey.pem", retry = 3)
            result = server.get(resturi, data = configreq)
            return dict(result[0]['result'])
        except Exception:
            e = sys.exc_info()
            if hasattr(e,"headers"):
                self.logger.error(str(e.headers))
            self.logger.debug("Error in getCountTasksByStatus:")
            pprint(e[1])
            traceback.print_tb(e[2])
            return []

    def getCountTasksByStatusAbs(self):
        try:
            resturi = "/crabserver/prod/task"
            configreq = { 'minutes': "1000000000", 'subresource': "counttasksbystatus" }
            server = HTTPRequests(self.resthost, "/data/certs/servicecert.pem", "/data/certs/servicekey.pem", retry = 10)
            result = server.get(resturi, data = configreq)
            return dict(result[0]['result'])
        except Exception:
            e = sys.exc_info()
            if hasattr(e,"headers"):
                self.logger.error(str(e.headers))
            self.logger.exception("Error in getCountTasksByStatusAbs:")
            pprint(e[1])
            traceback.print_tb(e[2])
            return []

    def getScheddsInfo(self):
        data = []
        try:
            #coll=htcondor.Collector(collName)                                               
            result = self.coll.query(htcondor.AdTypes.Schedd,'CMSGWMS_Type=?="crabschedd"',['Name','ShadowsRunning','TotalSchedulerJobsRunning','TotalIdleJobs','TotalRunningJobs','TotalHeldJobs'])
            for schedd in result:  # oneshadow[0].split('@')[1].split('.')[0]
                data.append([schedd['Name'],schedd['ShadowsRunning'],schedd['TotalSchedulerJobsRunning'],schedd['TotalIdleJobs'],schedd['TotalRunningJobs'],schedd['TotalHeldJobs']])
        except  Exception as e:
            self.logger.debug("Error in getShadowsRunning: %s"%str(e))
        return data

    def execute(self):
        from xml.etree.ElementTree import Element, SubElement, tostring
        root = Element('serviceupdate')
        root.set( "xmlns",  "http://sls.cern.ch/SLS/XML/update")
        child = SubElement(root, "id")
        child.text = gethostname().split('.')[0]

        subprocesses_config = 6 #  In this case 5 + 1 MasterWorker process
        sub_grep_command="ps -ef | grep MasterWorker | grep -v 'grep' | wc -l"
        # If any subprocess is dead or not working, modify percentage of availability
        # If subprocesses are not working - service availability 0%
        process_count = int(subprocess.Popen(sub_grep_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read())

        # Get current time
        now_utc = datetime.now().strftime(fmt)
        child_timestamp = SubElement(root, "timestamp")
        child_timestamp.text = str(now_utc)

        child_status = SubElement(root,"status")

        if subprocesses_config == process_count:
            # This means that everything is fine
            child_status.text = "available"
        else:
            child_status.text = "degraded"


        #child_status.text = "available"

        # print "TaskWorker version: %s "%__tw__version__ # does not work yet

        # Get the number of tasks per status
        twStatus = self.getCountTasksByStatus()
        data = SubElement(root, "data")

        if len(twStatus) > 0:
            for name in ['SUBMITTED', 'FAILED', 'QUEUED', 'NEW', 'KILLED', 'KILLFAILED', 'RESUBMITFAILED', 'SUBMITFAILED']:
                numericval = SubElement(data, "numericvalue")
                numericval.set("name","number_of_%s_tasks_in_the_last_minute"%(name))
                if name in twStatus:
                    numericval.text = str(twStatus[name])
                else:
                    numericval.text = '0'
        else:
            for name in ['SUBMITTED', 'FAILED', 'QUEUED', 'NEW', 'KILLED', 'KILLFAILED', 'RESUBMITFAILED', 'SUBMITFAILED']:
                numericval = SubElement(data, "numericvalue")
                numericval.set("name","number_of_%s_tasks_in_the_last_minute"%(name))
                numericval.text = 'n/a'
 

        # get the absolut number of tasks per status
        twStatus = self.getCountTasksByStatusAbs()

        if len(twStatus) > 0:
            for name in ['KILL', 'RESUBMIT', 'NEW', 'QUEUED', 'KILLFAILED', 'RESUBMITFAILED', 'SUBMITFAILED', 'UPLOADED']:
                numericval = SubElement(data, "numericvalue")
                numericval.set("name","numberOfTasksIn_%s_State"%(name))
                if name in twStatus:
                    numericval.text = str(twStatus[name])
                else:
                    numericval.text = '0'
        else:
            for name in ['KILL', 'RESUBMIT', 'NEW', 'QUEUED', 'KILLFAILED', 'RESUBMITFAILED', 'SUBMITFAILED', 'UPLOADED']:
                numericval = SubElement(data, "numericvalue")
                numericval.set("name","numberOfTasksIn_%s_State"%(name))
                numericval.text = 'n/a'

        # get the number of condor_shadown process per schedd
        ListOfSchedds = self.getScheddsInfo()
	totalRunningTasks = 0
	totalIdleTasks = 0
	totalRunningTP = 0
        # see https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
        pickSchedulerIdle = 'JobUniverse==7 && JobStatus==1'
        pickSchedulerRunning = 'JobUniverse==7 && JobStatus==2'
        pickLocalRunning = 'JobUniverse==12 && JobStatus==2'

        if len(ListOfSchedds) > 0:
            for oneSchedd in ListOfSchedds:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_shadows_process_for_%s"%(oneSchedd[0]))
                numericval.text = str(oneSchedd[1])

            for oneSchedd in ListOfSchedds:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_schedulers_jobs_running_for_%s"%(oneSchedd[0]))
                numericval.text = str(oneSchedd[2])
		totalRunningTasks += oneSchedd[2]

                scheddName = oneSchedd[0]
                # if one schedd does not answer, go on and try the others
                try:
                  scheddAdd = self.coll.locate(htcondor.DaemonTypes.Schedd,scheddName)
                except:
                  continue 
                schedd = htcondor.Schedd(scheddAdd)
                try: 
                  idleDags = list(schedd.xquery(pickSchedulerIdle))
                except:
                  idleDags = []
                  pass
                try:
                  runningTPs = list(schedd.xquery(pickLocalRunning))
                except:
                  runningTPs = []
                  pass
                numDagIdle = len(idleDags)
                numTPRun = len(runningTPs)
                totalIdleTasks += numDagIdle
                totalRunningTP += numTPRun

            for oneSchedd in ListOfSchedds:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_idle_jobs_for_at_%s"%(oneSchedd[0]))
                numericval.text = str(oneSchedd[3])

            for oneSchedd in ListOfSchedds:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_running_jobs_running_for_at_%s"%(oneSchedd[0]))
                numericval.text = str(oneSchedd[4])
                 
            for oneSchedd in ListOfSchedds:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_held_jobs_for_at_%s"%(oneSchedd[0]))
                numericval.text = str(oneSchedd[5])

	numericval = SubElement(data,"numericvalue")
        numericval.set("name","totalRunningTasks")
        numericval.text = str(totalRunningTasks)

        numericval = SubElement(data,"numericvalue")
        numericval.set("name","totalIdleTasks")
        numericval.text = str(totalIdleTasks)

        numericval = SubElement(data,"numericvalue")
        numericval.set("name","totalRunningTPs")
        numericval.text = str(totalRunningTP)


        # Write all this information to a temp file and move to correct location
        temp_xmllocation = self.xmllocation + ".temp"
        try:
            with open(temp_xmllocation, 'w') as f:
                f.write(tostring(root))
            os.system('mv %s %s' % (temp_xmllocation, self.xmllocation))
        except Exception as e:
            self.logger.debug(str(e))

if __name__ == '__main__':
    """ Simple main to execute the action standalon. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change resthost, resturi, and twconfig.
        If you want to monitor your own machine, you have to enable it in puppet configuration.
    """

    start_time = time.time()
    resthost = 'cmsweb.cern.ch'
    xmllocation = '/home/crab3/CRAB3_SCHEDD_XML_Report2.xml'
    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)

    pr = CRAB3CreateXML( resthost, xmllocation, logger)

# before running make sure no other instance of this script is running
    lockFile = '/home/crab3/CRAB3_SCHEDD_XML.Lock'
    if os.path.isfile(lockFile) :
       print ("%s already exists, abandon this run" % lockFile)
       exit()
    else:
       open(lockFile,'wa').close()  # create the lock
    pr.execute()

    # push the XML to elasticSearch 
    cmd = "curl -i -F file=@%s xsls.cern.ch" % xmllocation
    try:
        pu = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception as e:
        logger.debug(str(e))
    end_time = time.time()
    elapsed = end_time - start_time
    now = time.strftime("%H:%M:%S", time.gmtime(end_time))
    elapsed_min="%3d:%02d" % divmod(elapsed,60)
    # uncomment following two lines to keep a log of how long this script takes
    #with open ("timing.log","a") as f:
    #    f.write("at %s took %s\n" % (now, elapsed_min))

    os.remove(lockFile)

