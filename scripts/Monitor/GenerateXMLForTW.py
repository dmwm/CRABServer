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
from RESTInteractions import HTTPRequests
from socket import gethostname
from pprint import pprint
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
        collName = "cmsgwms-collector-global.cern.ch,cmssrv221.fnal.gov"
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

    def getShadowsRunning(self):
        #collName = "cmsgwms-collector-global.cern.ch"
        #collName = "cmssrv221.fnal.gov"
        #collName = "cmsgwms-collector-global.cern.ch,cmssrv221.fnal.gov"
        data = []
        try:
            #coll=htcondor.Collector(collName)                                               
            result = self.coll.query(htcondor.AdTypes.Schedd,'CMSGWMS_Type=?="crabschedd"',['Name','ShadowsRunning','TotalSchedulerJobsRunning','TotalIdleJobs','TotalRunningJobs','TotalHeldJobs'])
            for schedd in result:  # oneshadow[0].split('@')[1].split('.')[0]
                data.append([schedd['Name'],schedd['ShadowsRunning'],schedd['TotalSchedulerJobsRunning'],schedd['TotalIdleJobs'],schedd['TotalRunningJobs'],schedd['TotalHeldJobs']])
        except  Exception, e:
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
        # If any subproccess is dead or not working, modify percentage of availability
        # If subprocesses are not working - service availability 0%
        proccess_count = int(subprocess.Popen(sub_grep_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read())

        # Get current time
        now_utc = datetime.now().strftime(fmt)
        child_timestamp = SubElement(root, "timestamp")
        child_timestamp.text = str(now_utc)

        child_status = SubElement(root,"status")

        if subprocesses_config == proccess_count:
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
                if twStatus.has_key(name):
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
                if twStatus.has_key(name):
                    numericval.text = str(twStatus[name])
                else:
                    numericval.text = '0'
        else:
            for name in ['KILL', 'RESUBMIT', 'NEW', 'QUEUED', 'KILLFAILED', 'RESUBMITFAILED', 'SUBMITFAILED', 'UPLOADED']:
                numericval = SubElement(data, "numericvalue")
                numericval.set("name","numberOfTasksIn_%s_State"%(name))
                numericval.text = 'n/a'

        # get the number of condor_shadown process per schedd
        numberOfShadows = self.getShadowsRunning()
	totalRunningTasks = 0
	totalIdleTasks = 0
	totalRunningTP = 0
        pickSchedulerIdle = 'JobUniverse==7 && JobStatus==1'
        pickSchedulerRunning = 'JobUniverse==7 && JobStatus==2'
        pickLocalRunning = 'JobUniverse==12 && JobStatus==2'

        if len(numberOfShadows) > 0:
            for oneShadow in numberOfShadows:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_shadows_process_for_%s"%(oneShadow[0]))
                numericval.text = str(oneShadow[1])

            for oneShadow in numberOfShadows:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_schedulers_jobs_running_for_%s"%(oneShadow[0]))
                numericval.text = str(oneShadow[2])
		totalRunningTasks += oneShadow[2]

#STEFANO
# here I could fill totalIdleTasks if I had a way to retrieve
# the number of idle SchedulerJobs on a schedd :-(
# let's try
                scheddName = oneShadow[0]
                scheddAdd = self.coll.locate(htcondor.DaemonTypes.Schedd,scheddName)
                schedd = htcondor.Schedd(scheddAdd)
                idleDags = list(schedd.xquery(pickSchedulerIdle))
                #runningDags = list(schedd.xquery(pickSchedulerRunning))
                runningTPs =  list(schedd.xquery(pickLocalRunning))
                #numDagRun = len(runningDags)
                numDagIdle = len(idleDags)
                numTPRun = len(runningTPs)
                totalIdleTasks += numDagIdle
                totalRunningTP += numTPRun

#STEFANO
            for oneShadow in numberOfShadows:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_idle_jobs_for_at_%s"%(oneShadow[0]))
                numericval.text = str(oneShadow[3])

            for oneShadow in numberOfShadows:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_running_jobs_running_for_at_%s"%(oneShadow[0]))
                numericval.text = str(oneShadow[4])
                 
            for oneShadow in numberOfShadows:
                numericval = SubElement(data,"numericvalue")
                numericval.set("name","number_of_held_jobs_for_at_%s"%(oneShadow[0]))
                numericval.text = str(oneShadow[5])

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
        except Exception, e:
            self.logger.debug(str(e))

if __name__ == '__main__':
    """ Simple main to execute the action standalon. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change resthost, resturi, and twconfig.
        If you want to monitor your own machine, you have to enable it in puppet configuration.
    """
    resthost = 'cmsweb.cern.ch'
    #xmllocation = 'teste.xml'
    xmllocation = '/home/crab3/CRAB3_SCHEDD_XML_Report2.xml'
    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile

    pr = CRAB3CreateXML( resthost, xmllocation, logger)
    pr.execute()

    # push the XML to elasticSearch 
    cmd = "curl -i -F file=@/home/crab3/CRAB3_SCHEDD_XML_Report2.xml xsls.cern.ch"
    try:
        pu = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception, e:
        logger.debug(str(e))

