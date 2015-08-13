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
from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

fmt = "%Y-%m-%dT%H:%M:%S%z"

class CreateXML(BaseRecurringAction):
    pollingTime = 1 #minutes

    def _execute(self, resthost, resturi, config, task):
        renewer = CRAB3CreateXML(config, resthost, xmllocation, self.logger)
        renewer.execute()

class CRAB3CreateXML(object):

    def __init__(self, config, resthost, xmllocation, logger=None):
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
        self.config = config
        self.pool = ''
        self.schedds = []

    def getCountTasksByStatus(self):
        try:
            resthost = "cmsweb-testbed.cern.ch"
            resturi = "/crabserver/prod/task"
            configreq = { 'minutes': "120", 'subresource': "counttasksbystatus" }
            server = HTTPRequests(resthost, "/data/certs/servicecert.pem", "/data/certs/servicekey.pem", retry = 2)
            result = server.get(resturi, data = configreq)
            return dict(result[0]['result'])
        except Exception, e:
            self.logger.debug(str(e))
            return []

    def getQueueStats(self):
        collName = "vocms097.cern.ch"
        data = []
        try:
            coll=htcondor.Collector(collName)
            result = coll.query(htcondor.AdTypes.Schedd,'CMSGWMS_Type=?="crabschedd"',['Name','ShadowsRunning','TotalSchedulerJobsRunning','TotalIdleJobs','TotalRunningJobs','TotalHeldJobs'])
            for schedd in result:  # oneshadow[0].split('@')[1].split('.')[0]
                data.append([schedd['Name'].split('@')[1].split('.')[0],schedd['ShadowsRunning'],schedd['TotalSchedulerJobsRunning'],schedd['TotalIdleJobs'],schedd['TotalRunningJobs'],schedd['TotalHeldJobs']])
        except  Exception, e:
            self.logger.debug(str(e))
        return data

    def execute(self):
        from xml.etree.ElementTree import Element, SubElement, tostring
        if not hasattr(self.config.TaskWorker, 'XML_Report_ID'):
            self.logger.error("Not generating xml because no XML_Report_ID exists in TaskWorker configuration")
            return
        root = Element('serviceupdate')
        child = SubElement(root, "id")
        child.text = self.config.TaskWorker.XML_Report_ID
        # Get the number of task per status
        twStatus = self.getCountTasksByStatus()
        data = SubElement(root, "data")
        group= SubElement(data, "grp")
        group.set("name", "Crab3 Tasks by Status")

        if len(twStatus) > 0:
            for name in ['SUBMITTED', 'FAILED', 'QUEUED', 'NEW', 'KILLED']:
                numericval = SubElement(group,"numericvalue")
                numericval.set("name",name.lower())
                numericval.set("desc","number of %s tasks in the last minute"%(name))
                if twStatus.has_key(name):
                    numericval.text = str(twStatus[name])
                else:
                    numericval.text = '0'
        else:
            for name in ['SUBMITTED', 'FAILED', 'QUEUED', 'NEW', 'KILLED']:
                numericval = SubElement(group, "numericvalue")
                numericval.set("name",name.lower())
                numericval.set("desc","number of %s tasks in the last minute"%(name))
                numericval.text = 'n/a'

        queueStats = self.getQueueStats()
        if len(queueStats) > 0:
            group= SubElement(data, "grp")
            group.set("name", "Number of shadows per crab3 schedd")
            for oneShadow in queueStats:
                numericval = SubElement(group,"numericvalue")
                numericval.set("name",oneShadow[0])
                numericval.set("desc","number of shadows process for %s"%(oneShadow[0]))
                numericval.text = str(oneShadow[1])

            group= SubElement(data, "grp")
            group.set("name", "Number of schedulers jobs running")
            for oneShadow in queueStats:
                numericval = SubElement(group,"numericvalue")
                numericval.set("name",oneShadow[0])
                numericval.set("desc","number of schedulers jobs running for %s"%(oneShadow[0]))
                numericval.text = str(oneShadow[2])

            group= SubElement(data, "grp")
            group.set("name", "Number of idle jobs per schedd")
            for oneShadow in queueStats:
                numericval = SubElement(group,"numericvalue")
                numericval.set("name",oneShadow[0])
                numericval.set("desc","number of idle jobs for at %s"%(oneShadow[0]))
                numericval.text = str(oneShadow[3])

            group= SubElement(data, "grp")
            group.set("name", "Number of running jobs per schedd")
            for oneShadow in queueStats:
                numericval = SubElement(group,"numericvalue")
                numericval.set("name",oneShadow[0])
                numericval.set("desc","number of running jobs running for at %s"%(oneShadow[0]))
                numericval.text = str(oneShadow[4])

            group= SubElement(data, "grp")
            group.set("name", "Number of held jobs per schedd")
            for oneShadow in queueStats:
                numericval = SubElement(group,"numericvalue")
                numericval.set("name",oneShadow[0])
                numericval.set("desc","number of held jobs for at %s"%(oneShadow[0]))
                numericval.text = str(oneShadow[5])

        # data = SubElement(root, "data")
        # numericval = SubElement(data, "numericvalue")
        # Numericval has to have desc and name, all these values can be grouped and added additional information.
        # See example here http://vocms037.cern.ch/XRDFED_CMS-EU.xml
        # Now only added information about service availability
        subprocesses_config = self.config.TaskWorker.nslaves + 1 # +1 - MasterWorker process
        sub_grep_command="ps -ef | grep MasterWorker | grep -v 'grep' | wc -l"
        # If any subproccess is dead or not working, modify percentage of availability
        # If subprocesses are not working - service availability 0%
        proccess_count = int(subprocess.Popen(sub_grep_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read())

        # Get current time
        now_utc = datetime.now().strftime(fmt)
        child_timestamp = SubElement(root, "timestamp")
        child_timestamp.text = str(now_utc)
        child_availability = SubElement(root, "availability")
        if subprocesses_config == proccess_count:
            # This means that everything is fine
            child_availability.text = "100"
        else:
            child_availability.text = str((100/subprocesses_config)*proccess_count)

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
    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'
    xmllocation = '/data/srv/service_report/CRAB3_TW_XML_Report.xml'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    config = loadConfigurationFile(twconfig)

    pr = CRAB3CreateXML(config, resthost, xmllocation, logger)
    pr.execute()

    # push the XML to elasticSearch directly
    #cmd = "curl -F file=@/home/crab3/CRAB3_SCHEDD_XML_Report.xml xsls.cern.ch"
    #try:
    #    pu = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #    #logger.debug("Runned pretty well")
    #except Exception, e:
    #    logger.debug(str(e))
