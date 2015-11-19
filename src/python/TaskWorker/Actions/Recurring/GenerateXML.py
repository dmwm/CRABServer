import os
import sys
import time
import json
import urllib
import logging
import traceback
import subprocess
from datetime import datetime
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

    def execute(self):
        from xml.etree.ElementTree import Element, SubElement, tostring
        if not hasattr(self.config.TaskWorker, 'XML_Report_ID'):
            self.logger.error("Not generating xml because no XML_Report_ID exists in TaskWorker configuration")
            return
        root = Element('serviceupdate')
        root.set( "xmlns",  "http://sls.cern.ch/SLS/XML/update")
        child = SubElement(root, "id")
        child.text = self.config.TaskWorker.XML_Report_ID
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

        child_status = SubElement(root,"status")

        if subprocesses_config == proccess_count:
            # This means that everything is fine
            child_status.text = "available"
        else:
            child_status.text = "degraded"

        # Write all this information to a temp file and move to correct location
        temp_xmllocation = self.xmllocation + ".temp"
        with open(temp_xmllocation, 'w') as f:
            f.write(tostring(root))
        os.system('mv %s %s' % (temp_xmllocation, self.xmllocation))

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
