import json
import shutil
import os
import traceback

import htcondor
from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction


class GetAcceleratorSite(BaseRecurringAction):
    pollingTime = 1 # testing #60 * 12 #minutes



    def _execute(self, config, task):  # pylint: disable=unused-argument
        collector_host = 'vocms0207.cern.ch'
        htcondor.param['COLLECTOR_HOST'] = collector_host
        collector = htcondor.Collector()
        try:
            result = collector.query(htcondor.AdTypes.Any,
                                     'mytype=="glidefactory" && stringlistmember("CMSGPU", GLIDEIN_Supported_VOs)',
                                     ['GLIDEIN_CMSSite'])
        except Exception:
            traceback.print_exc()
            self.logger.error('Cannot fetch accelerator site from collector %s.',collector_host)
            return
        sites = sorted({x.get('GLIDEIN_CMSSite') for x in result})
        saveLocation = os.path.join(config.TaskWorker.scratchDir, "acceleratorSites.txt")

        tmpLocation = saveLocation + ".tmp"
        with open(tmpLocation, 'w', encoding='utf-8') as fd:
            json.dump(sites, fd)
        shutil.move(tmpLocation, saveLocation)

if __name__ == '__main__':
    # Simple main to execute the action standalone.
    # You just need to set the task worker environment and desired twconfig.
    import logging
    import sys

    # read config in root git dir
    twconfig = '../../../../../TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(twconfig)

    trs = GetAcceleratorSite(cfg.TaskWorker.logsDir)
    trs._execute(cfg, None)  # pylint: disable=protected-access
