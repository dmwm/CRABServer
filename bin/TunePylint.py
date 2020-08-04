"""
Script creates new .pylintrc based on a default .pylintrc defined in WMCore/standards
by appending messages which should be disabled for CRABClient/CRABServer testing.
"""

from __future__ import division
import configparser

directories = ['/home/dmwm/crabclient_test/', '/home/dmwm/crabserver_test/', '/home/dmwm/aso_test/']

config = configparser.ConfigParser()
for dir in directories:
    oldCfg = config.read(dir + '.default_pylintrc')
    config['MESSAGES CONTROL']['disable'] += ', C0103, W0703, R0912, R0914, R0915'
    with open(dir + '.pylintrc', 'w') as newCfg:
        config.write(newCfg)
