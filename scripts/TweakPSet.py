"""
Can be tested with something like:
python TweakPSet.py Analy /afs/cern.ch/user/m/mmascher/transformation/refactored/tmp '["/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0010/EA00B1E8-F8AD-E111-90C5-5404A6388697.root"]' '{"1": [[669684, 669684]]}'
"""

import os
import sys
import json
from ast import literal_eval
from optparse import OptionParser
from WMCore.Configuration import Configuration, ConfigSection
from WMCore.DataStructs.Mask import Mask
from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPset
from WMCore.WMRuntime.ScriptInterface import ScriptInterface
from TaskWorker.CRAB3SetupPset import SetupCMSSWPsetCore
print "Beginning TweakPSet"
print " arguments: %s" % sys.argv
agentNumber = 0
#lfnBase = '/store/temp/user/mmascher/RelValProdTTbar/mc/v6' #TODO how is this built?
lfnBase = None
outputMods = ['o'] #TODO should not be hardcoded but taken from the config (how?)

parser = OptionParser()
parser.add_option('--oneEventMode', dest='oneEventMode', default=False)
opts, args = parser.parse_args()
oneEventMode = opts.oneEventMode
if opts.oneEventMode:
    print "One event mode disabled until we can put together a decent version of WMCore."
    print "TweakPSet.py is going to force one event mode"
location = sys.argv[2]
inputFiles = literal_eval(sys.argv[3])
runAndLumis = literal_eval(sys.argv[4])

if sys.argv[1]=='MC':
    firstEvent=sys.argv[5]
    lastEvent=sys.argv[6]
    firstLumi=sys.argv[7]
    firstRun=sys.argv[8]
    seeding=sys.argv[9]
    lheInputFiles=bool(literal_eval(sys.argv[10]))
    pset = SetupCMSSWPsetCore( location, map(str, inputFiles), runAndLumis, agentNumber, lfnBase, outputMods, int(firstEvent), int(lastEvent), int(firstLumi),\
                    int(firstRun), seeding, lheInputFiles, oneEventMode = oneEventMode)
else:
    pset = SetupCMSSWPsetCore( location, map(str, inputFiles), runAndLumis, agentNumber, lfnBase, outputMods, oneEventMode=oneEventMode)

pset()
