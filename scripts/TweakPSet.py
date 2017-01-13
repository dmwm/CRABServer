"""
It is possible to run this TweakPSet script standalone. These are the requirements:

1) You need to set up the CMSSW environment (cmsenv)
2) Due to a restriction in WMCore [1] the PSet file should be called PSet.py and should be placed in `pwd`/WMTaskSpace/cmsRun/PSet.py where `pwd` is the location where you are running TweakPSet
    2a) If you are testing a CRAB3 PSet which .py version simply loads the PSet.pkl file [2] then you have to make sure that the PSet.pkl file is also located in `pwd`

[1] https://github.com/dmwm/WMCore/blob/c7603bc0d243947e3a766bd2b38acb92e91dd767/src/python/WMCore/WMRuntime/Scripts/SetupCMSSWPset.py#L226
[2] https://github.com/dmwm/CRABClient/blob/3.3.1509.rc3/src/python/CRABClient/JobType/CMSSWConfig.py#L70-L91

3) The output PSet.py and PSet.pkl are placed in the directory indicated by the --location param

Can be tested with something like:
4) Set up the pythonpath correctly. You need to include the current working directory (before we go into WMTaskSpace/cmsRun in the next step)
mkdir -p WMTaskSpace/cmsRun
mv <path-to-pset>/PSet.p* WMTaskSpace/cmsRun
cd  WMTaskSpace/cmsRun
python <path-to-TweakPSet>/TweakPSet.py --location=<path-to-output> --inputFile='job_input_file_list_1.txt' --runAndLumis='job_lumis_1.json' --firstEvent=0 --lastEvent=-1 --firstLumi=None --firstRun=1 --seeding=None --lheInputFiles=False --oneEventMode=0

job_lumis_1.json content:
{"1": [[669684, 669684]]}
job_input_file_list_1.json content:
["1.root"]
On Worker nodes it has a tarball with all files. but for debugging purpose it is also available to read directly from file
"""
from __future__ import print_function

from optparse import OptionParser
from WMCore.Configuration import Configuration, ConfigSection
from WMCore.DataStructs.Mask import Mask
from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPset
from WMCore.WMRuntime.ScriptInterface import ScriptInterface

class jobDict(dict):
    def __init__(self, lheInputFiles, seeding):
        self.lheInputFiles = lheInputFiles
        self.seeding = seeding
    def getBaggage(self):
        confSect = ConfigSection()
        confSect.seeding = self.seeding
        confSect.lheInputFiles = self.lheInputFiles
        return confSect

class StepConfiguration(Configuration):
    def __init__(self, lfnBase, outputMods):
        Configuration.__init__(self)
        for out in outputMods:
            setattr(self, out, ConfigSection("output"))
            getattr(self, out)._internal_name = "output"
            getattr(self, out).lfnBase = lfnBase #'/store/temp/user/mmascher/RelValProdTTbar/mc/v6'
        StepConfiguration.outputMods = outputMods

    def getTypeHelper(self):
        return self
    def listOutputModules(self):
        om = StepConfiguration.outputMods
        #like return {"output1" : self.output1, "output2" : self.output2 ...} for each output1, output2 ... in self.outputMods
        return dict(list(zip(om, map(lambda out: getattr(self, out), om))))
    def getOutputModule(self, name):
        return getattr(self, name)


class SetupCMSSWPsetCore(SetupCMSSWPset):
    """
    location:       the working directory. PSet.py must be located there. The output PSet.pkl is also put there.
                    N.B.: this parameter must end with WMTaskSpace.cmsRun.PSet
    inputFiles:     the input files of the job. This must be a list of dictionaries whose keys are "lfn" and "parents"
                    or a list of filenames.

                    For example a valid input for this parameter is [{"lfn": , "parents" : }, {"lfn": , "parents" : }]
                    If the "lfn"s start with MCFakeFile then mask.FirstLumi is used to tweak process.source.firstLuminosityBlock
                    If the len of the list is >1 then the "lfn" values are put in a list and used to tweak "process.source.fileNames"
                        In that case the parents values are merged to a single list and used to tweak process.source.secondaryFileNames
                    If no input files are used then the mask.FirstEvent parameter is used to tweak process.source.firstEvent (error otherwise)
    mask:           the parameter is used to twek some parameters of the process. In particular:
                        process.maxEvents.input:
                        process.source.skipEvent:
                        process.source.firstRun:
                        process.source.lumisToProcess:
    agentNumber:    together with lfnBase is used to create the path of the output lfn. This output lfn is the used to tweak process.OUTPUT.logicalFileName
                    where OUTPUT is the name of the output module
    lfnBase:        see above
    outputMods:     a list of names of the output modules, e.g. ['OUTPUT']. Used to tweak process.OUTPUT.filename with OUTPUT.root
    lheInputFiles:  if we have a MC task and this parameter is false it's used to tweak process.source.firstEvent
    firstEvent:     see above. Number to use to tweak also other parameters
    firstLumi:      used only if the task ia a MC. In this case it's a mandatory parameter. Used to tweak process.source.firstLuminosityBlock
    lastEvent:      together with firstEvent used to tweak process.maxEvents.input
    firstRun:       used to tweak process.source.firstRun. Set to 1 if it's None
    seeding:        used in handleSeeding
    oneEventMode:   toggles one event mode
    eventsPerLumi:  start a new lumi section after the specified amount of events.  None disables this.
    """
    def __init__(self, location, inputFiles, runAndLumis, agentNumber, lfnBase, outputMods, firstEvent=0, lastEvent=-1, firstLumi=None, \
                 firstRun=None, seeding=None, lheInputFiles=False, oneEventMode=False, eventsPerLumi=None, maxRuntime=None, numberOfCores=1):
        ScriptInterface.__init__(self)
        self.stepSpace = ConfigSection()
        self.stepSpace.location = location
        self.step = StepConfiguration(lfnBase, outputMods)
        self.step.section_("data")
        self.step.data._internal_name = "cmsRun"
        self.step.data.section_("application")
        self.step.data.application.section_("configuration")
        self.step.data.application.section_("command")
        self.step.data.application.section_("multicore")
        self.step.data.application.command.configuration = "PSet.py"
        self.step.data.application.command.oneEventMode = oneEventMode in ["1", "True", True]
        self.step.data.application.command.memoryCheck = False
        self.step.data.application.command.silentMemoryCheck = True
#        self.step.data.application.configuration.pickledarguments.globalTag/globalTagTransaction
        if eventsPerLumi:
            self.step.data.application.configuration.eventsPerLumi = eventsPerLumi
        if maxRuntime:
            self.step.data.application.configuration.maxSecondsUntilRampdown = maxRuntime
        #MM I do not anymore see "multicore.enabled" used anywhere in WMCore. Maybe we can remove it?
        self.step.data.application.multicore.enabled = False
        self.step.data.application.multicore.numberOfCores = numberOfCores
        self.step.data.section_("input")
        self.job = jobDict(lheInputFiles, seeding)
        self.job["input_files"] = []
        for inputF in inputFiles:
            if isinstance(inputF, basestring):
                self.job["input_files"].append({"lfn" : inputF, "parents" : ""})
            else:
                self.job["input_files"].append(inputF)

        self.job['mask'] = Mask()
        self.job['mask']["FirstEvent"] = firstEvent
        self.job['mask']["LastEvent"] = lastEvent
        self.job['mask']["FirstRun"] = firstRun
        self.job['mask']["FirstLumi"] = firstLumi
        self.job['mask']["runAndLumis"] = runAndLumis

        self.job['agentNumber'] = agentNumber
        self.job['counter'] = 0

import os
import sys
import json
import tarfile
from ast import literal_eval

def readFileFromTarball(file, tarball):
    content = '{}'
    if os.path.isfile(file):
        #This is only for Debugging
        print('DEBUGGING MODE!')
        with open(file, 'r') as f:
            content = f.read()
        return literal_eval(content)
    elif not os.path.exists(tarball):
        raise RuntimeError("Error getting %s file location" % tarball)
    tar_file = tarfile.open(tarball)
    for member in tar_file.getmembers():
        try:
            f = tar_file.extractfile(file)
            content = f.read()
            break
        except KeyError as er:
            #Don`t exit due to KeyError, print error. EventBased and FileBased does not have run and lumis
            print('Failed to get information from tarball %s and file %s. Error : %s' % (tarball, file, er))
            break
    tar_file.close()
    return literal_eval(content)

print("Beginning TweakPSet")
print(" arguments: %s" % sys.argv)
agentNumber = 0
#lfnBase = '/store/temp/user/mmascher/RelValProdTTbar/mc/v6' #TODO how is this built?
lfnBase = None
outputMods = [] #Don't need to tweak this as the client looks for the ouput names and pass them to the job wrapper which moves them


parser = OptionParser()
parser.add_option('--location', dest='location')
parser.add_option('--inputFile', dest='inputFile')
parser.add_option('--runAndLumis', dest='runAndLumis')
parser.add_option('--firstEvent', dest='firstEvent')
parser.add_option('--lastEvent', dest='lastEvent')
parser.add_option('--firstLumi', dest='firstLumi')
parser.add_option('--firstRun', dest='firstRun')
parser.add_option('--seeding', dest='seeding')
parser.add_option('--numberOfCores', dest='numberOfCores')
parser.add_option('--lheInputFiles', dest='lheInputFiles')
parser.add_option('--oneEventMode', dest='oneEventMode', default=False)
parser.add_option('--eventsPerLumi', dest='eventsPerLumi', default=None)
parser.add_option('--maxRuntime', dest='maxRuntime', default=None)
opts, args = parser.parse_args()

if opts.oneEventMode:
    print("One event mode disabled until we can put together a decent version of WMCore.")
    print("TweakPSet.py is going to force one event mode")

runAndLumis = {}
if opts.runAndLumis:
    runAndLumis = readFileFromTarball(opts.runAndLumis, 'run_and_lumis.tar.gz')
inputFile = {}
if opts.inputFile:
    inputFile = readFileFromTarball(opts.inputFile, 'input_files.tar.gz')
pset = SetupCMSSWPsetCore(opts.location, inputFile, runAndLumis, agentNumber, lfnBase, outputMods, \
                           literal_eval(opts.firstEvent), literal_eval(opts.lastEvent), literal_eval(opts.firstLumi), \
                           literal_eval(opts.firstRun), opts.seeding, literal_eval(opts.lheInputFiles), opts.oneEventMode, \
                           literal_eval(opts.eventsPerLumi), literal_eval(opts.maxRuntime), opts.numberOfCores)

pset()
