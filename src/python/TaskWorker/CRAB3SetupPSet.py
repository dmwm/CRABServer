import os
import sys
import json
from ast import literal_eval
from optparse import OptionParser
from WMCore.Configuration import Configuration, ConfigSection
from WMCore.DataStructs.Mask import Mask
from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPset
from WMCore.WMRuntime.ScriptInterface import ScriptInterface


class SetupCMSSWPsetCore(SetupCMSSWPset):
    """
    location:       the working directory. PSet.py must be located there. The output PSet.pkl is also put there.
                    N.B.: this parameter must end with WMTaskSpace.cmsRun.PSet
    inputFiles:     the input files of the job. This must be a list of dictionaries whose keys are "lfn" and "parents".
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
    """
    def __init__(self, location, inputFiles, runAndLumis, agentNumber, lfnBase, outputMods, firstEvent=0, lastEvent=-1, firstLumi=None,\
                    firstRun=None, seeding=None, lheInputFiles=False, oneEventMode=False):
        ScriptInterface.__init__(self)
        self.stepSpace = ConfigSection()
        self.stepSpace.location = location
        self.step = StepConfiguration(lfnBase, outputMods)
        self.step.section_("data")
        self.step.data._internal_name = "cmsRun"
        self.step.data.section_("application")
        self.step.data.application.section_("configuration")
        self.step.data.application.section_("command")
        self.step.data.application.command.configuration = "PSet.py"
        self.step.data.application.command.oneEventMode  = oneEventMode
#        self.step.data.application.configuration.pickledarguments.globalTag/globalTagTransaction
        self.step.data.section_("input")
        self.job = jobDict(lheInputFiles, seeding)
        self.job["input_files"] = []
        for inputF in inputFiles:
            self.job["input_files"].append({"lfn" : inputF, "parents" : ""})

        self.job['mask'] = Mask()
        self.job['mask']["FirstEvent"] = firstEvent

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
            getattr(self,out).lfnBase = lfnBase #'/store/temp/user/mmascher/RelValProdTTbar/mc/v6'
        StepConfiguration.outputMods = outputMods

    def getTypeHelper(self):
        return self
    def listOutputModules(self):
        om = StepConfiguration.outputMods
        #like return {"output1" : self.output1, "output2" : self.output2 ...} for each output1, output2 ... in self.outputMods
        return dict(zip(om, map( lambda out: getattr(self, out), om)))
    def getOutputModule(self, name):
        return getattr(self, name)


