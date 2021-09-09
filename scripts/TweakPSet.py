"""
New implementation by SB in 2021 to make it properly separate WMCore and CMSSW environments
Tweak PSet files in pickle format, as used by CRAB or WMA wrappers.
It will only work with WMCore 4.9 or later as it imports some utilities classes from
WMCore/src/python/PSetTweaks while not using anything from WMCore/src/python/WMCore
It is possible to run this TweakPSet script standalone. These are the requirements:

1) You need to set up the CMSSW environment (cmsenv) and add WMCore/src/python/PSetTweaks
   to the pythonpath (full WMCore/src/python like in WMCore.zip used by CRAB wrapper will also do)
2) the PSet file should be called PSet.pkl and should be placed in `pwd`/PSet.pkl where
   `pwd` is the location where you are running TweakPSet
2a) If you are testing a CRAB3 PSet which .py version simply loads the PSet.pkl file [2]
    then you have to make sure that the PSet.pkl file is also located in `pwd`
3) The input file is saved as PSet-In.pkl
4) The new PSet is written both in PSet.pkl and PSet-Out.pkl (can drop the latter when fully debugged)

Can be tested with something like:

python <path-to-TweakPSet>/TweakPSet.py --inputFile='job_input_file_list_1.txt' --runAndLumis='job_lumis_1.json' --firstEvent=0 --lastEvent=-1 --firstLumi=None --firstRun=1 --seeding=None --lheInputFiles=False --oneEventMode=0

job_lumis_1.json content:
{"1": [[669684, 669684]]}
job_input_file_list_1.json content:
["1.root"]
On Worker nodes it has a tarball with all files. but for debugging purpose it is also available to read directly from file

from old TweakPSet/SetupCMSSWPsetCore, some useful documentation of args
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
from __future__ import print_function

import os
import shutil
import sys
import subprocess
import tarfile
from ast import literal_eval
from optparse import OptionParser

from PSetTweaks.PSetTweak import PSetTweak

def readFileFromTarball(filename, tarball):
    content = '{}'
    if os.path.isfile(filename):
        #This is only for Debugging
        print('DEBUGGING MODE!')
        with open(filename, 'r') as f:
            content = f.read()
        return literal_eval(content)
    elif not os.path.exists(tarball):
        raise RuntimeError("Error getting %s file location" % tarball)
    tar_file = tarfile.open(tarball)
    for member in tar_file.getmembers():  # pylint: disable=unused-variable
        try:
            f = tar_file.extractfile(filename)
            content = f.read()
            break
        except KeyError as er:
            #Don`t exit due to KeyError, print error. EventBased and FileBased does not have run and lumis
            print('Failed to get information from tarball %s and file %s. Error : %s' %(tarball, filename, er))
            break
    tar_file.close()
    return literal_eval(content)

def applyPsetTweak(psetTweak, pklIn, pklOut, skipIfSet=False, allowFailedTweaks=False, createUntrackedPsets=False):
    """
    code borrowed from  https://github.com/dmwm/WMCore/blob/master/src/python/WMCore/WMRuntime/Scripts/SetupCMSSWPset.py#L223-L251
    Apply a tweak to a PSet.pkl file. The tweak is described in the psetTweak input object
    which must be an instance of the PSetTweak class from WMcore/src/python/PSetTweaks/PSetTweak.py
    https://github.com/dmwm/WMCore/blob/bb573b442a53717057c169b05ae4fae98f31063b/src/python/PSetTweaks/PSetTweak.py#L160
    Arguments:
        psetTweak: the tweak
        psPklIn: the path to the input PSet.pkl file to change
        psPklOut: the path to the new PSet.pkl file to create
    Options:
      skipIfSet: Do not apply a tweak to a parameter that has a value set already.
      allowFailedTweaks: If the tweak of a parameter fails, do not abort and continue tweaking the rest.
      createUntrackedPsets: It the tweak wants to add a new Pset
    """

    procScript = "edm_pset_tweak.py"
    psetTweakJson = "PSetTweak.json"
    psetTweak.persist(psetTweakJson, formatting='simplejson')

    cmd = "%s --input_pkl %s --output_pkl %s --json %s" % (
        procScript, pklIn, pklOut, psetTweakJson)
    if skipIfSet:
        cmd += " --skip_if_set"
    if allowFailedTweaks:
        cmd += " --allow_failed_tweaks"
    if createUntrackedPsets:
        cmd += " --create_untracked_psets"

    print("will execute:\n%s" % cmd)

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    exitcode = process.returncode
    # for Py3 compatibility
    stdout = out.decode(encoding='UTF-8') if out else ''
    stderr = err.decode(encoding='UTF-8') if err else ''
    if exitcode:
        print("Non zero exit code: %s" % exitcode)
        print("Error while tweaking pset.\nStdout:\n%s\nStderr:\%s" % (stdout, stderr))
    return exitcode

def enableLazyDownload(pklIn):
    """
    _enableLazyDownload_  code stolen from https://github.com/dmwm/WMCore/blob/bb573b442a53717057c169b05ae4fae98f31063b/src/python/WMCore/WMRuntime/Scripts/SetupCMSSWPset.py#L529-L542
    Set things to read data remotely
    will modify input picked Pset (pklIn) adding enabling of lazy download
    """
    procScript = "cmssw_enable_lazy_download.py"

    pklOut = pklIn + '-lazy'
    cmd = "%s --input_pkl %s --output_pkl %s" % (procScript, pklIn, pklOut)

    print("will execute:\n%s" % cmd)

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    exitcode = process.returncode
    # for Py3 compatibility
    stdout = out.decode(encoding='UTF-8') if out else ''
    stderr = err.decode(encoding='UTF-8') if err else ''
    if exitcode:
        print("Non zero exit code: %s" % exitcode)
        print("Error while enabling lazy download on pset.\nStdout:\n%s\nStderr:\%s" % (stdout, stderr))
    os.rename(pklOut, pklIn)
    return exitcode


print("Beginning TweakPSet")
print(" arguments: %s" % sys.argv)

parser = OptionParser()
parser.add_option('--inputFile', dest='inputFile')
parser.add_option('--runAndLumis', dest='runAndLumis')
parser.add_option('--firstEvent', dest='firstEvent')
parser.add_option('--lastEvent', dest='lastEvent')
parser.add_option('--firstLumi', dest='firstLumi')
parser.add_option('--firstRun', dest='firstRun')
parser.add_option('--seeding', dest='seeding')
parser.add_option('--lheInputFiles', dest='lheInputFiles')
parser.add_option('--oneEventMode', dest='oneEventMode', default=False)
parser.add_option('--eventsPerLumi', dest='eventsPerLumi')
parser.add_option('--maxRuntime', dest='maxRuntime')
opts, args = parser.parse_args()

# Note about ops: default value is None for all, but if, like usually happens,
# CRAB code calls this with inputs like --seeding=None
# the opts variable is set to the string 'None', not to the python type None
# let's make life easier by replacing 'None' with None. Avoid using
# ast.literal_eval as in old version since it does not port easily to python3
if opts.inputFile and opts.inputFile == 'None': opts.inputFile = None
if opts.firstEvent and opts.firstEvent == 'None': opts.firstEvent = None
if opts.runAndLumis and opts.runAndLumis == 'None': opts.runAndLumis = None
if opts.lastEvent and opts.lastEvent == 'None': opts.lastEvent = None
if opts.firstLumi and opts.firstLumi == 'None': opts.firstLumi = None
if opts.firstRun and opts.firstRun == 'None': opts.firstRun = None
if opts.seeding and opts.seeding == 'None': opts.seeding = None
if opts.lheInputFiles and opts.lheInputFiles == 'None': opts.lheInputFiles = None
if opts.eventsPerLumi and opts.eventsPerLumi == 'None': opts.eventsPerLumi = None
if opts.maxRuntime and opts.maxRuntime == 'None': opts.maxRuntime = None

if opts.oneEventMode in ["1", "True", True] :
    print("One event mode disabled until we can put together a decent version of WMCore.")
    print("TweakPSet.py is going to force one event mode")

runAndLumis = {}
if opts.runAndLumis:
    runAndLumis = readFileFromTarball(opts.runAndLumis, 'run_and_lumis.tar.gz')
inputFiles = {}
if opts.inputFile:
    inputFiles = readFileFromTarball(opts.inputFile, 'input_files.tar.gz')


# build a tweak object with the needed changes to be applied to PSet
tweak = PSetTweak()

# add tweaks

# preset option for edm_pset_tweak.py, will set to True if some param. tweaking needs it
untrackedPsets = False

# inputFile will always be present
# inputFile can have three formats depending on wether secondary input files are used:
# 1. a single LFN as a string : "/store/.....root"
# 2. a list of LFNs : ["/store/.....root", "/store/....root", ...]
# 3. a list of dictionaries (one per file) with keys: 'lfn' and 'parents'
#   value for 'lfn' is a string, value for 'parents' is a list of {'lfn':lfn} dictionaries
#   [{'lfn':inputlfn, 'parents':[{'lfn':parentlfn1},{'lfn':parentlfn2}], ....]},...]
# to properly prepare the tweak we reuse code fom WMTweak.py:
# https://github.com/dmwm/WMCore/blob/bb573b442a53717057c169b05ae4fae98f31063b/src/python/PSetTweaks/WMTweak.py#L415-L441
primaryFiles = []
secondaryFiles = []
for inputFile in inputFiles:
    # make sure input is always in format 3.
    if not isinstance(inputFile, dict):
        inputFile = {'lfn':inputFile, 'parents':[]}
    if inputFile["lfn"].startswith("MCFakeFile"):
        # for MC which uses "EmptySource" there must be no inputFile
        continue
    primaryFiles.append(inputFile["lfn"])
    for secondaryFile in inputFile["parents"]:
        secondaryFiles.append(secondaryFile["lfn"])
print("Adding %d files to 'fileNames' attr" % len(primaryFiles))
print("Adding %d files to 'secondaryFileNames' attr" % len(secondaryFiles))
if len(primaryFiles) > 0:
    tweak.addParameter("process.source.fileNames",
                       "customTypeCms.untracked.vstring(%s)" % primaryFiles)
    if len(secondaryFiles) > 0:
        tweak.addParameter("process.source.secondaryFileNames",
                           "customTypeCms.untracked.vstring(%s)" % secondaryFiles)

# for rearranging runsAndLumis into the structure needed by CMSSW, reuse code taken from
# https://github.com/dmwm/WMCore/blob/bb573b442a53717057c169b05ae4fae98f31063b/src/python/PSetTweaks/WMTweak.py#L482
if runAndLumis:
    lumisToProcess = []
    for run in runAndLumis.keys():
        lumiPairs = runAndLumis[run]
        for lumiPair in lumiPairs:
            if len(lumiPair) != 2:
                # Do nothing
                continue
            lumisToProcess.append("%s:%s-%s:%s" % (run, lumiPair[0], run, lumiPair[1]))
    tweak.addParameter("process.source.lumisToProcess",
                       "customTypeCms.untracked.VLuminosityBlockRange(%s)" % lumisToProcess)

# how many events to process
if opts.firstEvent:
    tweak.addParameter("process.source.firstEvent",
                       "customTypeCms.untracked.uint32(%s)" % opts.firstEvent)
if opts.firstEvent is None or opts.lastEvent is None:
    # what to process is define in runAndLumis, we do no split by events here
    maxEvents = -1
else:
    # for MC CRAB passes 1st/last event, but cmsRun wants 1st ev + MaxEvents
    maxEvents = int(opts.lastEvent) - int(opts.firstEvent) + 1
    opts.lastEvent = None  # for MC there has to be no lastEvent
tweak.addParameter("process.maxEvents.input",
                   "customTypeCms.untracked.int32(%s)" % maxEvents)
untrackedPsets = True

if opts.lastEvent:
    tweak.addParameter("process.source.lastEvent",
                       "customTypeCms.untracked.uint32(%s)" % opts.lastEvent)

# firstLimi, firstRun and eventsPerLumi are used for MC
if opts.firstLumi:
    tweak.addParameter("process.source.firstLuminosityBlock",
                       "customTypeCms.untracked.uint32(%s)" % opts.firstLumi)
if opts.firstRun:
    tweak.addParameter("process.source.firstRun",
                       "customTypeCms.untracked.uint32(%s)" % opts.firstRun)
if opts.eventsPerLumi:
    numberEventsInLuminosityBlock = "customTypeCms.untracked.uint32(%s)" % opts.eventsPerLumi
    tweak.addParameter("process.source.numberEventsInLuminosityBlock", numberEventsInLuminosityBlock)

#TODO
# always setup seeding since seeding is only and always set to 'AutomaticSeeding' in CRAB
# (of course only if a RandomeNumberGeneratorService is present in the PSET)
# so should use this code from SetupCMSSWPset.py
# https://github.com/dmwm/WMCore/blob/bb573b442a53717057c169b05ae4fae98f31063b/src/python/WMCore/WMRuntime/Scripts/SetupCMSSWPset.py#L254-L286
# but looking at it, it seems that for "AutomaticSeeding" there is nothing to do,
# maybe only make a call to cmssw_handle_random_seeds.py ?
# Need expert advice !!

# time-limited running is used by automatic splitting probe jobs
if opts.maxRuntime:
    maxSecondsUntilRampdown = "customTypeCms.untracked.int32(%s)" %opts.maxRuntime
    tweak.addParameter("process.maxSecondsUntilRampdown.input", maxSecondsUntilRampdown)
    untrackedPsets = True

# event limiter for testing
if opts.oneEventMode in ["1", "True", True] :
    tweak.addParameter("process.maxEvents.input", "customTypeCms.untracked.int32(1)")

# make sure that FJR contains useful statistics, reuse code from
# https://github.com/dmwm/WMCore/blob/c2fa70af3b4c5285d50e6a8bf48636232f738340/src/python/WMCore/WMRuntime/Scripts/SetupCMSSWPset.py#L289-L307
tweak.addParameter("process.CPU", "customTypeCms.Service('CPU')")
tweak.addParameter("process.Timing", "customTypeCms.Service('Timing', summaryOnly=cms.untracked.bool(True))")
tweak.addParameter("process.SimpleMemoryCheck",
                   "customTypeCms.Service('SimpleMemoryCheck', jobReportOutputOnly=cms.untracked.bool(True))")

# save original PSet.pkl
psPklIn =  "PSet-In.pkl"
shutil.copy('PSet.pkl', psPklIn)

# tweak !
psPklOut = "PSet-Out.pkl"
ret = applyPsetTweak(tweak, psPklIn, psPklOut, createUntrackedPsets=untrackedPsets)
if not ret and opts.lheInputFiles:
    ret = enableLazyDownload(psPklOut)

if ret:
    print ("tweak failed, leave PSet.pkl unchanged")
else:
    print("PSetTweak successful, overwrite PSet.pkl")
    shutil.copy(psPklOut, 'PSet.pkl')
