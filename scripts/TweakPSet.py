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

python <path-to-TweakPSet>/TweakPSet.py --inputFile='job_input_file_list_1.txt' --runAndLumis='job_lumis_1.json' \
             --firstEvent=0 --lastEvent=-1 --firstLumi=None --firstRun=1 --seeding=None \
             --lheInputFiles=False --oneEventMode=0

job_lumis_1.json content:
{"1": [[669684, 669684]]}
job_input_file_list_1.json content:
["1.root"]
On Worker nodes it has a tarball with all files. but for debugging purpose it is possible to read directly from file

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

import os
import shutil
import tarfile
from ast import literal_eval

from PSetTweaks.PSetTweak import PSetTweak
from Utils.Utilities import decodeBytesToUnicode


def readFileFromTarball(filename, tarball):
    """ returns a string with the content of once file in a tarball """
    content = '{}'
    if os.path.isfile(filename):
        # This is only for Debugging
        print("*********************")
        print(f"DEBUGGING MODE! WILL USE EXISTING {filename} INSTEAD OR GETTING IT FROM {tarball}")
        print("*********************")
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        return literal_eval(content)
    if not os.path.exists(tarball):
        raise RuntimeError(f"Error getting {tarball} file location")
    tarFile = tarfile.open(tarball)
    for member in tarFile.getmembers():  # pylint: disable=unused-variable
        try:
            f = tarFile.extractfile(filename)
            content = f.read()
            content = decodeBytesToUnicode(content)
            break
        except KeyError as er:
            # Don`t exit due to KeyError, print error. EventBased and FileBased does not have run and lumis
            print(f"Failed to get information from tarball {tarball} and file {filename}. Error : {er}")
            break
    tarFile.close()
    return literal_eval(content)


def createTweakingCommandLines(cmd, pklIn, pklOut):
    """
    create a bash script fragment to execute the cmd passed in arg to tweak a pickled PSet.
    Args are tailored for the syntax favored by edm_pset_tweak and cmssw_* comamnds.
    If command fails pklIn is left unchanged, otherwise it is overwritten with pklOut
    """
    cmdLines = "\n#\n"
    cmdLines += f'cmd="{cmd}"\n'
    cmdLines += 'echo "will execute: "$cmd\n'
    cmdLines += '$cmd\n'
    cmdLines += 'ec=$?\n'
    cmdLines += 'if ! [ $ec ]; then\n'
    cmdLines += '  echo "command failed with exit code $ec . Leave PSet unchanged"\n'
    cmdLines += 'else\n'
    cmdLines += f'  mv {pklOut} {pklIn}\n'
    cmdLines += 'fi\n'
    return cmdLines


def createScriptLines(opts, pklIn):
    """
    prepares a bash script fragment which tweaks the PSet params according to opts
    returns a string containing the script lines separated by '\n'
    """

    runAndLumis = {}
    if opts.runAndLumis:
        runAndLumis = readFileFromTarball(opts.runAndLumis, 'run_and_lumis.tar.gz')
    inputFiles = {}
    if opts.inputFileList:
        inputFiles = readFileFromTarball(opts.inputFileList, 'input_files.tar.gz')

    # build a tweak object with the needed changes to be applied to PSet
    tweak = PSetTweak()

    # add tweaks

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
            inputFile = {'lfn': inputFile, 'parents': []}
        if inputFile["lfn"].startswith("MCFakeFile"):
            # for MC which uses "EmptySource" there must be no inputFile
            continue
        primaryFiles.append(inputFile["lfn"])
        for secondaryFile in inputFile["parents"]:
            secondaryFiles.append(secondaryFile["lfn"])
    print(f"Adding {len(primaryFiles)} files to 'fileNames' attr")
    print(f"Adding { len(secondaryFiles)} files to 'secondaryFileNames' attr")
    if len(primaryFiles) > 0:
        tweak.addParameter("process.source.fileNames",
                           f"customTypeCms.untracked.vstring({primaryFiles})")
        if len(secondaryFiles) > 0:
            tweak.addParameter("process.source.secondaryFileNames",
                               f"customTypeCms.untracked.vstring({secondaryFiles})")

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
                lumisToProcess.append(f"{run}:{lumiPair[0]}-{run}:{lumiPair[1]}")
        tweak.addParameter("process.source.lumisToProcess",
                           f"customTypeCms.untracked.VLuminosityBlockRange({lumisToProcess})")

    # how many events to process
    if opts.firstEvent:
        tweak.addParameter("process.source.firstEvent",
                           f"customTypeCms.untracked.uint32({opts.firstEvent})")
    if opts.firstEvent is None or opts.lastEvent is None:
        # what to process is defined in runAndLumis, we do not split by events here
        maxEvents = -1
    else:
        # for MC CRAB passes 1st/last event, but cmsRun wants 1st ev + MaxEvents
        maxEvents = int(opts.lastEvent) - int(opts.firstEvent) + 1
        opts.lastEvent = None  # for MC there has to be no lastEvent
    tweak.addParameter("process.maxEvents.input",
                       f"customTypeCms.untracked.int32({maxEvents})")

    if opts.lastEvent:
        tweak.addParameter("process.source.lastEvent",
                           f"customTypeCms.untracked.uint32({opts.lastEvent})")

    if not inputFile["lfn"].startswith("MCFakeFile"):  # pylint: disable=undefined-loop-variable
        # Set skipEvents to 0 if config.JobType.pluginName = 'Analysis'
        # see https://github.com/dmwm/CRABServer/issues/7349
        tweak.addParameter("process.source.skipEvents", "customTypeCms.untracked.uint32(0)")
        print("skipEvents set to 0")

    # firstLumi, firstRun and eventsPerLumi are used for MC
    if opts.firstLumi:
        tweak.addParameter("process.source.firstLuminosityBlock",
                           f"customTypeCms.untracked.uint32({opts.firstLumi})")
    if opts.firstRun:
        tweak.addParameter("process.source.firstRun",
                           f"customTypeCms.untracked.uint32({opts.firstRun})")
    if opts.eventsPerLumi:
        numberEventsInLuminosityBlock = f"customTypeCms.untracked.uint32({opts.eventsPerLumi})"
        tweak.addParameter("process.source.numberEventsInLuminosityBlock", numberEventsInLuminosityBlock)

    # time-limited running is used by automatic splitting probe jobs
    if opts.maxRuntime:
        maxSecondsUntilRampdown = f"customTypeCms.untracked.int32({opts.maxRuntime})"
        tweak.addParameter("process.maxSecondsUntilRampdown.input", maxSecondsUntilRampdown)

    # event limiter for testing
    if opts.oneEventMode in ["1", "True", True]:
        tweak.addParameter("process.maxEvents.input", "customTypeCms.untracked.int32(1)")

    # make sure that FJR contains useful statistics, reuse code from
    # https://github.com/dmwm/WMCore/blob/c2fa70af3b4c5285d50e6a8bf48636232f738340/src/python/WMCore/WMRuntime/Scripts/SetupCMSSWPset.py#L289-L307
    tweak.addParameter("process.CPU", "customTypeCms.Service('CPU')")
    tweak.addParameter("process.Timing", "customTypeCms.Service('Timing', summaryOnly=cms.untracked.bool(True))")
    tweak.addParameter("process.SimpleMemoryCheck",
                       "customTypeCms.Service('SimpleMemoryCheck', jobReportOutputOnly=cms.untracked.bool(True))")

    # tweak !
    psetTweakJson = "PSetTweak.json"
    tweak.persist(psetTweakJson, formatting='simplejson')

    procScript = "edm_pset_tweak.py"
    pklOut = pklIn + '-tweaked'
    # we always create untracked psets in our tweaks
    cmd = f"{procScript} --input_pkl {pklIn} --output_pkl {pklOut} --json {psetTweakJson} --create_untracked_psets"
    commandLines = createTweakingCommandLines(cmd, pklIn, pklOut)

    # there a few more things to do which require running different EDM/CMSSW commands
    # 1. enable LazyDownload of LHE files (if needed)
    if opts.lheInputFiles == 'True':
        pklOut = pklIn + '-lazy'
        procScript = "cmssw_enable_lazy_download.py"
        cmd = f"{procScript} --input_pkl {pklIn} --output_pkl {pklOut}"
        moreLines = createTweakingCommandLines(cmd, pklIn, pklOut)
        commandLines += moreLines

    # 2. make sure random seeds are initialized
    pklOut = pklIn + '-seeds'
    procScript = "cmssw_handle_random_seeds.py"
    cmd = f"{procScript} --input_pkl {pklIn} --output_pkl {pklOut} --seeding dummy"
    moreLines = createTweakingCommandLines(cmd, pklIn, pklOut)
    commandLines += moreLines

    # 3. make sure that process.maxEvents.input is propagated to Producers, see:
    # https://github.com/dmwm/WMCore/blob/85d6d423f0a85fdedf78b65ca8b7b81af9263789/src/python/WMCore/WMRuntime/Scripts/SetupCMSSWPset.py#L448-L465
    # this only makes sense for produces which use nEvents internally, i.e. when running
    # MC production (GEN) using external scripts not directly controlled by cmsRun (forked)
    if inputFile["lfn"].startswith("MCFakeFile"):  # pylint: disable=undefined-loop-variable
        # see https://github.com/dmwm/CRABServer/issues/7650
        pklOut = pklIn + '-nEvents'
        procScript = 'cmssw_handle_nEvents.py'
        cmd = f"{procScript} --input_pkl {pklIn} --output_pkl {pklOut}"
        moreLines = createTweakingCommandLines(cmd, pklIn, pklOut)
        commandLines += moreLines

    return commandLines


def prepareTweakingScript(options, scriptName):
    """
    prepare a script to run in CMSSW environment which will do the needed
    tweaks using edm*.py commands and run it
    this is made to be called inside CMSRunAnalysis.py and it is passed the
    same list of options as used in there
    """
    # save original PSet.pkl
    pklIn = "PSet.pkl"
    shutil.copy(pklIn, 'PSet-Original.pkl')

    # tweak !

    commandLines = createScriptLines(options, pklIn)

    with open(scriptName, 'w', encoding='utf-8') as tweakScript:
        tweakScript.write("#/bin/bash\n")
        tweakScript.write(f"\n{commandLines}\n")
