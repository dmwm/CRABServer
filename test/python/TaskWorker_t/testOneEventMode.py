from TaskWorker.CRAB3SetupPset import SetupCMSSWPSetCore
from ast import literal_eval
class testPSetTweak:
    def testVerifyOneEventMode(self):
        testArgs = ['python',
                    'TweakPSet.py', 'Analy',
                    '"/afs/cern.ch/user/m/mmascher/transformation/refactored/tmp"',
                    '["/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0010/EA00B1E8-F8AD-E111-90C5-5404A6388697.root"]',
                    '{"1": [[669684, 669684]]}',
                    ]
        location = testArgs[2]
        inputFiles = literal_eval(testArgs[3])
        runAndLumis = literal_eval(testArgs[4])
        agentNumber = 0
        lfnBase = None
        outputMods = ['o'] #TODO should not be hardcoded but taken from the config (how?)
        pset = SetupCMSSWPsetCore( location, map(str, inputFiles),
                                  runAndLumis, agentNumber, lfnBase, outputMods,
                                  oneEventMode=False)
        assert pset.step.data.application.command.oneEventMode == False

        pset = SetupCMSSWPsetCore( location, map(str, inputFiles),
                                  runAndLumis, agentNumber, lfnBase, outputMods,
                                  oneEventMode=True)
        assert pset.step.data.application.command.oneEventMode == True
