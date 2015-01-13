import FWCore.ParameterSet.Config as cms
process = cms.Process('UseParent')
process.source = cms.Source('PoolSource',
        fileNames = cms.untracked.vstring(
            # '/store/data/Run2012C/MinimumBias/RAW-RECO/25Feb2013-v1/10000/0274B626-B27F-E211-A7F4-20CF305B04F5.root'
            '/store/data/Run2012C/MinimumBias/RECO/PromptReco-v2/000/201/196/142C80D9-AAEB-E111-88DA-5404A638869C.root'
        ),
)

process.dump = cms.EDAnalyzer("Validation")

process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 1

process.maxEvents = cms.untracked.PSet(
        input = cms.untracked.int32(10)
)

process.p = cms.Path(process.dump)
