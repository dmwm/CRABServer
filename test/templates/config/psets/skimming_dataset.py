import FWCore.ParameterSet.Config as cms
process = cms.Process("Demo")
process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 100
process.maxEvents = cms.untracked.PSet( input = cms.untracked.int32(100) )
process.source = cms.Source("PoolSource")
process.demo = cms.EDAnalyzer('DemoAnalyzer',
    sleepmsec = cms.untracked.uint32(500)
)
process.out = cms.OutputModule("PoolOutputModule",
      fileName = cms.untracked.string("skim.root"),
# recoPFJets_ak5PFJets ~10kB, recoPFCandidates_particleFlow ~40 kB, recoTracks_generalTracks ~80 kB
      outputCommands=cms.untracked.vstring("drop *","keep recoPFJets_ak5PFJets_*_*"),
#      outputCommands=cms.untracked.vstring("drop *","keep recoTracks_generalTracks_*_*"),      
#      outputCommands=cms.untracked.vstring("drop *","keep recoPFCandidates_particleFlow_*_*"),
      SelectEvents = cms.untracked.PSet(
      SelectEvents = cms.vstring("p")
      )
)
process.p = cms.Path(process.demo)
process.e = cms.EndPath(process.out)
