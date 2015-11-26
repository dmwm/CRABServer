import FWCore.ParameterSet.Config as cms
process = cms.Process('NoSplit')
process.source = cms.Source('PoolSource',
    fileNames = cms.untracked.vstring("/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0010/FC85224E-EAAD-E111-AB01-0025901D629C.root"),
    skipEvents = cms.untracked.uint32(0),
)

process.dump = cms.EDAnalyzer("EventContentAnalyzer", listContent=cms.untracked.bool(False), getData=cms.untracked.bool(True))
process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 10

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(50)
)

process.Timing = cms.Service("Timing",
    useJobReport = cms.untracked.bool(True),
    summaryOnly = cms.untracked.bool(True),
)

process.TFileService = cms.Service("TFileService", fileName = cms.string("histo.root") )

process.o = cms.OutputModule("PoolOutputModule", fileName = cms.untracked.string("dumper.root"), outputCommands=cms.untracked.vstring("drop *"))

process.out = cms.EndPath(process.o)

process.p = cms.Path(process.dump)

