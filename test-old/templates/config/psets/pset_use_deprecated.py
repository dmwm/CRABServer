import FWCore.ParameterSet.Config as cms
process = cms.Process('UseDeprecated')
process.source = cms.Source('PoolSource',
        fileNames = cms.untracked.vstring('/store/data/Run2012C/SingleMu/RAW/v1/000/198/050/18073F7C-2DC4-E111-BEE0-003048D2BDD8.root'),
)

process.options = cms.untracked.PSet(
        SkipEvent = cms.untracked.vstring('ProductNotFound')
)

process.dump = cms.EDAnalyzer(
       "DumpFEDRawDataProduct",
       label = cms.untracked.string("rawDataCollector"),
       feds = cms.untracked.vint32(0)
)

process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 1

process.maxEvents = cms.untracked.PSet(
        input = cms.untracked.int32(10)
)

process.p = cms.Path(process.dump)
