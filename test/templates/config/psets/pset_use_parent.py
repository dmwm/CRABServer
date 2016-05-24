import FWCore.ParameterSet.Config as cms
process = cms.Process('UseParent')
process.source = cms.Source('PoolSource',
        fileNames = cms.untracked.vstring('/store/data/Run2015B/MinimumBias/RAW/v1/000/250/974/00000/DA155786-0E24-E511-8FA7-02163E014624.root'),
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
