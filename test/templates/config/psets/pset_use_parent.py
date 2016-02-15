import FWCore.ParameterSet.Config as cms
process = cms.Process('UseParent')
process.source = cms.Source('PoolSource',
        fileNames = cms.untracked.vstring(
            # '/store/data/Run2012C/MinimumBias/RAW-RECO/25Feb2013-v1/10000/0274B626-B27F-E211-A7F4-20CF305B04F5.root'
            '/store/data/Run2012C/SingleMu/RAW/v1/000/198/050/18073F7C-2DC4-E111-BEE0-003048D2BDD8.root'
            #'/store/data/Run2012A/MinimumBias/AOD/22Jan2013-v1/20000/001D07E3-CF67-E211-B063-002590596468.root'
        ),
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
