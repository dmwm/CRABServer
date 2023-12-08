from __future__ import division
import FWCore.ParameterSet.Config as cms

process = cms.Process('NoSplit')

process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring('root://cms-xrd-global.cern.ch///store/mc/HC/GenericTTbar/AODSIM/CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/00000/8ADD04E5-1776-E711-A1BA-FA163E6741E0.root'))
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(10))
process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))

process.output = cms.OutputModule("PoolOutputModule",
    outputCommands = cms.untracked.vstring("drop *", "keep recoTracks_globalMuons_*_*"),
    fileName = cms.untracked.string('output.root'),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string('OutOne'),
    )
)
process.out1 = cms.EndPath(process.output)

process.secondoutput = cms.OutputModule("PoolOutputModule",
    outputCommands = cms.untracked.vstring("drop *", "keep recoTracks_globalMuons_*_*"),
    fileName = cms.untracked.string('secondoutput.root'),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string('OutTwo'),
    )

)
process.out2 = cms.EndPath(process.secondoutput)
