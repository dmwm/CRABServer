#
"""
Standalone (rucio dependent) script to check if a dataset is available
for processing via CRAB, i.e. on disk on non black-listed RSE's
usage:  pythpn3 ./CheckDiskAvailability.py -h 
example output:

Checking disk availability of dataset: /DPS_Bottomonium_Charmonium_UpsilonToMuMu_JPsiToMuMu_4MuFilter_TuneCP5_13TeV-pythia8/RunIISummer20UL17RECO-106X_mc2017_realistic_v6-v2/AODSIM
 only fully available (i.e. complete) block replicas are considered 
dataset has 14 blocks
Checking blocks availability on disk ...
  block: 14
   0 blocks have  0 disk replicas
   5 blocks have  1 disk replicas
   6 blocks have  2 disk replicas
   3 blocks have  3 disk replicas
 Dataset is fully available

 Block locations:
 T1_US_FNAL_Disk hosts  14 blocks
 T2_UK_London_IC hosts   3 blocks
 T2_US_UCSD      hosts   2 blocks
 T2_CH_CERN      hosts   2 blocks
 T1_IT_CNAF_Disk hosts   1 blocks
 T1_RU_JINR_Disk hosts   1 blocks
 T2_DE_DESY      hosts   1 blocks
 T1_ES_PIC_Disk  hosts   1 blocks
 T2_RU_JINR      hosts   1 blocks

AFTER APPLYING CRAB SITE BLACKLIST:
   0 blocks have  0 disk replicas
   5 blocks have  1 disk replicas
   6 blocks have  2 disk replicas
   3 blocks have  3 disk replicas
 Dataset is fully available

Rules on this dataset:
               ID                     account         state        expiration                  RSEs             
8bd6221520a547599b87bd327efea03b   wmcore_output        OK            None               T0_CH_CERN_Tape
"""
import argparse
import os
import subprocess
import json

def main():
    rucio = ensure_environment()
    scope, dataset, blocks = get_inputs(rucio)
    blackListedSites = get_crab_blacklist()
    nBlocks = len(blocks)
    print("Checking blocks availabiliyt on disk ...")
    locationsMap = create_locations_map(blocks, rucio)
    (nbFORnr, nbFORrse) = createBlockMaps(locationsMap=locationsMap, blackList=[])
    print_blocks_per_replica_map(nbFORnr)

    print("\n Block locations:")
    for rse in nbFORrse.keys():
        msg = f" {rse:15} hosts {nbFORrse[rse]:3} blocks"
        if rse in blackListedSites:
            msg += "  *SITE BLACKLISTED IN CRAB*"
        print(msg)
    print("")

    print("AFTER APPLYING CRAB SITE BLACKLIST:")
    (nbFORnr, nbFORrse) = createBlockMaps(locationsMap=locationsMap, blackList=blackListedSites)
    print_blocks_per_replica_map(nbFORnr)

    print("\nRules on this dataset:")
    rule_gens=rucio.list_did_rules(scope=scope, name=dataset)
    rules = list(rule_gens)
    if rules:
        pattern = '{:^32}{:^20}{:^10}{:^20}{:^30}'
        print(pattern.format('ID','account','state','expiration','RSEs'))
        for r in rules:
            print(pattern.format(r['id'],r['account'],r['state'], str(r['expires_at']), r['rse_expression']))
    else:
        print("NONE")


def createBlockMaps(locationsMap={}, blackList=[]):
    # creates 2 maps: nReplicas-->nBlocks and rse-->nBlocks
    # locationsMap is a dictionary {block:[replica1, replica2..]}
    nbFORnr = {}
    nbFORnr[0] = 0  # this will not be filled in the loop if every block has locations
    nbFORrse = {}
    for block in locationsMap:
        replicas = locationsMap[block]
        if replicas:
            for rse in replicas.copy():
                if rse in blackList:
                    replicas.remove(rse)
            try:
                nbFORnr[len(replicas)] += 1
            except:
                nbFORnr[len(replicas)] = 1
            for rse in locationsMap[block]:
                try:
                    nbFORrse[rse] += 1
                except:
                    nbFORrse[rse] = 1
        else:
            nbFORnr[0] += 1
    return (nbFORnr, nbFORrse)


def ensure_environment():
    if os.getenv("CMSSW_BASE"):
        print("Must use a shell w/o CMSSW environent")
        exit()
    try:
        from rucio.client import Client
    except ModuleNotFoundError:
        print("Setup Rucio first via:\n source /cvmfs/cms.cern.ch/rucio/setup-py3.sh; export RUCIO_ACCOUNT=`whoami`")
        exit()
    # make sure Rucio client is initialized
    rucio = Client()
    return(rucio)


def get_inputs(rucio):
    # get name of object to test and check what it is
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', help='DBS dataset (or block) name, or Rucio DID (scope:name)', required=True)
    args = parser.parse_args()

    if ':' in args.dataset:
        scope = args.dataset.split(':')[0]
        dataset = args.dataset.split(':')[1]
        datasetKind = 'Container'
    else:
        scope = 'cms'
        dataset = args.dataset
        datasetKind = 'Container'
    if scope == 'cms':
        print(f"Checking disk availability of dataset: {dataset}")
    else:
        print(f"Checking disk availability of container: {args.dataset}")
    if scope != 'cms':
        print(" container in USER scope. Assume it contains datasets(blocks) in CMS scope")
    print(" only fully available (i.e. complete) block replicas are considered ")

    # get list of blocks (datasets in Rucio)
    if '#' in dataset:
        print("Input is a DBS-block (Rucio-dataset) will check that one")
        blocks = [dataset]
    else:
        dss = rucio.list_content(scope, dataset)
        blocks = [ds['name'] for ds in dss]
        print(f"dataset has {len(blocks)} blocks")
        
    return (scope, dataset, blocks)


def get_crab_blacklist():
    # let's makle a list of sites where CRAB will not run
    usableSitesUrl = 'https://cmssst.web.cern.ch/cmssst/analysis/usableSites.json'
    result = subprocess.run(f"curl -s {usableSitesUrl}", shell=True, stdout=subprocess.PIPE)
    usableSites = json.loads(result.stdout.decode('utf-8'))
    blackListedSites=[]
    for site in usableSites:
        if 'value' in site and site['value'] == 'not_usable':
            blackListedSites.append(site['name'])
    return(blackListedSites)


def create_locations_map(blocks, rucio):
    # following loop is copied from CRAB DBSDataDiscovery
    # locationsMap is a dictionary: key=blockName, value=list of RSEs}
    # nbFORnr is dictionary: key= number of RSEs with a block, value=number of blocks with that # or RSEs
    # nbFORrse is a dictionary: key=RSEname, value=number of blocks available at that RSE
    # this should be rewritten so that the two dicionaries are filled via a separate loop on
    # locationsMap content. Makes it easier to read, debug, improve
    locationsMap = {}
    nb=0
    for blockName in blocks:
        nb += 1
        print(f'  block: {nb}', end='\r')
        replicas = set()
        response = rucio.list_dataset_replicas(scope='cms', name=blockName, deep=True)
        for item in response:
            if 'Tape' in item['rse']:
                continue  # skip tape locations
            if 'T3_CH_CERN_OpenData' in item['rse']:
                continue  # ignore OpenData until it is accessible by CRAB
            if item['state'].upper() == 'AVAILABLE':  # means all files in the block are on disk
                replicas.add(item['rse'])
        locationsMap[blockName] = replicas
    print("")
    return(locationsMap)


def print_blocks_per_replica_map(nbFORnr):
    nBlocks = 0
    for nr in sorted(list(nbFORnr.keys())):
        nBlocks += nbFORnr[nr]
        msg = f" {nbFORnr[nr]:3} blocks have {nr:2} disk replicas"
        if nr == 0 and nbFORnr[0]>0:
            msg += " *THESE BLOCKS WILL NOT BE ACCESSIBLE*"
        print(msg)
    if not nbFORnr[0]:
        print(" Dataset is fully available")
    else:
        nAvail = nBlocks - nbFORnr[0]
        print(f" Only {nAvail}/{nBlocks} are available")
    return


if __name__ == '__main__':
    main()
