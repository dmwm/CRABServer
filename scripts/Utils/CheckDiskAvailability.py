#
import argparse
# ensure environment
import os
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

# get name of object to test and check what it is
parser = argparse.ArgumentParser()
parser.add_argument('--dataset', help='DBS dataset (or block) name, or Rucio DID (scope:name)', required=True)
args = parser.parse_args()

if ':' in args.dataset:
    scope = args.dataset.split(':')[0]
    dbsDataset = args.dataset.split(':')[1]
    datasetKind = 'Container'
else:
    scope = 'cms'
    dbsDataset = args.dataset
    datasetKind = 'Container'
if scope == 'cms':
    print(f"Checking disk availability of dataset: {dbsDataset}")
else:
    print(f"Checking disk availability of container: {args.dataset}")
if scope != 'cms':
    print(" container in USER scope. Assume it contains datasets(blocks) in CMS scope")
print(" only fully available (i.e. complete) block replicas are considered ")

if '#' in dbsDataset:
    print("Input is a DBS-block (Rucio-dataset) will check that one")
    blocks=[dbsDataset]
else:
    dss = rucio.list_content(scope, dbsDataset)
    blocks = [ds['name'] for ds in dss]
    print(f"dataset has {len(blocks)} blocks")

# OK. Real work starts here

# following loop is copied from CRAB DBSDataDiscovery
# locationsMap is a dictionary: key=blockName, value=list of RSEs}
# nbFORnr is dictionary: key= number of RSEs with a block, value=number of blocks with that # or RSEs
# nbFORrse is a dictionary: key=RSEname, value=number of blocks available at that RSE
# this should be rewritten so that the two dicionaries are filled via a separate loop on
# locationsMap content. Makes it easier to read, debug, improve
locationsMap = {}
nbFORnr = {}
nbFORnr[0] = 0  # this will not be filled in the loop if every block has locations
nbFORrse = {}

print("Checking blocks availabiliyt on disk...")
nb=0
for blockName in blocks:
    nb += 1
    print(f'  block: {nb}', end='\r')
    replicas = set()
    response = rucio.list_dataset_replicas(scope='cms', name=blockName, deep=True)
    for item in response:
        #print(f"{blockName}...")
        if 'T2_UA_KIPT' in item['rse']:
            continue  # skip Ucrainan T2 until further notice
        if 'Tape' in item['rse']:
            continue  # skip tape locations
        if 'T3_CH_CERN_OpenData' in item['rse']:
            continue  # ignore OpenData until it is accessible by CRAB
        if item['state'].upper() == 'AVAILABLE':  # means all files in the block are on disk
            replicas.add(item['rse'])
    if replicas:  # only fill map for blocks which have at least one location
        locationsMap[blockName] = replicas
        try:
            nbFORnr[len(replicas)] += 1
        except:
            nbFORnr[len(replicas)] = 1
        for rse in replicas:
            try:
                nbFORrse[rse] +=1
            except:
                nbFORrse[rse] = 1
    else:
        nbFORnr[0] += 1
print()

# this should be rewritten as a series of print, no need to create a msg
print(f"dataset has {len(locationsMap)} available blocks")
msg = ""
for nr in sorted(list(nbFORnr.keys())):
    msg += f"\n {nbFORnr[nr]:3} blocks have {nr:2} disk replicas"
if not nbFORnr[0]:
    msg += f"\n Dataset is fully available on disk\n"
msg += f"\n Site location"
for rse in nbFORrse.keys():
    msg += f"\n {rse:15} hosts {nbFORrse[rse]:3} blocks"
msg += f"\n "
print(msg)

print("Rules on this dataset:")
rule_gens=rucio.list_did_rules(scope=scope, name=dbsDataset)
rules = list(rule_gens)
if rules:
    pattern = '{:^32}{:^20}{:^10}{:^20}{:^30}'
    print(pattern.format('ID','account','state','expiration','RSEs'))
    for r in rules:
        print(pattern.format(r['id'],r['account'],r['state'], str(r['expires_at']), r['rse_expression']))
else:
    print("NONE")
