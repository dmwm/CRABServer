
import argparse

from rucio.client import Client
rucio=Client()

parser = argparse.ArgumentParser()
parser.add_argument('--dataset', help='DBS dataset name', required=True)
args = parser.parse_args()
dbsDataset = args.dataset

print(f"Checking disk availability of dataset: {dbsDataset}")
print(" only blocks fully replicated are listed ")

scope = 'cms'
dss = rucio.list_content(scope, dbsDataset)
blocks = [ds['name'] for ds in dss]


# locationsMap is a dictionary: key=blockName, value=list of RSEs}
# nbFORnr is dictionary: key= number of RSEs with a block, value=number of blocks with that # or RSEs
locationsMap = {}
nbFORnr = {}
nbFORnr[0] = 0  # this will not be filled in the loop if every block has locations
nbFORrse = {}

#pbar = tqdm.tqdm(total=len(blocks))
nb=0
for blockName in blocks:
    nb += 1
    print(f'  block: {nb}', end='\r')
    replicas = set()
    response = rucio.list_dataset_replicas(scope=scope, name=blockName, deep=True)
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
#    pbar.update()
print()

print(f"dataset has {len(locationsMap)} blocks")
msg = "\n"
for nr in sorted(list(nbFORnr.keys())):
    msg += f"\n {nbFORnr[nr]:3} blocks have {nr:2} disk replicas"
msg += f"\n "
if not nbFORnr[0]:
    msg += f"\n Dataset is fully available on disk\n"
msg += f"\n Site location"
for rse in nbFORrse.keys():
    msg += f"\n {rse:15} hosts {nbFORrse[rse]:3} blocks"
msg += f"\n "
print(msg)

print("Rules on this dataset:")
rule_gens=rucio.list_did_rules(scope='cms', name=dbsDataset)
rules = list(rule_gens)
if rules:
    pattern = '{:^32}{:^20}{:^10}{:^12}{:^20}'
    print(pattern.format('ID','account','state','expiration','RSEs'))
    for r in rules:
        print(pattern.format(r['id'],r['account'],r['state'], str(r['expires_at']), r['rse_expression']))
else:
    print("NONE")
