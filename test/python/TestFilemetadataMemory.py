""" This is a module written to test the memory usage of a critical API of the REST interface of CRAB3
    The API is getFiles in filemetadata (https://github.com/dmwm/CRABServer/blob/3.3.12.rc1/src/python/CRABInterface/DataFileMetadata.py#L22-L47)

    While debugging a high memory usage in CMSWEB (see https://github.com/dmwm/CRABServer/issues/5071)
    we found out that a lot of data from this API was in a coredump of the python process.

    This API is returning an iterator over big dictionaries (see filemetadataDict) which is converted to a list and then
    json.dump(ed) in the body of the HTTP request.

    During my investigation I discovered that the API itself is not "leaking data", but the python process does not always
    return the memory to the OS since it does its own internal memory optimization (avoid a lot of malloc/free calls with small data).
    Therefore, if small data are present in the dicts then the python process will consume a lot of memory even no objects
    are referenced. The same things does not happen if the dictionaries are converted to strings before returning (yielding) them.

    Take for example the code below that simulate the behavior of this API: filemetadataDict returns an iterator over big dictionaries
    and with the command line we can control if we want to convert these dicts to string (either using str or json.dumps)

    The following values are obtained, running the code:

    DICT 1:15 minutes, memory usage 419320 Kb after allocating the big list of dict, memory usage at the end of the program 343156 Kb
    STR  1:19 minutes, memory usage 160864 Kb after allocating the big list of dict, memory usage at the end of the program 7848 Kb
    JSON 1:35 minutes, memory usage 160904 Kb after allocating the big list of dict, memory usage at the end of the program 7860 Kb

    You can see that if dicts are not converted to strings the allocated memory at the end of the process is 300 Mb.
    This was tested with python 2.6.6 (but I observed the same on python 2.7.5).
"""
from __future__ import division
from __future__ import print_function


import gc
import os
import sys
import json
import psutil
import random
import string

#global variable containing the function used to convert the dictionary
g_conversion = None

def randstr(size):
    """ Generate a random string of size charaters
        For example randstr(20) returns something like:
        'L2YEEREND4THO0RP4ZYA'
    """
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in xrange(size))


def bigdict(size):
    """ Return a big dictionary that is as similar as possible to the one used in the
        runlumi field of the filemetadata.

        Return something like:
            {'140295': ['81809',
                '56876',
                '106697',
                '141068',
                '86853',
                '59688',
                '72787',
                '102868',
                '57237']
            }
    """
    res = {}
    runnum = str(random.randint(50000, 150000))
    res[runnum] = []
    for _ in xrange(1, size):
        res[runnum].append(str(random.randint(50000, 150000)))

    return res


def filemetadataDict(nfiles, lumisPerFile):
    """ Return some data as similar as possible to the filemetadata
    """
    for _ in xrange(nfiles):
        yield g_conversion({
            "cksum": 0,
            "publishname": randstr(random.randint(40,60)), #"TopTree_ZJets_25ns-00000000000000000000000000000000"
            "adler32": "0",
            "globaltag": "None",
            "outdataset": randstr(random.randint(70,160)), #"/FakeDataset/fakefile-FakePublish-5b6a581e4ddd41b130711a045d5fecb9/USER"
            "lfn": randstr(random.randint(150,250)),#"/store/mc/RunIISpring15MiniAODv2/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/F6D2F58C-B170-E511-9177-002618943885.root_34"
            "pandajobid": random.randint(1,10000),
            "filesize": 0,
            "filetype": random.choice(["POOLIN","TFILE","EDM","LOG"]),
            "swversion": "CMSSW_7_4_%s" % random.randint(1,50),
            "state": "None",
            "created": randstr(30),#"2015-12-11 07:28:06.941284"
            "runlumi": bigdict(lumisPerFile),#{"1": ["68101", ... "304376"] ...}
            "tmplfn": randstr(random.randint(150,250)),#"/store/mc/RunIISpring15MiniAODv2/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/MINIAODSIM/74X_mcRun2_asymptotic_v2-v1/60000/F6D2F58C-B170-E511-9177-002618943885.root_34"
            "parents": [randstr(random.randint(150,250)) for _ in xrange(10,20)], #list of parents
            "location": randstr(1),#"T2_ES_CIEMAT"
            "inevents": random.randint(50000,250000),
            "taskname": randstr(random.randint(30,60)),#"151210_191604:mabarrio_crab_TopTree_ZJets"
            "acquisitionera": "null",
            "tmplocation": randstr(1),#"T2_US_Wisconsin"
            "md5": "0"
        })


def printInfo(nfiles, lumisPerFile):
    """ Print the information after creating a dict of nfiles files each
        containing lumisPerFile lumis. Print the memory info after garbage collecting the object
    """
    print("Task with %s files and %s lumis" % (nfiles, lumisPerFile))
    d = list(filemetadataDict(nfiles, lumisPerFile))
    s = str(d)
    print("Dictionary generated and converted to string. String len is %s. Memory usage" % (len(s)/1024))
    print(process.get_memory_info()[0]/1024)
    d = None
    s = None
    gc.collect()
    print("Memory usage after running the garbage collector")
    print(process.get_memory_info()[0]/1024)


if __name__ == "__main__":
    convFunctions = {
        'DICT' : lambda x: x, #identity
        'JSON' : json.dumps,
        'STR' : str,
    }

    dataType = sys.argv[1]
    if dataType not in convFunctions:
        print("Argument of the script is the type of conversion of the bigdict" % convFunctions.keys())

    g_conversion = convFunctions[dataType]

    process = psutil.Process(os.getpid())

    print("Memory starting the program")
    print(process.get_memory_info()[0]/1024)

    printInfo(100, 30) #small task
    printInfo(1000, 100) #Average case (1k jobs, 300 lumis per job) ?
    printInfo(100, 30) #small task
    printInfo(10000, 500) #Big task (10k jobs, 500 lumis per job) ?
    printInfo(100, 30)
    printInfo(1000, 100)
    printInfo(100, 30)
    printInfo(10000, 500)
