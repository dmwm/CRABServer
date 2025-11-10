import re
from WMCore.Lexicon import lfnParts, DATASET_RE


## This regular expression matches any single-line string. It is useful for example for
## functions that require a regular expression against which they will match a
## given value, but we don't really want to do that matching.
RX_ANYTHING = re.compile(r"^.*$")
# This regular expression matches any string, also multi-line strings.
# as of 2022-03 it is used for warning and failure messages, which were previously
# encoded in b64 and matched with RX_TEXT_FAIL. see #7106
RX_MANYLINES_SHORT = re.compile(r"(?s)^.{0,1100}$")

RX_ANYTHING_10K = re.compile(r"^.{0,10000}$")
# TODO: we should start replacing most of the regex here with what we have in WMCore.Lexicon
#       (this probably requires to adapt something on Lexicon)
pNameRE      = r"(?=.{0,400}$)[a-zA-Z0-9\-_.]+"
lfnParts.update( {'publishname' : pNameRE,
                  'psethash'    : r'[a-f0-9]+',
                  'filename'    : r'[a-zA-Z0-9\-_\.]'}
)
## Although the taskname column in the TaskDB accepts tasknames of up to 255
## characters, we limit the taskname to something less than that in order to
## have room to define filenames based on the taskname (filenames have a limit
## of 255 characters). The 232 we chose is a relic from releases < 3.3.1512 when
## we were using RX_TASKNAME to validate both 'crab_<requestname>' from the
## client and '<taskname>' (or '<workflow>') from the TaskWorker. We keep the
## 232 to not break backward compatibility.
RX_TASKNAME_LEN = 232
RX_TASKNAME  = re.compile(r"^[a-zA-Z0-9\-_:]{1,%s}$" % RX_TASKNAME_LEN)
#activity defaults to "None". Use cases: analysis*=for use in case of need;  hctest*=site readiness; test*=middlewere validation
RX_ACTIVITY  = re.compile(r'^(?=.{0,100}$)(analysis|hctest|test)[a-zA-Z0-9\-\._]*$')
RX_PUBLISH   = re.compile('^'+pNameRE+'$')
#RX_LFN       = re.compile(r'^(?=.{0,500}$)/store/(temp/)?(user|group)/%(hnName)s/%(primDS)s/%(publishname)s/%(psethash)s/%(counter)s/(log/)?%(filename)s+$' % lfnParts)
RX_LFN       = re.compile(r'^(?=[a-zA-Z0-9\-\._/]{0,500}$)/store/(temp/)?(user/%(hnName)s|group|local|test)/?' % lfnParts)
RX_PARENTLFN = re.compile(r'^(/[a-zA-Z0-9\-_\.]+/?)+$')
RX_OUTDSLFN  = re.compile(r'^(?=.{0,500}$)/%(primDS)s/(%(hnName)s|%(physics_group)s)-%(publishname)s-%(psethash)s/USER$' % lfnParts)
RX_CAMPAIGN  = RX_TASKNAME
RX_JOBTYPE   = re.compile(r"^(?=.{0,255}$)[A-Za-z]*$")
RX_GENERATOR = re.compile(r'^(lhe|pythia)$')
RX_LUMIEVENTS = re.compile(r'^\d+$')
RX_CMSSW     = re.compile(r"^(?=.{0,255}$)CMSSW[a-zA-Z0-9-_]*$") #using a lookahead (?=.{0,255}$) to check maximum size of the regex
RX_ARCH      = re.compile(r"^(?=.{0,255}$)[a-z]+[0-9]{1,2}_[a-z0-9]+_gcc[a-z0-9]+(_[a-z0-9]+)?$")
RX_MICROARCH = re.compile(r"^(?=.{0,255}$)[a-zA-Z0-9\-\_\.]+$")
RX_DATASET   = re.compile(DATASET_RE) #See https://github.com/dmwm/WMCore/issues/6054#issuecomment-135475550
RX_LFNPRIMDS = re.compile(r"^%(primDS)s$" % lfnParts)
RX_BLOCK     = re.compile(r"^(/[a-zA-Z0-9\.\-_]{1,100}){3}#[a-zA-Z0-9\.\-_]{1,100}$")
RX_SPLIT     = re.compile(r"^(Automatic|FileBased|EventBased|LumiBased|EventAwareLumiBased)$")
RX_ADDFILE   = re.compile(r"^(?=.{0,255}$)([a-zA-Z0-9\-\._]+)$")
# Can be a LFN or PFN (anything CMSSW accepts is fine here)
RX_USERFILE  = re.compile(r"^(?=.{0,255}$)([a-zA-Z0-9\-._:?/=]+)$")
RX_CACHENAME = RX_USERFILE
RX_TARBALLNAME = RX_USERFILE
RX_CMSSITE   = re.compile(r"^(?=.{0,255}$)T[0-3](_[A-Z]{2}((_[A-Za-z0-9]+)|_?\*$)+|_?\*)$")
RX_DBSURL    = re.compile(r"^(?=.{0,255}$)https?://([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_PUBLICATION = re.compile(r"^[TF]")
RX_VOPARAMS  = re.compile(r"^(?=.{0,255}$)[A-Za-z0-9]*$")
RX_OUTFILES  = re.compile(r"^(?=.{0,255}$)%s$"%lfnParts['root'])
RX_JOBID     = re.compile(r"^\d+(-\d+){0,1}$")
RX_RUNS      = re.compile(r"^\d+$")
RX_LUMIRANGE = re.compile(r"^\d+,\d+(,\d+,\d+)*$")
RX_RUCIORULE = re.compile(r"^(?=.{32}$)[A-Za-z0-9]*$")
RX_RUCIODID =  re.compile(r"^.*:.*$")  # Rucio has no limitations: scope:name is a valid DID
RX_RUCIOSCOPE = re.compile(r"^(cms)|(logs)|(user\.[a-z0-9-_]{1,20})$|(group\.[a-z0-9-_]{1,19})$")

# Supports two formats for backward compatibility:
# '1,4,6,8,9,10...' (old format which is still present in old postjobs)
# '1:20,4:20,6:21,8:20,9:20,10:21...' (new format with events per each lumi)
RX_LUMILIST = re.compile(r"^(\d+(,\d+)*)$|^((\d+:(\d+|None))(,\d+:(\d+|None))*)$")
RX_GLOBALTAG = re.compile(r'^[a-zA-Z0-9\s\.\-_:]{1,100}$')
RX_OUTTYPES  = re.compile(r'^(EDM|LOG|TFILE|FAKE|POOLIN|DQM)$')
RX_CHECKSUM  = re.compile(r'^[A-Za-z0-9\-]+$')
RX_FILESTATE  = re.compile(r'^(TRANSFERRING|FINISHED|FAILED|COOLOFF)$')
RX_LFNPATH   = re.compile(r"^(?=.{0,500}$)%(subdir)s(/%(subdir)s)*/?$" % lfnParts)
RX_HOURS   = re.compile(r"^\d{0,6}$") #should be able to erase the last 100 years with 6 digits
RX_URL = re.compile(r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w :\.-])*$")
RX_SCRIPTARGS = re.compile(r'^[+a-zA-Z0-9\-.,_:?/"]+ ?= ?[a-zA-Z0-9\-=.,_:?/"()]+$')
RX_SCHEDD_NAME = re.compile(r"^[A-Za-z0-9._-]+[@.][A-Za-z0-9._-]+\.[A-Za-z]{2,6}$")
RX_COLLECTOR = re.compile(r"^(([A-Za-z0-9._-]+\.[A-Za-z]{2,6}),?)+$")
#TODO!
RX_OUT_DATASET = re.compile(r"^.*$")
RX_CLUSTERID = re.compile(r"^[0-9.]+$")
#basic certificate check -- used for proxies retrieved from myproxy
RX_CERT = re.compile(r'^[-]{5}BEGIN CERTIFICATE[-]{5}[\w\W]+[-]{5}END CERTIFICATE[-]{5}\n$')

#subresourced of DataUserWorkflow (/workflow) resource
RX_SUBRESTAT = re.compile(r"^(errors|report2|logs|data|logs2|data2|resubmit|resubmit2|proceed|publicationstatus|taskads)$")

#subresources of the ServerInfo (/info) and Task (/task) resources
RX_SUBRES_SI = re.compile(r"^(delegatedn|backendurls|version|bannedoutdest|scheddaddress|ignlocalityblacklist)$")
RX_SUBRES_TASK = re.compile(r"^(allinfo|allusers|summary|search|status|taskbystatus|taskbyddmreqid|getpublishurl|addwarning|deletewarnings|addwebdir|addoutputdatasets|addddmreqid|webdir|webdirprx|counttasksbystatus|counttasksbyuserandstatus|lastfailures|lastrefused|updateschedd|updatepublicationtime|addrucioasoinfo|edit)$")
RX_TASK_COLUMN = re.compile(r"^[a-z_]{1,50}$")  # ideally could like all table columns.... but how do we maintain it ?

#subresources of Cache resource
RX_SUBRES_CACHE = re.compile(r"^(upload|download|retrieve|list|used)$")
RX_CACHE_OBJECTTYPE = re.compile(r"^(clientlog|twlog|sandbox|debugfiles|runtimefiles)$")
RX_PRESIGNED_CLIENT_METHOD = re.compile(r"^(get_object|head_object)$")

#worker workflow
RX_WORKER_NAME = re.compile(r"^[A-Za-z0-9\-\._%]{1,100}$")
## this can be improved by putting a dependency on CAFUtilities task state machine
RX_STATUS = re.compile(r"^[A-Za-z_]{1,20}$")

RX_USERNAME = re.compile(r"^[A-Za-z_:0-9]{1,100}$")
RX_DATE = re.compile(r"^(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])$")

## user dn
RX_DN = re.compile(r"^/(?:C|O|DC)=.*/CN=.")
## worker subresources
RX_SUBPOSTWORKER = re.compile(r"^(state|start|failure|success|process|lumimask)$")

# Schedulers
RX_SCHEDULER = re.compile(r"^(condor)$")



# File Transfers api
RX_SUBPOSTTRANSFER = re.compile(r"^(acquireTransfers|acquirePublication|updateTransfers|updateRucioInfo|updatePublication|retryPublication|retryTransfers|killTransfers)$")
RX_SUBGETTRANSFER = re.compile(r"^(acquiredTransfers|acquiredPublication|getVOMSAttributesForTask|groupedTransferStatistics|groupedPublishStatistics|activeUsers|getTransfersToKill)$")

RX_USERGROUP = RX_ANYTHING
RX_USERROLE = RX_ANYTHING
# 0 - False
# 1 - True
RX_PUBLICATION_STATE = re.compile(r"^[01]")
# 0 - new
# 1 - acquired
# 2 - failed
# 3 - done
# 4 - retry
# 5 - submitted
# 6 - kill
# 7 - killed
RX_TRANSFER_STATE = re.compile(r"^[01234567]")
# 0 - new
# 1 - acquired
# 2 - failed
# 3 - done
# 4 - retry
RX_PUBLISH_STATE = re.compile(r"^[01234]")
RX_ASO_WORKERNAME = RX_WORKER_NAME

RX_SUBGETUSERTRANSFER = re.compile(r"^(getById|getTransferStatus|getPublicationStatus)$")
RX_SUBPOSTUSERTRANSFER = re.compile(r"^(killTransfers|retryPublication|retryTransfers|killTransfersById|updateDoc)$")

# CUDAVersion style,  i.e. 11.4, 515.43.04
RX_CUDA_VERSION = re.compile(r"^\d+\.\d+(\.\d+)?$")
