import re
from WMCore.Lexicon import lfnParts, SEARCHDATASET_RE


## This regular expression matches anything. It is useful for example for
## functions that require a regular expression against which they will match a
## given value, but we don't really want to do that matching.
RX_ANYTHING = re.compile(r"^.*$")
# TODO: we should start replacing most of the regex here with what we have in WMCore.Lexicon
#       (this probably requires to adapt something on Lexicon)
wfBase = r"^[a-zA-Z0-9\-_:]{1,%s}$"
pNameRE      = r"(?=.{0,400}$)[a-zA-Z0-9\-_.]+"
lfnParts.update( {'publishname' : pNameRE,
                  'psethash'    : '[a-f0-9]+',
                  'filename'    : '[a-zA-Z0-9\-_\.]'}
)
RX_WORKFLOW  = re.compile( wfBase % 232) #232 = column length in the db (255) - username (8) - timestamp (12) - unserscores (3)
#analysis-crab3=prod jobs; analysistest=preprod jobs; analysis-crab3-hc=special HC tests (CSA14, AAA, ...); hctest=site readiness; test=middlewere validation
RX_ACTIVITY  = re.compile(r'^analysis(test|-crab3(-hc)?)|hc(test|xrootd)|test$')
RX_UNIQUEWF  = re.compile( wfBase % 255)
RX_PUBLISH   = re.compile('^'+pNameRE+'$')
#RX_LFN       = re.compile(r'^(?=.{0,500}$)/store/(temp/)?(user|group)/%(hnName)s/%(primDS)s/%(publishname)s/%(psethash)s/%(counter)s/(log/)?%(filename)s+$' % lfnParts)
RX_LFN       = re.compile(r'^(?=[a-zA-Z0-9\-\._/]{0,500}$)/store/(temp/)?(user/%(hnName)s|group)/?' % lfnParts)
RX_PARENTLFN = re.compile(r'^(/[a-zA-Z0-9\-_\.]+/?)+$')
RX_OUTDSLFN  = re.compile(r'^(?=.{0,500}$)/%(primDS)s/%(hnName)s-%(publishname)s-%(psethash)s/USER$' % lfnParts)
RX_CAMPAIGN  = RX_UNIQUEWF
RX_JOBTYPE   = re.compile(r"^(?=.{0,255}$)[A-Za-z]*$")
RX_GENERATOR = re.compile(r'^lhe$')
RX_LUMIEVENTS = re.compile(r'^\d+$')
RX_CMSSW     = re.compile(r"^(?=.{0,255}$)CMSSW[a-zA-Z0-9-_]*$") #using a lookahead (?=.{0,255}$) to check maximum size of the regex
RX_ARCH      = re.compile(r"^(?=.{0,255}$)slc[0-9]{1}_[a-z0-9]+_gcc[a-z0-9]+(_[a-z0-9]+)?$")
RX_DATASET   = re.compile(SEARCHDATASET_RE)
RX_BLOCK     = re.compile(r"^(/[a-zA-Z0-9\.\-_]{1,100}){3}#[a-zA-Z0-9\.\-_]{1,100}$")
RX_SPLIT     = re.compile(r"^FileBased|EventBased|LumiBased|EventAwareLumiBased$")
RX_CACHEURL  = re.compile(r"^https?://([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_ADDFILE   = re.compile(r"^(?=.{0,255}$)([a-zA-Z0-9\-\._]+)$")
# Can be a LFN or PFN (anything CMSSW accepts is fine here)
RX_USERFILE  = re.compile(r"^(?=.{0,255}$)([a-zA-Z0-9\-._:?/=]+)$")
RX_CACHENAME = RX_USERFILE
RX_CMSSITE   = re.compile(r"^(?=.{0,255}$)T[0-3](_[A-Z]{2}((_[A-Za-z0-9]+)|\*$)+|\*)$")
RX_DBSURL    = re.compile(r"^(?=.{0,255}$)https?://([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_PUBLICATION = re.compile(r"^[TF]")
RX_VOPARAMS  = re.compile(r"^(?=.{0,255}$)[A-Za-z0-9]*$")
RX_OUTFILES  = re.compile(r"^(?=.{0,255}$)%s$"%lfnParts['root'])
RX_RUNS      = re.compile(r"^\d+$")
RX_LUMIRANGE = re.compile(r"^\d+,\d+(,\d+,\d+)*$")
RX_LUMILIST  = re.compile(r"^\d+(,\d+)*$")
RX_GLOBALTAG = re.compile(r'^[a-zA-Z0-9\s\.\-_:]{1,100}$')
RX_OUTTYPES  = re.compile(r'^EDM|LOG|TFILE|FAKE|POOLIN$')
RX_CHECKSUM  = re.compile(r'^[A-Za-z0-9\-]+$')
RX_FILESTATE  = re.compile(r'^TRANSFERRING|FINISHED|FAILED|COOLOFF$')
RX_LFNPATH   = re.compile(r"^(?=.{0,500}$)%(subdir)s(/%(subdir)s)*/?$" % lfnParts)
RX_HOURS   = re.compile(r"^\d{0,6}$") #should be able to erase the last 100 years with 6 digits
RX_ASOURL = RX_DBSURL
RX_URL = re.compile(r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w :\.-])*$")
RX_SCRIPTARGS = re.compile(r'^[+a-zA-Z0-9\-._:?/"]+=[a-zA-Z0-9\-._:?/"]+$')
RX_SCHEDD_NAME = re.compile(r"^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+\.[A-Za-z]{2,6}$")
RX_COLLECTOR = re.compile(r"^(([A-Za-z0-9._-]+\.[A-Za-z]{2,6}),?)+$")
#TODO!
RX_OUT_DATASET = re.compile(r"^.*$")

#basic certificate check -- used for proxies retrieved from myproxy
RX_CERT = re.compile(r'^[-]{5}BEGIN CERTIFICATE[-]{5}[\w\W]+[-]{5}END CERTIFICATE[-]{5}\n$')

#subresourced of DataUserWorkflow (/workflow) resource
RX_SUBRESTAT = re.compile(r"^errors|report|logs|data$")

#subresources of the ServerInfo (/info) and Task (/task) resources
RX_SUBRES_SI = re.compile(r"^delegatedn|backendurls|version|bannedoutdest|scheddaddress|ignlocalityblacklist|$")
RX_SUBRES_TASK = re.compile(r"^allinfo|allusers|summary|search|taskbystatus|addwarning|addwebdir|addoutputdatasets|webdir$")

#worker workflow
RX_WORKER_NAME = re.compile(r"^[A-Za-z0-9\-\._]{1,100}$")
## this can be improved by putting a dependency on CAFUtilities task state machine
RX_STATUS = re.compile(r"^[A-Za-z_]{1,20}$")

##inserted by eric obeng summer student for username validation
RX_USERNAME = re.compile(r"^[A-Za-z_]{1,20}$")
RX_DATE = re.compile(r"^(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])$")

## need to be careful with this
RX_TEXT_FAIL = re.compile(r"^[A-Za-z0-9\-\._\s\=\+]{0,10000}$")
## user dn
RX_DN = re.compile(r"^/(?:C|O|DC)=.*/CN=.")
## worker subresources
RX_SUBPOSTWORKER = re.compile(r"^state|start|failure|success|process|lumimask$")
RX_SUBGETWORKER = re.compile(r"jobgroup")

# Schedulers
RX_SCHEDULER = re.compile(r"^panda|condor$")
