import re
from WMCore.Lexicon import lfnParts

# TODO: we should start replacing most of the regex here with what we have in WMCore.Lexicon
#       (this probably requires to adapt something on Lexicon)
wfBase = r"^[a-zA-Z0-9\.\-_]{1,%s}$"
pNameRE      = r"(?=.{0,400}$)[a-zA-Z0-9\-_]+"
lfnParts.update( {'publishname' : pNameRE,
                  'psethash'    : '[a-f0-9]+',
                  'filename'    : '[a-zA-Z0-9\-_\.]'}
)
RX_WORKFLOW  = re.compile( wfBase % 232) #232 = column length in the db (255) - username (8) - timestamp (12) - unserscores (3)
RX_UNIQUEWF  = re.compile( wfBase % 255)
RX_PUBLISH   = re.compile(pNameRE)
RX_LFN       = re.compile(r'^(?=.{0,500}$)/store/(temp/)?(user|group)/%(hnName)s/%(primDS)s/%(publishname)s/%(psethash)s/%(counter)s/(log/)?%(filename)s+(.root|.tgz)$' % lfnParts)
RX_PARENTLFN = re.compile(r'^(/[a-zA-Z0-9\-_\.]+/?)+$')
RX_OUTDSLFN  = re.compile(r'^(?=.{0,500}$)/%(primDS)s/%(hnName)s-%(publishname)s-%(psethash)s/USER$' % lfnParts)
RX_CACHENAME = RX_WORKFLOW
RX_CAMPAIGN  = RX_UNIQUEWF
RX_JOBTYPE   = re.compile(r"^(?=.{0,255}$)[A-Za-z]*$")
RX_CMSSW     = re.compile(r"^(?=.{0,255}$)CMSSW(_\d+){3}(_[a-zA-Z0-9_]+)?$") #using a lookahead (?=.{0,255}$) to check maximum size of the regex
RX_ARCH      = re.compile(r"^(?=.{0,255}$)slc[0-9]{1}_[a-z0-9]+_gcc[a-z0-9]+(_[a-z0-9]+)?$")
RX_DATASET   = re.compile(r"^(?=.{0,500}$)/(\*|[a-zA-Z\*][a-zA-Z0-9_\-\*]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,100})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$")
RX_BLOCK     = re.compile(r"^(/[a-zA-Z0-9\.\-_]{1,100}){3}#[a-zA-Z0-9\.\-_]{1,100}$")
RX_SPLIT     = re.compile(r"^FileBased|EventBased|LumiBased$")
#RX_CACHEURL  = re.compile(r"^(?=.{1,255}$)[0-9A-Za-z](?:(?:[0-9A-Za-z]|\b-){0,61}[0-9A-Za-z])?(?:\.[0-9A-Za-z](?:(?:[0-9A-Za-z]|\b-){0,61}[0-9A-Za-z])?)*\.?$")
RX_CACHEURL  = re.compile(r"^https?://([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_ADDFILE   = re.compile(r"^(?=.{0,255}$)([a-zA-Z0-9\-\._]+)$")
RX_CMSSITE   = re.compile(r"^(?=.{0,255}$)T[0-3%]((_[A-Z]{2}(_[A-Za-z0-9]+)*)?)$")
RX_DBSURL    = re.compile(r"^(?=.{0,255}$)https?://([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_PUBLICATION = re.compile(r"^[TF]")
RX_VOPARAMS  = re.compile(r"^(?=.{0,255}$)[A-Za-z0-9]*$")
RX_OUTFILES  = re.compile(r"^(?=.{0,255}$)[A-Za-z0-9\.\-_]*\.root$")
RX_RUNS      = re.compile(r"^\d+$")
RX_LUMIRANGE = re.compile(r"^\d+,\d+(,\d+,\d+)*$")
RX_LUMILIST  = re.compile(r"^\d+(,\d+)*$")
RX_GLOBALTAG = re.compile(r'^[a-zA-Z0-9\s\.\-_:]{1,100}$')
RX_OUTTYPES  = re.compile(r'^EDM|LOG|TFILE$')
RX_CHECKSUM  = re.compile(r'^[A-Za-z0-9\-]+$')

#basic certificate check -- used for proxies retrieved from myproxy
RX_CERT = re.compile(r'^[-]{5}BEGIN CERTIFICATE[-]{5}[\w\W]+[-]{5}END CERTIFICATE[-]{5}\n$')

#subresourced of DataUserWorkflow (/workflow) resource
RX_SUBRESTAT = re.compile(r"^errors|report|logs|data$")

#subresources of the ServerInfo (/info) resource
RX_SUBRES_SI = re.compile(r"^delegatedn$")
