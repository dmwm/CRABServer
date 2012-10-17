import re

# TODO: we should start replacing most of the regex here with what we have in WMCore.Lexicon
#       (this probably requires to adapt something on Lexicon)
wfBase = r"^[a-zA-Z0-9\.\-_]{1,%s}$"
RX_WORKFLOW  = re.compile( wfBase % 80 )
#  8  +  1        +    80     +         1 +6 + 1 + 6
#username_OriginalWorkflowNameMax80Chars_121004_100926 => username is max 8 chars, timestamp is twelve chars, 3 underscores
RX_UNIQUEWF  = re.compile( wfBase % (8 + 1 + 80 + 1 + 6 + 1 + 6))
RX_CAMPAIGN  = RX_UNIQUEWF
RX_JOBTYPE   = re.compile(r"^[A-Za-z]*$")
RX_CMSSW     = re.compile(r"^CMSSW(_\d+){3}(_[a-zA-Z0-9_]+)?$")
RX_ARCH      = re.compile(r"^slc[0-9]{1}_[a-z0-9]+_gcc[a-z0-9]+(_[a-z0-9]+)?$")
RX_DATASET   = re.compile(r"^/(\*|[a-zA-Z\*][a-zA-Z0-9_\-\*]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,100})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$")
RX_BLOCK     = re.compile(r"^(/[a-zA-Z0-9\.\-_]{1,100}){3}#[a-zA-Z0-9\.\-_]{1,100}$")
RX_SPLIT     = re.compile(r"^EventBased|FileBased|LumiBased|RunBased|SizeBased$")
RX_CFGDOC    = re.compile(r"^[A-Za-z0-9]*$")
RX_CACHEURL  = re.compile(r"^https?://([-\w\.]+)+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_ADDFILE   = re.compile(r"^([a-zA-Z0-9\-\._]+)$")
RX_PUBLISH   = re.compile(r"[a-zA-Z0-9\-_]+")
RX_CMSSITE   = re.compile(r"^T[0-3%](\*?|(_[A-Z]{2}(_[A-Za-z0-9]+)*)?)$")
RX_DBSURL    = re.compile(r"^https?://cmsweb([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_PUBDBSURL = re.compile(r"^https?://([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_ACDCDOC   = re.compile(r"^[A-Za-z0-9]*$")
RX_GLOBALTAG = re.compile(r"[a-zA-Z0-9\s\.\-_:]{1,100}$")
RX_RUNS      = re.compile(r"^\d+$")
RX_LUMILIST  = re.compile(r"^\d+,\d+(,\d+,\d+)*$")
RX_VOROLES  = re.compile(r"^[A-Za-z]{1,20}$")

RX_SUBRESTAT = re.compile(r"^errors|report|logs|data|schema|configcache|type|fwjr$")
#subresources of the ServerInfo (/info) resource
RX_SUBRES_SI = re.compile(r"^delegatedn$")

RX_WFRESUB   = re.compile(r"^[a-zA-Z0-9\.\-_]*_resubmit_[0-9]+")
