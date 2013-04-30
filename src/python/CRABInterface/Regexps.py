import re

# TODO: we should start replacing most of the regex here with what we have in WMCore.Lexicon
#       (this probably requires to adapt something on Lexicon)
RX_WORKFLOW  = re.compile(r"^[a-zA-Z0-9\.\-_]{1,80}$")
RX_CAMPAIGN  = RX_WORKFLOW
RX_JOBTYPE   = re.compile(r"^[A-Za-z]*$")
RX_CMSSW     = re.compile(r"^CMSSW(_\d+){3}(_[a-zA-Z0-9_]+)?$")
RX_ARCH      = re.compile(r"^slc[0-9]{1}_[a-z0-9]+_gcc[a-z0-9]+(_[a-z0-9]+)?$")
RX_DATASET   = re.compile(r"^/(\*|[a-zA-Z\*][a-zA-Z0-9_\-\*]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,100})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$")
RX_BLOCK     = re.compile(r"^(/[a-zA-Z0-9\.\-_]{1,100}){3}#[a-zA-Z0-9\.\-_]{1,100}$")
RX_SPLIT     = re.compile(r"^FileBased|EventBased|LumiBased$")
RX_CFGDOC    = re.compile(r"^[A-Za-z0-9]*$")
#RX_CACHEURL  = re.compile(r"^(?=.{1,255}$)[0-9A-Za-z](?:(?:[0-9A-Za-z]|\b-){0,61}[0-9A-Za-z])?(?:\.[0-9A-Za-z](?:(?:[0-9A-Za-z]|\b-){0,61}[0-9A-Za-z])?)*\.?$")
RX_CACHEURL  = re.compile(r"^https?://([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_ADDFILE   = re.compile(r"^([a-zA-Z0-9\-\._]+)$")
RX_PUBLISH   = re.compile(r"[a-zA-Z0-9\-_]+")
RX_CMSSITE   = re.compile(r"^T[0-3%]((_[A-Z]{2}(_[A-Za-z0-9]+)*)?)$")
RX_DBSURL    = re.compile(r"^https?://cmsweb([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_PUBDBSURL = re.compile(r"^https?://([-\w\.]*)\.cern\.ch+(:\d+)?(/([\w/_\.]*(\?\S+)?)?)?$")
RX_VOPARAMS  = re.compile(r"^[A-Za-z0-9]*$")
RX_CACHENAME = RX_WORKFLOW
RX_OUTFILES  = re.compile(r"^[A-Za-z0-9\.\-_]*\.root$")
RX_RUNS      = re.compile(r"^\d+$")
RX_LUMIRANGE  = re.compile(r"^\d+,\d+(,\d+,\d+)*$")
RX_LUMILIST  = re.compile(r"^\d+(,\d+)*$")
RX_GLOBALTAG = re.compile(r'^[a-zA-Z0-9\s\.\-_:]{1,100}$')
RX_OUTTYPES  = re.compile(r'^EDM|LOG$')
RX_LFN       = re.compile(r'.*')#TODO find out the right regular expression. Maybe in relation with previous WMCore Lexicon merge(see TODO at the beginning). BTW, Lexicon has 5 regexp for lfn
RX_CHECKSUM  = re.compile(r'^[A-Za-z0-9\-]+$')#TODO check the format of checksum

#basic certificate check -- used for proxies retrieved from myproxy
RX_CERT = re.compile(r'^[-]{5}BEGIN CERTIFICATE[-]{5}[\w\W]+[-]{5}END CERTIFICATE[-]{5}\n$')

#subresourced of DataUserWorkflow (/workflow) resource
RX_SUBRESTAT = re.compile(r"^errors|report|logs|data$")

#subresources of the ServerInfo (/info) resource
RX_SUBRES_SI = re.compile(r"^delegatedn$")
