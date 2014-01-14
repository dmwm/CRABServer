
import WMCore.Configuration as Configuration

config = Configuration.Configuration()

config.section_("Services")
config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'

config.section_("Sites")
config.Sites.available = ["T2_US_Nebraska"]

