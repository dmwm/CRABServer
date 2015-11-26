import os
import optparse

from TaskWorker.Actions.Recurring.RenewRemoteProxies import CRAB3ProxyRenewer
from WMCore.Configuration import loadConfigurationFile

def main():
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", dest="config", help="TaskWorker configuration file.")
    opts, args = parser.parse_args()

    configuration = loadConfigurationFile( os.path.abspath(opts.config) )
    renewer = CRAB3ProxyRenewer(configuration)
    renewer.execute()

if __name__ == '__main__':
    main()

