#!/usr/bin/env python2.6
"""
    NOTE: This is intended to run automagically. Keep the deps minimal
"""
import sys, os, os.path, re, shutil, string
from distutils.core import setup, Command
from distutils.command.build import build
from distutils.command.install import install
from distutils.spawn import spawn
from glob import glob
import os


systems = \
{
  'CRABClient': #Will be used if we moved the CRABClient repository
  {
    'py_modules' : ['PandaServerInterface', 'RESTInteractions', 'ServerUtilities'],
    'python': [],
  },
  'CRABInterface':
  {
    'py_modules' : ['PandaServerInterface','CRABQuality', 'HTCondorUtils', 'HTCondorLocator', 'ServerUtilities'],
    'python': ['CRABInterface','CRABInterface/Pages',
               'Databases',
                 'Databases/FileMetaDataDB', 'Databases/FileMetaDataDB/Oracle',
                 'Databases/FileMetaDataDB/Oracle/FileMetaData',
                 'Databases/TaskDB', 'Databases/TaskDB/Oracle',
                 'Databases/TaskDB/Oracle/JobGroup',
                 'Databases/TaskDB/Oracle/Task'],},
  'TaskWorker':
  {
    'py_modules' : ['PandaServerInterface', 'RESTInteractions', 'ApmonIf',
                    'apmon', 'DashboardAPI', 'Logger', 'ProcInfo',
                    'CRABQuality', 'HTCondorUtils', 'HTCondorLocator',
                    'ServerUtilities', 'MultiProcessingLog', 'CMSGroupMapper'],
    'python': ['TaskWorker', 'TaskWorker/Actions', 'TaskWorker/DataObjects',
                'TaskWorker/Actions/Recurring', 'taskbuffer']
  },
  'UserFileCache':
  {
    'py_modules' : [''],
    'python': ['UserFileCache']
  },
  'All':
  {
    'python': ['TaskWorker', 'CRABInterface', 'UserFileCache', 'CRABClient']
  }
}

# These repos come from git clone, so we need to speicify the repo and ref
DEFAULT_CMSDIST_REPO = "git@github.com:cms-sw/cmsdist.git"
DEFAULT_CMSDIST_REF = "comp"
DEFAULT_PKGTOOLS_REPO = "git@github.com:cms-sw/cmsdist.git"
DEFAULT_PKGTOOLS_REF = "V00-21-XX"

DEFAULT_CRABCLIENT = "git://github.com/dmwm/CRABClient.git?obj=master/%{realversion}&export=CRABClient-%{realversion}&output=/CRABClient-%{realversion}.tar.gz"
DEFAULT_WMCORE = "git://github.com/dmwm/WMCore.git?obj=master/%{wmcver}&export=WMCore-%{wmcver}&output=/WMCore-%{n}-%{wmcver}.tar.gz"
# crabclient spec and crabserver specs have different defaults
DEFAULT_CS_CRABSERVER = "git://github.com/dmwm/CRABServer.git?obj=master/%{realversion}&export=CRABServer-%{realversion}&output=/CRABServer-%{realversion}.tar.gz"
DEFAULT_CC_CRABSERVER = "git://github.com/dmwm/CRABServer.git?obj=master/%{crabserverver}&export=CRABServer-%{crabserverver}&output=/CRABServer-%{crabserverver}.tar.gz"

class PackageCommand(Command):
    description = """
        Handles building RPM(s)

        By default, all sources are used from their official locations. However
        command line options give the user the option to ovveride the different
        repositories with either different GH tags or a local directory.
        """
    user_options = []
    user_options.append(("targets=", None,
                        "Package specified systems (default: CRABClient,CRABServer,TaskWorker)"))
    user_options.append(("crabServerPath=", None, "Override CRABServer repo location"))
    user_options.append(("crabClientPath=", None, "Override CRABClient repo location"))
    user_options.append(("wmCorePath=", None, "Override WMCore repo location"))

    user_options.append(("pkgToolsRepo=", None, "Path to existing pkgtools repo"))
    user_options.append(("pkgToolsRef=", None, "If pkgToolsPath is a git url, what ref to get"))
    user_options.append(("cmsdistRepo=", None, "Override cmsdist repo location"))
    user_options.append(("cmsdistRef=", None, "If cmsdistPath is a git url, what ref to get"))

    def initialize_options(self):
        self.targets = "CRABClient,CRABServer,TaskWorker"
        # I'll need to fix things later for the crabserver ref
        self.crabServerPath = None
        self.crabClientPath = DEFAULT_CRABCLIENT
        self.wmCorePath = DEFAULT_WMCORE
        self.pkgToolsRepo = DEFAULT_PKGTOOLS_REPO
        self.pkgToolsRef = DEFAULT_PKGTOOLS_REF
        self.cmsdistRepo = DEFAULT_CMSDIST_REPO
        self.cmsdistRef  = DEFAULT_CMSDIST_REF

    def finalize_options(self):
        if self.crabServerPath:
            self.crabCCServerPath = self.crabServerPath
            self.crabCSServerPath = self.crabServerPath
        else:
            self.crabCCServerPath = DEFAULT_CC_CRABSERVER
            self.crabCSServerPath = DEFAULT_CS_CRABSERVER

    def run(self):
        """
            Need to do a few things here:
        """

def get_relative_path():
  return os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), sys.argv[0])))

def define_the_build(self, dist, system_name, patch_x = ''):
  # Expand various sources.
  docroot = "doc/build/html"
  system = systems[system_name]
  #binsrc = sum((glob("bin/%s" % x) for x in system['bin']), [])

  py_version = (string.split(sys.version))[0]
  pylibdir = '%slib/python%s/site-packages' % (patch_x, py_version[0:3])
  dist.py_modules = system['py_modules']
  dist.packages = system['python']
  #dist.data_files = [('%sbin' % patch_x, binsrc)]
  #dist.data_files = [ ("%sdata" % (patch_x, ), "scripts/%s" % (x,))
  #				for x in ['CMSRunAnalysis.sh']]
  #dist.data_files = ['scripts/CMSRunAnalysis.sh']
  if os.path.exists(docroot):
    for dirpath, dirs, files in os.walk(docroot):
      dist.data_files.append(("%sdoc%s" % (patch_x, dirpath[len(docroot):]),
                              ["%s/%s" % (dirpath, fname) for fname in files
                               if fname != '.buildinfo']))

class BuildCommand(Command):
  """Build python modules for a specific system."""
  description = \
    "Build python modules for the specified system. The two supported systems\n" + \
    "\t\t   at the moment are 'CRABInterface' and 'UserFileCache'. Use with --force \n" + \
    "\t\t   to ensure a clean build of only the requested parts.\n"
  user_options = build.user_options
  user_options.append(('system=', 's', 'build the specified system (default: CRABInterface)'))
  user_options.append(('skip-docs=', 'd' , 'skip documentation'))

  def initialize_options(self):
    self.system = "CRABInterface,TaskWorker"
    self.skip_docs = False

  def finalize_options(self):
    if self.system not in systems:
      print "System %s unrecognised, please use '-s CRABInterface'" % self.system
      sys.exit(1)

    # Expand various sources and maybe do the c++ build.
    define_the_build(self, self.distribution, self.system, '')

    # Force rebuild.
    shutil.rmtree("%s/build" % get_relative_path(), True)
    shutil.rmtree("doc/build", True)

  def generate_docs(self):
    if not self.skip_docs:
      os.environ["PYTHONPATH"] = "%s/../WMCore/src/python/:%s" % (os.getcwd(), os.environ["PYTHONPATH"])
      os.environ["PYTHONPATH"] = "%s/build/lib:%s" % (os.getcwd(), os.environ["PYTHONPATH"])
      spawn(['make', '-C', 'doc', 'html', 'PROJECT=%s' % 'crabserver' ])

  def run(self):
    command = 'build'
    if self.distribution.have_run.get(command): return
    cmd = self.distribution.get_command_obj(command)
    cmd.force = self.force
    cmd.ensure_finalized()
    cmd.run()
    self.generate_docs()
    self.distribution.have_run[command] = 1

class InstallCommand(install):
  """Install a specific system."""
  description = \
    "Install a specific system. You can patch an existing\n" + \
    "\t\tinstallation instead of normal full installation using the '-p' option.\n"
  user_options = install.user_options
  user_options.append(('system=', 's', 'install the specified system (default: CRABInterface)'))
  user_options.append(('patch', None, 'patch an existing installation (default: no patch)'))
  user_options.append(('skip-docs=', 'd' , 'skip documentation'))

  def initialize_options(self):
    install.initialize_options(self)
    self.system = "CRABInterface"
    self.patch = None
    self.skip_docs = False

  def finalize_options(self):
    # Check options.
    if self.system not in systems:
      print "System %s unrecognised, please use '-s CRABInterface'" % self.system
      sys.exit(1)
    if self.patch and not os.path.isdir("%s/xbin" % self.prefix):
      print "Patch destination %s does not look like a valid location." % self.prefix
      sys.exit(1)

    # Expand various sources, but don't build anything from c++ now.
    define_the_build(self, self.distribution, self.system, (self.patch and 'x') or '')

    # Whack the metadata name.
    self.distribution.metadata.name = self.system
    assert self.distribution.get_name() == self.system

    # Pass to base class.
    install.finalize_options(self)

    # Mangle paths if we are patching. Most of the mangling occurs
    # already in define_the_build(), but we need to fix up others.
    if self.patch:
      self.install_lib = re.sub(r'(.*)/lib/python(.*)', r'\1/xlib/python\2', self.install_lib)
      self.install_scripts = re.sub(r'(.*)/bin$', r'\1/xbin', self.install_scripts)
      self.install_data = re.sub(r'(.*)/data$', r'\1/xdata', self.install_data)

  def run(self):
    for cmd_name in self.get_sub_commands():
      cmd = self.distribution.get_command_obj(cmd_name)
      cmd.distribution = self.distribution
      if cmd_name == 'install_data':
        data_dir = '/xdata' if self.patch else '/data'
        cmd.install_dir = self.prefix + data_dir
      else:
        cmd.install_dir = self.install_lib
      cmd.ensure_finalized()
      self.run_command(cmd_name)
      self.distribution.have_run[cmd_name] = 1

class TestCommand(Command):
    """
    Test harness entry point
    """
    description = "Runs tests"
    user_options = []
    user_options.append(("integration", None, "Run integration tests"))
    user_options.append(("integrationHost", None,
                            "Host to run integration tests against"))
    def initialize_options(self):
        self.integration = False
        self.integrationHost = "crab3-gwms-1.cern.ch"
    def finalize_options(self):
        pass
    def run(self):
        #import here, cause we don't want to bomb if nose doesn't exist
        import CRABQuality
        mode = 'default'
        if self.integration:
            mode = 'integration'
        sys.exit(CRABQuality.runTests(mode=mode,
                                      integrationHost=self.integrationHost))

def getWebDir():
    res = []
    for dir in ['css','html','script']:
        for root,dirs,files in os.walk('src/'+dir):
            res.append((root[4:], [os.path.join(root,x) for x in files]))#4: for removing src
    return res

setup(name = 'crabserver',
      version = '3.2.0',
      maintainer_email = 'hn-cms-crabdevelopment@cern.ch',
      cmdclass = { 'build_system': BuildCommand,
                   'install_system': InstallCommand,
                   'test' : TestCommand },
      #include_package_data=True,
      # base directory for all the packages
      package_dir = { '' : 'src/python' },
      data_files = ['scripts/%s' % x for x in \
                        ['CMSRunAnalysis.sh', 'cmscp.py',
                        'gWMS-CMSRunAnalysis.sh', 'dag_bootstrap_startup.sh',
                        'dag_bootstrap.sh', 'AdjustSites.py']] + getWebDir(),
)
