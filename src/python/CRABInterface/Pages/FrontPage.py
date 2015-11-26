from WMCore.REST.Server import RESTFrontPage
import os, re, cjson

class FrontPage(RESTFrontPage):
  """CRABServer front page.
  """

  def __init__(self, app, config, mount):
    """
    :arg app: reference to the application object.
    :arg config: reference to the configuration.
    :arg str mount: URL mount point."""
    CONTENT = os.path.abspath(__file__).rsplit('/', 6)[0]
    roots = \
    {
      "html":
      {
        "root": "%s/data/html/" % CONTENT,
        "rx": re.compile(r".*")
      },
      "script":
      {
        "root": "%s/data/script/" % CONTENT,
        "rx": re.compile(r".*")
      },
      "css":
      {
        "root": "%s/data/css/" % CONTENT,
        "rx": re.compile(r".*")
      }
    }

    frontpage = "html/index.html"#crabserver?

    RESTFrontPage.__init__(self, app, config, mount, frontpage, roots,
                           instances = lambda: app.views["data"]._db)
