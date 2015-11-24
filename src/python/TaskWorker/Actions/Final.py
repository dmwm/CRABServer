

import os
import urllib

from RESTInteractions import HTTPRequests

# From the HTCondor documentation, dag_status has the following meaning
#0: OK
#1: error; an error condition different than those listed here
#2: one or more nodes in the DAG have failed
#3: the DAG has been aborted by an ABORT-DAG-ON specification
#4: removed; the DAG has been removed by condor_rm
#5: cycle; a cycle was found in the DAG
#6: halted; the DAG has been halted

# We mark the task as failed in the DB for cases 1, 2, or 3.
# 5 is impossible in this case; 4 or 6 would be done by
# CRAB3, which is responsible for the status update in that case.

class Final:

    """
    Perform any final cleanup necessary for the DAG.  Right now, it just
    sets the final DB status in the case of failure.
    """

    def execute(self, *args):
        dag_status = int(args[0])
        failed_count = int(args[1])
        restinstance = args[2]
        resturl = args[3]
        if dag_status in [1, 2, 3]:
            if dag_status == 3:
                msg = "Task aborted because the maximum number of failures was hit; %d total failed jobs." % failed_count
            else:
                msg = "Task failed overall; %d failed jobs" % failed_count
            configreq = {'workflow': kw['task']['tm_taskname'],
                         'substatus': "FAILED",
                         'subfailure': base64.b64encode(str(e)),}
            data = urllib.urlencode(configreq)
            server = HTTPRequests(restinstance, os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY'])
            server.put(resturl, data=data)

        return dag_status


