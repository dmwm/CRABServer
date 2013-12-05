
import os

class PreJob():

    def alter_submit(self, retry, id):
        new_submit_text = '+CRAB_Retry = %d\n' % retry
        with open("Job.%d.submit" % id, "r") as fd:
            new_submit_text += fd.read()
        with open("Job.%d.submit" % id, "w") as fd:
            fd.write(new_submit_text)

    def execute(self, *args):
        retry_num = int(args[0])
        crab_id = int(args[1])
        self.alter_submit(retry_num, crab_id)
        os.execv("/bin/sleep", ["sleep", str(int(args[0])*60)])

