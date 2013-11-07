
import os

class PreJob():

    def execute(self, *args):
        os.execv("/bin/sleep", ["sleep", str(int(args[0])*60)])

