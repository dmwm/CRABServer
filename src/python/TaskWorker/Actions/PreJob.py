
import os

class PostJob():

    def execute(self, *args):
        os.execv("/bin/sleep", ["sleep", str(int(args[0])*60)])

