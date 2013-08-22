"""
    CRABInterface.Dagman.Fork - provides a "with" block that handles setting
        up pipes, forking a subprocess and waiting on the result

    Usage:
    with ReadFork() as pid, rpipe, wpipe:
        if pid == 0:
            # we're in the child
            wpipe.write('OK')
        
    result = rpipe.read()
    if result != 'OK':
        # uhoh


"""
import os
class ReadFork():
    def __enter__(self):
        self.r, self.w = os.pipe()
        self.rpipe = os.fdopen(self.r, 'r')
        self.wpipe = os.fdopen(self.w, 'w')
        self.pid = os.fork()
        if self.pid == 0:
            self.rpipe.close()
        else:
            self.wpipe.close()
        return self.pid, self.rpipe, self.wpipe

    def __exit__(self, a, b, c):
        if self.pid == 0:
            if (a == None and b == None and c == None):
                self.wpipe.close()
                os._exit(0)
            else:
                msg = "Trapped exception in Dagman.Fork: %s %s %s" % (a,b,c)
                self.wpipe.write(msg)
                self.wpipe.close()
                os._exit(1)
        else:
            os.waitpid(self.pid, 0)

if __name__ == "__main__":
    # check the pipes
    with ReadFork() as (test, r, w):
        if test == 0:
            w.write('testing')
    assert(r.read() == 'testing')

    # check the fork actualy worked
    with ReadFork() as (test, r, w):
        if test == 0:
            os.environ['beep'] = 'boop'
    assert(os.environ.get('beep', None) != 'boop')

    # check hat excptions are trapped
    with ReadFork() as (test, r, w):
        if test == 0:
            raise RuntimeError, "DagForkTestString"
    assert( "DagForkTestString" in r.read() )
