class TestWorker(object):
    """ TestWorker class providing a sequential execution of the work in the same thread of the caller
        This is useful for debugging purposes because because there are problems executing pdb with
        multiple threads.
    """

    def __init__(self, config, resthost, dbInstance):
        self.config = config
        self.resthost = resthost
        self.dbInstance = dbInstance
        self.nworkers = 3

    def pendingTasks(self):
        return 0

    def queuedTasks(self):
        return 0

    def freeSlaves(self):
        return 0

    def begin(self):
        pass

    def queueableTasks(self):
        return 1

    def injectWorks(self, works):
        if works:
            func, task, _, args = works[0]
            try:
                func(self.resthost, self.dbInstance, self.config, task, 0, args)
            except Exception:
                pass
    def checkFinished(self):
        return []

    def end(self):
        pass
