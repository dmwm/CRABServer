class TestWorker(object):
    """ TestWorker class providing a sequential execution of the work in the same thread of the caller
        This is useful for debugging purposes because because there are problems executing pdb with
        multiple threads.
    """

    def __init__(self, config, resthost, resturi):
        self.config = config
        self.resthost = resthost
        self.resturi = resturi

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
            func, task, _ = works[0]
            func(self.resthost, self.resturi, self.config, task, 0)

    def checkFinished(self):
        return []

    def end(self):
        pass
