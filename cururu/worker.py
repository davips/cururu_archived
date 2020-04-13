from dataclasses import dataclass
from multiprocessing import Queue
from queue import Empty


@dataclass
class Worker:
    """Intended to get IO out of the way,
    so storing of results doesn't affect the execution time."""
    timeout: float = 0.25
    parallel: bool = False

    def __post_init__(self):
        self.queue = Queue()
        if self.parallel:
            import multiprocessing
            self.lock = multiprocessing.Lock()
            self.klass = multiprocessing.Process
        else:
            import threading
            self.lock = threading.Lock()
            self.klass = threading.Thread

    def put(self, function):
        """Add a new function to the queue to be executed."""
        self.queue.put(function)

        # Creates a new thread if there is none alive.
        if self.lock.acquire(False):
            self._new()

    def _new(self):
        thread = self.klass(target=self._worker, daemon=False)
        thread.start()

    def _worker(self):
        while True:
            try:
                function = self.queue.get(timeout=self.timeout)
                function()
            except Empty:
                break
        self.lock.release()
