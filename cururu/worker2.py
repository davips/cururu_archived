import multiprocessing
import threading
from dataclasses import dataclass
from multiprocessing import JoinableQueue
from multiprocessing import Queue
from queue import Empty


class Nothing(type):
    """Singleton"""


@dataclass
class Worker2:
    """Intended to get IO out of the way,
    so storing of results doesn't affect the execution time."""
    multiprocess: bool = False
    timeout: float = 2  # Time spent hoping the thread will be useful again.
    queue = Queue()
    outqueue = JoinableQueue()
    process_lock = multiprocessing.Lock()
    thread_lock = threading.Lock()

    def __post_init__(self):
        print('new worker......................................')
        if self.multiprocess:
            self.lock = self.process_lock
            self.klass = multiprocessing.Process
        else:
            self.lock = self.thread_lock
            self.klass = threading.Thread

    def put(self, function):
        """Add a new function to the queue to be executed."""
        self.queue.put(function)

        # Create a new thread if there is none alive.
        if self.lock.acquire(False):
            self._new()

    @classmethod
    def join(cls):
        """Wait for the last task to end. Calling this method is optional."""
        cls.process_lock.acquire()
        cls.process_lock.release()
        cls.thread_lock.acquire()
        cls.thread_lock.release()

    def _new(self):
        mythread = self.klass(target=self._worker, daemon=False)
        mythread.start()

    def _worker(self):
        while True:
            try:
                f, kwargs = self.queue.get(timeout=self.timeout)
                ret = f(**kwargs)
                if ret is not Nothing:
                    self.outqueue.put(ret)
                    self.outqueue.join()
            except Empty:
                break
        self.lock.release()
