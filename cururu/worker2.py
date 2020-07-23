import multiprocessing
import threading
from abc import abstractmethod
from dataclasses import dataclass
from multiprocessing import JoinableQueue
from multiprocessing import Queue
from queue import Empty


@dataclass
class Worker2:
    """Intended to get IO out of the way,
    so storing of results doesn't affect the execution time."""

    multiprocess: bool = False
    timeout: float = 2  # Time spent hoping the thread will be useful again.
    queue = Queue()
    alias = None
    outqueue = JoinableQueue()
    process_lock = multiprocessing.Lock()
    thread_lock = threading.Lock()

    def __post_init__(self):
        if self.multiprocess:
            self.lock = self.process_lock
            self.klass = multiprocessing.Process
        else:
            self.lock = self.thread_lock
            self.klass = threading.Thread

    def put(self, method_name, locals_=None, wait=False):
        """Add a new function to the queue to be executed."""
        kwargs = self._prepare_args(locals_) if locals_ else {}
        tup = method_name, kwargs, wait
        self.queue.put(tup)

        # Create a new thread if there is none alive.
        if self.lock.acquire(False):
            print("new thread......................................")
            self._new()

        # Wait for result if asked.
        if wait:
            try:
                ret = self.outqueue.get()
                self.outqueue.task_done()
                return ret
            except Exception as e:
                print("Problem while expecting storage reply:", e)
                try:
                    self.outqueue.get()
                    self.outqueue.task_done()
                finally:
                    exit(0)

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
        backend = self.backend(self.alias)
        while True:
            try:
                method_name, kwargs, wait = self.queue.get(timeout=self.timeout)
                ret = getattr(backend, method_name)(**kwargs)

                if wait:
                    self.outqueue.put(ret)
                    self.outqueue.join()
            except Empty:
                break
            # else:
            #     break
        self.lock.release()

    @staticmethod
    def _prepare_args(locals_):
        locals_ = locals_.copy()
        del locals_["self"]
        return locals_

    @staticmethod
    @abstractmethod
    def backend(alias):
        pass
