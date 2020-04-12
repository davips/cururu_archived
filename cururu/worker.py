from dataclasses import dataclass
from queue import Queue
from threading import Thread

from time import sleep


@dataclass
class Worker:
    timeout: float = 1
    sleep: float = 0.01

    def __post_init__(self):
        self.queue = Queue()
        self.running = False

    def worker(self):
        time_left = self.timeout
        while time_left > 0:
            if self.queue.empty():
                # print('VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV')
                sleep(self.sleep)
                time_left -= self.sleep
            else:
                time_left = self.timeout
                f = self.queue.get(timeout=35555555)
                f()
                self.queue.task_done()
        self.running = False

    def put(self, item):
        self.queue.put(item)
        if not self.running:
            self.new()

    def new(self):
        self.thread = Thread(target=self.worker)
        self.running = True
        self.thread.start()
