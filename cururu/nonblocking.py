from cururu.persistence import Persistence
from cururu.worker import Worker, Nothing


class NonBlocking(Persistence):
    worker = Worker()

    def __init__(self, setup, setup_kwargs, multiprocess):
        self.worker.updated(setup, setup_kwargs, multiprocess)
        pass

    @staticmethod
    def _store(backend, **kwargs):
        backend.store(**kwargs)
        return Nothing

    @staticmethod
    def _fetch(backend, **kwargs):
        return backend.fetch(**kwargs)

    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        kwargs = locals().copy()
        del kwargs['self']
        self.worker.put((self._store, kwargs))

    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        kwargs = locals().copy()
        del kwargs['self']
        self.worker.put((self._fetch, kwargs))
        ret = self.worker.outqueue.get()
        self.worker.outqueue.task_done()
        return ret

    def list_by_name(self, substring, only_historyless=True):
        pass
