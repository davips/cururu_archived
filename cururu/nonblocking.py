from functools import partial

from cururu.persistence import Persistence
from cururu.pickleserver import PickleServer
from cururu.worker import Worker, Nothing


class NonBlocking(Persistence):
    worker = Worker()

    def __init__(self, setup, setup_kwargs, multiprocess):
        self.worker.updated(setup, setup_kwargs, multiprocess)
        pass

    def _store(self, backend, **kwargs):
        print('f()sto')
        ret = backend.store(**kwargs)
        print('stored', ret)
        return ret

    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        kwargs = locals().copy()
        del kwargs['self']
        print('sto putting')
        self.worker.put((self._store, kwargs))
        print('sto put!')
        return Nothing

    def _fetch(self, backend, **kwargs):
        print('f()')
        ret = backend.fetch(**kwargs)
        print('fetched', ret)
        return ret

    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        kwargs = locals().copy()
        del kwargs['self']
        print('putting')
        self.worker.put((self._fetch, kwargs))
        print('put!')
        ret = None #self.worker.outqueue.get()
        print('pegou')
        # ret = self.worker.outqueue.task_done()
        print('done')
        return ret

    def list_by_name(self, substring, only_historyless=True):
        pass
