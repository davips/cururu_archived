from cururu.amnesia import Amnesia
from cururu.persistence import Persistence
from cururu.pickleserver import PickleServer
from cururu.worker2 import Worker2, Nothing


class Storage(Persistence):
    def __init__(self, engine="dump", db='/tmp/cururu', settings=None):
        """"""
        if settings is None:
            self.settings = {}
        self.settings['db'] = db

        if engine == "amnesia":
            self.backend = Amnesia()
        elif engine == "mysql":
            from cururu.sql.mysql import MySQL
            # TODO: does mysql already have extra settings now?
            self.backend = MySQL(**self.settings)
        elif engine == "sqlite":
            from cururu.sql.sqlite import SQLite
            self.backend = SQLite(**self.settings)
        elif engine == "mysqla":
            from cururu.sql.sqla_backends import MySQLA
            # TODO: does mysql already have extra settings now?
            self.backend = MySQLA(**self.settings)
        elif engine == "sqlitea":
            from cururu.sql.sqla_backends import SQLiteA
            self.backend = SQLiteA(**self.settings)
        elif engine == "dump":
            self.backend = PickleServer(**self.settings)
        else:
            raise Exception('Unknown engine:', engine)

        self.worker = Worker2()

    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        kwargs = locals().copy()
        del kwargs['self']
        self.worker.put((self._store, kwargs))

    def _fetch_impl(self, hollow_data, fields, training_data_uuid='',
                    lock=False):
        kwargs = locals().copy()
        del kwargs['self']
        self.worker.put((self._fetch, kwargs))
        ret = self.worker.outqueue.get()
        self.worker.outqueue.task_done()
        return ret

    def fetch_matrix(self, name):
        self.worker.put((self._fetch_matrix, name))
        ret = self.worker.outqueue.get()
        self.worker.outqueue.task_done()
        return ret

    def unlock(self, hollow_data, training_data_uuid=None):
        self.worker.put(self._unlock)

    def list_by_name(self, substring, only_historyless=True):
        pass

    # TIP: Methods required to be at the same scope level as worker due to
    # threading issues. ==================================================
    @staticmethod
    def _store(backend, **kwargs):
        backend.store(**kwargs)
        return Nothing

    @staticmethod
    def _fetch(backend, **kwargs):
        return backend.fetch(**kwargs)

    @staticmethod
    def _fetch_matrix(backend, **kwargs):
        return backend.fetch_matrix(**kwargs)

    @staticmethod
    def _unlock(backend):
        return backend.unlock()

    #                  ==================================================
