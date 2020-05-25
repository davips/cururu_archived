import json

from cururu.amnesia import Amnesia
from cururu.persistence import Persistence
from cururu.pickleserver import PickleServer
from cururu.worker2 import Worker2

# global provisorio
with open('config.json', 'r') as f:
    Global = json.load(f)
Global['default_dump'] = {'engine': 'dump'}
Global['default_sqlite'] = {'engine': 'sqlite'}


class Storage(Worker2, Persistence):
    def __init__(self, alias):
        """"""
        super().__init__()
        self.alias = alias

    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        self.put('store', locals())

    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        return self.put('fetch', locals(), wait=True)

    def fetch_matrix(self, name):
        return self.put('fetch_matrix', locals(), wait=True)

    def unlock(self, hollow_data, training_data_uuid=None):
        self.put('unlock', locals())

    def list_by_name(self, substring, only_historyless=True):
        pass

    # TIP: self.lock and SQLite cannot be passe dto threads, due to pickle err
    # TIP: sqlite3.ProgrammingError: SQLite objects created in a thread can
    # only be used in that same thread.
    # ==================================================

    @staticmethod
    def backend(alias):
        kwargs = Global.get(alias, {})
        engine = kwargs.pop('engine') if 'engine' in kwargs else alias

        if engine == "amnesia":
            return Amnesia()
        elif engine == "mysql":
            from cururu.sql.mysql import MySQL
            # TODO: does mysql already have extra settings now?
            return MySQL(**kwargs)
        elif engine == "sqlite":
            from cururu.sql.sqlite import SQLite
            return SQLite(**kwargs)
        elif engine == "mysqla":
            from cururu.sql.sqla_backends import MySQLA
            # TODO: does mysql already have extra settings now?
            return MySQLA(**kwargs)
        elif engine == "sqlitea":
            from cururu.sql.sqla_backends import SQLiteA
            return SQLiteA(**kwargs)
        elif engine == "dump":
            return PickleServer(**kwargs)
        else:
            raise Exception('Unknown engine:', engine)
