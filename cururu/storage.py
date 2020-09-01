from cururu.amnesia import Amnesia
from cururu.persistence import Persistence
from cururu.pickleserver import PickleServer
from cururu.worker2 import Worker2
from pjdata.config import STORAGE_CONFIG
from pjdata.types import Data


class Storage(Worker2, Persistence):
    """Multithreaded* persistence.

    * -> Two options:
        monothread (Python threading peculiar concept)
        multiprocess (Python multiprocessing concept)

    ps. É global, mas sem perder a integridade referencial.
    'New' garante o mesmo objeto para mesmo argumento."""

    def __new__(cls, alias):
        if alias not in STORAGE_CONFIG["storages"]:
            STORAGE_CONFIG["storages"][alias] = object.__new__(cls)
        return STORAGE_CONFIG["storages"][alias]

    def __init__(self, alias):
        super().__init__()
        self.alias = alias

    def store(self, data: Data, check_dup: bool = True):
        self.put("store", locals())

    def _fetch_impl(self, data: Data, lock: bool = False) -> Data:
        return self.put("fetch", locals(), wait=True)

    def fetch_matrix(self, id):
        return self.put("fetch_matrix", locals(), wait=True)

    def unlock(self, data, training_data_uuid=None):
        self.put("unlock", locals())

    def list_by_name(self, substring, only_historyless=True):
        pass

    # TIP: self.lock and SQLite cannot be passed to threads, due to pickle err
    # TIP: sqlite3.ProgrammingError: SQLite objects created in a thread can
    # only be used in that same thread.
    # ==================================================

    @staticmethod
    def backend(alias):
        kwargs = STORAGE_CONFIG.get(alias, {})
        engine = kwargs.pop("engine") if "engine" in kwargs else alias

        if engine == "amnesia":
            return Amnesia()
        elif engine == "mysql":
            from cururu.sql.mysql import MySQL
            # TODO: does mysql already have extra settings now?
            return MySQL(storage_info=alias, **kwargs)
        elif engine == "okapost":
            from cururu.okaserver import OkaServer
            return OkaServer(storage_info=alias, post=True, **kwargs)
        elif engine == "oka":
            from cururu.okaserver import OkaServer
            return OkaServer(storage_info=alias, post=False, **kwargs)
        elif engine == "sqlite":
            from cururu.sql.sqlite import SQLite
            return SQLite(storage_info=alias, **kwargs)
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
            raise Exception("Unknown engine:", engine)
