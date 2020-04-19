from dataclasses import dataclass

from cururu.amnesia import Amnesia
from cururu.pickleserver import PickleServer
from cururu.nonblocking import NonBlocking


@dataclass
class Storer:
    """Manage threads for storage backends."""
    # WARN: Global state!
    workers = {}

    @classmethod
    def get(cls, engine, db, settings, blocking, multiprocess=False):
        """Get an active connection or create a new one if needed."""
        return cls.workers.get(
            engine,
            cls.factory(engine, db, settings, blocking, multiprocess)
        )

    @classmethod
    def factory(cls, engine, db, settings, blocking, multiprocess=False):
        """Create a new connection."""
        #  TIP: imports are hidden to avoid errors due to missing pip packages
        setup_kwargs = {'db': db}
        setup_kwargs.update(settings)

        def create_worker(backend, kwargs=None):
            if kwargs is None:
                kwargs = setup_kwargs
            if blocking:
                return backend(**kwargs)
            return NonBlocking(
                setup=backend, setup_kwargs=kwargs, multiprocess=multiprocess
            )

        if engine == "amnesia":
            worker = create_worker(Amnesia, kwargs={})
        elif engine == "mysql":
            from cururu.sql.backends import MySQL
            # TODO: does mysql already have extra settings now?
            worker = create_worker(MySQL)
        elif engine == "sqlite":
            from cururu.sql.backends import SQLite
            worker = create_worker(SQLite)
        elif engine == "dump":
            worker = create_worker(PickleServer)
        else:
            raise Exception('Unknown engine:', engine)

        cls.workers[engine] = worker
        return worker
