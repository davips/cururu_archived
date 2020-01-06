from cururu.amnesia import Amnesia
from cururu.pickleserver import PickleServer
from pjdata.data_creation import read_arff


class Storer:
    """Trait/mixin providing storage configuration."""

    def _set_storage(self, engine, settings):
        if engine == "amnesia":
            self.storage = Amnesia()
        elif engine == "mysql":
            from cururu.mysql import MySQL
            self.storage = MySQL(**settings)
        elif engine == "sqlite":
            from cururu.sqlite import SQLite
            self.storage = SQLite(**settings)
        elif engine == "cachedmysql":
            from cururu.mysql import MySQL
            from cururu.sqlite import SQLite
            self.storage = MySQL(db=settings['db'], nested=SQLite())
        elif engine == "dump":
            self.storage = PickleServer(**settings)
        # elif engine == "file":
        #     if settings['path'].endswith('/'):
        #         raise Exception('Path should not end with /', settings['path'])
        #     if settings['name'].endswith('arff'):
        #         self.data = read_arff(settings['path'] + '/' + settings['name'])
        #     else:
        #         raise Exception('Unrecognized file extension:',
        #                         settings['name'])
        else:
            raise Exception('Unknown engine:', engine)
