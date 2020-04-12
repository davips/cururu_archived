from cururu.amnesia import Amnesia
from cururu.pickleserver import PickleServer


class Storer:
    """Trait/mixin providing storage configuration."""

    def _set_storage(self, engine, settings, blocking):
        if engine == "amnesia":
            self.storage = Amnesia()
        elif engine == "mysql":
            from cururu.mysql import MySQL
            self.storage = MySQL(blocking=blocking, **settings)
        elif engine == "sqlite":
            from cururu.sqlite import SQLite
            self.storage = SQLite(blocking=blocking, **settings)
        elif engine == "dump":
            self.storage = PickleServer(blocking=blocking, **settings)
        # elif engine == "file":
        #     if settings['path'].endswith('/'):
        #         raise Exception('Path should not end with /', settings[
        #         'path'])
        #     if settings['name'].endswith('arff'):
        #         self.data = read_arff(settings['path'] + '/' + settings[
        #         'name'])
        #     else:
        #         raise Exception('Unrecognized file extension:',
        #                         settings['name'])
        else:
            raise Exception('Unknown engine:', engine)
