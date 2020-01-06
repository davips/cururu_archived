import os

from cururu.compression import unpack_object, pack_object
from cururu.disk import save, load
from cururu.persistence import Persistence, LockedEntryException, \
    FailedEntryException, DuplicateEntryException, UnlockedEntryException
import _pickle as pickle
from pathlib import Path
from glob import glob


class PickleServer(Persistence):
    def __init__(self, optimize='speed', db='/tmp/cururu'):
        self.db = db
        self.speed = optimize == 'speed'  # vs 'space'
        if not Path(db).exists():
            os.mkdir(db)

    def _filename(self, prefix, previous_data, transformation):
        # TODO: move NoThing uuids to parent
        prev_uuid = 'DØØØØØØØØØØØØØØØØØØ0' if previous_data is None \
            else previous_data.uuid
        transf_uuid = 'TØØØØØØØØØØØØØØØØØØ0' if transformation is None \
            else transformation.uuid
        rest = '-' + prev_uuid + '-' + transf_uuid + '.dump'
        if prefix == '*':
            query = self.db + '/*' + rest
            lst = glob(query)
            if len(lst) > 1:
                raise Exception('Multiple files found:', query, lst)
            if len(lst) == 1:
                return lst[0]
            else:
                return self.db + '/' + rest
        else:
            return self.db + '/' + prefix + rest

    def fetch(self, previous_data, transformation=None, fields=None,
              lock=False):
        # TODO: deal with fields and missing fields?
        if fields is None:
            fields = ['X', 'Y']

        filename = self._filename('*', previous_data, transformation)

        # Not started yet?
        if not Path(filename).exists():
            # print('W: Not started.', filename)
            if lock:
                print('W: Locking...', filename)
                Path(filename).touch()
            return None

        # Locked?
        if Path(filename).stat().st_size == 0:
            print('W: Previously locked by other process.', filename)
            raise LockedEntryException

        transformed_data = self._load(filename)

        # Failed?
        if transformed_data.failure is not None:
            raise FailedEntryException(transformed_data.failure)

        return transformed_data

    def store(self, data, previous_data=None, transformation=None, fields=None,
              check_dup=True):
        """The dataset name of data_out will be the filename prefix for
        convenience."""
        # TODO: deal with fields and missing fields?
        if bool(previous_data) != bool(transformation):
            raise Exception('It is not possible to store data with '
                            'previous_data, but without transformation - '
                            'and vice-versa!')
        if previous_data is None:
            previous_data = data
        if fields is None:
            fields = ['X', 'Y']

        filename = self._filename(data.dataset.name,
                                  previous_data, transformation)

        # Already exists?
        if check_dup and Path(filename).exists():
            raise DuplicateEntryException('Already exists:', filename)

        locked = self._filename('', previous_data, transformation)
        if Path(locked).exists():
            os.remove(locked)

        self._dump(data, filename)

    def list_by_name(self, substring):
        datas = []
        for file in sorted(glob(self.db + f'/*{substring}*-*.dump'),
                           key=os.path.getmtime):
            data = self._load(file)
            datas.append(data.phantom)
        return datas

    def _load(self, filename):
        """
        Retrieve a Data object from disk.
        :param filename: file dataset
        :return: Data
        """
        if self.speed:
            f = open(filename, 'rb')
            res = pickle.load(f)
            f.close()
            return res
        else:
            return load(filename)

    def _dump(self, data, filename):
        """
        Dump a Data object to disk.
        :param data: Data
        :param filename: file dataset
        :return: None
        """
        if self.speed:
            f = open(filename, 'wb')
            pickle.dump(data, f)
            f.close()
        else:
            save(filename, data)

    # Obsolete since we can use Data.phantom
    # @staticmethod
    # def _erase(data):
    #     """
    #     Remove matrices from Data.
    #     Keep identity.
    #     :param data:
    #     :return:
    #     """
    #     matrices = []
    #     for arg in data.__dict__:
    #         if len(arg) == 1:
    #             matrices.append(arg)
    #     for mat in matrices:
    #         del data.__dict__[mat]
    def unlock(self, previous_data, transformation):
        filename = self._filename('*', previous_data, transformation)
        if not Path(filename).exists():
            raise UnlockedEntryException
        os.remove(filename)
