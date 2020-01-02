from cururu.compression import unpack_object, pack_object
from cururu.file import save, load
from cururu.persistence import Persistence, LockedEntryException, \
    FailedEntryException, DuplicateEntryException
import _pickle as pickle
from pathlib import Path
from glob import glob


class PickleServer(Persistence):
    def __init__(self, optimize='speed', db='/tmp/'):
        self.db = db
        self.speed = optimize == 'speed'  # vs 'space'

    def _filename(self, data_in, transformation):
        return self.db + data_in.dataset.name + '-' + \
               data_in.uuid + '-' + transformation.uuid + '.dump'

    def store(self, data_in, transformation, fields, data_out, check_dup=True):
        # TODO: deal with fields and missing fields?
        filename = self._filename(data_in, transformation)

        # Already exists?
        if check_dup and Path(filename).exists():
            raise DuplicateEntryException

        self._dump(data_out, filename)

    def fetch(self, data_in, transformation, fields, lock=False):
        # TODO: deal with fields and missing fields?
        filename = self._filename(data_in, transformation)

        # Not started yet?
        if not Path(filename).exists():
            print('W: Not started.', filename)
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
            raise FailedEntryException

        return transformed_data

    def list_by_name(self, substring):
        datas = []
        for file in glob(self.db + f'*{substring}*-*.dump'):
            data = self._load(file)
            datas.append(data.empty())
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
