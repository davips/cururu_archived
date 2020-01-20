import _pickle as pickle
import os
from glob import glob
from pathlib import Path

from cururu.disk import save, load
from cururu.persistence import Persistence, LockedEntryException, \
    FailedEntryException, DuplicateEntryException, UnlockedEntryException


class PickleServer(Persistence):
    def __init__(self, db='/tmp/cururu', optimize='speed'):
        self.db = db
        self.speed = optimize == 'speed'  # vs 'space'
        if not Path(db).exists():
            os.mkdir(db)

    def fetch(self, data, transformations=None, fields=None, lock=False):
        # TODO: deal with fields and missing fields?
        filename = self._filename('*', data, transformations)

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

    def store(self, data, fields=None, check_dup=True):
        """The dataset name of data_out will be the filename prefix for
        convenience."""
        # TODO: deal with fields and missing fields?

        filename = self._filename(data.dataset.name, data)

        # Already exists?
        if check_dup and Path(filename).exists():
            raise DuplicateEntryException('Already exists:', filename)

        locked = self._filename('', data)
        if Path(locked).exists():
            os.remove(locked)

        self._dump(data, filename)

    def list_by_name(self, substring, only_historyless=True):
        datas = []
        for file in sorted(glob(self.db + f'/*{substring}*-*.dump'),
                           key=os.path.getmtime):
            data = self._load(file)
            if only_historyless and data.history.size == 0:
                datas.append(data.phantom)
        return datas

    def unlock(self, data, transformations):
        print('qqqqqqqqqqqqq unlocking', data, transformations)
        filename = self._filename('*', data, transformations)
        if not Path(filename).exists():
            raise UnlockedEntryException
        os.remove(filename)

    def _filename(self, prefix, data, transformations=None):
        # TODO: move NoThing uuids to parent (Persistence)
        if transformations is None:
            transformations = []

        transf_sids = []
        for transf in data.history.transformations + transformations:
            # Using sid since linux file names are limited to 255 characters.
            transf_sids.append(transf.sid)

        path = self.db + '/'
        rest = '-' + data.dataset.uuid + '-' + ','.join(transf_sids) + '.dump'
        if prefix == '*':
            query = path + '*' + rest
            lst = glob(query)
            if len(lst) > 1:
                raise Exception('Multiple files found:', query, lst)
            if len(lst) == 1:
                return lst[0]
            else:
                return path + data.dataset.name + rest
        else:
            return path + prefix + rest

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
