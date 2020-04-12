import os
import traceback
from time import sleep

from cururu.disk import save, load
from cururu.persistence import Persistence, LockedEntryException, \
    FailedEntryException, DuplicateEntryException, UnlockedEntryException
import _pickle as pickle
from pathlib import Path
from glob import glob


class PickleServer(Persistence):
    def __init__(self, db='/tmp/cururu', optimize='speed', blocking=False):
        super().__init__(blocking=blocking)
        self.db = db
        self.speed = optimize == 'speed'  # vs 'space'
        if not Path(db).exists():
            os.mkdir(db)

    def fetch(self, hollow_data, fields=None, training_data_uuid='', lock=False):
        # TODO: deal with fields and missing fields?
        filename = self._filename('*', hollow_data, training_data_uuid)

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
            raise LockedEntryException(filename)

        transformed_data = self._load(filename)

        # Failed?
        if transformed_data.failure is not None:
            raise FailedEntryException(transformed_data.failure)

        return transformed_data

    def _store_impl(self, data, fields, training_data_uuid, check_dup):
        """The dataset name of data_out will be the filename prefix for
        convenience."""
        # TODO: deal with fields and missing fields?
        if fields is None:
            fields = ['X', 'Y']

        filename = self._filename(data.name, data, training_data_uuid)
        # sleep(0.020)  # Latency simulator.

        # Already exists?
        if check_dup and Path(filename).exists():
            raise DuplicateEntryException('Already exists:', filename)

        locked = self._filename('', data, training_data_uuid)
        if Path(locked).exists():
            os.remove(locked)

        self._dump(data, filename)

    def list_by_name(self, substring, only_original=True):
        datas = []
        path = self.db + f'/*{substring}*-*.dump'
        for file in sorted(glob(path), key=os.path.getmtime):
            data = self._load(file)
            if only_original and data.history.size == 1:
                datas.append(data.hollow)
        return datas

    def _filename(self, prefix, data, training_data_uuid=''):
        uuids = [tr.sid for tr in data.history.transformations]
        rest = f'-{training_data_uuid}-' + '-'.join(uuids) + \
               f'.{self.speed}.dump'
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

    def _load(self, filename):
        """
        Retrieve a Data object from disk.
        :param filename: file dataset
        :return: Data
        """
        try:
            if self.speed:
                f = open(filename, 'rb')
                res = pickle.load(f)
                f.close()
                return res
            else:
                return load(filename)
        except Exception as e:
            traceback.print_exc()
            print('Problems loading', filename)
            exit(0)

    def _dump(self, data, filename):
        """
        Dump a Data object to disk.
        :param data: Data
        :param filename: file dataset
        :return: None
        """
        print('W: Storing...', filename)
        if self.speed:
            f = open(filename, 'wb')
            pickle.dump(data, f)
            f.close()
        else:
            save(filename, data)

    def unlock(self, data, training_data_uuid=''):
        filename = self._filename('*', data, training_data_uuid)
        if not Path(filename).exists():
            raise UnlockedEntryException('Cannot unlock something that is not '
                                         'locked!', filename)
        print('W: Unlocking...', filename)
        os.remove(filename)
