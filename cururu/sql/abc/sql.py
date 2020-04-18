import os
import traceback
from abc import abstractmethod

from time import sleep

from cururu.disk import save, load
from cururu.persistence import Persistence, LockedEntryException, \
    FailedEntryException, DuplicateEntryException, UnlockedEntryException
import _pickle as pickle
from pathlib import Path
from glob import glob


class SQL(Persistence):
    def _setup(self):
        print(self.name, 'creating tables...', self.info)

        # History of Data ======================================================
        self.query(f'''
            create table if not exists hist (
                n integer NOT NULL primary key {self._auto_incr()},

                hid char(19) NOT NULL UNIQUE, 

                nested LONGBLOB NOT NULL
            )''')

        # Columns of Data ======================================================
        self.query(f'''
            create table if not exists attr (
                n integer NOT NULL primary key {self._auto_incr()},

                aid char(19) NOT NULL UNIQUE,

                cols LONGBLOB NOT NULL
            )''')

        # Names of Data ========================================================
        self.query(f'''
            create table if not exists dataset (
                n integer NOT NULL primary key {self._auto_incr()},

                dsid char(19) NOT NULL UNIQUE,

                des TEXT NOT NULL,

                attr char(19),

                FOREIGN KEY (attr) REFERENCES attr(aid)
            )''')
        self.query(f'CREATE INDEX nam0 ON dataset (des{self._keylimit()})')
        self.query(f'CREATE INDEX nam1 ON dataset (attr)')

        # Dump of Component instances  =========================================
        self.query(f'''
            create table if not exists inst (
                n integer NOT NULL primary key {self._auto_incr()},

                iid char(19) NOT NULL UNIQUE,
                dump LONGBLOB NOT NULL
            )''')

        # Logs for Component ===================================================
        self.query(f'''
            create table if not exists log (
                n integer NOT NULL primary key {self._auto_incr()},

                lid char(19) NOT NULL UNIQUE,

                msg TEXT NOT NULL,
                insl timestamp NOT NULL
            )''')
        self.query(f'CREATE INDEX log0 ON log (msg{self._keylimit()})')
        self.query(f'CREATE INDEX log1 ON log (insl)')

        # Components ===========================================================
        self.query(f'''
            create table if not exists config (
                n integer NOT NULL primary key {self._auto_incr()},

                cid char(19) NOT NULL UNIQUE,

                cfg LONGBLOB NOT NULL,

                insc timestamp NOT NULL
            )''')
        self.query(f'CREATE INDEX config0 ON config (insc)')

        # Matrices/vectors
        # =============================================================
        self.query(f'''
            create table if not exists mat (
                n integer NOT NULL primary key {self._auto_incr()},

                mid char(19) NOT NULL UNIQUE,

                w integer,
                h integer,

                val LONGBLOB NOT NULL                 
            )''')
        self.query(f'CREATE INDEX mat0 ON mat (w)')
        self.query(f'CREATE INDEX mat1 ON mat (h)')

        # Datasets =============================================================
        self.query(f'''
            create table if not exists data (
                n integer NOT NULL primary key {self._auto_incr()},

                did char(19) NOT NULL UNIQUE,

                dataset char(19) NOT NULL,
                hist char(19) NOT NULL,

                X char(19),
                Y char(19),
                Z char(19),
                P char(19),

                U char(19),
                V char(19),
                W char(19),
                Q char(19),

                R char(19),
                S char(19),

                l char(19),
                m char(19),

                T char(19),

                C char(19),

                insd timestamp NOT NULL,
                upd timestamp,

                unique(dataset, hist),
                FOREIGN KEY (dataset) REFERENCES dataset(dsid),
                FOREIGN KEY (hist) REFERENCES hist(hid),
                
                FOREIGN KEY (X) REFERENCES mat(mid),
                FOREIGN KEY (Y) REFERENCES mat(mid),
                FOREIGN KEY (Z) REFERENCES mat(mid),
                FOREIGN KEY (P) REFERENCES mat(mid),
                FOREIGN KEY (U) REFERENCES mat(mid),
                FOREIGN KEY (V) REFERENCES mat(mid),
                FOREIGN KEY (W) REFERENCES mat(mid),
                FOREIGN KEY (Q) REFERENCES mat(mid),
                FOREIGN KEY (R) REFERENCES mat(mid),
                FOREIGN KEY (S) REFERENCES mat(mid),
                FOREIGN KEY (l) REFERENCES mat(mid),
                FOREIGN KEY (m) REFERENCES mat(mid),
                FOREIGN KEY (T) REFERENCES mat(mid),
                FOREIGN KEY (C) REFERENCES mat(mid)
               )''')
        # guardar last comp nao adianta pq o msm comp pode ser aplicado
        # varias vezes
        # history não vai conter comps inuteis como pipes e switches, apenas
        # quem transforma

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
               f'.{self.optimize}.dump'
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

    @abstractmethod
    def _on_conflict(self, fields=None):
        pass

    @abstractmethod
    def _keylimit(self):
        pass

    @abstractmethod
    def _now_function(self):
        pass

    @abstractmethod
    def _auto_incr(self):
        pass