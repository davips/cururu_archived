import warnings
from abc import abstractmethod
from typing import Optional

from cururu.persistence import Persistence, DuplicateEntryException, \
    UnlockedEntryException
from pjdata.aux.compression import unpack, pack
from pjdata.aux.encoders import UUID
from pjdata.data import Data
from pjdata.step.transformation import Transformation


class SQL(Persistence):
    cursor = None

    # TODO: remove training_data_uuid from here and put it inside transformations
    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        # The sequence of queries is planned to minimize traffic and CPU load,
        # otherwise it would suffice to just send 'insert or ignore' of dumps.
        uuid = data.uuid00.id
        self.query(f'select t from data where id=?', [uuid])
        rone = self.get_one()

        # Remove lock.
        locked = rone and rone['t'] == '0000-00-00 00:00:00'
        if locked:
            self.query(f'delete from data where id=?', [uuid])

        # Already exists?
        elif check_dup and rone:
            raise DuplicateEntryException('Already exists:', uuid)

        # Check if dumps of matrices/vectors already exist (improbable).
        hashes = [u.id for u in data.uuids.values()]
        qmarks = ','.join(['?'] * len(hashes))
        self.query(f'select id from dump where id in ({qmarks})', hashes)
        rall = self.get_all()
        stored_hashes = [row['id'] for row in rall]

        # Insert only dumps that are missing in storage
        for name, u in data.uuids.items():
            if u.id not in stored_hashes:
                self.store_dump(u.id, data.field_dump(name))

        # Insert history.  #TODO: would a transaction be faster here?
        for transf in data.history:
            self.store_dump(transf.uuid00.id, pack(transf.serialized))

        # Create row at table 'data'. ---------------------
        sql = f'insert into data values (NULL, ?, ?, ?, ?, NULL)'
        data_args = [uuid,
                     data.matrix_names_str,
                     data.uuids_str,
                     data.history_str]
        from sqlite3 import IntegrityError as IntegrityErrorSQLite
        from pymysql import IntegrityError as IntegrityErrorMySQL
        # try:
        self.query(sql, data_args)
        # unfortunately,
        # it seems that FKs generate the same exception as reinsertion.
        # so, missing FKs might not be detected here.
        # not a worrying issue whatsoever.
        # TODO: it seems to be capturing errors other these here:
        # except IntegrityErrorSQLite as e:
        #     print(f'Unexpected: Data already stored before!', uuid)
        # except IntegrityErrorMySQL as e:
        #     print(f'Unexpected: Data already stored before!', uuid)
        # else:
        print(f': Data inserted', uuid)

    def _fetch_impl(self, hollow_data, fields, training_data_uuid='', lock=False):
        # Fetch data info.
        uuid = hollow_data.uuid00.id
        self.query(f"select * from data where id=?", [uuid])
        result = self.get_one()
        if result is None:
            return None
        # values_by_id = {row['id']: row['value'] for row in rall}
        names = result['names'].split(',')
        muuids = result['matrices'].split(',')
        huuids = result['history'].split(',')

        name_by_muuid = {muuid: name for muuid, name in zip(muuids, names)}

        # Fetch matrices and history.
        # TODO: postpone fetching to LazyData, or bring only the needed ones.
        # TODO: where is failure stored??
        matrices_by_muuid = self.fetch_dumps(muuids)
        matrices_by_name = {
            name_by_muuid[muuid]: matrices_by_muuid[muuid] for muuid in muuids
        }

        # Create Data.
        history = [Transformation.materialize(tr)
                   for tr in self.fetch_dumps(huuids).values()]
        uuids = {
            name_by_muuid[muuid]: UUID.from_pretty(muuid) for muuid in muuids
        }
        data = Data(uuid=uuid, uuids=uuids, history=history, failure=None,
                    **matrices_by_name)

        # TODO: mesclar outputdata com matrizes do inputdata.
        #  Basta checar presença e uuid da matriz
        #  no input_data para saber a atualidade.
        return data

    def fetch_matrix(self, uuid):
        self.query(f'select value from dump where id=?', [uuid])
        rone = self.get_one()
        return unpack(rone['value'])

    def fetch_dumps(self, uuids):
        qmarks = ','.join(['?'] * len(uuids))
        sql = f'select id,value from dump where id in ({qmarks})'
        self.query(sql, uuids)
        rall = self.get_all()
        id_value = {row['id']: unpack(row['value']) for row in rall}
        return {uuid: id_value[uuid] for uuid in uuids}

    def unlock(self, hollow_data, training_data_uuid=None):
        # locked = rone and rone['t'] == '0000-00-00 00:00:00'
        # if not locked:
        #     raise UnlockedEntryException('Cannot unlock if it is not locked!')
        self.query(f'delete from data where id=?', [hollow_data.uuid00.id])

    def list_by_name(self, substring, only_historyless=True):
        # TODO: Pra fins de fetchbylist, pode ser usado o próprio Data se a
        #       implementação passar a ser lazy. (ORM-like behavior)
        pass

    @staticmethod
    @abstractmethod
    def _on_conflict(fields=None):
        pass

    @staticmethod
    @abstractmethod
    def _keylimit():
        pass

    @staticmethod
    @abstractmethod
    def _now_function():
        pass

    @staticmethod
    @abstractmethod
    def _auto_incr():
        pass

    def _setup(self):
        print('creating tables...')

        # Data - Up to 102 matrices and 3277 transformations per row
        # ========================================================
        self.query(f'''
            create table if not exists data (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(19) NOT NULL UNIQUE,
                names VARCHAR(255) NOT NULL,
                matrices VARCHAR(2048), 
                history VARCHAR(65535),
                t TIMESTAMP 
            )''')
        self.query(f'''
            create table if not exists dump (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(19) NOT NULL UNIQUE,
                value LONGBLOB NOT NULL
            )''')

    def get_one(self) -> Optional[dict]:
        """
        Get a single result after a query, no more than that.
        :return:
        """
        row = self.cursor.fetchone()
        if row is None:
            return None
        row2 = self.cursor.fetchone()
        if row2 is not None:
            print('first row', row)
            while row2:
                print('extra row', row2)
                row2 = self.cursor.fetchone()
            raise Exception('  Excess of rows')
        return dict(row)

    def get_all(self) -> list:
        """
        Get a list of results after a query.
        :return:
        """
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]

    def store_dump(self, uuid_, value):
        """Store the given pair uuid-dump of a matrix/vector."""
        sql = f'insert or ignore into dump values (null, ?, ?)'
        from cururu.sql.sqlite import SQLite
        dump = memoryview(value) if isinstance(self, SQLite) else value
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.query(sql, [uuid_, dump])

    def lock_impl(self, data):
        uuid = data.uuid00.id
        if self.debug:
            print('Locking...', uuid)

        sql = f"insert into data values (null,?,?,?,?,'0000-00-00 00:00:00')"
        args = [uuid, '', '', '']
        from sqlite3 import IntegrityError as IntegrityErrorSQLite
        from pymysql import IntegrityError as IntegrityErrorMySQL
        try:
            self.query(sql, args)
        except IntegrityErrorSQLite as e:
            print(f'Unexpected lock! '
                  f'Giving up my turn on {uuid} ppy/se', e)
        except IntegrityErrorMySQL as e:
            print(f'Unexpected lock! '
                  f'Giving up my turn on {uuid} ppy/se', e)
        else:
            print(f'Now locked for {uuid}')

    def query(self, sql, args=None):
        if self.read_only and not sql.startswith('select '):
            print('========================================\n',
                  'Attempt to write onto read-only storage!', sql)
            self.cursor.execute('select 1')
            return
        if args is None:
            args = []
        from cururu.sql.mysql import MySQL
        msg = self._interpolate(sql, args)
        if self.debug:
            print(msg)
        if isinstance(self, MySQL):
            sql = sql.replace('?', '%s')
            sql = sql.replace('insert or ignore', 'insert ignore')
            # self.connection.ping(reconnect=True)

        try:
            self.cursor.execute(sql, args)
        except Exception as ex:
            # From a StackOverflow answer...
            import sys
            import traceback
            msg = self.info + '\n' + msg
            # Gather the information from the original exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # Format the original exception for a nice printout:
            traceback_string = ''.join(traceback.format_exception(
                exc_type, exc_value, exc_traceback))
            # Re-raise a new exception of the same class as the original one
            raise type(ex)(
                "%s\norig. trac.:\n%s\n" % (msg, traceback_string))

    def __del__(self):
        try:
            self.connection.close()
        except Exception as e:
            # print('Couldn\'t close database, but that\'s ok...', e)
            pass

    @staticmethod
    def _interpolate(sql, lst0):
        lst = [str(w)[:100] for w in lst0]
        zipped = zip(sql.replace('?', '"?"').split('?'), map(str, lst + ['']))
        return ''.join(list(sum(zipped, ()))).replace('"None"', 'NULL')

    # FOREIGN KEY (attr) REFERENCES attr(aid)
    # self.query(f'CREATE INDEX nam0 ON dataset (des{self._keylimit()})')
    # self.query(f'CREATE INDEX nam1 ON dataset (attr)')
    # insl timestamp NOT NULL     # unique(dataset, hist),
    # spent FLOAT,        # fail TINYINT,      # start TIMESTAMP NOT NULL,
    #     self._now_function()
    # update data set {','.join([f'{k}=?' for k in to_update.keys()])}
    # insd=insd, upd={self._now_function()} where did=?
    #     x = coalesce(values(x), x),
    #     from res left join data on dout = did
    #     left join dataset on dataset = dsid where
