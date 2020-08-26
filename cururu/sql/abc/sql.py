import json
import warnings
from abc import abstractmethod
from threading import Thread
from time import sleep
from typing import Optional

from cururu.persistence import Persistence, DuplicateEntryException, LockedEntryException
from pjdata.aux.compression import unpack, pack
from pjdata.aux.uuid import UUID
from pjdata.content.data import Data
from pjdata.history import History


class SQL(Persistence):
    cursor = None
    storage_info = None

    # TODO: remove training_data_uuid from here and put it inside transformations
    def store(self, data: Data, check_dup: bool = True):
        # The sequence of queries is planned to minimize traffic and CPU load,
        # otherwise it would suffice to just send 'insert or ignore' of dumps.
        uuid = data.uuid
        self.query(f"select t from data where id=?", [uuid.id])
        rone = self.get_one()

        if rone:
            # Remove lock.
            locked = rone["t"] == "0000-00-00 00:00:00"
            if locked:
                self.query(f"delete from data where id=?", [uuid.id])

            # Already exists?
            elif check_dup:
                raise DuplicateEntryException("Already exists:", uuid.id)

        # Check if dumps of matrices/vectors already exist.
        qmarks = ",".join(["?"] * len(data.uuids))
        self.query(f"select id from dump where id in ({qmarks})", data.ids_lst)
        rall = self.get_all()
        stored_hashes = [row["id"] for row in rall]

        # Insert only dumps that are missing in storage
        for name, u in data.uuids.items():
            if u.id not in stored_hashes:
                self.store_dump(u.id, data.field_dump(name))

        # Insert history.  #TODO: would a transaction be faster here?
        for transf in data.history:
            self.store_dump(transf.id, pack(transf.serialized))

        # Create row at table 'data'. ---------------------
        sql = f"insert into data values (NULL, ?, ?, ?, ?, {self._now_function()})"

        data_args = [uuid.id, data.matrix_names_str, data.ids_str, data.history_str]
        # from sqlite3 import IntegrityError as IntegrityErrorSQLite
        # from pymysql import IntegrityError as IntegrityErrorMySQL
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
        print(f": Data inserted", uuid)

    def _fetch_impl(self, data: Data, lock: bool = False) -> Data:
        # Fetch data info.
        uuid = data.uuid
        self.query(f"select * from data where id=?", [uuid.id])
        result = self.get_one()

        if result is None:
            if lock:
                self.lock(data)
            return None
        # values_by_id = {row['id']: row['value'] for row in rall}

        if result["names"] == "":
            print("W: Previously locked by other process.", data)
            raise LockedEntryException(data)

        names = result["names"].split(",")
        mids = result["matrices"].split(",")
        hids = result["history"].split(",")

        name_by_mid = dict(zip(mids, names))

        # Fetch matrices (lazily, if storage_info is provided).
        new_mids = [mid for mid in mids if mid not in data.ids_lst]
        matrices = data.matrices
        if self.storage_info is None:
            matrices_by_mid = self.fetch_dumps(new_mids)
            for mid in new_mids:
                matrices[name_by_mid[mid]] = matrices_by_mid[mid]
        else:
            for mid in new_mids:
                matrices[name_by_mid[mid]] = UUID(mid)

        # Fetch history.
        serialized_tranfs = self.fetch_dumps(hids, aslist=True)
        # TODO: deserializar antes de por no histórico
        history = History(serialized_tranfs)

        # TODO: failure and frozen should be stored/fetched!
        # TODO: would it be worth to update uuid/uuids here, instead of recalculating it from the start at Data.init?
        uuids = data.uuids
        uuids.update(dict(zip(names, map(UUID, mids))))
        return Data(
            uuid=uuid,
            uuids=uuids,
            history=history,
            failure=None,
            frozen=False,
            hollow=False,
            stream=None,
            storage_info=self.storage_info,
            **matrices,
        )

    def fetch_matrix(self, id):
        # TODO: quando faz select em algo que não existe, fica esperando
        #  infinitamente algum lock liberar
        self.query(f"select value from dump where id=?", [id])
        rone = self.get_one()
        if rone is None:
            raise Exception("Matrix not found!", id)
        return unpack(rone["value"])

    def fetch_dumps(self, duids, aslist=False):
        if len(duids) == 0:
            return [] if aslist else dict()
        qmarks = ",".join(["?"] * len(duids))
        sql = f"select id,value from dump where id in ({qmarks}) order by n"
        self.query(sql, duids)
        rall = self.get_all()
        id_value = {row["id"]: unpack(row["value"]) for row in rall}
        if aslist:
            return [id_value[duid] for duid in duids]
        else:
            return {duid: id_value[duid] for duid in duids}

    def unlock(self, data, training_data_uuid=None):
        # locked = rone and rone['t'] == '0000-00-00 00:00:00'
        # if not locked:
        #     raise UnlockedEntryException('Cannot unlock if it is not locked!')
        self.query(f"delete from data where id=?", [data.uuid.id])

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
        print("creating tables...")

        # Data - Up to 102 matrices and 3277 transformations per row
        # ========================================================
        self.query(
            f"""
            create table if not exists data (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(18) NOT NULL UNIQUE,
                names VARCHAR(255) NOT NULL,
                matrices VARCHAR(2048), 
                history VARCHAR(65535),
                t TIMESTAMP 
            )"""
        )
        self.query(
            f"""
            create table if not exists dump (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(18) NOT NULL UNIQUE,
                value LONGBLOB NOT NULL
            )"""
        )

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
            print("first row", row)
            while row2:
                print("extra row", row2)
                row2 = self.cursor.fetchone()
            raise Exception("  Excess of rows")
        return dict(row)

    def get_all(self) -> list:
        """
        Get a list of results after a query.
        :return:
        """
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]

    def store_dump(self, duid, value):
        """Store the given pair uuid-dump of a matrix/vector."""
        sql = f"insert or ignore into dump values (null, ?, ?)"
        from cururu.sql.sqlite import SQLite

        dump = memoryview(value) if isinstance(self, SQLite) else value
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.query(sql, [duid, dump])

    def lock(self, data):
        did = data.uuid.id
        if self.debug:
            print("Locking...", did)

        sql = f"insert into data values (null,?,?,?,?,'0000-00-00 00:00:00')"
        args = [did, "", "", ""]
        from sqlite3 import IntegrityError as IntegrityErrorSQLite
        from pymysql import IntegrityError as IntegrityErrorMySQL

        try:
            self.query(sql, args)
        except IntegrityErrorSQLite as e:
            print(f"Unexpected lock! " f"Giving up my turn on {did} ppy/se", e)
        except IntegrityErrorMySQL as e:
            print(f"Unexpected lock! " f"Giving up my turn on {did} ppy/se", e)
        else:
            print(f"Now locked for {did}")

    def query(self, sql, args=None):
        if self.read_only and not sql.startswith("select "):
            print("========================================\n", "Attempt to write onto read-only storage!", sql)
            self.cursor.execute("select 1")
            return
        if args is None:
            args = []
        from cururu.sql.mysql import MySQL

        msg = self._interpolate(sql, args)
        if self.debug:
            print(msg)
        if isinstance(self, MySQL):
            sql = sql.replace("?", "%s")
            sql = sql.replace("insert or ignore", "insert ignore")
            # self.connection.ping(reconnect=True)

        try:
            self.cursor.execute(sql, args)
        except Exception as ex:
            # From a StackOverflow answer...
            import sys
            import traceback

            msg = self.info + "\n" + msg
            # Gather the information from the original exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # Format the original exception for a nice printout:
            traceback_string = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            # Re-raise a new exception of the same class as the original one
            raise type(ex)("%s\norig. trac.:\n%s\n" % (msg, traceback_string))

    def __del__(self):
        try:
            self.connection.close()
        except Exception as e:
            # print('Couldn\'t close database, but that\'s ok...', e)
            pass

    @staticmethod
    def _interpolate(sql, lst0):
        lst = [str(w)[:100] for w in lst0]
        zipped = zip(sql.replace("?", '"?"').split("?"), map(str, lst + [""]))
        return "".join(list(sum(zipped, ()))).replace('"None"', "NULL")

    # FOREIGN KEY (attr) REFERENCES attr(aid)
    # self.query(f'CREATE INDEX nam0 ON dataset (des{self._keylimit()})')
    # self.query(f'CREATE INDEX nam1 ON dataset (attr)')
    # insl timestamp NOT NULL     # unique(dataset, hist),
    # spent FLOAT,        # fail TINYINT,      # start TIMESTAMP NOT NULL,
    # update data set {','.join([f'{k}=?' for k in to_update.keys()])}
    # insd=insd, upd={self._now_function()} where did=?
    #     x = coalesce(values(x), x),
    #     from res left join data on dout = did
    #     left join dataset on dataset = dsid where
