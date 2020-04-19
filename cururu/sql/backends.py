import sqlalchemy as sa
from sqlalchemy import event

from cururu.sql.abc.sql import SQL


class MySQL(SQL):
    def __init__(self, db='user:pass@ip/db'):
        if '-' in db:
            raise Exception("'-' not allowed in url!")  # because of db name
        # TIP: latin1 is to ensure 1 byte per char when storing UUIDs.
        self.engine = sa.create_engine('mysql+pymysql://' + db,
                                       encoding='latin1',
                                       pool_recycle=3600, echo=True)
        super().__init__()


class SQLite(SQL):
    def __init__(self, db='/tmp/cururu'):
        """

        Parameters
        ----------
        db
            When empty, uses volatile fast memory (RAM).
        blocking
        """
        if db == '':
            db = ':memory:'
        else:
            db += '.db'
        # TIP: latin1 is to ensure 1 byte per char when storing UUIDs.
        self.engine = sa.create_engine('sqlite:///' + db, encoding='latin1',
                                       echo=True)

        @event.listens_for(self.engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            # disable pysqlite's emitting of the BEGIN statement entirely.
            # also stops it from emitting COMMIT before any DDL.
            dbapi_connection.isolation_level = None

        # TODO: discover what is this for:
        @event.listens_for(self.engine, "begin")
        def do_begin(conn):
            # emit our own BEGIN
            conn.execute("BEGIN")

        super().__init__()
