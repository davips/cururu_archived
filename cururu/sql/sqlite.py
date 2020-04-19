import socket
import sqlite3

from cururu.sql.abc.sql import SQL


class SQLite(SQL):
    def __init__(self, db='/tmp/cururu', debug=not False, read_only=False):
        self.info = db
        self.read_only = read_only
        self.hostname = socket.gethostname()
        self.database = db + '.db'
        self.debug = debug
        self._open()

    def _open(self):
        # isolation_level=None -> SQLite autocommiting
        # isolation_level='DEFERRED' -> SQLite transactioning
        self.connection = sqlite3.connect(self.database, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

        # Create tables if they don't exist yet.
        try:
            self.query(f"select 1 from data")
        except:
            if self.debug:
                print('creating database', self.database, '...')
            self._setup()

    @staticmethod
    def _now_function():
        return 'datetime()'

    @staticmethod
    def _auto_incr():
        return 'AUTOINCREMENT'

    @staticmethod
    def _keylimit():
        return ''

    @staticmethod
    def _on_conflict(fields=''):
        return f'ON CONFLICT{fields} DO UPDATE SET'
