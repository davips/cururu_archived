import socket

import pymysql
import pymysql.cursors

from cururu.sql.abc.sql import SQL


class MySQL(SQL):
    def __init__(self, db='user:pass@ip/db', debug=False, read_only=False):
        server = db.split('/')[0]
        db = db.split('/')[1]
        self.info = server + ', ' + db
        self.read_only = read_only
        self.database = server
        credentials, self.host = server.split('@')
        self.user, self.password = credentials.split(':')
        self.db = db
        self.debug = debug
        if '-' in db:
            raise Exception("'-' not allowed in db name!")
        self.hostname = socket.gethostname()
        self._open()

    def _open(self):
        """
        Each reconnection has a cost of approximately 150ms in ADSL (ping=30ms).
        :return:
        """
        if self.debug:
            print('getting connection...')
        self.connection = pymysql.connect(host=self.host,
                                          user=self.user,
                                          password=self.password,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        # self.connection.client_flag &= pymysql.constants.CLIENT.MULTI_STATEMENTS
        self.connection.autocommit(True)

        if self.debug:
            print('getting cursor...')
        self.cursor = self.connection.cursor()

        # Create db if it doesn't exist yet.
        self.query(f"SHOW DATABASES LIKE '{self.db}'")
        setup = self.get_one() is None
        if setup:
            if self.debug:
                print('creating database', self.db, 'on', self.database, '...')
            self.cursor.execute("create database if not exists " + self.db)

        if self.debug:
            print('using database', self.db, 'on', self.database, '...')
        self.cursor.execute("use " + self.db)

        if setup:
            self._setup()
        return self

    @staticmethod
    def _now_function():
        return 'now()'

    @staticmethod
    def _auto_incr():
        return 'AUTO_INCREMENT'

    @staticmethod
    def _keylimit():
        return '(190)'

    @staticmethod
    def _on_conflict(fields=None):
        return 'ON DUPLICATE KEY UPDATE'
