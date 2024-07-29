class ConexionDBMySQL:
    def __init__(self, host=None, port=None, database=None, username=None, password=None, select=None, insert=None,
                 update=None, delete=None, history=None):
        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password
        self._select = select
        self._insert = insert
        self._update = update
        self._delete = delete
        self._history = history

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def database(self):
        return self._database

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def select(self):
        return self._select

    @property
    def insert(self):
        return self._insert

    @property
    def update(self):
        return self._update

    @property
    def delete(self):
        return self._delete

    @property
    def history(self):
        return self._history

    @history.setter
    def history(self, history):
        self._history = history