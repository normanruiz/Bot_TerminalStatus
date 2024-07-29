class ConexionDBSQLServer:
    def __init__(self, driver, server, database, username, password, select, insert, update, delete, history):
        self._driver = driver
        self._server = server
        self._database = database
        self._username = username
        self._password = password
        self._select = select
        self._insert = insert
        self._update = update
        self._delete = delete
        self._history = history

    @property
    def driver(self):
        return self._driver

    @driver.setter
    def driver(self, driver):
        self._driver = driver

    @property
    def server(self):
        return self._server

    @server.setter
    def server(self, server):
        self._server = server

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, database):
        self._database = database

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, username):
        self._username = username

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, password):
        self._password = password

    @property
    def select(self):
        return self._select

    @select.setter
    def select(self, select):
        self._select = select

    @property
    def insert(self):
        return self._insert

    @insert.setter
    def insert(self, insert):
        self._insert = insert

    @property
    def update(self):
        return self._update

    @update.setter
    def update(self, update):
        self._update = update

    @property
    def delete(self):
        return self._delete

    @delete.setter
    def delete(self, delete):
        self._delete = delete

    @property
    def history(self):
        return self._history

    @history.setter
    def history(self, history):
        self._history = history
