class ApiSalesforce:
    def __init__(self, org=None, client_id=None, client_secret=None, username=None, password=None, version=None, select=None):
        self._org = org
        self._client_id = client_id
        self._client_secret = client_secret
        self._username = username
        self._password = password
        self._token = None
        self._instanceUrl = None
        self._version = version
        self._select = select

    @property
    def org(self):
        return self._org

    @org.setter
    def org(self, org):
        self._org = org

    @property
    def client_id(self):
        return self._client_id

    @client_id.setter
    def client_id(self, client_id):
        self._client_id = client_id

    @property
    def client_secret(self):
        return self._client_secret

    @client_secret.setter
    def client_secret(self, client_secret):
        self._client_secret = client_secret

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
    def token(self):
        return self._token

    @token.setter
    def token(self, token):
        self._token = token

    @property
    def instanceUrl(self):
        return self._instanceUrl

    @instanceUrl.setter
    def instanceUrl(self, instanceUrl):
        self._instanceUrl = instanceUrl

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def select(self):
        return self._select

    @select.setter
    def select(self, select):
        self._select = select