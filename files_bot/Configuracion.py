import xmltodict


class Configuracion:
    def __init__(self, log):
        self._log = log
        self._configfile = 'config.xml'
        self._bot = None
        self._conexiones = []

    @property
    def bot(self):
        return self._bot

    @bot.setter
    def bot(self, bot):
        self._bot = bot

    @property
    def conexiones(self):
        return self._conexiones

    @conexiones.setter
    def conexiones(self, conexiones):
        self._conexiones = conexiones

    @property
    def log(self):
        return self._log

    def cargar(self):
        try:
            mensaje = f"Cargando configuracion..."
            self.log.Escribir(mensaje)
            with open(self._configfile, 'r', encoding='utf8') as xmlfile:
                xmlconfig = xmlfile.read()
                config = xmltodict.parse(xmlconfig)
            autor = Autor(config["parametros"]["bot"]["autor"]["nombre"],
                          config["parametros"]["bot"]["autor"]["nombre"])
            bot = Bot(config["parametros"]["bot"]["nombre"], True if config["parametros"]["bot"]["estado"] == 'True' else False,
                      int(config["parametros"]["bot"]["hilos"]), autor)
            self.bot = bot
            origen = Conexion(config["parametros"]["conexiones"]["origen"]["driver"],
                              config["parametros"]["conexiones"]["origen"]["server"],
                              config["parametros"]["conexiones"]["origen"]["database"],
                              config["parametros"]["conexiones"]["origen"]["username"],
                              config["parametros"]["conexiones"]["origen"]["password"],
                              config["parametros"]["conexiones"]["origen"]["select"], None, None, None)
            destino = Conexion(config["parametros"]["conexiones"]["destino"]["driver"],
                               config["parametros"]["conexiones"]["destino"]["server"],
                               config["parametros"]["conexiones"]["destino"]["database"],
                               config["parametros"]["conexiones"]["destino"]["username"],
                               config["parametros"]["conexiones"]["destino"]["password"],
                               config["parametros"]["conexiones"]["destino"]["select"],
                               config["parametros"]["conexiones"]["destino"]["insert"],
                               config["parametros"]["conexiones"]["destino"]["update"],
                               config["parametros"]["conexiones"]["destino"]["delete"])
            conexiones = [origen, destino]
            self.conexiones = conexiones

            mensaje = f"Configuracion cargada correctamente..."
            self.log.Escribir(mensaje)
            mensaje = f"Subproceso finalizado..."
            self.log.Escribir(mensaje)
        except Exception as excepcion:
            mensaje = f"ERROR - Cargando configuracion: {str(excepcion)}"
            self.log.Escribir(mensaje)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.Escribir(mensaje)
        finally:
            mensaje = f" {'-' * 128}"
            self.log.Escribir(mensaje, tiempo=False)


class Autor:
    def __init__(self, nombre=None, correo=None):
        self._nombre = nombre
        self._correo = correo

    @property
    def nombre(self):
        return self._nombre

    @nombre.setter
    def nombre(self, nombre):
        self._nombre = nombre

    @property
    def correo(self):
        return self._correo

    @correo.setter
    def correo(self, correo):
        self._correo = correo


class Bot:
    def __init__(self, nombre=None, estado=None, hilos=None, autor=None):
        self._nombre = nombre
        self._estado = estado
        self._hilos = hilos
        self._autor = autor

    @property
    def nombre(self):
        return self._nombre

    @nombre.setter
    def nombre(self, nombre):
        self._nombre = nombre

    @property
    def estado(self):
        return self._estado

    @estado.setter
    def estado(self, estado):
        self._estado = estado

    @property
    def hilos(self):
        return self.hilos

    @hilos.setter
    def hilos(self, hilos):
        self._hilos = hilos

    @property
    def autor(self):
        return self._autor

    @autor.setter
    def autor(self, autor):
        self._autor = autor


class Conexion:
    def __init__(self, driver, server, database, username, password, select, insert, update, delete):
        self._driver = driver
        self._server = server
        self._database = database
        self._username = username
        self._password = password
        self._select = select
        self._insert = insert
        self._update = update
        self._delete = delete

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
