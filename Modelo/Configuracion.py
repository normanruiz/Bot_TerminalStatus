class Configuracion:
    def __init__(self):
        self._configfile = 'config.xml'
        self._bot = None
        self._conexiones = []

    @property
    def configfile(self):
        return self._configfile

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