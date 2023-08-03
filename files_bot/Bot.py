from files_bot.Configuracion import Configuracion


class Bot:
    def __init__(self):
        self._estado = True
        self._configuracion = None

    @property
    def estado(self):
        return self._estado

    @estado.setter
    def estado(self, estado):
        self._estado = estado

    @property
    def configuracion(self):
        return self._configuracion

    @configuracion.setter
    def configuracion(self, configuracion):
        self._configuracion = configuracion

    def iniciar(self):
        print("Iniciando Bot's TerminalStatus...")
        configuracion = Configuracion()
        configuracion.cargar()
        self.configuracion = configuracion
        self.estado = self.configuracion.bot.estado
        if not(self.estado):
            print("Bot apagado por configuracion...")
            return
        print("Continuo la operacion...")
