class Bot:
    def __init__(self):
        self._estado = True

    @property
    def estado(self):
        return self._estado

    @estado.setter
    def estado(self, estado):
        self._estado = estado

    def iniciar(self):
        print("Iniciando Bot's TerminalStatus...")
