from files_bot.Configuracion import Configuracion
from files_bot.Log import Log


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
        status_code = 0
        try:
            log = Log()
            log.Verificar_archivo_log()
            mensaje = f" {'='*128 }"
            log.Escribir(mensaje, tiempo=False)
            mensaje = f"Iniciando TerminalStatus Bot's..."
            log.Escribir(mensaje)
            mensaje = f" {'~'*128 }"
            log.Escribir(mensaje, tiempo=False)
            configuracion = Configuracion(log)
            configuracion.cargar()
            self.configuracion = configuracion
            self.estado = self.configuracion.bot.estado
            if not(self.estado):
                mensaje = f"Bot apagado por configuracion..."
                log.Escribir(mensaje)
                return
        except Exception as excepcion:
            status_code = 1
            mensaje = f" {'-'*128 }"
            log.Escribir(mensaje, tiempo=False)
            mensaje = f"ERROR - Ejecucion principal: {str(excepcion)}"
            log.Escribir(mensaje)
        finally:
            if not (self.estado):
                mensaje = f" {'-' * 128}"
                log.Escribir(mensaje, tiempo=False)
                mensaje = f"WARNING!!! - Proceso principal interrumpido, no se realizaran mas acciones..."
                log.Escribir(mensaje)

            mensaje = f" {'~' * 128}"
            log.Escribir(mensaje, tiempo=False)
            mensaje = f"Finalizando TerminalStatus Bot's..."
            log.Escribir(mensaje)
            mensaje = f" {'='*128 }"
            log.Escribir(mensaje, tiempo=False)
            print()
            return status_code

