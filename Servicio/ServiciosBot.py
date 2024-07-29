from Modelo.Bot import Bot
from Servicio.ServiciosConfiguracion import ServiciosConfiguracion
from Servicio.ServiciosETL import ServiciosETL
from Servicio.ServiciosLog import ServiciosLog


class ServiciosBot:
    def __init__(self):
        self._bot = Bot()

    @property
    def bot(self):
        return self._bot

    @bot.setter
    def bot(self, bot):
        self._bot = bot

    def iniciar(self):
        status_code = 0
        servicioslog = None
        try:
            servicioslog = ServiciosLog()
            if self.bot.estado is False:
                return
            self.bot.estado = servicioslog.verificar_archivo_log()
            mensaje = f" {'='*128 }"
            servicioslog.escribir(mensaje, tiempo=False)
            mensaje = f"Iniciando TerminalStatus Bot's..."
            servicioslog.escribir(mensaje)
            mensaje = f" {'~'*128 }"
            servicioslog.escribir(mensaje, tiempo=False)

            serviciosconfiguracion = ServiciosConfiguracion()
            self.bot.estado = serviciosconfiguracion.cargar(servicioslog)
            if self.bot.estado is False:
                return
            self.bot.estado = serviciosconfiguracion.configuracion.bot.estado
            if self.bot.estado is False:
                mensaje = f"Bot apagado por configuracion..."
                servicioslog.escribir(mensaje)
                return

            serviciosetl = ServiciosETL(serviciosconfiguracion.configuracion)
            self.bot.estado = serviciosetl.extract(servicioslog)
            if self.bot.estado is False:
                return
            self.bot.estado = serviciosetl.transform(servicioslog)
            if self.bot.estado is False:
                return
            self.bot.estado = serviciosetl.load(servicioslog)
            if self.bot.estado is False:
                return

        except Exception as excepcion:
            status_code = 1
            mensaje = f" {'-' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)
            mensaje = f"ERROR - Ejecucion principal: {type(excepcion)} - {str(excepcion)}"
            servicioslog.escribir(mensaje)
        finally:
            if not self.bot.estado:
                mensaje = f" {'-' * 128}"
                servicioslog.escribir(mensaje, tiempo=False)
                mensaje = f"WARNING!!! - Proceso principal interrumpido, no se realizaran mas acciones..."
                servicioslog.escribir(mensaje)

            mensaje = f" {'~' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)
            mensaje = f"Finalizando TerminalStatus Bot's..."
            servicioslog.escribir(mensaje)
            mensaje = f" {'=' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)
            return status_code
