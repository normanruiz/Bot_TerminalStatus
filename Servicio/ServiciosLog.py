import os
from datetime import date
import time

from Modelo.Log import Log


class ServiciosLog:
    def __init__(self):
        self._log = Log()

    @property
    def log(self):
        return self._log

    @log.setter
    def log(self, log):
        self._log = log

    def verificar_archivo_log(self):
        estado = True
        try:
            if not os.path.exists(self.log.carpeta_log):
                os.mkdir(self.log.carpeta_log)
            if not os.path.exists(self.log.filename):
                with open(os.path.join(self.log.filename), "w", encoding="utf8") as file_log:
                    file_log.write(" " + "=" * 128 + "\n")
                    file_log.write(f"  {str(date.today())} {time.strftime('%H:%M:%S', time.localtime())} - Archivo de "
                                   f"log generado\n")
        except Exception as excepcion:
            estado = False
            print(f"  Error - Fallo la verificacion del archivo de logs: {type(excepcion)} - {str(excepcion)}")
        finally:
            return estado

    def escribir(self, mensaje, tiempo=True, archivo=True, pantalla=True):
        estado = True
        hora = time.strftime('%H:%M:%S', time.localtime())
        registro = ''
        try:
            if tiempo:
                registro += f"  {hora} {mensaje}"
            else:
                registro += f"{mensaje}"
            if pantalla:
                print(registro)
            if archivo:
                with open(os.path.join(self.log.filename), "a", encoding="utf8") as file_log:
                    file_log.write(f"{registro}\n")
        except Exception as excepcion:
            estado = False
            print(f"  ERROR - Escribiendo log: {type(excepcion)} - {str(excepcion)}")
        finally:
            return estado