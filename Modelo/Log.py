from datetime import date
import os


class Log:
    def __init__(self):
        self._carpeta_log = os.path.join("", "files_log")
        self._archivo_log = f"log-{str(date.today())}.txt"
        self._filename = os.path.join(self._carpeta_log, self._archivo_log)

    @property
    def carpeta_log(self):
        return self._carpeta_log

    @property
    def archivo_log(self):
        return self._archivo_log

    @property
    def filename(self):
        return self._filename