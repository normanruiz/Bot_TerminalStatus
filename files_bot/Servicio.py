from files_bot.ConexionDB import ConexionDB
from files_bot.Terminal import Terminal

class Servicio:
    def __init__(self, log, configuracion):
        self._log = log
        self._configuracion = configuracion
        self._datos_origen = []
        self._datos_destino = []
        self._terminales_insert = []
        self._terminales_delete = []
        self._terminales_update = []

    @property
    def log(self):
        return self._log

    @property
    def configuracion(self):
        return self._configuracion

    @property
    def datos_origen(self):
        return self._datos_origen

    @datos_origen.setter
    def datos_origen(self, datos_origen):
        self._datos_origen = datos_origen

    @property
    def datos_destino(self):
        return self._datos_destino

    @datos_destino.setter
    def datos_destino(self, datos_destino):
        self._datos_destino = datos_destino

    @property
    def terminales_insert(self):
        return self._terminales_insert

    @terminales_insert.setter
    def terminales_insert(self, terminales_insert):
        self._terminales_insert = terminales_insert

    @property
    def terminales_delete(self):
        return self._terminales_delete

    @terminales_delete.setter
    def terminales_delete(self, terminales_delete):
        self._terminales_delete = terminales_delete

    @property
    def terminales_update(self):
        return self._terminales_update

    @terminales_update.setter
    def terminales_update(self, terminales_update):
        self._terminales_update = terminales_update

    def recolectar_datos_origen(self):
        estado = True
        dataset_origen = []
        dataset_procesado = {}
        dataset = {}
        conexion = None

        consulta_actividad = self.configuracion.conexiones[1].select
        consulta_segemento = self.configuracion.conexiones[2].select
        consulta_inicializacion = self.configuracion.conexiones[3].select

        try:
            mensaje = f"Recolectando datos de origen..."
            self.log.escribir(mensaje)

            datos_conexion = self.configuracion.conexiones[0]
            conexion = ConexionDB(self.log)
            conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
            dataset_origen = conexion.ejecutar_consulta(datos_conexion.select)
            conexion.desconectar()
            if len(dataset_origen) == 0:
                raise Exception('La tabla de origen se encuentra vacia')
            for registro in dataset_origen:
                terminal = Terminal()
                terminal.terminal = registro[0]
                terminal.super_estado = registro[1]
                terminal.sub_estado = registro[2]
                dataset[terminal.terminal] = terminal
            dataset_origen.clear()

            datos_conexion = self.configuracion.conexiones[1]
            conexion = ConexionDB(self.log)
            conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
            dataset_origen = conexion.ejecutar_consulta(datos_conexion.select)
            conexion.desconectar()
            if len(dataset_origen) == 0:
                raise Exception('La tabla de origen se encuentra vacia')
            for registro in dataset_origen:
                dataset_procesado[registro[0]] = registro[1]

            for numero_terminal, objeto_terminal in dataset.items():
                if numero_terminal in dataset_procesado:
                    dataset[numero_terminal].estado = dataset_procesado[numero_terminal]
                else:
                    dataset[numero_terminal].estado = None
            dataset_origen.clear()
            dataset_procesado.clear()

            datos_conexion = self.configuracion.conexiones[2]
            conexion = ConexionDB(self.log)
            conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
            dataset_origen = conexion.ejecutar_consulta(datos_conexion.select)
            conexion.desconectar()
            if len(dataset_origen) == 0:
                raise Exception('La tabla de origen se encuentra vacia')
            for registro in dataset_origen:
                dataset_procesado[registro[0]] = registro[1]

            for numero_terminal, objeto_terminal in dataset.items():
                if numero_terminal in dataset_procesado:
                    dataset[numero_terminal].segmento = dataset_procesado[numero_terminal]
                else:
                    dataset[numero_terminal].segmento = None
            dataset_origen.clear()
            dataset_procesado.clear()

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Recolectando datos de origen: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            return dataset if estado else estado
