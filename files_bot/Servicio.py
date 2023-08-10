from files_bot.ConexionDB import ConexionDB
from files_bot.Terminal import Terminal

class Servicio:
    def __init__(self, log, configuracion):
        self._log = log
        self._configuracion = configuracion
        self._datos_origen = {}
        self._datos_destino = {}
        self._terminales_insert = {}
        self._terminales_delete = {}
        self._terminales_update = {}

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

            self.datos_origen = dataset

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

    def recolectar_datos_destino(self):
        estado = True
        dataset_origen = []
        dataset_procesado = {}
        dataset = {}
        conexion = None

        try:
            mensaje = f"Recolectando datos de destino..."
            self.log.escribir(mensaje)

            datos_conexion = self.configuracion.conexiones[4]
            conexion = ConexionDB(self.log)
            conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
            dataset_origen = conexion.ejecutar_consulta(datos_conexion.select)
            conexion.desconectar()
            for registro in dataset_origen:
                terminal = Terminal()
                terminal.terminal = registro[0]
                terminal.super_estado = registro[1]
                terminal.sub_estado = registro[2]
                terminal.estado = registro[3]
                terminal.segmento = registro[4]
                terminal.fecha_ultima_inicializacion_ok = registro[5]
                terminal.fecha_cracion = registro[6]
                terminal.fecha_ultima_actualizacion = registro[7]
                dataset[terminal.terminal] = terminal

            dataset_origen.clear()

            self.datos_destino = dataset

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Recolectando datos de destino: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            return dataset if estado else estado

    def generar_lote_insert(self):
        estado = True
        terminales_insert = {}
        try:
            mensaje = f"Generando lote de nuevas terminales a insertar..."
            self.log.escribir(mensaje)

            terminales_insert = self.datos_origen.keys() - self.datos_destino.keys()
            for terminal in terminales_insert:
                self.terminales_insert[terminal] = self.datos_origen[terminal]
            mensaje = f"Total de nuevas terminales a insertar: {len(self.terminales_insert)}"
            self.log.escribir(mensaje)

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"Error - Generando lote de nuevas terminales a insertar: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            return estado

    def generar_lote_delete(self):
        estado = True
        terminales_delete = {}
        try:
            mensaje = f"Generando lote de terminales a eliminar..."
            self.log.escribir(mensaje)

            terminales_delete = self.datos_destino.keys() - self.datos_origen.keys()
            for terminal in terminales_delete:
                self.terminales_delete[terminal] = self.datos_destino[terminal]
            mensaje = f"Total de terminales existentes a eliminar: {len(self.terminales_delete)}"
            self.log.escribir(mensaje)

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"Error - Generando lote de terminales a eliminar: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            return estado

    def generar_lote_update(self):
        estado = True
        terminales_update = []
        try:
            mensaje = f"Generando lote de terminales a actualizar..."
            self.log.escribir(mensaje)

            terminales_update = (self.datos_origen.keys() - self.terminales_delete.keys()) - self.terminales_insert.keys()
            for terminal in terminales_update:
                if self.datos_origen[terminal] != self.datos_destino[terminal]:
                    self.terminales_update[terminal] = self.datos_origen[terminal]

            mensaje = f"Total de terminales existentes a actualizar: {len(self.terminales_update)}"
            self.log.escribir(mensaje)

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"Error - Generando lote de terminales a actualizar: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            return estado

    def impactar_inserts(self):
        estado = True
        datos_insert = []
        try:
            mensaje = f"Impactando lote de terminales a insertar..."
            self.log.escribir(mensaje)

            if len(self.terminales_insert) > 0:
                for numero, terminal in self.terminales_insert.items():
                    datos_insert.append(terminal.to_insert())

                datos_conexion = self.configuracion.conexiones[4]
                conexion = ConexionDB(self.log)
                conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
                conexion.ejecutar_insert(datos_conexion.insert, tuple(datos_insert))
                conexion.desconectar()
            else:
                mensaje = f"Lote vacio, no hay terminales para insertar..."
                self.log.escribir(mensaje)

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"Error - Impactando lote de terminales a insertar: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            return estado

    def impactar_deletes(self):
        estado = True
        try:
            mensaje = f"Impactando lote de terminales a eliminar..."
            self.log.escribir(mensaje)

            if len(self.terminales_delete) > 0:
                datos_conexion = self.configuracion.conexiones[4]
                conexion = ConexionDB(self.log)
                conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
                conexion.ejecutar_delete(datos_conexion.delete, tuple(self.terminales_delete.keys()))
                conexion.desconectar()
            else:
                mensaje = f"Lote vacio, no hay terminales para eliminar..."
                self.log.escribir(mensaje)

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"Error - Impactando lote de terminales a eliminar: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            return estado

    def impactar_updates(self):
        estado = True
        datos_update = []
        try:
            mensaje = f"Impactando lote de terminales a actualizar..."
            self.log.escribir(mensaje)
            if len(self.terminales_update) > 0:
                for numero, terminal in self.terminales_update.items():
                    datos_update.append(terminal.to_update())
                datos_conexion = self.configuracion.conexiones[4]
                conexion = ConexionDB(self.log)
                conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
                conexion.ejecutar_update(datos_conexion.update, tuple(datos_update))
                conexion.desconectar()
            else:
                mensaje = f"Lote vacio, no hay terminales para actualizar..."
                self.log.escribir(mensaje)

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"Error - Impactando lote de terminales a actualizar: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            return estado

    def generar_historial(self):
        name = f"[sis].[uspa_history_terminal_status]"
        estado = True
        try:
            mensaje = f"Generando historia..."
            self.log.escribir(mensaje)

            datos_conexion = self.configuracion.conexiones[4]
            conexion = ConexionDB(self.log)
            conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
            conexion.ejecutar_sp(name, datos_conexion.history)
            conexion.desconectar()

            mensaje = f"Subproceso finalizado..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"Error - Generando historia: {str(excepcion)}"
            self.log.escribir(mensaje)
            mensaje = f" {'-'*128}"
            self.log.escribir(mensaje, tiempo=False)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            self.log.escribir(mensaje)
        finally:
            return estado