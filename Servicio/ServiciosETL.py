from Servicio.ServiciosConexionDBSQLServer import ServiciosConexionDBSQLServer
from Servicio.ServiciosSalesfoce import ServiciosSalesforce
from Modelo.Terminal import Terminal


class ServiciosETL:
    def __init__(self, configuracion):
        self._configuracion = configuracion
        self._terminales = {}
        self._terminales_existentes = {}
        self._datos_atd = []
        self._datos_ini = []
        self._datos_origen = {}
        self._datos_destino = []
        self._terminales_insert = {}
        self._terminales_update = {}
        self._terminales_delete = {}

    @property
    def configuracion(self):
        return self._configuracion

    @configuracion.setter
    def configuracion(self, configuracion):
        self._configuracion = configuracion

    @property
    def terminales(self):
        return self._terminales

    @terminales.setter
    def terminales(self, terminales):
        self._terminales = terminales

    @property
    def terminales_existentes(self):
        return self._terminales_existentes

    @terminales_existentes.setter
    def terminales_existentes(self, terminales_existentes):
        self._terminales_existentes = terminales_existentes

    @property
    def datos_atd(self):
        return self._datos_atd

    @datos_atd.setter
    def datos_atd(self, datos_atd):
        self._datos_atd = datos_atd

    @property
    def datos_ini(self):
        return self._datos_ini

    @datos_ini.setter
    def datos_ini(self, datos_ini):
        self._datos_ini = datos_ini

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
    def terminales_update(self):
        return self._terminales_update

    @terminales_update.setter
    def terminales_update(self, terminales_update):
        self._terminales_update = terminales_update

    @property
    def terminales_delete(self):
        return self._terminales_delete

    @terminales_delete.setter
    def terminales_delete(self, terminales_delete):
        self._terminales_delete = terminales_delete

    def extract(self, servicioslog):
        estado = True
        try:
            mensaje = f"Iniciando extraccion de datos..."
            servicioslog.escribir(mensaje)

            servicios_salesforce = ServiciosSalesforce(servicioslog, self.configuracion)
            estado, self.datos_origen = servicios_salesforce.buscarterminales()
            if estado is False:
                return

            mensaje = f" {'-' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)

            serviciosadicionalteradata = ServiciosConexionDBSQLServer(servicioslog)
            datos_conexion = self.configuracion.conexiones[1]
            estado = serviciosadicionalteradata.conectar(datos_conexion.driver, datos_conexion.server,
                                                         datos_conexion.database, datos_conexion.username,
                                                         datos_conexion.password)
            if estado is False:
                return
            estado, self.datos_atd = serviciosadicionalteradata.ejecutar_consulta(datos_conexion.select)
            if estado is False:
                return
            serviciosadicionalteradata.desconectar()
            if len(self.datos_atd) == 0:
                raise Exception('La tabla de origen se encuentra vacia')

            mensaje = f" {'-' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)

            serviciosinicializacion = ServiciosConexionDBSQLServer(servicioslog)
            datos_conexion = self.configuracion.conexiones[2]
            estado = serviciosinicializacion.conectar(datos_conexion.driver, datos_conexion.server,
                                                      datos_conexion.database, datos_conexion.username,
                                                      datos_conexion.password)
            if estado is False:
                return
            estado, self.datos_ini = serviciosinicializacion.ejecutar_consulta(datos_conexion.select)
            if estado is False:
                return
            serviciosinicializacion.desconectar()
            if len(self.datos_ini) == 0:
                raise Exception('La tabla de origen se encuentra vacia')

            mensaje = f" {'-' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)

            serviciodestino = ServiciosConexionDBSQLServer(servicioslog)
            datos_conexion = self.configuracion.conexiones[3]
            estado = serviciodestino.conectar(datos_conexion.driver, datos_conexion.server,
                                              datos_conexion.database, datos_conexion.username,
                                              datos_conexion.password)
            if estado is False:
                return
            estado, self.datos_destino = serviciodestino.ejecutar_consulta(datos_conexion.select)
            if estado is False:
                return
            serviciodestino.desconectar()

            mensaje = f"Subproceso finalizado..."
            servicioslog.escribir(mensaje)

        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Extraccion de datos: {type(excepcion)} - {str(excepcion)}"
            servicioslog.escribir(mensaje)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            servicioslog.escribir(mensaje)

        finally:
            mensaje = f" {'-' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)
            return estado

    def transform(self, servicioslog):
        estado = True
        try:
            mensaje = f"Iniciando transformacion de datos..."
            servicioslog.escribir(mensaje)

            mensaje = f"Recompilando terminales con datos de origen ..."
            servicioslog.escribir(mensaje)

            for numero, super_estado in self.datos_origen.items():
                terminal = Terminal()
                terminal.numero = numero
                terminal.super_estado = super_estado
                self.terminales[terminal.numero] = terminal
            self.datos_origen.clear()

            for registro in self.datos_atd:
                if registro[0] in self.terminales.keys():
                    self.terminales[registro[0]].estado = registro[1]
                    self.terminales[registro[0]].segmento = registro[2]
            self.datos_atd.clear()

            for registro in self.datos_ini:
                if registro[0] in self.terminales.keys():
                    self.terminales[registro[0]].fecha_ultima_inicializacion_ok = registro[1]
            self.datos_ini.clear()

            mensaje = f"Recompilando terminales exitentes ..."
            servicioslog.escribir(mensaje)

            for registro in self.datos_destino:
                terminal = Terminal()
                terminal.numero = registro[0]
                terminal.super_estado = registro[1]
                terminal.estado = registro[2]
                terminal.segmento = registro[3]
                terminal.fecha_ultima_inicializacion_ok = registro[4]
                terminal.fecha_cracion = registro[5]
                terminal.fecha_ultima_actualizacion = registro[6]
                self.terminales_existentes[terminal.numero] = terminal

            self.datos_destino.clear()

            mensaje = f"Generando lote de terminales a insertar ..."
            servicioslog.escribir(mensaje)

            terminales_insert = self.terminales.keys() - self.terminales_existentes.keys()
            for terminal in terminales_insert:
                self.terminales_insert[terminal] = self.terminales[terminal]
            mensaje = f"Total de nuevas terminales a insertar: {len(self.terminales_insert)}"
            servicioslog.escribir(mensaje)

            mensaje = f"Generando lote de terminales a eliminar ..."
            servicioslog.escribir(mensaje)

            terminales_delete = self.terminales_existentes.keys() - self.terminales.keys()
            for terminal in terminales_delete:
                self.terminales_delete[terminal] = self.terminales_existentes[terminal]
            mensaje = f"Total de terminales existentes a eliminar: {len(self.terminales_delete)}"
            servicioslog.escribir(mensaje)

            mensaje = f"Generando lote de terminales a actualizar ..."
            servicioslog.escribir(mensaje)

            terminales_update = (self.terminales.keys() - self.terminales_delete.keys()) - self.terminales_insert.keys()
            for terminal in terminales_update:
                if self.terminales[terminal] != self.terminales_existentes[terminal]:
                    self.terminales_update[terminal] = self.terminales[terminal]
            mensaje = f"Total de terminales existentes a actualizar: {len(self.terminales_update)}"
            servicioslog.escribir(mensaje)

            self.terminales.clear()
            self.terminales_existentes.clear()

            mensaje = f"Subproceso finalizado..."
            servicioslog.escribir(mensaje)

        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Transformacion de datos: {type(excepcion)} - {str(excepcion)}"
            servicioslog.escribir(mensaje)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            servicioslog.escribir(mensaje)

        finally:
            mensaje = f" {'-' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)
            return estado

    def load(self, servicioslog):
        estado = True
        datos_insert = []
        datos_update = []
        name = f"[sis].[uspa_history_terminal_status]"
        try:
            mensaje = f"Iniciando carga de datos..."
            servicioslog.escribir(mensaje)

            # insertar altas
            mensaje = f"Impactando lote de terminales a insertar..."
            servicioslog.escribir(mensaje)

            if len(self.terminales_insert) > 0:
                for numero, terminal in self.terminales_insert.items():
                    datos_insert.append(terminal.to_insert())
                datos_conexion = self.configuracion.conexiones[3]
                conexion = ServiciosConexionDBSQLServer(servicioslog)
                conexion.conectar(datos_conexion.driver, datos_conexion.server,
                                  datos_conexion.database, datos_conexion.username,
                                  datos_conexion.password)
                conexion.ejecutar_insert(datos_conexion.insert, tuple(datos_insert))
                conexion.desconectar()
            else:
                mensaje = f"Lote vacio, no hay terminales para insertar..."
                servicioslog.escribir(mensaje)

            # eliminar bajas
            mensaje = f"Impactando lote de terminales a eliminar..."
            servicioslog.escribir(mensaje)

            if len(self.terminales_delete) > 0:
                datos_conexion = self.configuracion.conexiones[3]
                conexion = ServiciosConexionDBSQLServer(servicioslog)
                conexion.conectar(datos_conexion.driver, datos_conexion.server,
                                  datos_conexion.database, datos_conexion.username,
                                  datos_conexion.password)
                conexion.ejecutar_delete(datos_conexion.delete, tuple(self.terminales_delete.keys()))
                conexion.desconectar()
            else:
                mensaje = f"Lote vacio, no hay terminales para eliminar..."
                servicioslog.escribir(mensaje)

            # actualizar cambios
            mensaje = f"Impactando lote de terminales a actualizar..."
            servicioslog.escribir(mensaje)
            if len(self.terminales_update) > 0:
                for numero, terminal in self.terminales_update.items():
                    datos_update.append(terminal.to_update())
                datos_conexion = self.configuracion.conexiones[3]
                conexion = ServiciosConexionDBSQLServer(servicioslog)
                conexion.conectar(datos_conexion.driver, datos_conexion.server,
                                  datos_conexion.database, datos_conexion.username,
                                  datos_conexion.password)
                conexion.ejecutar_update(datos_conexion.update, tuple(datos_update))
                conexion.desconectar()
            else:
                mensaje = f"Lote vacio, no hay terminales para actualizar..."
                servicioslog.escribir(mensaje)

            # generar historial
            mensaje = f"Generando historia..."
            servicioslog.escribir(mensaje)

            datos_conexion = self.configuracion.conexiones[3]
            conexion = ServiciosConexionDBSQLServer(servicioslog)
            conexion.conectar(datos_conexion.driver, datos_conexion.server,
                              datos_conexion.database, datos_conexion.username,
                              datos_conexion.password)
            conexion.ejecutar_sp(name, datos_conexion.history)
            conexion.desconectar()

            mensaje = f"Subproceso finalizado..."
            servicioslog.escribir(mensaje)

        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Carga de datos: {type(excepcion)} - {str(excepcion)}"
            servicioslog.escribir(mensaje)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            servicioslog.escribir(mensaje)

        finally:
            return estado
