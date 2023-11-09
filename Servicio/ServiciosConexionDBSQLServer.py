import pyodbc


class ServiciosConexionDBSQLServer:
    def __init__(self, log):
        self._log = log
        self._conexion = None

    @property
    def log(self):
        return self._log

    @log.setter
    def log(self, log):
        self._log = log

    @property
    def conexion(self):
        return self._conexion

    @conexion.setter
    def conexion(self, conexion):
        self._conexion = conexion

    def conectar(self, driver, server, database, usuario, contrasenia):
        estado = True
        self.database = database
        try:
            mensaje = f"Conectando a base de datos {database}..."
            self.log.escribir(mensaje)
            cadena_de_conexion = f'DRIVER={driver};' \
                                 f'SERVER={server};' \
                                 f'DATABASE={database};' \
                                 f'UID={usuario};' \
                                 f'PWD={contrasenia};' \
                                 f'TrustServerCertificate=yes;'
            self.conexion = pyodbc.connect(cadena_de_conexion)
            mensaje = f"Conexion establecida con base de datos {database}..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Conectando a base de datos {database}: {str(excepcion)}"
            self.log.escribir(mensaje)
        finally:
            return estado

    def desconectar(self):
        estado = True
        try:
            mensaje = f"Cerrando conexion con base de datos {self.database}..."
            self.log.escribir(mensaje)
            self.conexion.close()
            mensaje = f"Conexion a base de datos {self.database} cerrada..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Cerrando conexion a base de datos {self.database}: {str(excepcion)}"
            self.log.escribir(mensaje)
        finally:
            return estado

    def ejecutar_consulta(self, consulta):
        estado = True
        data = []
        cursor = None
        try:
            mensaje = f"Ejecutando query contra {self.database}..."
            self.log.escribir(mensaje)
            mensaje = f"Query: {consulta}"
            self.log.escribir(mensaje)
            mensaje = f"Generando cursor..."
            self.log.escribir(mensaje)
            cursor = self.conexion.cursor()
            mensaje = f"Comenzando lectura de datos..."
            self.log.escribir(mensaje)
            cursor.execute(consulta)
            data = cursor.fetchall()
            mensaje = f"Datos obtenidos: {len(data)} registros..."
            self.log.escribir(mensaje)
            mensaje = f"Lectura de datos finalizada..."
            self.log.escribir(mensaje)

        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Ejecutando query: {str(excepcion)}"
            self.log.escribir(mensaje)
        finally:
            if cursor:
                cursor.close()
                mensaje = f"Destruyendo cursor..."
                self.log.escribir(mensaje)
            return estado, data

    def ejecutar_insert(self, consulta, datos):
        estado = True
        cursor = None
        procesadas = 0
        try:
            mensaje = f"Ejecutando insert contra {self.database}..."
            self.log.escribir(mensaje)
            mensaje = f"Query: {consulta}"
            self.log.escribir(mensaje)
            mensaje = f"Generando cursor..."
            self.log.escribir(mensaje)
            conexion = self.conexion
            cursor = conexion.cursor()
            mensaje = f"Comenzando escritura de datos..."
            self.log.escribir(mensaje)

            cursor.fast_executemany = True
            cursor.executemany(consulta, datos)
            conexion.commit()
            procesadas = len(datos)

            mensaje = f"Total de terminales nuevas insertadas: {procesadas}"
            self.log.escribir(mensaje)

            mensaje = f"Escritura de datos finalizada..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Ejecutando insert: {str(excepcion)}"
            self.log.escribir(mensaje)
        finally:
            if cursor:
                cursor.close()
                mensaje = f"Destruyendo cursor..."
                self.log.escribir(mensaje)
            return estado

    def ejecutar_delete(self, consulta, datos):
        estado = True
        cursor = None
        procesadas = 0
        try:
            mensaje = f"Ejecutando delete contra {self.database}..."
            self.log.escribir(mensaje)
            mensaje = f"Query: {consulta}"
            self.log.escribir(mensaje)
            mensaje = f"Generando cursor..."
            self.log.escribir(mensaje)
            conexion = self.conexion
            cursor = conexion.cursor()
            mensaje = f"Comenzando escritura de datos..."
            self.log.escribir(mensaje)

            for terminal in datos:
                cursor.execute(consulta, terminal)
                procesadas += 1
            conexion.commit()

            mensaje = f"Total de terminales eliminadas: {procesadas}"
            self.log.escribir(mensaje)

            mensaje = f"Escritura de datos finalizada..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Ejecutando delete: {str(excepcion)}"
            self.log.escribir(mensaje)
        finally:
            if cursor:
                cursor.close()
                mensaje = f"Destruyendo cursor..."
                self.log.escribir(mensaje)
            return estado

    def ejecutar_update(self, consulta, datos):
        estado = True
        cursor = None
        procesadas = 0
        try:
            mensaje = f"Ejecutando update contra {self.database}..."
            self.log.escribir(mensaje)
            mensaje = f"Query: {consulta}"
            self.log.escribir(mensaje)
            mensaje = f"Generando cursor..."
            self.log.escribir(mensaje)
            conexion = self.conexion
            cursor = conexion.cursor()
            mensaje = f"Comenzando escritura de datos..."
            self.log.escribir(mensaje)

            cursor.fast_executemany = True
            cursor.executemany(consulta, datos)
            conexion.commit()
            procesadas = len(datos)

            mensaje = f"Total de terminales actualizadas: {procesadas}"
            self.log.escribir(mensaje)

            mensaje = f"Escritura de datos finalizada..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Ejecutando update: {str(excepcion)}"
            self.log.escribir(mensaje)
        finally:
            if cursor:
                cursor.close()
                mensaje = f"Destruyendo cursor..."
                self.log.escribir(mensaje)
            return estado

    def ejecutar_sp(self, name, consulta):
        estado = True
        cursor = None
        try:
            mensaje = f"Ejecutando procedimiento almacenado {name} contra {self.database}..."
            self.log.escribir(mensaje)
            mensaje = f"Query: {consulta}"
            self.log.escribir(mensaje)
            mensaje = f"Generando cursor..."
            self.log.escribir(mensaje)
            conexion = self.conexion
            cursor = conexion.cursor()
            mensaje = f"Comenzando escritura de datos..."
            self.log.escribir(mensaje)

            cursor.execute(consulta)
            conexion.commit()

            mensaje = f"Generado historial..."
            self.log.escribir(mensaje)

            mensaje = f"Escritura de datos finalizada..."
            self.log.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Ejecutando procedimiento almacenado {name}: {str(excepcion)}"
            self.log.escribir(mensaje)
        finally:
            if cursor:
                cursor.close()
                mensaje = f"Destruyendo cursor..."
                self.log.escribir(mensaje)
            return estado