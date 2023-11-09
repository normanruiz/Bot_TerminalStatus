import xmltodict

from Modelo.ApiSalesforce import ApiSalesforce
from Modelo.Configuracion import Configuracion, Autor, Bot
from Modelo.ConexionDBSQLServer import ConexionDBSQLServer

class ServiciosConfiguracion:
    def __init__(self):
        self._configuracion = Configuracion()

    @property
    def configuracion(self):
        return self._configuracion

    @configuracion.setter
    def configuracion(self, configuracion):
        self._configuracion = configuracion

    def cargar(self, servicioslog):
        estado = True
        try:
            mensaje = f"Cargando configuracion..."
            servicioslog.escribir(mensaje)
            with open(self.configuracion.configfile, 'r', encoding='utf8') as xmlfile:
                xmlconfig = xmlfile.read()
                config = xmltodict.parse(xmlconfig)
                autor = Autor(config["parametros"]["bot"]["autor"]["nombre"],
                              config["parametros"]["bot"]["autor"]["correo"])
                bot = Bot(config["parametros"]["bot"]["nombre"],
                          True if config["parametros"]["bot"]["estado"] == 'True' else False,
                          int(config["parametros"]["bot"]["hilos"]), autor)
                self.configuracion.bot = bot

                api_salesforce = ApiSalesforce(config["parametros"]["conexiones"]["api_salesforce"]["org"],
                                               config["parametros"]["conexiones"]["api_salesforce"]["client_id"],
                                               config["parametros"]["conexiones"]["api_salesforce"]["client_secret"],
                                               config["parametros"]["conexiones"]["api_salesforce"]["username"],
                                               config["parametros"]["conexiones"]["api_salesforce"]["password"],
                                               config["parametros"]["conexiones"]["api_salesforce"]["version"],
                                               config["parametros"]["conexiones"]["api_salesforce"]["select"])
                self.configuracion.conexiones.append(api_salesforce)

                origen_actividad_segmento = ConexionDBSQLServer(config["parametros"]["conexiones"]["origen_actividad_segmento"]["driver"],
                                  config["parametros"]["conexiones"]["origen_actividad_segmento"]["server"],
                                  config["parametros"]["conexiones"]["origen_actividad_segmento"]["database"],
                                  config["parametros"]["conexiones"]["origen_actividad_segmento"]["username"],
                                  config["parametros"]["conexiones"]["origen_actividad_segmento"]["password"],
                                  config["parametros"]["conexiones"]["origen_actividad_segmento"]["consulta_actividad"], None, None, None, None)
                self.configuracion.conexiones.append(origen_actividad_segmento)

                origen_inicializacion = ConexionDBSQLServer(config["parametros"]["conexiones"]["origen_inicializacion"]["driver"],
                                  config["parametros"]["conexiones"]["origen_inicializacion"]["server"],
                                  config["parametros"]["conexiones"]["origen_inicializacion"]["database"],
                                  config["parametros"]["conexiones"]["origen_inicializacion"]["username"],
                                  config["parametros"]["conexiones"]["origen_inicializacion"]["password"],
                                  config["parametros"]["conexiones"]["origen_inicializacion"]["consulta_inicializacion"], None, None, None, None)
                self.configuracion.conexiones.append(origen_inicializacion)

                destino = ConexionDBSQLServer(config["parametros"]["conexiones"]["destino"]["driver"],
                                   config["parametros"]["conexiones"]["destino"]["server"],
                                   config["parametros"]["conexiones"]["destino"]["database"],
                                   config["parametros"]["conexiones"]["destino"]["username"],
                                   config["parametros"]["conexiones"]["destino"]["password"],
                                   config["parametros"]["conexiones"]["destino"]["select"],
                                   config["parametros"]["conexiones"]["destino"]["insert"],
                                   config["parametros"]["conexiones"]["destino"]["update"],
                                   config["parametros"]["conexiones"]["destino"]["delete"],
                                   config["parametros"]["conexiones"]["destino"]["history"])
                self.configuracion.conexiones.append(destino)

            mensaje = f"Configuracion cargada correctamente..."
            servicioslog.escribir(mensaje)
            mensaje = f"Subproceso finalizado..."
            servicioslog.escribir(mensaje)
        except Exception as excepcion:
            estado = False
            mensaje = f"ERROR - Cargando configuracion: {type(excepcion)} - {str(excepcion)}"
            servicioslog.escribir(mensaje)
            mensaje = f"WARNING!!! - Subproceso interrumpido..."
            servicioslog.escribir(mensaje)
        finally:
            mensaje = f" {'-' * 128}"
            servicioslog.escribir(mensaje, tiempo=False)
            return estado

