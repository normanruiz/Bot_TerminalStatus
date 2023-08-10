class Terminal:
    def __init__(self, terminal=None, super_estado=None, sub_estado=None, estado=None, segmento=None,
                 fecha_ultima_inicializacion_ok=None, fecha_cracion=None, fecha_ultima_actualizacion=None):
        self._terminal = terminal
        self._super_estado = super_estado
        self._sub_estado = sub_estado
        self._estado = estado
        self._segmento = segmento
        self._fecha_ultima_inicializacion_ok = fecha_ultima_inicializacion_ok
        self._fecha_cracion = fecha_cracion
        self._fecha_ultima_actualizacion = fecha_ultima_actualizacion

    @property
    def terminal(self):
        return self._terminal

    @terminal.setter
    def terminal(self, terminal):
        self._terminal = terminal

    @property
    def super_estado(self):
        return self._super_estado

    @super_estado.setter
    def super_estado(self, super_estado):
        self._super_estado = super_estado

    @property
    def sub_estado(self):
        return self._sub_estado

    @sub_estado.setter
    def sub_estado(self, sub_estado):
        self._sub_estado = sub_estado

    @property
    def estado(self):
        return self._estado

    @estado.setter
    def estado(self, estado):
        self._estado = estado

    @property
    def segmento(self):
        return self._segmento

    @segmento.setter
    def segmento(self, segmento):
        segmentos = ['Small', 'Medium', 'Large', 'Micro - Merchant']
        if segmento not in segmentos:
            self._segmento = None
        else:
            self._segmento = segmento

    @property
    def fecha_ultima_inicializacion_ok(self):
        return self._fecha_ultima_inicializacion_ok

    @fecha_ultima_inicializacion_ok.setter
    def fecha_ultima_inicializacion_ok(self, fecha_ultima_inicializacion_ok):
        self._fecha_ultima_inicializacion_ok = fecha_ultima_inicializacion_ok

    @property
    def fecha_cracion(self):
        return self._fecha_cracion

    @fecha_cracion.setter
    def fecha_cracion(self, fecha_cracion):
        self._fecha_cracion = fecha_cracion

    @property
    def fecha_ultima_actualizacion(self):
        return self._fecha_ultima_actualizacion

    @fecha_ultima_actualizacion.setter
    def fecha_ultima_actualizacion(self, fecha_ultima_actualizacion):
        self._fecha_ultima_actualizacion = fecha_ultima_actualizacion

    #def __str__(self):
    #    return f"Terminal: {self.terminal} [super_estado: {self.super_estado} - sub_estado: {self.sub_estado} - estado: {self.estado} - segmento: {self.segmento} - fecha_ultima_inicializacion_ok: {self.fecha_ultima_inicializacion_ok} - fecha_cracion: {self.fecha_cracion} - fecha_ultima_actualizacion: {self.fecha_ultima_actualizacion}]"

    def __ne__(self, other):
        if self.super_estado != other.super_estado or self.sub_estado != other.sub_estado or self.estado != other.estado or self.segmento != other.segmento or self.fecha_ultima_inicializacion_ok != other.fecha_ultima_inicializacion_ok:
            return True
        else:
            return False

    def to_insert(self):
        return (self.terminal, self.super_estado, self.sub_estado, self.estado, self.segmento, self.fecha_ultima_inicializacion_ok)

    def to_update(self):
        return (self.super_estado, self.sub_estado, self.estado, self.segmento, self.fecha_ultima_inicializacion_ok, self.terminal)
