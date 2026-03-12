import datetime

class Trabajador:
    def __init__(self, numero, dni, nombres, d_leg, fecha_ingreso):
        self.numero = numero
        self.dni = dni
        self.nombres = nombres
        self.d_leg = d_leg
        self.fecha_ingreso = datetime.datetime.strptime(fecha_ingreso, "%Y-%m-%d").date()
        self.vacaciones = []
        self.solicitudes = []
        self.memorandos = []

    def calcular_periodo_vacacional(self):
        # Según D.Leg. 276: 30 días por año cumplido
        hoy = datetime.date.today()
        antiguedad = (hoy.year - self.fecha_ingreso.year) - \
                     ((hoy.month, hoy.day) < (self.fecha_ingreso.month, self.fecha_ingreso.day))
        return antiguedad

    def registrar_vacaciones(self, fecha_inicio, dias, documento, mad, observaciones=""):
        fecha_inicio = datetime.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        fecha_fin = fecha_inicio + datetime.timedelta(days=dias)
        periodo = self.calcular_periodo_vacacional()

        registro = {
            "Periodo Vacacional": periodo,
            "Fecha Inicio": fecha_inicio,
            "Fecha Fin": fecha_fin,
            "N° Días": dias,
            "Documento": documento,
            "MAD": mad,
            "Observaciones": observaciones
        }
        self.vacaciones.append(registro)
        return registro

    def registrar_solicitud(self, descripcion):
        solicitud = {
            "Fecha": datetime.date.today(),
            "Descripcion": descripcion
        }
        self.solicitudes.append(solicitud)
        return solicitud

    def registrar_memorando(self, descripcion):
        memorando = {
            "Fecha": datetime.date.today(),
            "Descripcion": descripcion
        }
        self.memorandos.append(memorando)
        return memorando

    def mostrar_historial(self):
        return {
            "Vacaciones": self.vacaciones,
            "Solicitudes": self.solicitudes,
            "Memorandos": self.memorandos
        }


# Ejemplo de uso
trabajador1 = Trabajador("001", "12345678", "Pérez Gómez Juan", "276", "2015-03-01")

# Registrar vacaciones
vac = trabajador1.registrar_vacaciones("2026-03-15", 30, "Resolución 123", "MAD-01", "Vacaciones acumuladas")

# Registrar solicitud
sol = trabajador1.registrar_solicitud("Solicitud de vacaciones por 30 días")

# Registrar memorando
mem = trabajador1.registrar_memorando("Memorando de autorización de vacaciones")

print(trabajador1.mostrar_historial())
