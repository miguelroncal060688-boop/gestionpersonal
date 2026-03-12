import streamlit as st
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

# Clase Trabajador
class Trabajador:
    def __init__(self, numero, dni, nombres, d_leg, fecha_ingreso):
        self.numero = numero
        self.dni = dni
        self.nombres = nombres
        self.d_leg = d_leg
        self.fecha_ingreso = datetime.datetime.strptime(fecha_ingreso, "%Y-%m-%d").date()
        self.vacaciones = []  # cada registro incluye periodo
        self.solicitudes = []
        self.memorandos = []

    def calcular_periodos(self):
        """Genera los periodos vacacionales desde la fecha de ingreso"""
        periodos = []
        inicio = self.fecha_ingreso
        hoy = datetime.date.today()
        while inicio < hoy:
            fin_ciclo = inicio + relativedelta(years=1) - datetime.timedelta(days=1)
            goce_hasta = fin_ciclo + relativedelta(years=1)
            acum_hasta = goce_hasta + relativedelta(years=1)
            periodos.append({
                "Inicio Ciclo": inicio,
                "Fin Ciclo": fin_ciclo,
                "Goce Hasta": goce_hasta,
                "Acumulable Hasta": acum_hasta,
                "Dias Tomados": 0
            })
            inicio = inicio + relativedelta(years=1)
        return periodos

    def registrar_vacaciones(self, periodo_idx, fecha_inicio, dias, documento, mad, observaciones="", fraccionamiento=False):
        fecha_inicio = datetime.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        fecha_fin = fecha_inicio + datetime.timedelta(days=dias)

        # Validar reglas de fraccionamiento
        if dias < 7 and not fraccionamiento:
            return {"Error": "El mínimo permitido sin acuerdo de fraccionamiento es 7 días continuos."}

        periodos = self.calcular_periodos()
        if periodo_idx >= len(periodos):
            return {"Error": "Periodo inválido."}
        periodo = periodos[periodo_idx]

        # Validar acumulación de días
        dias_tomados = sum(v["N° Días"] for v in self.vacaciones if v["Periodo"] == periodo_idx)
        if dias_tomados + dias > 30:
            return {"Error": "Ya se han tomado 30 días en este periodo, no puede registrar más."}

        registro = {
            "Periodo": periodo_idx,
            "Fecha Inicio": fecha_inicio,
            "Fecha Fin": fecha_fin,
            "N° Días": dias,
            "Documento": documento,
            "MAD": mad,
            "Observaciones": observaciones,
            "Fraccionamiento": fraccionamiento
        }
        self.vacaciones.append(registro)
        return registro

    def registrar_solicitud(self, descripcion, dias, periodo_idx, fraccionamiento=False):
        if dias < 7 and not fraccionamiento:
            return {"Error": "La solicitud debe ser mínimo de 7 días continuos o tener acuerdo de fraccionamiento."}
        solicitud = {"Fecha": datetime.date.today(), "Descripcion": descripcion, "Días": dias, "Periodo": periodo_idx, "Fraccionamiento": fraccionamiento}
        self.solicitudes.append(solicitud)
        return solicitud

    def registrar_memorando(self, descripcion, dias, periodo_idx, fraccionamiento=False):
        if dias < 7 and not fraccionamiento:
            return {"Error": "El memorando debe respetar mínimo 7 días continuos o tener acuerdo de fraccionamiento."}
        memorando = {"Fecha": datetime.date.today(), "Descripcion": descripcion, "Días": dias, "Periodo": periodo_idx, "Fraccionamiento": fraccionamiento}
        self.memorandos.append(memorando)
        return memorando

    def mostrar_historial(self):
        return {
            "Vacaciones": self.vacaciones,
            "Solicitudes": self.solicitudes,
            "Memorandos": self.memorandos,
            "Periodos": self.calcular_periodos()
        }

# --- Interfaz Streamlit ---
st.set_page_config(page_title="Gestión de Vacaciones", layout="wide")
st.title("📊 Dashboard de Gestión de Vacaciones - D.Leg. 276")

if "trabajadores" not in st.session_state:
    st.session_state["trabajadores"] = {}

# Menú lateral
menu = st.sidebar.radio("Menú", [
    "Registrar Trabajador",
    "Registrar Vacaciones",
    "Solicitudes",
    "Memorandos",
    "Dashboard",
    "Reporte de Trabajadores"
])

# Registrar trabajador
if menu == "Registrar Trabajador":
    st.header("Registro de Trabajador")
    numero = st.text_input("N°")
    dni = st.text_input("DNI")
    nombres = st.text_input("Apellidos y Nombres")
    d_leg = st.text_input("D.Leg.")
    fecha_ingreso = st.date_input("Fecha de Ingreso")

    if st.button("Guardar Trabajador"):
        trabajador = Trabajador(numero, dni, nombres, d_leg, fecha_ingreso.strftime("%Y-%m-%d"))
        st.session_state["trabajadores"][nombres] = trabajador
        st.success(f"Trabajador {nombres} registrado correctamente.")

# Registrar vacaciones
elif menu == "Registrar Vacaciones":
    st.header("Registrar Vacaciones")
    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trabajador = st.session_state["trabajadores"][nombre]
        periodos = trabajador.calcular_periodos()
        periodo_idx = st.selectbox("Seleccione periodo", range(len(periodos)), format_func=lambda i: f"{periodos[i]['Inicio Ciclo']} - {periodos[i]['Fin Ciclo']}")
        fecha_inicio = st.date_input("Fecha Inicio de Vacaciones")
        dias = st.number_input("N° de Días", min_value=1, max_value=30, value=7)
        documento = st.text_input("Documento")
        mad = st.text_input("MAD")
        observaciones = st.text_area("Observaciones")
        fraccionamiento = st.checkbox("¿Hay acuerdo de fraccionamiento?")

        if st.button("Guardar Vacaciones"):
            registro = trabajador.registrar_vacaciones(periodo_idx, fecha_inicio.strftime("%Y-%m-%d"), dias, documento, mad, observaciones, fraccionamiento)
            st.write(registro)
    else:
        st.warning("Primero registre un trabajador.")

# Solicitudes
elif menu == "Solicitudes":
    st.header("Registrar Solicitud")
    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trabajador = st.session_state["trabajadores"][nombre]
        periodos = trabajador.calcular_periodos()
        periodo_idx = st.selectbox("Seleccione periodo", range(len(periodos)), format_func=lambda i: f"{periodos[i]['Inicio Ciclo']} - {periodos[i]['Fin Ciclo']}")
        solicitud_desc = st.text_area("Descripción de la solicitud")
        dias = st.number_input("N° de Días solicitados", min_value=1, max_value=30, value=7)
        fraccionamiento = st.checkbox("¿Hay acuerdo de fraccionamiento?")
        if st.button("Guardar Solicitud"):
            sol = trabajador.registrar_solicitud(solicitud_desc, dias, periodo_idx, fraccionamiento)
            st.write(sol)
    else:
        st.warning("Primero registre un trabajador.")

# Memorandos
elif menu == "Memorandos":
    st.header("Registrar Memorando")
    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trabajador = st.session_state["trabajadores"][nombre]
        periodos = trabajador.calcular_periodos()
        periodo_idx = st.selectbox("Seleccione periodo", range(len(periodos)), format_func=lambda i: f"{periodos[i]['Inicio Ciclo']} - {periodos[i]['Fin Ciclo']}")
        memorando_desc = st.text_area("Descripción del memorando")
        dias = st.number_input("N° de Días aprobados", min_value=1, max_value=30, value=30)
        fraccionamiento = st.checkbox("¿Hay acuerdo de fraccionamiento?")
        if st.button("Guardar Memorando"):
            mem = trabajador.registrar_memorando(memorando_desc, dias, periodo_idx, fraccionamiento)
            st.write(mem)
    else:
        st.warning("Primero registre un trabajador.")

# Dashboard
elif menu == "Dashboard":
    st.header("📊 Reportes de Vacaciones")
    if st.session_state["trabajadores"]:
        data = []
        vencimientos = []
        for nombre, trab in st.session_state["trabajadores"].items():
            for vac in trab.vacaciones:
                periodo = trab.cal
