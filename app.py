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
        self.vacaciones = []  # lista de dicts con periodo y detalle

    def calcular_periodos(self):
        """Genera los periodos vacacionales desde la fecha de ingreso"""
        periodos = []
        inicio = self.fecha_ingreso
        hoy = datetime.date.today()
        while inicio < hoy:
            fin_ciclo = inicio + relativedelta(years=1) - datetime.timedelta(days=1)
            goce_hasta = fin_ciclo + relativedelta(years=1)
            acum_hasta = goce_hasta + relativedelta(years=1)
            dias_tomados = sum(v["N° Días"] for v in self.vacaciones if v["Periodo"]["Inicio Ciclo"] == inicio)
            periodos.append({
                "Inicio Ciclo": inicio,
                "Fin Ciclo": fin_ciclo,
                "Goce Hasta": goce_hasta,
                "Acumulable Hasta": acum_hasta,
                "Dias Tomados": dias_tomados
            })
            inicio = inicio + relativedelta(years=1)
        return periodos

    def registrar_vacaciones(self, periodo, fecha_inicio, dias, tipo, documento, mad, observaciones="", fraccionamiento=False):
        fecha_inicio = datetime.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        fecha_fin = fecha_inicio + datetime.timedelta(days=dias)

        # Validar reglas de fraccionamiento
        if dias < 7 and not fraccionamiento:
            return {"Error": "El mínimo permitido sin acuerdo de fraccionamiento es 7 días continuos."}

        # Validar acumulación de días
        if periodo["Dias Tomados"] + dias > 30:
            return {"Error": "Ya se han tomado 30 días en este periodo, no puede registrar más."}

        registro = {
            "Periodo": periodo,
            "Fecha Inicio": fecha_inicio,
            "Fecha Fin": fecha_fin,
            "N° Días": dias,
            "Tipo": tipo,  # solicitud, memorando o resolución
            "Documento": documento,
            "MAD": mad,
            "Observaciones": observaciones,
            "Fraccionamiento": fraccionamiento
        }
        self.vacaciones.append(registro)
        return registro

# --- Interfaz Streamlit ---
st.set_page_config(page_title="Gestión de Vacaciones", layout="wide")
st.title("📊 Sistema de Gestión de Vacaciones - D.Leg. 276")

if "trabajadores" not in st.session_state:
    st.session_state["trabajadores"] = {}

# Menú lateral
menu = st.sidebar.radio("Menú", [
    "Registrar Trabajador",
    "Registrar Vacaciones",
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
    st.header("Registrar Vacaciones / Solicitud / Memorando / Resolución")
    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trabajador = st.session_state["trabajadores"][nombre]
        periodos = trabajador.calcular_periodos()
        periodo = st.selectbox("Seleccione periodo", periodos,
                               format_func=lambda p: f"{p['Inicio Ciclo']} - {p['Fin Ciclo']} (Tomados: {p['Dias Tomados']} días)")
        tipo = st.radio("Tipo de registro", ["Solicitud", "Memorando", "Resolución"])
        fecha_inicio = st.date_input("Fecha Inicio de Vacaciones")
        dias = st.number_input("N° de Días", min_value=1, max_value=30, value=7)
        documento = st.text_input("Documento")
        mad = st.text_input("MAD")
        observaciones = st.text_area("Observaciones")
        fraccionamiento = st.checkbox("¿Hay acuerdo de fraccionamiento?")
        integro = st.checkbox("¿Gozará íntegro de 30 días?")

        if st.button("Guardar Registro"):
            registro = trabajador.registrar_vacaciones(periodo, fecha_inicio.strftime("%Y-%m-%d"), dias, tipo, documento, mad, observaciones, fraccionamiento)
            st.write(registro)
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
                data.append({
                    "Trabajador": nombre,
                    "Periodo": f"{vac['Periodo']['Inicio Ciclo']} - {vac['Periodo']['Fin Ciclo']}",
                    "Tipo": vac["Tipo"],
                    "Inicio": vac["Fecha Inicio"],
                    "Fin": vac["Fecha Fin"],
                    "Días": vac["N° Días"],
                    "Documento": vac["Documento"],
                    "MAD": vac["MAD"],
                    "Observaciones": vac["Observaciones"],
                    "Fraccionamiento": vac["Fraccionamiento"]
                })
                if (vac["Fecha Fin"] - datetime.date.today()).days <= 30:
                    vencimientos.append({
                        "Trabajador": nombre,
                        "Periodo": f"{vac['Periodo']['Inicio Ciclo']} - {vac['Periodo']['Fin Ciclo']}",
                        "Fin": vac["Fecha Fin"],
                        "Días Restantes": (vac["Fecha Fin"] - datetime.date.today()).days
                    })

        if data:
            st.subheader("📋 Vacaciones Registradas")
            st.dataframe(pd.DataFrame(data))

        if vencimientos:
            st.subheader("⚠️ Vacaciones Próximas a Vencer")
            st.dataframe(pd.DataFrame(vencimientos))
        else:
            st.info("No hay vacaciones próximas a vencer.")
    else:
        st.warning("No hay trabajadores registrados.")

# Reporte de todos los trabajadores
elif menu == "Reporte de Trabajadores":
    st.header("📑 Reporte General de Trabajadores")
    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trabajador = st.session_state["trabajadores"][nombre]
        periodos = trabajador.calcular_periodos()
        reporte = []
        for p in periodos:
            reporte.append({
                "Inicio Ciclo": p["Inicio Ciclo"],
                "Fin Ciclo": p["Fin Ciclo"],
                "Goce Hasta": p["Goce Hasta"],
                "Acumulable Hasta": p["Acumulable Hasta"],
                "Dias Tomados": p["Dias Tomados"]
            })
        st.subheader(f"Periodos de {nombre}")
        st.dataframe(pd.DataFrame(reporte))

        if trabajador.vacaciones:
            vac_data = []
            for vac in trabajador.vacaciones:
                vac_data.append({
                    "Periodo": f"{vac['Periodo']['Inicio Ciclo']} - {vac['Periodo']['Fin Ciclo']}",
                    "Tipo": vac["Tipo"],
                    "Inicio": vac["Fecha Inicio"],
                    "Fin": vac["Fecha Fin"],
                    "Días": vac["N° Días"],
                    "Documento": vac["Documento"],
                    "MAD": vac["MAD"],
                    "Observaciones": vac["Observaciones"],
                    "Fraccionamiento": vac["Fraccionamiento"]
                })
            st.subheader("Vacaciones Tomadas")
            st.dataframe(pd.DataFrame(vac_data))
        else:
            st.info("Este trabajador no tiene vacaciones registradas.")
