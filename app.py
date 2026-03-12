import streamlit as st
import datetime
import pandas as pd

# Clase Trabajador
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

    def calcular_periodos(self):
        """Genera los periodos vacacionales desde la fecha de ingreso"""
        periodos = []
        inicio = self.fecha_ingreso
        hoy = datetime.date.today()
        while inicio < hoy:
            fin_ciclo = inicio.replace(year=inicio.year + 1) - datetime.timedelta(days=1)
            goce_hasta = fin_ciclo.replace(year=fin_ciclo.year + 1)
            acum_hasta = goce_hasta.replace(year=goce_hasta.year + 1)
            periodos.append({
                "Inicio Ciclo": inicio,
                "Fin Ciclo": fin_ciclo,
                "Goce Hasta": goce_hasta,
                "Acumulable Hasta": acum_hasta
            })
            inicio = inicio.replace(year=inicio.year + 1)
        return periodos

    def registrar_vacaciones(self, fecha_inicio, dias, documento, mad, observaciones=""):
        fecha_inicio = datetime.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        fecha_fin = fecha_inicio + datetime.timedelta(days=dias)

        # Validar reglas de fraccionamiento
        if dias < 7:
            return {"Error": "El fraccionamiento mínimo permitido es de 7 días."}

        registro = {
            "Fecha Inicio": fecha_inicio,
            "Fecha Fin": fecha_fin,
            "N° Días": dias,
            "Documento": documento,
            "MAD": mad,
            "Observaciones": observaciones
        }
        self.vacaciones.append(registro)
        return registro

    def registrar_solicitud(self, descripcion, dias):
        if dias < 7:
            return {"Error": "La solicitud debe ser mínimo de 7 días continuos o cumplir reglas de fraccionamiento."}
        solicitud = {"Fecha": datetime.date.today(), "Descripcion": descripcion, "Días": dias}
        self.solicitudes.append(solicitud)
        return solicitud

    def registrar_memorando(self, descripcion, dias):
        if dias < 7:
            return {"Error": "El memorando debe respetar mínimo 7 días continuos."}
        memorando = {"Fecha": datetime.date.today(), "Descripcion": descripcion, "Días": dias}
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
menu = st.sidebar.radio("Menú", ["Registrar Trabajador", "Registrar Vacaciones", "Solicitudes", "Memorandos", "Dashboard", "Reporte de Trabajadores"])

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
        st.session_state["trabajadores"][dni] = trabajador
        st.success(f"Trabajador {nombres} registrado correctamente.")

# Registrar vacaciones
elif menu == "Registrar Vacaciones":
    st.header("Registrar Vacaciones")
    if st.session_state["trabajadores"]:
        dni = st.selectbox("Seleccione DNI del trabajador", list(st.session_state["trabajadores"].keys()))
        fecha_inicio = st.date_input("Fecha Inicio de Vacaciones")
        dias = st.number_input("N° de Días", min_value=1, max_value=60, value=30)
        documento = st.text_input("Documento")
        mad = st.text_input("MAD")
        observaciones = st.text_area("Observaciones")

        if st.button("Guardar Vacaciones"):
            registro = st.session_state["trabajadores"][dni].registrar_vacaciones(
                fecha_inicio.strftime("%Y-%m-%d"), dias, documento, mad, observaciones
            )
            st.write(registro)
    else:
        st.warning("Primero registre un trabajador.")

# Solicitudes
elif menu == "Solicitudes":
    st.header("Registrar Solicitud")
    if st.session_state["trabajadores"]:
        dni = st.selectbox("Seleccione DNI del trabajador", list(st.session_state["trabajadores"].keys()))
        solicitud_desc = st.text_area("Descripción de la solicitud")
        dias = st.number_input("N° de Días solicitados", min_value=1, max_value=30, value=7)
        if st.button("Guardar Solicitud"):
            sol = st.session_state["trabajadores"][dni].registrar_solicitud(solicitud_desc, dias)
            st.write(sol)
    else:
        st.warning("Primero registre un trabajador.")

# Memorandos
elif menu == "Memorandos":
    st.header("Registrar Memorando")
    if st.session_state["trabajadores"]:
        dni = st.selectbox("Seleccione DNI del trabajador", list(st.session_state["trabajadores"].keys()))
        memorando_desc = st.text_area("Descripción del memorando")
        dias = st.number_input("N° de Días aprobados", min_value=1, max_value=30, value=30)
        if st.button("Guardar Memorando"):
            mem = st.session_state["trabajadores"][dni].registrar_memorando(memorando_desc, dias)
            st.write(mem)
    else:
        st.warning("Primero registre un trabajador.")

# Dashboard
elif menu == "Dashboard":
    st.header("📊 Reportes de Vacaciones")
    if st.session_state["trabajadores"]:
        data = []
        vencimientos = []
        for dni, trab in st.session_state["trabajadores"].items():
            for vac in trab.vacaciones:
                data.append({
                    "DNI": dni,
                    "Nombres": trab.nombres,
                    "Inicio": vac["Fecha Inicio"],
                    "Fin": vac["Fecha Fin"],
                    "Días": vac["N° Días"],
                    "Documento": vac["Documento"],
                    "MAD": vac["MAD"],
                    "Observaciones": vac["Observaciones"]
                })
                if (vac["Fecha Fin"] - datetime.date.today()).days <= 30:
                    vencimientos.append({
                        "DNI": dni,
                        "Nombres": trab.nombres,
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
        reporte = []
        for dni, trab in st.session_state["trabajadores"].items():
            periodos = trab.calcular_periodos()
            for p in periodos:
                reporte.append({
                    "DNI": dni,
                    "Nombres": trab.nombres,
                    "Inicio Ciclo": p["Inicio Ciclo"],
                    "Fin Ciclo": p["Fin Ciclo"],
                    "Goce Hasta": p["Goce Hasta"],
                    "Acumulable Hasta": p["Acumulable Hasta"]
                })
        st.dataframe(pd.DataFrame(reporte))
    else:
        st.warning("No hay trabajadores registrados.")
