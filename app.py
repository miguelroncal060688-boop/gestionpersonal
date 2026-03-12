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

    def calcular_periodo_vacacional(self):
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
        solicitud = {"Fecha": datetime.date.today(), "Descripcion": descripcion}
        self.solicitudes.append(solicitud)
        return solicitud

    def registrar_memorando(self, descripcion):
        memorando = {"Fecha": datetime.date.today(), "Descripcion": descripcion}
        self.memorandos.append(memorando)
        return memorando

    def mostrar_historial(self):
        return {
            "Vacaciones": self.vacaciones,
            "Solicitudes": self.solicitudes,
            "Memorandos": self.memorandos
        }

# --- Interfaz Streamlit ---
st.set_page_config(page_title="Gestión de Vacaciones", layout="wide")
st.title("📊 Dashboard de Gestión de Vacaciones - D.Leg. 276")

# Inicializar lista de trabajadores en sesión
if "trabajadores" not in st.session_state:
    st.session_state["trabajadores"] = {}

# Menú lateral
menu = st.sidebar.radio("Menú", ["Registrar Trabajador", "Registrar Vacaciones", "Solicitudes", "Memorandos", "Dashboard"])

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
            st.success("Vacaciones registradas correctamente.")
            st.write(registro)
    else:
        st.warning("Primero registre un trabajador.")

# Solicitudes
elif menu == "Solicitudes":
    st.header("Registrar Solicitud")
    if st.session_state["trabajadores"]:
        dni = st.selectbox("Seleccione DNI del trabajador", list(st.session_state["trabajadores"].keys()))
        solicitud_desc = st.text_area("Descripción de la solicitud")
        if st.button("Guardar Solicitud"):
            sol = st.session_state["trabajadores"][dni].registrar_solicitud(solicitud_desc)
            st.success("Solicitud registrada correctamente.")
            st.write(sol)
    else:
        st.warning("Primero registre un trabajador.")

# Memorandos
elif menu == "Memorandos":
    st.header("Registrar Memorando")
    if st.session_state["trabajadores"]:
        dni = st.selectbox("Seleccione DNI del trabajador", list(st.session_state["trabajadores"].keys()))
        memorando_desc = st.text_area("Descripción del memorando")
        if st.button("Guardar Memorando"):
            mem = st.session_state["trabajadores"][dni].registrar_memorando(memorando_desc)
            st.success("Memorando registrado correctamente.")
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
                    "Periodo": vac["Periodo Vacacional"],
                    "Inicio": vac["Fecha Inicio"],
                    "Fin": vac["Fecha Fin"],
                    "Días": vac["N° Días"],
                    "Documento": vac["Documento"],
                    "MAD": vac["MAD"],
                    "Observaciones": vac["Observaciones"]
                })
                # Detectar vencimientos próximos (menos de 30 días)
                if (vac["Fecha Fin"] - datetime.date.today()).days <= 30:
                    vencimientos.append({
                        "DNI": dni,
                        "Nombres": trab.nombres,
                        "Fin": vac["Fecha Fin"],
                        "Días Restantes": (vac["Fecha Fin"] - datetime.date.today()).days
                    })

        if data:
            df = pd.DataFrame(data)
            st.subheader("📋 Vacaciones Registradas")
            st.dataframe(df)

        if vencimientos:
            df_venc = pd.DataFrame(vencimientos)
            st.subheader("⚠️ Vacaciones Próximas a Vencer")
            st.dataframe(df_venc)
        else:
            st.info("No hay vacaciones próximas a vencer.")
    else:
        st.warning("No hay trabajadores registrados.")
