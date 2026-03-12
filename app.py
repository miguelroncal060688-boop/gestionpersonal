import streamlit as st
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

# ---------------------------------------------------------
# PARCHE DE COMPATIBILIDAD PARA TRABAJADORES ANTIGUOS
# ---------------------------------------------------------
def asegurar_regimen(trab):
    """Garantiza que todos los trabajadores tengan atributo regimen."""
    if not hasattr(trab, "regimen"):
        trab.regimen = "Decreto Legislativo N° 276"
    return trab

# ---------------------------------------------------------
# CLASE TRABAJADOR
# ---------------------------------------------------------
class Trabajador:
    def __init__(self, numero, dni, nombres, regimen, fecha_ingreso):
        self.numero = numero
        self.dni = dni
        self.nombres = nombres
        self.regimen = regimen
        self.fecha_ingreso = datetime.datetime.strptime(fecha_ingreso, "%Y-%m-%d").date()
        self.vacaciones = []  # lista de dicts

    def calcular_periodos(self):
        """Genera periodos completos (solo si ya cumplió el año)."""
        periodos = []
        inicio = self.fecha_ingreso
        hoy = datetime.date.today()

        while inicio < hoy:
            fin_ciclo = inicio + relativedelta(years=1) - datetime.timedelta(days=1)
            if fin_ciclo > hoy:
                break

            goce_hasta = fin_ciclo + relativedelta(years=1)
            acum_hasta = goce_hasta + relativedelta(years=1)

            dias_tomados = sum(
                v["N° Días"] for v in self.vacaciones
                if v.get("Periodo Inicio") == inicio
            )

            periodos.append({
                "Inicio Ciclo": inicio,
                "Fin Ciclo": fin_ciclo,
                "Goce Hasta": goce_hasta,
                "Acumulable Hasta": acum_hasta,
                "Dias Tomados": dias_tomados
            })

            inicio = inicio + relativedelta(years=1)

        return periodos

    def registrar_vacaciones(self, periodo, fecha_inicio, dias, tipo, documento, mad, observaciones="", fraccionamiento=False, integro=False):
        fecha_inicio = datetime.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        dias = int(dias)
        fecha_fin = fecha_inicio + datetime.timedelta(days=dias)

        if dias < 7 and not fraccionamiento:
            return {"Error": "El mínimo sin fraccionamiento es 7 días continuos."}

        if periodo["Dias Tomados"] + dias > 30:
            return {"Error": "Este periodo ya alcanzó los 30 días permitidos."}

        registro = {
            "Periodo Inicio": periodo["Inicio Ciclo"],
            "Periodo Fin": periodo["Fin Ciclo"],
            "Fecha Inicio": fecha_inicio,
            "Fecha Fin": fecha_fin,
            "N° Días": dias,
            "Tipo": tipo,
            "Documento": documento,
            "MAD": mad,
            "Observaciones": observaciones,
            "Fraccionamiento": fraccionamiento,
            "Integro": integro
        }

        self.vacaciones.append(registro)
        return registro


# ---------------------------------------------------------
# CONFIGURACIÓN GENERAL
# ---------------------------------------------------------
st.set_page_config(page_title="Gestión de Vacaciones", layout="wide")
st.title("📊 Sistema de Gestión de Vacaciones – Multirégimen")

if "trabajadores" not in st.session_state:
    st.session_state["trabajadores"] = {}

# ---------------------------------------------------------
# MENÚ
# ---------------------------------------------------------
menu = st.sidebar.radio(
    "Menú",
    [
        "Registrar Trabajador",
        "Resoluciones",
        "Registrar Vacaciones",
        "Dashboard",
        "Reporte de Trabajadores",
        "Administrar Registros"
    ]
)

# ---------------------------------------------------------
# REGISTRAR TRABAJADOR
# ---------------------------------------------------------
if menu == "Registrar Trabajador":
    st.header("Registro de Trabajador")

    numero = st.text_input("N°")
    dni = st.text_input("DNI")
    nombres = st.text_input("Apellidos y Nombres")

    regimen = st.selectbox(
        "Régimen",
        [
            "Decreto Legislativo N° 1057",
            "Decreto Legislativo N° 276",
            "Decreto Legislativo N° 728",
            "Carrera Especial"
        ]
    )

    fecha_ingreso = st.date_input("Fecha de Ingreso")

    if st.button("Guardar Trabajador"):
        trabajador = Trabajador(
            numero,
            dni,
            nombres,
            regimen,
            fecha_ingreso.strftime("%Y-%m-%d")
        )
        st.session_state["trabajadores"][nombres] = trabajador
        st.success("Trabajador registrado correctamente.")

# ---------------------------------------------------------
# RESOLUCIONES
# ---------------------------------------------------------
elif menu == "Resoluciones":
    st.header("Registro de Resoluciones por periodo")

    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trabajador = asegurar_regimen(st.session_state["trabajadores"][nombre])

        periodos = trabajador.calcular_periodos()
        if periodos:
            periodo = st.selectbox(
                "Seleccione periodo",
                periodos,
                format_func=lambda p: f"{p['Inicio Ciclo']} - {p['Fin Ciclo']} (Tomados: {p['Dias Tomados']} días)"
            )

            fecha_inicio = st.date_input("Fecha Inicio programada")
            dias = st.number_input("Días autorizados", min_value=1, max_value=30, value=30)
            documento = st.text_input("N° de Resolución")
            mad = st.text_input("MAD")
            observaciones = st.text_area("Observaciones")
            fraccionamiento = st.checkbox("¿Fraccionamiento?")
            integro = st.checkbox("¿Gozará íntegro de 30 días?")

            if st.button("Guardar Resolución"):
                registro = trabajador.registrar_vacaciones(
                    periodo,
                    fecha_inicio.strftime("%Y-%m-%d"),
                    dias,
                    "Resolución",
                    documento,
                    mad,
                    observaciones,
                    fraccionamiento,
                    integro
                )
                st.write(registro)
        else:
            st.info("Este trabajador aún no tiene periodos completos.")
    else:
        st.warning("No hay trabajadores registrados.")

# ---------------------------------------------------------
# REGISTRAR VACACIONES (SOLICITUD / MEMORANDO)
# ---------------------------------------------------------
elif menu == "Registrar Vacaciones":
    st.header("Registrar Vacaciones (Solicitud / Memorando)")

    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trabajador = asegurar_regimen(st.session_state["trabajadores"][nombre])

        periodos = trabajador.calcular_periodos()
        if periodos:
            periodo = st.selectbox(
                "Seleccione periodo",
                periodos,
                format_func=lambda p: f"{p['Inicio Ciclo']} - {p['Fin Ciclo']} (Tomados: {p['Dias Tomados']} días)"
            )

            tipo = st.radio("Tipo", ["Solicitud", "Memorando"])
            fecha_inicio = st.date_input("Fecha Inicio")
            dias = st.number_input("Días", min_value=1, max_value=30, value=7)
            documento = st.text_input("Documento")
            mad = st.text_input("MAD")
            observaciones = st.text_area("Observaciones")
            fraccionamiento = st.checkbox("¿Fraccionamiento?")
            integro = st.checkbox("¿Integro 30 días?")

            if st.button("Guardar Registro"):
                registro = trabajador.registrar_vacaciones(
                    periodo,
                    fecha_inicio.strftime("%Y-%m-%d"),
                    dias,
                    tipo,
                    documento,
                    mad,
                    observaciones,
                    fraccionamiento,
                    integro
                )
                st.write(registro)
        else:
            st.info("Este trabajador aún no tiene periodos completos.")
    else:
        st.warning("No hay trabajadores registrados.")

# ---------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------
elif menu == "Dashboard":
    st.header("📊 Dashboard de Vacaciones")

    hoy = datetime.date.today()
    data = []
    en_curso = []
    proximos_30 = []
    vencidos = []

    for nombre, trab in st.session_state["trabajadores"].items():
        trab = asegurar_regimen(trab)

        for vac in trab.vacaciones:
            fila = {
                "Trabajador": nombre,
                "Régimen": trab.regimen,
                "Periodo": f"{vac.get('Periodo Inicio')} - {vac.get('Periodo Fin')}",
                "Tipo": vac.get("Tipo"),
                "Inicio": vac.get("Fecha Inicio"),
                "Fin": vac.get("Fecha Fin"),
                "Días": vac.get("N° Días"),
                "Documento": vac.get("Documento"),
                "MAD": vac.get("MAD"),
                "Fraccionamiento": vac.get("Fraccionamiento"),
                "Integro": vac.get("Integro")
            }
            data.append(fila)

            if vac["Fecha Inicio"] <= hoy <= vac["Fecha Fin"]:
                en_curso.append(fila)

            if hoy < vac["Fecha Inicio"] <= hoy + datetime.timedelta(days=30):
                proximos_30.append(fila)

        periodos = trab.calcular_periodos()
        no_usados = [p for p in periodos if p["Dias Tomados"] < 30]
        if len(no_usados) >= 2:
            vencidos.append({
                "Trabajador": nombre,
                "Régimen": trab.regimen,
                "Periodo": f"{no_usados[0]['Inicio Ciclo']} - {no_usados[0]['Fin Ciclo']}",
                "Estado": "Vencido por acumular 2 periodos"
            })

    st.subheader("📋 Vacaciones Registradas")
    st.dataframe(pd.DataFrame(data))

    st.subheader("🟢 Vacaciones en curso")
    st.dataframe(pd.DataFrame(en_curso))

    st.subheader("🟡 Próximos 30 días")
    st.dataframe(pd.DataFrame(proximos_30))

    st.subheader("🔴 Periodos vencidos")
    st.dataframe(pd.DataFrame(vencidos))

# ---------------------------------------------------------
# REPORTE DE TRABAJADORES
# ---------------------------------------------------------
elif menu == "Reporte de Trabajadores":
    st.header("📑 Reporte por Trabajador")

    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trab = asegurar_regimen(st.session_state["trabajadores"][nombre])

        st.subheader("Periodos")
        st.dataframe(pd.DataFrame(trab.calcular_periodos()))

        st.subheader("Vacaciones Tomadas")
        st.dataframe(pd.DataFrame(trab.vacaciones))
    else:
        st.warning("No hay trabajadores registrados.")

# ---------------------------------------------------------
# ADMINISTRAR REGISTROS
# ---------------------------------------------------------
elif menu == "Administrar Registros":
    st.header("Administrar Trabajadores y Vacaciones")

    if st.session_state["trabajadores"]:
        nombre = st.selectbox("Seleccione trabajador", list(st.session_state["trabajadores"].keys()))
        trab = asegurar_regimen(st.session_state["trabajadores"][nombre])

        st.write("### Datos del trabajador")
        st.write(f"N°: {trab.numero}")
        st.write(f"DNI: {trab.dni}")
        st.write(f"Régimen: {trab.regimen}")
        st.write(f"Fecha ingreso: {trab.fecha_ingreso}")

        if st.button("Borrar trabajador"):
            st.session_state["trabajadores"].pop(nombre)
            st.success("Trabajador eliminado.")

        st.write("### Vacaciones registradas")
        df = pd.DataFrame(trab.vacaciones)
        st.dataframe(df)

        if trab.vacaciones:
            idx = st.number_input("Índice a borrar", min_value=0, max_value=len(trab.vacaciones)-1, step=1)
            if st.button("Borrar registro de vacaciones"):
                trab.vacaciones.pop(idx)
                st.success("Registro eliminado.")
    else:
        st.warning("No hay trabajadores registrados.")
