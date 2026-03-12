import streamlit as st
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

# -----------------------------
# Clase Trabajador
# -----------------------------
class Trabajador:
    def __init__(self, numero, dni, nombres, d_leg, fecha_ingreso):
        self.numero = numero
        self.dni = dni
        self.nombres = nombres
        self.d_leg = d_leg
        self.fecha_ingreso = datetime.datetime.strptime(fecha_ingreso, "%Y-%m-%d").date()
        # Cada registro de vacaciones tendrá SIEMPRE las mismas claves
        self.vacaciones = []  # lista de dicts

    def calcular_periodos(self):
        """
        Genera los periodos vacacionales desde la fecha de ingreso,
        solo los que ya cumplieron el año (periodos completos).
        """
        periodos = []
        inicio = self.fecha_ingreso
        hoy = datetime.date.today()

        while inicio < hoy:
            fin_ciclo = inicio + relativedelta(years=1) - datetime.timedelta(days=1)
            if fin_ciclo > hoy:
                # Este periodo aún no se completa, no se considera
                break

            goce_hasta = fin_ciclo + relativedelta(years=1)
            acum_hasta = goce_hasta + relativedelta(years=1)

            # Días tomados en este periodo (cualquier tipo: resolución, solicitud, memorando)
            dias_tomados = sum(
                v["N° Días"]
                for v in self.vacaciones
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

    def registrar_vacaciones(
        self,
        periodo,
        fecha_inicio,
        dias,
        tipo,
        documento,
        mad,
        observaciones="",
        fraccionamiento=False,
        integro=False
    ):
        """
        Registra vacaciones para un periodo ya calculado.
        tipo: 'Resolución', 'Solicitud', 'Memorando'
        """
        fecha_inicio = datetime.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        fecha_fin = fecha_inicio + datetime.timedelta(days=dias)

        # Validar reglas de fraccionamiento
        if dias < 7 and not fraccionamiento:
            return {"Error": "El mínimo permitido sin acuerdo de fraccionamiento es 7 días continuos."}

        # Validar acumulación de días en el periodo (máximo 30)
        dias_tomados_actual = periodo["Dias Tomados"]
        if dias_tomados_actual + dias > 30:
            return {"Error": "Ya se han tomado 30 días en este periodo, no puede registrar más."}

        registro = {
            "Periodo Inicio": periodo["Inicio Ciclo"],
            "Periodo Fin": periodo["Fin Ciclo"],
            "Fecha Inicio": fecha_inicio,
            "Fecha Fin": fecha_fin,
            "N° Días": dias,
            "Tipo": tipo,  # Resolución, Solicitud, Memorando
            "Documento": documento,
            "MAD": mad,
            "Observaciones": observaciones,
            "Fraccionamiento": fraccionamiento,
            "Integro": integro
        }

        self.vacaciones.append(registro)
        return registro


# -----------------------------
# Configuración de la app
# -----------------------------
st.set_page_config(page_title="Gestión de Vacaciones", layout="wide")
st.title("📊 Sistema de Gestión de Vacaciones - D.Leg. 276")

if "trabajadores" not in st.session_state:
    st.session_state["trabajadores"] = {}

# -----------------------------
# Menú lateral
# -----------------------------
menu = st.sidebar.radio(
    "Menú",
    [
        "Registrar Trabajador",
        "Registrar Vacaciones",
        "Dashboard",
        "Reporte de Trabajadores"
    ]
)

# -----------------------------
# Registrar Trabajador
# -----------------------------
if menu == "Registrar Trabajador":
    st.header("Registro de Trabajador")
    numero = st.text_input("N°")
    dni = st.text_input("DNI")
    nombres = st.text_input("Apellidos y Nombres")
    d_leg = st.text_input("D.Leg.")
    fecha_ingreso = st.date_input("Fecha de Ingreso")

    if st.button("Guardar Trabajador"):
        if nombres.strip() == "":
            st.error("El nombre del trabajador es obligatorio.")
        else:
            trabajador = Trabajador(
                numero,
                dni,
                nombres,
                d_leg,
                fecha_ingreso.strftime("%Y-%m-%d")
            )
            # Clave: nombre del trabajador
            st.session_state["trabajadores"][nombres] = trabajador
            st.success(f"Trabajador {nombres} registrado correctamente.")

# -----------------------------
# Registrar Vacaciones
# -----------------------------
elif menu == "Registrar Vacaciones":
    st.header("Registrar Vacaciones / Solicitud / Memorando / Resolución")

    if st.session_state["trabajadores"]:
        nombre = st.selectbox(
            "Seleccione trabajador",
            list(st.session_state["trabajadores"].keys())
        )
        trabajador = st.session_state["trabajadores"][nombre]

        periodos = trabajador.calcular_periodos()
        if periodos:
            periodo = st.selectbox(
                "Seleccione periodo",
                periodos,
                format_func=lambda p: f"{p['Inicio Ciclo']} - {p['Fin Ciclo']} (Tomados: {p['Dias Tomados']} días)"
            )

            tipo = st.radio("Tipo de registro", ["Resolución", "Solicitud", "Memorando"])
            fecha_inicio = st.date_input("Fecha Inicio de Vacaciones")
            dias = st.number_input("N° de Días", min_value=1, max_value=30, value=7)
            documento = st.text_input("Documento")
            mad = st.text_input("MAD")
            observaciones = st.text_area("Observaciones")
            fraccionamiento = st.checkbox("¿Hay acuerdo de fraccionamiento?")
            integro = st.checkbox("¿Gozará íntegro de 30 días?")

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
            st.info("Este trabajador aún no tiene periodos completos generados (no ha cumplido un año).")
    else:
        st.warning("Primero registre un trabajador.")

# -----------------------------
# Dashboard
# -----------------------------
elif menu == "Dashboard":
    st.header("📊 Dashboard de Vacaciones")

    if st.session_state["trabajadores"]:
        hoy = datetime.date.today()
        en_curso = []
        proximos_30 = []
        data = []
        vencimientos = []

        for nombre, trab in st.session_state["trabajadores"].items():
            # Vacaciones registradas
            for vac in trab.vacaciones:
                periodo_str = f"{vac.get('Periodo Inicio')} - {vac.get('Periodo Fin')}"
                fila = {
                    "Trabajador": nombre,
                    "Periodo": periodo_str,
                    "Tipo": vac.get("Tipo"),
                    "Inicio": vac.get("Fecha Inicio"),
                    "Fin": vac.get("Fecha Fin"),
                    "Días": vac.get("N° Días"),
                    "Documento": vac.get("Documento"),
                    "MAD": vac.get("MAD"),
                    "Observaciones": vac.get("Observaciones"),
                    "Fraccionamiento": vac.get("Fraccionamiento"),
                    "Integro": vac.get("Integro")
                }
                data.append(fila)

                # Vacaciones en curso (hoy entre inicio y fin)
                if vac.get("Fecha Inicio") <= hoy <= vac.get("Fecha Fin"):
                    en_curso.append(fila)

                # Vacaciones que inician en los próximos 30 días
                if hoy < vac.get("Fecha Inicio") <= hoy + datetime.timedelta(days=30):
                    proximos_30.append(fila)

            # Vencimientos: no acumular más de 2 periodos completos sin usar
            periodos = trab.calcular_periodos()
            periodos_no_usados = [p for p in periodos if p["Dias Tomados"] < 30]
            if len(periodos_no_usados) >= 2:
                vencimientos.append({
                    "Trabajador": nombre,
                    "Periodo": f"{periodos_no_usados[0]['Inicio Ciclo']} - {periodos_no_usados[0]['Fin Ciclo']}",
                    "Estado": "Vencido por acumulación de 2 periodos completos sin uso"
                })

        # Tabla general
        if data:
            st.subheader("📋 Vacaciones Registradas")
            st.dataframe(pd.DataFrame(data))
        else:
            st.info("No hay vacaciones registradas aún.")

        # Vacaciones en curso
        st.subheader("🟢 Vacaciones en curso (hoy)")
        if en_curso:
            st.dataframe(pd.DataFrame(en_curso))
        else:
            st.info("No hay vacaciones en curso hoy.")

        # Vacaciones que inician en los próximos 30 días
        st.subheader("🟡 Vacaciones que inician en los próximos 30 días")
        if proximos_30:
            st.dataframe(pd.DataFrame(proximos_30))
        else:
            st.info("No hay vacaciones programadas para iniciar en los próximos 30 días.")

        # Vacaciones vencidas por acumulación de 2 periodos
        st.subheader("🔴 Vacaciones vencidas por acumulación de 2 periodos")
        if vencimientos:
            st.dataframe(pd.DataFrame(vencimientos))
        else:
            st.info("No hay periodos vencidos por acumulación.")
    else:
        st.warning("No hay trabajadores registrados.")

# -----------------------------
# Reporte de Trabajadores
# -----------------------------
elif menu == "Reporte de Trabajadores":
    st.header("📑 Reporte General de Trabajadores")

    if st.session_state["trabajadores"]:
        nombre = st.selectbox(
            "Seleccione trabajador",
            list(st.session_state["trabajadores"].keys())
        )
        trabajador = st.session_state["trabajadores"][nombre]

        # Periodos
        periodos = trabajador.calcular_periodos()
        reporte_periodos = []
        for p in periodos:
            reporte_periodos.append({
                "Inicio Ciclo": p["Inicio Ciclo"],
                "Fin Ciclo": p["Fin Ciclo"],
                "Goce Hasta": p["Goce Hasta"],
                "Acumulable Hasta": p["Acumulable Hasta"],
                "Dias Tomados": p["Dias Tomados"]
            })

        st.subheader(f"Periodos de {nombre}")
        if reporte_periodos:
            st.dataframe(pd.DataFrame(reporte_periodos))
        else:
            st.info("Este trabajador aún no tiene periodos completos generados.")

        # Vacaciones tomadas
        if trabajador.vacaciones:
            vac_data = []
            for vac in trabajador.vacaciones:
                vac_data.append({
                    "Periodo": f"{vac.get('Periodo Inicio')} - {vac.get('Periodo Fin')}",
                    "Tipo": vac.get("Tipo"),
                    "Inicio": vac.get("Fecha Inicio"),
                    "Fin": vac.get("Fecha Fin"),
                    "Días": vac.get("N° Días"),
                    "Documento": vac.get("Documento"),
                    "MAD": vac.get("MAD"),
                    "Observaciones": vac.get("Observaciones"),
                    "Fraccionamiento": vac.get("Fraccionamiento"),
                    "Integro": vac.get("Integro")
                })
            st.subheader("Vacaciones Tomadas")
            st.dataframe(pd.DataFrame(vac_data))
        else:
            st.info("Este trabajador no tiene vacaciones registradas.")
    else:
        st.warning("No hay trabajadores registrados.")
