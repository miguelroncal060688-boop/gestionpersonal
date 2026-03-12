# ---------------------------------------------------------
# app.py - Sistema de Gestión de Vacaciones (todo en uno)
# SQLite + CSV + Panel de Control + Resoluciones + Vacaciones
# ---------------------------------------------------------

import streamlit as st
import sqlite3
import datetime
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# CONFIGURACIÓN GENERAL
# ---------------------------------------------------------

DB_PATH = "vacaciones.db"

def get_conn():
    """
    Devuelve una conexión a la base de datos SQLite.
    check_same_thread=False permite usar la conexión en Streamlit.
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ---------------------------------------------------------
# MÓDULO DB: CREACIÓN DE TABLAS
# ---------------------------------------------------------

def init_db():
    """
    Crea las tablas si no existen.
    No borra datos existentes.
    Seguro para producción.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Tabla de trabajadores
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trabajadores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT,
        dni TEXT,
        nombres TEXT NOT NULL,
        regimen TEXT NOT NULL,
        fecha_ingreso TEXT NOT NULL
    )
    """)

    # Tabla de periodos (normalizados)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS periodos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        inicio_ciclo TEXT NOT NULL,
        fin_ciclo TEXT NOT NULL,
        goce_hasta TEXT NOT NULL,
        acumulable_hasta TEXT NOT NULL,
        UNIQUE(trabajador_id, inicio_ciclo),
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE
    )
    """)

    # Tabla de resoluciones (no descuentan días)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS resoluciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        periodo_id INTEGER NOT NULL,
        numero_resolucion TEXT NOT NULL,
        fecha_programada TEXT NOT NULL,
        dias_autorizados INTEGER NOT NULL,
        mad TEXT,
        observaciones TEXT,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE,
        FOREIGN KEY(periodo_id) REFERENCES periodos(id) ON DELETE CASCADE
    )
    """)

    # Tabla de vacaciones (sí descuentan días)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        periodo_id INTEGER NOT NULL,
        tipo TEXT NOT NULL, -- Solicitud / Memorando
        fecha_inicio TEXT NOT NULL,
        fecha_fin TEXT NOT NULL,
        dias INTEGER NOT NULL,
        documento TEXT,
        mad TEXT,
        observaciones TEXT,
        fraccionamiento INTEGER DEFAULT 0,
        integro INTEGER DEFAULT 0,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE,
        FOREIGN KEY(periodo_id) REFERENCES periodos(id) ON DELETE CASCADE
    )
    """)

    conn.commit()
    conn.close()

# ---------------------------------------------------------
# MÓDULO DB: HELPERS DE FECHAS
# ---------------------------------------------------------

def to_date(s: str) -> datetime.date:
    """Convierte 'YYYY-MM-DD' a datetime.date."""
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()

def from_date(d: datetime.date) -> str:
    """Convierte datetime.date a 'YYYY-MM-DD'."""
    return d.strftime("%Y-%m-%d")
# ---------------------------------------------------------
# MÓDULO DB: CRUD DE TRABAJADORES
# ---------------------------------------------------------

def crear_trabajador(numero, dni, nombres, regimen, fecha_ingreso):
    """
    Crea un trabajador y genera automáticamente sus periodos.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trabajadores (numero, dni, nombres, regimen, fecha_ingreso)
        VALUES (?, ?, ?, ?, ?)
    """, (numero, dni, nombres, regimen, fecha_ingreso))
    trabajador_id = cur.lastrowid
    conn.commit()
    conn.close()

    generar_periodos_para_trabajador(trabajador_id, fecha_ingreso)
    return trabajador_id


def listar_trabajadores():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trabajadores ORDER BY nombres")
    rows = cur.fetchall()
    conn.close()
    return rows


def obtener_trabajador(trabajador_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trabajadores WHERE id = ?", (trabajador_id,))
    row = cur.fetchone()
    conn.close()
    return row


def actualizar_trabajador(trabajador_id, numero, dni, nombres, regimen, fecha_ingreso):
    """
    Actualiza datos del trabajador y regenera periodos si cambia la fecha de ingreso.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE trabajadores
        SET numero = ?, dni = ?, nombres = ?, regimen = ?, fecha_ingreso = ?
        WHERE id = ?
    """, (numero, dni, nombres, regimen, fecha_ingreso, trabajador_id))
    conn.commit()
    conn.close()

    generar_periodos_para_trabajador(trabajador_id, fecha_ingreso)


def borrar_trabajador(trabajador_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM trabajadores WHERE id = ?", (trabajador_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------
# MÓDULO DB: CRUD DE PERIODOS
# ---------------------------------------------------------

def generar_periodos_para_trabajador(trabajador_id, fecha_ingreso_str):
    """
    Genera periodos anuales desde la fecha de ingreso hasta hoy.
    No borra periodos existentes, solo agrega los faltantes.
    """
    conn = get_conn()
    cur = conn.cursor()

    fecha_ingreso = to_date(fecha_ingreso_str)
    hoy = datetime.date.today()
    inicio = fecha_ingreso

    while inicio < hoy:
        fin_ciclo = inicio + datetime.timedelta(days=365) - datetime.timedelta(days=1)
        if fin_ciclo > hoy:
            break

        goce_hasta = fin_ciclo.replace(year=fin_ciclo.year + 1)
        acumulable_hasta = goce_hasta.replace(year=goce_hasta.year + 1)

        cur.execute("""
            INSERT OR IGNORE INTO periodos (trabajador_id, inicio_ciclo, fin_ciclo, goce_hasta, acumulable_hasta)
            VALUES (?, ?, ?, ?, ?)
        """, (
            trabajador_id,
            from_date(inicio),
            from_date(fin_ciclo),
            from_date(goce_hasta),
            from_date(acumulable_hasta)
        ))

        inicio = inicio.replace(year=inicio.year + 1)

    conn.commit()
    conn.close()


def listar_periodos_con_dias(trabajador_id):
    """
    Lista periodos con el total de días tomados (solo vacaciones reales).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            p.id,
            p.trabajador_id,
            p.inicio_ciclo,
            p.fin_ciclo,
            p.goce_hasta,
            p.acumulable_hasta,
            IFNULL(SUM(v.dias), 0) AS dias_tomados
        FROM periodos p
        LEFT JOIN vacaciones v ON v.periodo_id = p.id
        WHERE p.trabajador_id = ?
        GROUP BY p.id
        ORDER BY p.inicio_ciclo
    """, (trabajador_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def listar_periodos_no_usados(trabajador_id):
    """
    Devuelve periodos con menos de 30 días tomados.
    """
    periodos = listar_periodos_con_dias(trabajador_id)
    return [p for p in periodos if p["dias_tomados"] < 30]


# ---------------------------------------------------------
# MÓDULO DB: CRUD DE RESOLUCIONES (NO descuentan días)
# ---------------------------------------------------------

def crear_resolucion(trabajador_id, periodo_id, numero_resolucion, fecha_programada, dias_autorizados, mad, observaciones):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO resoluciones (trabajador_id, periodo_id, numero_resolucion, fecha_programada, dias_autorizados, mad, observaciones)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (trabajador_id, periodo_id, numero_resolucion, fecha_programada, dias_autorizados, mad, observaciones))
    conn.commit()
    conn.close()


def listar_resoluciones_por_trabajador(trabajador_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.*, p.inicio_ciclo, p.fin_ciclo
        FROM resoluciones r
        JOIN periodos p ON p.id = r.periodo_id
        WHERE r.trabajador_id = ?
        ORDER BY r.fecha_programada
    """, (trabajador_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def borrar_resolucion(resolucion_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM resoluciones WHERE id = ?", (resolucion_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------
# MÓDULO DB: CRUD DE VACACIONES (SÍ descuentan días)
# ---------------------------------------------------------

def crear_vacacion(trabajador_id, periodo_id, tipo, fecha_inicio, fecha_fin, dias, documento, mad, observaciones, fraccionamiento, integro):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO vacaciones (trabajador_id, periodo_id, tipo, fecha_inicio, fecha_fin, dias, documento, mad, observaciones, fraccionamiento, integro)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        trabajador_id,
        periodo_id,
        tipo,
        fecha_inicio,
        fecha_fin,
        dias,
        documento,
        mad,
        observaciones,
        1 if fraccionamiento else 0,
        1 if integro else 0
    ))
    conn.commit()
    conn.close()


def listar_vacaciones_por_trabajador(trabajador_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT v.*, p.inicio_ciclo, p.fin_ciclo
        FROM vacaciones v
        JOIN periodos p ON p.id = v.periodo_id
        WHERE v.trabajador_id = ?
        ORDER BY v.fecha_inicio
    """, (trabajador_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def borrar_vacacion(vacacion_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM vacaciones WHERE id = ?", (vacacion_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------
# MÓDULO DB: CONSULTAS PARA DASHBOARD
# ---------------------------------------------------------

def listar_todas_vacaciones_con_trabajador():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            v.*,
            t.nombres AS trabajador,
            t.regimen,
            p.inicio_ciclo,
            p.fin_ciclo
        FROM vacaciones v
        JOIN trabajadores t ON t.id = v.trabajador_id
        JOIN periodos p ON p.id = v.periodo_id
        ORDER BY v.fecha_inicio
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def listar_todos_trabajadores_con_periodos_no_usados():
    trabajadores = listar_trabajadores()
    resultado = []
    for t in trabajadores:
        no_usados = listar_periodos_no_usados(t["id"])
        if len(no_usados) >= 2:
            resultado.append((t, no_usados))
    return resultado


# ---------------------------------------------------------
# MÓDULO DB: EXPORTACIÓN CSV Y RESET TOTAL
# ---------------------------------------------------------

def exportar_tabla_csv(nombre_tabla: str) -> bytes:
    conn = get_conn()
    df = pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)
    conn.close()
    return df.to_csv(index=False).encode("utf-8")


def resetear_todo():
    """
    Borra TODAS las tablas y las recrea.
    Requiere confirmación desde la UI.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS vacaciones")
    cur.execute("DROP TABLE IF EXISTS resoluciones")
    cur.execute("DROP TABLE IF EXISTS periodos")
    cur.execute("DROP TABLE IF EXISTS trabajadores")
    conn.commit()
    conn.close()
    init_db()


def db_file_bytes():
    """
    Devuelve el archivo SQLite como bytes para descarga.
    """
    p = Path(DB_PATH)
    if p.exists():
        return p.read_bytes()
    return b""
# ---------------------------------------------------------
# MÓDULO LÓGICA: FUNCIONES DE NEGOCIO
# ---------------------------------------------------------

def calcular_fecha_fin(fecha_inicio: datetime.date, dias: int) -> datetime.date:
    """
    Calcula la fecha fin sumando 'dias' a la fecha de inicio.
    """
    return fecha_inicio + datetime.timedelta(days=dias)


def validar_fraccionamiento(dias: int, fraccionamiento: bool) -> bool:
    """
    Regla: sin fraccionamiento, el mínimo permitido es 7 días.
    """
    if not fraccionamiento and dias < 7:
        return False
    return True


def periodo_tiene_espacio(periodo, dias_nuevos: int) -> bool:
    """
    Verifica si un periodo tiene espacio para registrar más días.
    """
    return periodo["dias_tomados"] + dias_nuevos <= 30


# ---------------------------------------------------------
# INICIO DE LA INTERFAZ STREAMLIT
# ---------------------------------------------------------

st.set_page_config(page_title="Gestión de Vacaciones", layout="wide")
st.title("📊 Sistema de Gestión de Vacaciones – SQLite + CSV (Todo en uno)")

# Inicializar BD si no existe
init_db()

# ---------------------------------------------------------
# MENÚ LATERAL
# ---------------------------------------------------------

menu = st.sidebar.radio(
    "Menú",
    [
        "Registrar Trabajador",
        "Resoluciones",
        "Registrar Vacaciones",
        "Dashboard",
        "Reporte de Trabajadores",
        "Administrar Registros",
        "Panel de Control"
    ]
)

# ---------------------------------------------------------
# UI: REGISTRAR TRABAJADOR
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
        if not nombres.strip():
            st.error("El nombre es obligatorio.")
        else:
            crear_trabajador(
                numero,
                dni,
                nombres,
                regimen,
                fecha_ingreso.strftime("%Y-%m-%d")
            )
            st.success("Trabajador registrado correctamente y periodos generados.")

    st.subheader("Trabajadores registrados")
    trabajadores = listar_trabajadores()
    if trabajadores:
        st.dataframe(pd.DataFrame(trabajadores))
    else:
        st.info("No hay trabajadores registrados.")


# ---------------------------------------------------------
# UI: RESOLUCIONES (NO descuentan días)
# ---------------------------------------------------------

elif menu == "Resoluciones":
    st.header("Registro de Resoluciones (NO descuentan días)")

    trabajadores = listar_trabajadores()
    if not trabajadores:
        st.warning("Primero registre trabajadores.")
    else:
        mapa = {t["nombres"]: t["id"] for t in trabajadores}
        nombre_sel = st.selectbox("Trabajador", list(mapa.keys()))
        trabajador_id = mapa[nombre_sel]

        periodos = listar_periodos_con_dias(trabajador_id)
        if not periodos:
            st.info("Este trabajador aún no tiene periodos completos.")
        else:
            opciones = {
                f"{p['inicio_ciclo']} - {p['fin_ciclo']} (Tomados: {p['dias_tomados']} días)": p["id"]
                for p in periodos
            }
            etiqueta_periodo = st.selectbox("Periodo", list(opciones.keys()))
            periodo_id = opciones[etiqueta_periodo]

            numero_resolucion = st.text_input("N° de Resolución")
            fecha_programada = st.date_input("Fecha programada")
            dias_autorizados = st.number_input("Días autorizados", min_value=1, max_value=30, value=30)
            mad = st.text_input("MAD / Referencia")
            observaciones = st.text_area("Observaciones")

            if st.button("Guardar Resolución"):
                crear_resolucion(
                    trabajador_id,
                    periodo_id,
                    numero_resolucion,
                    fecha_programada.strftime("%Y-%m-%d"),
                    int(dias_autorizados),
                    mad,
                    observaciones
                )
                st.success("Resolución registrada correctamente (NO descuenta días).")

        st.subheader("Resoluciones del trabajador")
        resoluciones = listar_resoluciones_por_trabajador(trabajador_id)
        if resoluciones:
            st.dataframe(pd.DataFrame(resoluciones))
        else:
            st.info("No hay resoluciones registradas.")
# ---------------------------------------------------------
# UI: REGISTRAR VACACIONES (SÍ descuentan días)
# ---------------------------------------------------------

elif menu == "Registrar Vacaciones":
    st.header("Registrar Vacaciones (Solicitud / Memorando)")

    trabajadores = listar_trabajadores()
    if not trabajadores:
        st.warning("Primero registre trabajadores.")
    else:
        # Selección de trabajador
        mapa = {t["nombres"]: t["id"] for t in trabajadores}
        nombre_sel = st.selectbox("Trabajador", list(mapa.keys()))
        trabajador_id = mapa[nombre_sel]

        # Periodos del trabajador
        periodos = listar_periodos_con_dias(trabajador_id)
        if not periodos:
            st.info("Este trabajador aún no tiene periodos completos.")
        else:
            opciones = {
                f"{p['inicio_ciclo']} - {p['fin_ciclo']} (Tomados: {p['dias_tomados']} días)": p
                for p in periodos
            }
            etiqueta_periodo = st.selectbox("Periodo", list(opciones.keys()))
            periodo = opciones[etiqueta_periodo]
            periodo_id = periodo["id"]

            # Datos de la vacación
            tipo = st.radio("Tipo de registro", ["Solicitud", "Memorando"])
            fecha_inicio = st.date_input("Fecha Inicio")
            dias = st.number_input("N° de días", min_value=1, max_value=30, value=7)
            documento = st.text_input("Documento")
            mad = st.text_input("MAD / Referencia")
            observaciones = st.text_area("Observaciones")
            fraccionamiento = st.checkbox("¿Hay acuerdo de fraccionamiento?")
            integro = st.checkbox("¿Gozará íntegro de 30 días?")

            if st.button("Guardar Vacaciones"):
                # Validación de fraccionamiento
                if not validar_fraccionamiento(int(dias), fraccionamiento):
                    st.error("Sin fraccionamiento, el mínimo es 7 días continuos.")
                # Validación de espacio en el periodo
                elif not periodo_tiene_espacio(periodo, int(dias)):
                    st.error("Este periodo ya alcanzó o superaría los 30 días.")
                else:
                    fecha_fin = calcular_fecha_fin(fecha_inicio, int(dias))
                    crear_vacacion(
                        trabajador_id,
                        periodo_id,
                        tipo,
                        fecha_inicio.strftime("%Y-%m-%d"),
                        fecha_fin.strftime("%Y-%m-%d"),
                        int(dias),
                        documento,
                        mad,
                        observaciones,
                        fraccionamiento,
                        integro
                    )
                    st.success("Vacaciones registradas correctamente.")

        # Mostrar vacaciones del trabajador
        st.subheader("Vacaciones del trabajador")
        vacaciones = listar_vacaciones_por_trabajador(trabajador_id)
        if vacaciones:
            st.dataframe(pd.DataFrame(vacaciones))
        else:
            st.info("No hay vacaciones registradas.")


# ---------------------------------------------------------
# UI: DASHBOARD
# ---------------------------------------------------------

elif menu == "Dashboard":
    st.header("📊 Dashboard General")

    hoy = datetime.date.today()
    todas = listar_todas_vacaciones_con_trabajador()

    if not todas:
        st.info("No hay vacaciones registradas.")
    else:
        df = pd.DataFrame(todas)
        df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"])
        df["fecha_fin"] = pd.to_datetime(df["fecha_fin"])

        # ----------------------------------------------
        # Vacaciones registradas
        # ----------------------------------------------
        st.subheader("📋 Vacaciones registradas")
        st.dataframe(df[[
            "trabajador", "regimen", "inicio_ciclo", "fin_ciclo",
            "tipo", "fecha_inicio", "fecha_fin", "dias", "documento", "mad"
        ]])

        # ----------------------------------------------
        # Vacaciones en curso
        # ----------------------------------------------
        en_curso = df[
            (df["fecha_inicio"].dt.date <= hoy) &
            (df["fecha_fin"].dt.date >= hoy)
        ]

        st.subheader("🟢 Vacaciones en curso (hoy)")
        if not en_curso.empty:
            st.dataframe(en_curso)
        else:
            st.info("No hay vacaciones en curso hoy.")

        # ----------------------------------------------
        # Próximos 30 días
        # ----------------------------------------------
        proximos_30 = df[
            (df["fecha_inicio"].dt.date > hoy) &
            (df["fecha_inicio"].dt.date <= hoy + datetime.timedelta(days=30))
        ]

        st.subheader("🟡 Vacaciones que inician en los próximos 30 días")
        if not proximos_30.empty:
            st.dataframe(proximos_30)
        else:
            st.info("No hay vacaciones próximas a iniciar en 30 días.")

        # ----------------------------------------------
        # Periodos vencidos (2 periodos sin usar)
        # ----------------------------------------------
        st.subheader("🔴 Periodos vencidos (2 periodos completos sin usar)")

        vencidos = listar_todos_trabajadores_con_periodos_no_usados()
        if vencidos:
            filas = []
            for t, periodos_no_usados in vencidos:
                p0 = periodos_no_usados[0]
                filas.append({
                    "Trabajador": t["nombres"],
                    "Régimen": t["regimen"],
                    "Periodo más antiguo": f"{p0['inicio_ciclo']} - {p0['fin_ciclo']}",
                    "N° periodos no usados": len(periodos_no_usados)
                })
            st.dataframe(pd.DataFrame(filas))
        else:
            st.info("No hay trabajadores con 2 periodos completos sin usar.")
# ---------------------------------------------------------
# UI: REPORTE POR TRABAJADOR
# ---------------------------------------------------------

elif menu == "Reporte de Trabajadores":
    st.header("📑 Reporte por Trabajador")

    trabajadores = listar_trabajadores()
    if not trabajadores:
        st.warning("No hay trabajadores registrados.")
    else:
        # Selección de trabajador
        mapa = {t["nombres"]: t["id"] for t in trabajadores}
        nombre_sel = st.selectbox("Trabajador", list(mapa.keys()))
        trabajador_id = mapa[nombre_sel]

        # -------------------------
        # Periodos del trabajador
        # -------------------------
        st.subheader("Periodos")
        periodos = listar_periodos_con_dias(trabajador_id)
        if periodos:
            st.dataframe(pd.DataFrame(periodos))
        else:
            st.info("No hay periodos generados.")

        # -------------------------
        # Vacaciones del trabajador
        # -------------------------
        st.subheader("Vacaciones tomadas")
        vacaciones = listar_vacaciones_por_trabajador(trabajador_id)
        if vacaciones:
            st.dataframe(pd.DataFrame(vacaciones))
        else:
            st.info("No hay vacaciones registradas.")

        # -------------------------
        # Resoluciones del trabajador
        # -------------------------
        st.subheader("Resoluciones")
        resoluciones = listar_resoluciones_por_trabajador(trabajador_id)
        if resoluciones:
            st.dataframe(pd.DataFrame(resoluciones))
        else:
            st.info("No hay resoluciones registradas.")


# ---------------------------------------------------------
# UI: ADMINISTRAR REGISTROS
# ---------------------------------------------------------

elif menu == "Administrar Registros":
    st.header("Administrar Trabajadores y Registros")

    trabajadores = listar_trabajadores()
    if not trabajadores:
        st.warning("No hay trabajadores registrados.")
    else:
        # Selección de trabajador
        mapa = {t["nombres"]: t for t in trabajadores}
        nombre_sel = st.selectbox("Trabajador", list(mapa.keys()))
        t = mapa[nombre_sel]

        # -------------------------
        # EDITAR TRABAJADOR
        # -------------------------
        st.subheader("Editar trabajador")

        nuevo_numero = st.text_input("N°", value=t["numero"] or "")
        nuevo_dni = st.text_input("DNI", value=t["dni"] or "")
        nuevo_nombre = st.text_input("Nombres", value=t["nombres"])

        regimenes = [
            "Decreto Legislativo N° 1057",
            "Decreto Legislativo N° 276",
            "Decreto Legislativo N° 728",
            "Carrera Especial"
        ]
        nuevo_regimen = st.selectbox("Régimen", regimenes, index=regimenes.index(t["regimen"]))

        fecha_ingreso = datetime.datetime.strptime(t["fecha_ingreso"], "%Y-%m-%d").date()
        nueva_fecha_ingreso = st.date_input("Fecha de ingreso", value=fecha_ingreso)

        if st.button("Guardar cambios del trabajador"):
            actualizar_trabajador(
                t["id"],
                nuevo_numero,
                nuevo_dni,
                nuevo_nombre,
                nuevo_regimen,
                nueva_fecha_ingreso.strftime("%Y-%m-%d")
            )
            st.success("Trabajador actualizado. (Recarga la página para ver cambios).")

        # -------------------------
        # BORRAR TRABAJADOR
        # -------------------------
        if st.button("Borrar trabajador y todos sus registros"):
            borrar_trabajador(t["id"])
            st.success("Trabajador eliminado completamente.")
            st.stop()

        st.markdown("---")

        # -------------------------
        # BORRAR VACACIONES INDIVIDUALES
        # -------------------------
        st.subheader("Borrar vacaciones individuales")

        vacaciones = listar_vacaciones_por_trabajador(t["id"])
        if vacaciones:
            dfv = pd.DataFrame(vacaciones)
            st.dataframe(dfv)

            ids = [v["id"] for v in vacaciones]
            id_sel = st.selectbox("ID de vacación a borrar", ids)

            if st.button("Borrar vacación seleccionada"):
                borrar_vacacion(id_sel)
                st.success("Vacación borrada.")
        else:
            st.info("No hay vacaciones para este trabajador.")

        st.markdown("---")

        # -------------------------
        # BORRAR RESOLUCIONES INDIVIDUALES
        # -------------------------
        st.subheader("Borrar resoluciones individuales")

        resoluciones = listar_resoluciones_por_trabajador(t["id"])
        if resoluciones:
            dfr = pd.DataFrame(resoluciones)
            st.dataframe(dfr)

            ids_r = [r["id"] for r in resoluciones]
            idr_sel = st.selectbox("ID de resolución a borrar", ids_r)

            if st.button("Borrar resolución seleccionada"):
                borrar_resolucion(idr_sel)
                st.success("Resolución borrada.")
        else:
            st.info("No hay resoluciones para este trabajador.")
# ---------------------------------------------------------
# UI: PANEL DE CONTROL
# ---------------------------------------------------------

elif menu == "Panel de Control":
    st.header("⚙️ Panel de Control del Sistema")

    # ----------------------------------------------
    # DESCARGAR BASE DE DATOS SQLITE
    # ----------------------------------------------
    st.subheader("📥 Descargar base de datos SQLite")

    db_bytes = db_file_bytes()
    if db_bytes:
        st.download_button(
            "Descargar vacaciones.db",
            data=db_bytes,
            file_name="vacaciones.db",
            mime="application/octet-stream"
        )
    else:
        st.info("Aún no existe archivo de base de datos.")

    st.markdown("---")

    # ----------------------------------------------
    # EXPORTAR TABLAS A CSV
    # ----------------------------------------------
    st.subheader("📤 Exportar tablas a CSV")

    tablas = ["trabajadores", "periodos", "resoluciones", "vacaciones"]

    for tabla in tablas:
        st.write(f"Tabla: **{tabla}**")
        if st.button(f"Generar CSV de {tabla}"):
            csv_bytes = exportar_tabla_csv(tabla)
            st.download_button(
                f"Descargar {tabla}.csv",
                data=csv_bytes,
                file_name=f"{tabla}.csv",
                mime="text/csv"
            )

    st.markdown("---")

    # ----------------------------------------------
    # RESET TOTAL DEL SISTEMA
    # ----------------------------------------------
    st.subheader("🧨 Resetear TODO el sistema")

    st.warning(
        "Esta acción borrará **TODOS** los trabajadores, periodos, resoluciones y vacaciones.\n"
        "No se puede deshacer."
    )

    confirm = st.text_input("Escriba exactamente: QUIERO RESETEAR TODO")

    if st.button("Resetear base de datos"):
        if confirm.strip() == "QUIERO RESETEAR TODO":
            resetear_todo()
            st.success("Base de datos reseteada correctamente. Reinicie la aplicación.")
        else:
            st.error("Confirmación incorrecta. No se realizó el reset.")
