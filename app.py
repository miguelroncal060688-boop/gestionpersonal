# =========================================================
# =====================  BLOQUE 1  ========================
# ========== CONFIGURACIÓN, IMPORTS Y TABLAS DB ===========
# =========================================================

import streamlit as st
import sqlite3
import datetime
import pandas as pd
from pathlib import Path
import hashlib

DB_PATH = "vacaciones.db"

# ---------------------------------------------------------
# CONEXIÓN A BD
# ---------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ---------------------------------------------------------
# HELPERS GENERALES
# ---------------------------------------------------------
def to_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()

def from_date(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

# ---------------------------------------------------------
# CREACIÓN DE TABLAS (MODELO A COMPLETO)
# ---------------------------------------------------------
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # -------------------------
    # USUARIOS (LOGIN)
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        rol TEXT NOT NULL
    )
    """)

    # -------------------------
    # DIRECCIONES
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS direcciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        director_id INTEGER
    )
    """)

    # -------------------------
    # UNIDADES (depende de dirección)
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS unidades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        direccion_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        jefe_unidad_id INTEGER,
        FOREIGN KEY(direccion_id) REFERENCES direcciones(id) ON DELETE CASCADE
    )
    """)

    # -------------------------
    # ÁREAS (depende de unidad)
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS areas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unidad_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        jefe_area_id INTEGER,
        FOREIGN KEY(unidad_id) REFERENCES unidades(id) ON DELETE CASCADE
    )
    """)

    # -------------------------
    # JEFES (dirigen un área)
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jefes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombres TEXT NOT NULL,
        cargo TEXT,
        area_id INTEGER NOT NULL,
        FOREIGN KEY(area_id) REFERENCES areas(id) ON DELETE CASCADE
    )
    """)

    # -------------------------
    # TRABAJADORES (pertenecen a un área)
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trabajadores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT,
        dni TEXT,
        nombres TEXT NOT NULL,
        cargo TEXT,
        regimen TEXT NOT NULL,
        fecha_ingreso TEXT NOT NULL,
        area_id INTEGER NOT NULL,
        jefe_id INTEGER NOT NULL,
        FOREIGN KEY(area_id) REFERENCES areas(id) ON DELETE CASCADE,
        FOREIGN KEY(jefe_id) REFERENCES jefes(id) ON DELETE SET NULL
    )
    """)

    # -------------------------
    # PERIODOS
    # -------------------------
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

    # -------------------------
    # RESOLUCIONES
    # -------------------------
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

    # -------------------------
    # VACACIONES
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        periodo_id INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        fecha_inicio TEXT NOT NULL,
        fecha_fin TEXT NOT NULL,
        dias INTEGER NOT NULL,
        documento TEXT,
        mad TEXT,
        observaciones TEXT,
        fraccionamiento INTEGER DEFAULT 0,
        integro INTEGER DEFAULT 0,
        jefe_id INTEGER,
        autorizado_rrhh INTEGER DEFAULT 0,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE,
        FOREIGN KEY(periodo_id) REFERENCES periodos(id) ON DELETE CASCADE,
        FOREIGN KEY(jefe_id) REFERENCES jefes(id) ON DELETE SET NULL
    )
    """)

    conn.commit()

    # Crear usuario admin si no existe
    cur.execute("SELECT COUNT(*) AS c FROM usuarios")
    if cur.fetchone()["c"] == 0:
        cur.execute("""
            INSERT INTO usuarios (usuario, password_hash, rol)
            VALUES (?, ?, ?)
        """, ("admin", hash_password("admin"), "admin"))
        conn.commit()

    conn.close()
# =========================================================
# =====================  BLOQUE 2  ========================
# ======================   CRUD DB   ======================
# =========================================================

# ---------------------------------------------------------
# CRUD USUARIOS
# ---------------------------------------------------------
def obtener_usuario_por_nombre(usuario):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM usuarios WHERE usuario = ?", (usuario,))
    row = cur.fetchone()
    conn.close()
    return row

def crear_usuario(usuario, password, rol):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO usuarios (usuario, password_hash, rol)
        VALUES (?, ?, ?)
    """, (usuario, hash_password(password), rol))
    conn.commit()
    conn.close()

def listar_usuarios():
    conn = get_conn()
    df = pd.read_sql_query("SELECT id, usuario, rol FROM usuarios ORDER BY usuario", conn)
    conn.close()
    return df

def actualizar_rol_usuario(usuario_id, nuevo_rol):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE usuarios SET rol = ? WHERE id = ?", (nuevo_rol, usuario_id))
    conn.commit()
    conn.close()

# ---------------------------------------------------------
# CRUD DIRECCIONES
# ---------------------------------------------------------
def crear_direccion(nombre):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO direcciones (nombre) VALUES (?)", (nombre,))
    conn.commit()
    conn.close()

def listar_direcciones():
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM direcciones ORDER BY nombre", conn)
    conn.close()
    return df

# ---------------------------------------------------------
# CRUD UNIDADES
# ---------------------------------------------------------
def crear_unidad(direccion_id, nombre):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO unidades (direccion_id, nombre)
        VALUES (?, ?)
    """, (direccion_id, nombre))
    conn.commit()
    conn.close()

def listar_unidades_por_direccion(direccion_id):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT * FROM unidades
        WHERE direccion_id = ?
        ORDER BY nombre
    """, conn, params=(direccion_id,))
    conn.close()
    return df

def listar_unidades():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT u.id, u.nombre, d.nombre AS direccion
        FROM unidades u
        JOIN direcciones d ON d.id = u.direccion_id
        ORDER BY d.nombre, u.nombre
    """, conn)
    conn.close()
    return df

# ---------------------------------------------------------
# CRUD ÁREAS
# ---------------------------------------------------------
def crear_area(unidad_id, nombre):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO areas (unidad_id, nombre)
        VALUES (?, ?)
    """, (unidad_id, nombre))
    conn.commit()
    conn.close()

def listar_areas_por_unidad(unidad_id):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT * FROM areas
        WHERE unidad_id = ?
        ORDER BY nombre
    """, conn, params=(unidad_id,))
    conn.close()
    return df

def listar_areas():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT a.id, a.nombre, u.nombre AS unidad, d.nombre AS direccion
        FROM areas a
        JOIN unidades u ON u.id = a.unidad_id
        JOIN direcciones d ON d.id = u.direccion_id
        ORDER BY d.nombre, u.nombre, a.nombre
    """, conn)
    conn.close()
    return df

# ---------------------------------------------------------
# CRUD JEFES
# ---------------------------------------------------------
def crear_jefe(nombres, cargo, area_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO jefes (nombres, cargo, area_id)
        VALUES (?, ?, ?)
    """, (nombres, cargo, area_id))
    conn.commit()
    conn.close()

def listar_jefes():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT j.id, j.nombres, j.cargo,
               a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion
        FROM jefes j
        JOIN areas a ON a.id = j.area_id
        JOIN unidades u ON u.id = a.unidad_id
        JOIN direcciones d ON d.id = u.direccion_id
        ORDER BY d.nombre, u.nombre, a.nombre, j.nombres
    """, conn)
    conn.close()
    return df

# ---------------------------------------------------------
# CRUD TRABAJADORES
# ---------------------------------------------------------
def obtener_siguiente_numero_trabajador():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT IFNULL(MAX(CAST(numero AS INTEGER)), 0) + 1 AS n FROM trabajadores")
    n = cur.fetchone()["n"]
    conn.close()
    return str(n)

def crear_trabajador(numero, dni, nombres, cargo, regimen, fecha_ingreso, area_id, jefe_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trabajadores (numero, dni, nombres, cargo, regimen, fecha_ingreso, area_id, jefe_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (numero, dni, nombres, cargo, regimen, fecha_ingreso, area_id, jefe_id))
    trabajador_id = cur.lastrowid
    conn.commit()
    conn.close()
    generar_periodos_para_trabajador(trabajador_id, fecha_ingreso)
    return trabajador_id

def listar_trabajadores():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT t.id, t.numero, t.dni, t.nombres, t.cargo, t.regimen, t.fecha_ingreso,
               a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion,
               j.nombres AS jefe
        FROM trabajadores t
        JOIN areas a ON a.id = t.area_id
        JOIN unidades u ON u.id = a.unidad_id
        JOIN direcciones d ON d.id = u.direccion_id
        JOIN jefes j ON j.id = t.jefe_id
        ORDER BY t.nombres
    """, conn)
    conn.close()
    return df

# ---------------------------------------------------------
# CRUD PERIODOS
# ---------------------------------------------------------
def generar_periodos_para_trabajador(trabajador_id, fecha_ingreso_str):
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
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT p.*,
               IFNULL((SELECT SUM(v.dias) FROM vacaciones v WHERE v.periodo_id = p.id), 0) AS dias_tomados
        FROM periodos p
        WHERE p.trabajador_id = ?
        ORDER BY p.inicio_ciclo
    """, conn, params=(trabajador_id,))
    conn.close()
    return df

# ---------------------------------------------------------
# CRUD RESOLUCIONES
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
    df = pd.read_sql_query("""
        SELECT r.*, p.inicio_ciclo, p.fin_ciclo
        FROM resoluciones r
        JOIN periodos p ON p.id = r.periodo_id
        WHERE r.trabajador_id = ?
        ORDER BY r.fecha_programada
    """, conn, params=(trabajador_id,))
    conn.close()
    return df

# ---------------------------------------------------------
# CRUD VACACIONES
# ---------------------------------------------------------
def crear_vacacion(trabajador_id, periodo_id, tipo, fecha_inicio, fecha_fin, dias,
                   documento, mad, observaciones, fraccionamiento, integro,
                   jefe_id, autorizado_rrhh):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO vacaciones (
            trabajador_id, periodo_id, tipo, fecha_inicio, fecha_fin, dias,
            documento, mad, observaciones, fraccionamiento, integro,
            jefe_id, autorizado_rrhh
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        trabajador_id, periodo_id, tipo, fecha_inicio, fecha_fin, dias,
        documento, mad, observaciones,
        1 if fraccionamiento else 0,
        1 if integro else 0,
        jefe_id,
        1 if autorizado_rrhh else 0
    ))
    conn.commit()
    conn.close()

def listar_vacaciones_por_trabajador(trabajador_id):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT v.*, p.inicio_ciclo, p.fin_ciclo,
               j.nombres AS jefe_autoriza
        FROM vacaciones v
        JOIN periodos p ON p.id = v.periodo_id
        LEFT JOIN jefes j ON j.id = v.jefe_id
        WHERE v.trabajador_id = ?
        ORDER BY v.fecha_inicio
    """, conn, params=(trabajador_id,))
    conn.close()
    return df

# ---------------------------------------------------------
# EXPORTACIÓN Y RESET
# ---------------------------------------------------------
def exportar_tabla_csv(nombre_tabla):
    conn = get_conn()
    df = pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)
    conn.close()
    return df.to_csv(index=False).encode("utf-8")

def resetear_todo():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS vacaciones")
    cur.execute("DROP TABLE IF EXISTS resoluciones")
    cur.execute("DROP TABLE IF EXISTS periodos")
    cur.execute("DROP TABLE IF EXISTS trabajadores")
    cur.execute("DROP TABLE IF EXISTS jefes")
    cur.execute("DROP TABLE IF EXISTS areas")
    cur.execute("DROP TABLE IF EXISTS unidades")
    cur.execute("DROP TABLE IF EXISTS direcciones")
    cur.execute("DROP TABLE IF EXISTS usuarios")
    conn.commit()
    conn.close()
    init_db()
# =========================================================
# =====================  BLOQUE 3  ========================
# ============== LOGIN, ROLES Y MENÚ PRINCIPAL ============
# =========================================================

# Inicializar BD al arrancar
init_db()

# Estado de sesión
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
if "rol" not in st.session_state:
    st.session_state["rol"] = None

# ---------------------------------------------------------
# LOGIN
# ---------------------------------------------------------
def login():
    st.title("SISTEMA DE GESTIÓN DE VACACIONES - DRE CAJAMARCA")
    st.subheader("Inicio de sesión")

    usuario = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Ingresar"):
        row = obtener_usuario_por_nombre(usuario)
        if row and row["password_hash"] == hash_password(password):
            st.session_state["usuario"] = usuario
            st.session_state["rol"] = row["rol"]
            st.success("Acceso concedido.")
            st.experimental_rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")

def logout():
    st.session_state["usuario"] = None
    st.session_state["rol"] = None
    st.experimental_rerun()

# Si no está logueado, mostrar login y detener
if st.session_state["usuario"] is None:
    login()
    st.stop()

ROL = st.session_state["rol"]

# ---------------------------------------------------------
# MENÚ SEGÚN ROL
# ---------------------------------------------------------
MENU_ADMIN = [
    "Direcciones / Unidades / Áreas / Jefes",
    "Registrar Trabajador",
    "Reporte de Trabajadores",
    "Gestión de Usuarios",
    "Reset del Sistema"
]

MENU_RESPONSABLE = [
    "Direcciones / Unidades / Áreas / Jefes",
    "Registrar Trabajador",
    "Reporte de Trabajadores"
]

MENU_REGISTRADOR = [
    "Registrar Trabajador",
    "Reporte de Trabajadores"
]

if ROL == "admin":
    menu = st.sidebar.radio("Menú", MENU_ADMIN)
elif ROL == "responsable":
    menu = st.sidebar.radio("Menú", MENU_RESPONSABLE)
else:
    menu = st.sidebar.radio("Menú", MENU_REGISTRADOR)

st.sidebar.write(f"Usuario: {st.session_state['usuario']} ({ROL})")
if st.sidebar.button("Cerrar sesión"):
    logout()

st.title("SISTEMA DE GESTIÓN DE VACACIONES - DIRECCIÓN REGIONAL DE EDUCACIÓN CAJAMARCA")
# =========================================================
# =====================  BLOQUE 4  ========================
# = DIRECCIONES, UNIDADES, ÁREAS, JEFES Y TRABAJADORES =
# =========================================================

# ---------------------------------------------------------
# GESTIÓN DE DIRECCIONES / UNIDADES / ÁREAS / JEFES
# ---------------------------------------------------------
if menu == "Direcciones / Unidades / Áreas / Jefes":
    st.header("Direcciones, Unidades, Áreas y Jefes")

    # ----- Direcciones -----
    st.subheader("Direcciones")
    df_dir = listar_direcciones()
    st.dataframe(df_dir.rename(columns={"id": "ID", "nombre": "Dirección"}))

    nueva_dir = st.text_input("Nueva Dirección")
    if st.button("Agregar Dirección"):
        if nueva_dir.strip():
            crear_direccion(nueva_dir.strip())
            st.success("Dirección registrada.")
            st.experimental_rerun()
        else:
            st.error("Ingrese un nombre de dirección.")

    st.markdown("---")

    # ----- Unidades -----
    st.subheader("Unidades por Dirección")
    df_dir = listar_direcciones()
    if df_dir.empty:
        st.info("Primero registre al menos una Dirección.")
    else:
        mapa_dir = {row["nombre"]: row["id"] for _, row in df_dir.iterrows()}
        dir_sel = st.selectbox("Seleccione Dirección", list(mapa_dir.keys()))
        dir_id = mapa_dir[dir_sel]

        df_uni = listar_unidades_por_direccion(dir_id)
        st.dataframe(df_uni.rename(columns={"id": "ID", "nombre": "Unidad"}))

        nueva_unidad = st.text_input("Nueva Unidad")
        if st.button("Agregar Unidad"):
            if nueva_unidad.strip():
                crear_unidad(dir_id, nueva_unidad.strip())
                st.success("Unidad registrada.")
                st.experimental_rerun()
            else:
                st.error("Ingrese un nombre de unidad.")

    st.markdown("---")

    # ----- Áreas -----
    st.subheader("Áreas por Unidad")
    df_uni_all = listar_unidades()
    if df_uni_all.empty:
        st.info("Primero registre unidades.")
    else:
        mapa_uni = {
            f"{row['direccion']} - {row['nombre']}": row["id"]
            for _, row in df_uni_all.iterrows()
        }
        unidad_sel = st.selectbox("Seleccione Unidad", list(mapa_uni.keys()))
        unidad_id = mapa_uni[unidad_sel]

        df_area = listar_areas_por_unidad(unidad_id)
        st.dataframe(df_area.rename(columns={"id": "ID", "nombre": "Área"}))

        nueva_area = st.text_input("Nueva Área")
        if st.button("Agregar Área"):
            if nueva_area.strip():
                crear_area(unidad_id, nueva_area.strip())
                st.success("Área registrada.")
                st.experimental_rerun()
            else:
                st.error("Ingrese un nombre de área.")

    st.markdown("---")

    # ----- Jefes -----
    st.subheader("Jefes por Área")
    df_areas_all = listar_areas()
    if df_areas_all.empty:
        st.info("Primero registre áreas.")
    else:
        mapa_area = {
            f"{row['direccion']} - {row['unidad']} - {row['nombre']}": row["id"]
            for _, row in df_areas_all.iterrows()
        }
        area_sel = st.selectbox("Área del jefe", list(mapa_area.keys()))
        area_id = mapa_area[area_sel]

        nombre_jefe = st.text_input("Nombres del jefe")
        cargo_jefe = st.text_input("Cargo del jefe")

        if st.button("Guardar Jefe"):
            if not nombre_jefe.strip():
                st.error("Ingrese el nombre del jefe.")
            else:
                crear_jefe(nombre_jefe.strip(), cargo_jefe.strip(), area_id)
                st.success("Jefe registrado.")
                st.experimental_rerun()

        st.subheader("Listado de jefes")
        df_jefes = listar_jefes()
        if not df_jefes.empty:
            st.dataframe(df_jefes.rename(columns={
                "id": "ID",
                "nombres": "Jefe",
                "cargo": "Cargo",
                "area": "Área",
                "unidad": "Unidad",
                "direccion": "Dirección"
            }))
        else:
            st.info("No hay jefes registrados aún.")

# ---------------------------------------------------------
# REGISTRO DE TRABAJADORES
# ---------------------------------------------------------
elif menu == "Registrar Trabajador":
    st.header("Registro de Trabajador")

    numero = obtener_siguiente_numero_trabajador()
    st.write(f"N° (automático): **{numero}**")

    dni = st.text_input("DNI")
    nombres = st.text_input("Apellidos y Nombres")
    cargo = st.text_input("Cargo del trabajador")

    regimen = st.selectbox("Régimen", [
        "Decreto Legislativo N° 1057",
        "Decreto Legislativo N° 276",
        "Decreto Legislativo N° 728",
        "Carrera Especial"
    ])

    fecha_ingreso = st.date_input("Fecha de ingreso")

    # Selección de área
    df_areas_all = listar_areas()
    if df_areas_all.empty:
        st.warning("Primero registre Direcciones, Unidades y Áreas.")
        st.stop()

    mapa_area = {
        f"{row['direccion']} - {row['unidad']} - {row['nombre']}": row["id"]
        for _, row in df_areas_all.iterrows()
    }
    area_sel = st.selectbox("Área del trabajador", list(mapa_area.keys()))
    area_id = mapa_area[area_sel]

    # Selección de jefe inmediato
    df_jefes = listar_jefes()
    if df_jefes.empty:
        st.warning("Primero registre jefes.")
        st.stop()

    mapa_jefes = {
        f"{row['direccion']} - {row['unidad']} - {row['area']} - {row['nombres']}": row["id"]
        for _, row in df_jefes.iterrows()
    }
    jefe_sel = st.selectbox("Jefe inmediato", list(mapa_jefes.keys()))
    jefe_id = mapa_jefes[jefe_sel]

    if st.button("Guardar Trabajador"):
        if not nombres.strip():
            st.error("El nombre del trabajador es obligatorio.")
        else:
            crear_trabajador(
                numero,
                dni.strip(),
                nombres.strip(),
                cargo.strip(),
                regimen,
                fecha_ingreso.strftime("%Y-%m-%d"),
                area_id,
                jefe_id
            )
            st.success("Trabajador registrado y periodos generados.")

    st.subheader("Trabajadores registrados")
    df_trab = listar_trabajadores()
    if not df_trab.empty:
        st.dataframe(df_trab.rename(columns={
            "id": "ID",
            "numero": "N°",
            "dni": "DNI",
            "nombres": "Apellidos y Nombres",
            "cargo": "Cargo",
            "regimen": "Régimen",
            "fecha_ingreso": "Fecha de ingreso",
            "area": "Área",
            "unidad": "Unidad",
            "direccion": "Dirección",
            "jefe": "Jefe inmediato"
        }))
    else:
        st.info("No hay trabajadores registrados aún.")
# =========================================================
# =====================  BLOQUE 5  ========================
# ========== RESOLUCIONES Y PROGRAMACIÓN DE VACACIONES ====
# =========================================================

# ---------------------------------------------------------
# CÁLCULOS Y VALIDACIONES
# ---------------------------------------------------------

def calcular_fecha_fin(fecha_inicio: datetime.date, dias: int) -> datetime.date:
    return fecha_inicio + datetime.timedelta(days=dias - 1)

def validar_fraccionamiento(dias: int) -> bool:
    return dias >= 7

def obtener_dias_resolucion_restantes(resolucion_id):
    conn = get_conn()
    cur = conn.cursor()

    # Días autorizados
    cur.execute("SELECT dias_autorizados FROM resoluciones WHERE id = ?", (resolucion_id,))
    row = cur.fetchone()
    if not row:
        return 0
    autorizados = row["dias_autorizados"]

    # Días ya usados
    cur.execute("""
        SELECT IFNULL(SUM(dias), 0) AS usados
        FROM vacaciones
        WHERE mad = ? AND tipo = 'Resolución'
    """, (resolucion_id,))
    usados = cur.fetchone()["usados"]

    conn.close()
    return max(0, autorizados - usados)

def obtener_dias_periodo_restantes(periodo_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT 30 - IFNULL((SELECT SUM(dias) FROM vacaciones WHERE periodo_id = ?), 0) AS restantes", (periodo_id,))
    restantes = cur.fetchone()["restantes"]

    conn.close()
    return max(0, restantes)

# ---------------------------------------------------------
# REGISTRO DE RESOLUCIONES
# ---------------------------------------------------------

if menu == "Reporte de Trabajadores":
    st.header("Reporte de Trabajadores")
    df = listar_trabajadores()
    st.dataframe(df)

# ---------------------------------------------------------
# RESOLUCIONES
# ---------------------------------------------------------

if menu == "Resoluciones":
    st.header("Registro de Resoluciones")

    df_trab = listar_trabajadores()
    if df_trab.empty:
        st.warning("No hay trabajadores registrados.")
        st.stop()

    mapa_trab = {row["nombres"]: row["id"] for _, row in df_trab.iterrows()}
    trab_sel = st.selectbox("Seleccione trabajador", list(mapa_trab.keys()))
    trabajador_id = mapa_trab[trab_sel]

    df_periodos = listar_periodos_con_dias(trabajador_id)
    if df_periodos.empty:
        st.warning("El trabajador no tiene periodos generados.")
        st.stop()

    mapa_periodos = {
        f"{row['inicio_ciclo']} a {row['fin_ciclo']} (usados: {row['dias_tomados']})": row["id"]
        for _, row in df_periodos.iterrows()
    }
    periodo_sel = st.selectbox("Periodo", list(mapa_periodos.keys()))
    periodo_id = mapa_periodos[periodo_sel]

    numero_res = st.text_input("Número de resolución")
    fecha_prog = st.date_input("Fecha programada")
    dias_aut = st.number_input("Días autorizados", min_value=1, max_value=30)
    mad = st.text_input("MAD / Documento")
    obs = st.text_area("Observaciones")

    if st.button("Registrar Resolución"):
        crear_resolucion(
            trabajador_id,
            periodo_id,
            numero_res,
            fecha_prog.strftime("%Y-%m-%d"),
            dias_aut,
            mad,
            obs
        )
        st.success("Resolución registrada correctamente.")

    st.subheader("Resoluciones registradas")
    df_res = listar_resoluciones_por_trabajador(trabajador_id)
    st.dataframe(df_res)

# ---------------------------------------------------------
# PROGRAMACIÓN DE VACACIONES
# ---------------------------------------------------------

if menu == "Registrar Vacaciones":
    st.header("Programación de Vacaciones")

    df_trab = listar_trabajadores()
    if df_trab.empty:
        st.warning("No hay trabajadores registrados.")
        st.stop()

    mapa_trab = {row["nombres"]: row["id"] for _, row in df_trab.iterrows()}
    trab_sel = st.selectbox("Seleccione trabajador", list(mapa_trab.keys()))
    trabajador_id = mapa_trab[trab_sel]

    df_periodos = listar_periodos_con_dias(trabajador_id)
    mapa_periodos = {
        f"{row['inicio_ciclo']} a {row['fin_ciclo']} (usados: {row['dias_tomados']})": row["id"]
        for _, row in df_periodos.iterrows()
    }
    periodo_sel = st.selectbox("Periodo", list(mapa_periodos.keys()))
    periodo_id = mapa_periodos[periodo_sel]

    tipo = st.selectbox("Tipo de programación", ["Solicitud", "Memorando", "Resolución"])

    autorizado_res = st.checkbox("Autorizado por Resolución")

    df_res = listar_resoluciones_por_trabajador(trabajador_id)
    resolucion_id = None

    if autorizado_res:
        if df_res.empty:
            st.error("No hay resoluciones registradas para este trabajador.")
            st.stop()

        mapa_res = {
            f"{row['numero_resolucion']} - {row['dias_autorizados']} días": row["id"]
            for _, row in df_res.iterrows()
        }
        res_sel = st.selectbox("Seleccione resolución", list(mapa_res.keys()))
        resolucion_id = mapa_res[res_sel]

        dias_rest_res = obtener_dias_resolucion_restantes(resolucion_id)
        st.info(f"Días restantes en resolución: {dias_rest_res}")

    fecha_inicio = st.date_input("Fecha de inicio")
    dias = st.number_input("Días solicitados", min_value=1, max_value=30)

    fecha_fin = calcular_fecha_fin(fecha_inicio, dias)
    st.write(f"Fecha fin: **{fecha_fin}**")

    # Validaciones
    dias_rest_periodo = obtener_dias_periodo_restantes(periodo_id)
    st.info(f"Días restantes en periodo: {dias_rest_periodo}")

    if autorizado_res:
        if dias > dias_rest_res:
            st.error("Los días solicitados exceden los días autorizados por la resolución.")
            st.stop()
    else:
        if dias < 7:
            st.warning("Fraccionamiento: menos de 7 días.")
        if dias > dias_rest_periodo:
            st.error("Los días solicitados exceden los días disponibles del periodo.")
            st.stop()

    jefe_aut = st.selectbox("Jefe que autoriza", df_trab["jefe"].unique())
    autorizado_rrhh = st.checkbox("Autorizado por RRHH")

    if st.button("Registrar Vacación"):
        crear_vacacion(
            trabajador_id,
            periodo_id,
            tipo,
            fecha_inicio.strftime("%Y-%m-%d"),
            fecha_fin.strftime("%Y-%m-%d"),
            dias,
            resolucion_id,
            None,
            "",
            not autorizado_res and dias < 7,
            not autorizado_res and dias == 30,
            trabajador_id,
            autorizado_rrhh
        )
        st.success("Vacación registrada correctamente.")

    st.subheader("Vacaciones registradas")
    df_vac = listar_vacaciones_por_trabajador(trabajador_id)
    st.dataframe(df_vac)
# =========================================================
# =====================  BLOQUE 6  ========================
# =====================   DASHBOARD   =====================
# =========================================================

if menu == "Dashboard":
    st.header("Dashboard General de Vacaciones")

    # -----------------------------------------------------
    # Cargar datos base
    # -----------------------------------------------------
    df_trab = listar_trabajadores()
    if df_trab.empty:
        st.warning("No hay trabajadores registrados.")
        st.stop()

    # Cargar periodos
    conn = get_conn()
    df_periodos = pd.read_sql_query("""
        SELECT p.*, 
               t.nombres AS trabajador,
               a.nombre AS area,
               u.nombre AS unidad,
               d.nombre AS direccion,
               IFNULL((SELECT SUM(v.dias) FROM vacaciones v WHERE v.periodo_id = p.id), 0) AS dias_tomados
        FROM periodos p
        JOIN trabajadores t ON t.id = p.trabajador_id
        JOIN areas a ON a.id = t.area_id
        JOIN unidades u ON u.id = a.unidad_id
        JOIN direcciones d ON d.id = u.direccion_id
        ORDER BY p.inicio_ciclo
    """, conn)
    conn.close()

    # -----------------------------------------------------
    # FILTROS
    # -----------------------------------------------------
    st.subheader("Filtros")

    direcciones = sorted(df_periodos["direccion"].unique())
    direccion_sel = st.selectbox("Dirección", ["Todas"] + direcciones)

    if direccion_sel != "Todas":
        df_periodos = df_periodos[df_periodos["direccion"] == direccion_sel]

    unidades = sorted(df_periodos["unidad"].unique())
    unidad_sel = st.selectbox("Unidad", ["Todas"] + unidades)

    if unidad_sel != "Todas":
        df_periodos = df_periodos[df_periodos["unidad"] == unidad_sel]

    areas = sorted(df_periodos["area"].unique())
    area_sel = st.selectbox("Área", ["Todas"] + areas)

    if area_sel != "Todas":
        df_periodos = df_periodos[df_periodos["area"] == area_sel]

    # -----------------------------------------------------
    # Cálculos de estado de periodos
    # -----------------------------------------------------
    hoy = datetime.date.today()

    df_periodos["goce_hasta_date"] = df_periodos["goce_hasta"].apply(to_date)
    df_periodos["acumulable_hasta_date"] = df_periodos["acumulable_hasta"].apply(to_date)

    df_periodos["estado"] = df_periodos.apply(
        lambda row: "Vencido" if hoy > row["acumulable_hasta_date"]
        else ("Por vencer" if hoy > row["goce_hasta_date"] else "Vigente"),
        axis=1
    )

    # -----------------------------------------------------
    # RESUMEN EJECUTIVO
    # -----------------------------------------------------
    st.subheader("Resumen Ejecutivo")

    total_trab = df_trab.shape[0]
    total_periodos = df_periodos.shape[0]
    vencidos = df_periodos[df_periodos["estado"] == "Vencido"].shape[0]
    por_vencer = df_periodos[df_periodos["estado"] == "Por vencer"].shape[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Trabajadores", total_trab)
    col2.metric("Periodos", total_periodos)
    col3.metric("Periodos Vencidos", vencidos)
    col4.metric("Por Vencer", por_vencer)

    st.markdown("---")

    # -----------------------------------------------------
    # PERIODOS VENCIDOS
    # -----------------------------------------------------
    st.subheader("Periodos Vencidos")

    df_vencidos = df_periodos[df_periodos["estado"] == "Vencido"]
    if df_vencidos.empty:
        st.info("No hay periodos vencidos.")
    else:
        st.dataframe(df_vencidos[[
            "trabajador", "direccion", "unidad", "area",
            "inicio_ciclo", "fin_ciclo", "goce_hasta", "acumulable_hasta",
            "dias_tomados"
        ]])

    st.markdown("---")

    # -----------------------------------------------------
    # PERIODOS POR VENCER
    # -----------------------------------------------------
    st.subheader("Periodos por Vencer")

    df_por_vencer = df_periodos[df_periodos["estado"] == "Por vencer"]
    if df_por_vencer.empty:
        st.info("No hay periodos por vencer.")
    else:
        st.dataframe(df_por_vencer[[
            "trabajador", "direccion", "unidad", "area",
            "inicio_ciclo", "fin_ciclo", "goce_hasta", "acumulable_hasta",
            "dias_tomados"
        ]])

    st.markdown("---")

    # -----------------------------------------------------
    # TRABAJADORES CON DÍAS PENDIENTES
    # -----------------------------------------------------
    st.subheader("Trabajadores con días pendientes")

    df_periodos["dias_pendientes"] = 30 - df_periodos["dias_tomados"]
    df_pendientes = df_periodos[df_periodos["dias_pendientes"] > 0]

    if df_pendientes.empty:
        st.info("Todos los trabajadores han usado sus días.")
    else:
        st.dataframe(df_pendientes[[
            "trabajador", "direccion", "unidad", "area",
            "inicio_ciclo", "fin_ciclo", "dias_tomados", "dias_pendientes"
        ]])

    st.markdown("---")

    # -----------------------------------------------------
    # TABLA COMPLETA
    # -----------------------------------------------------
    st.subheader("Tabla Completa de Periodos")

    st.dataframe(df_periodos[[
        "trabajador", "direccion", "unidad", "area",
        "inicio_ciclo", "fin_ciclo", "goce_hasta", "acumulable_hasta",
        "dias_tomados", "estado"
    ]])
# =========================================================
# =====================  BLOQUE 7  ========================
# =====================   REPORTES   ======================
# =========================================================

if menu == "Reportes":
    st.header("Reportes Avanzados de Vacaciones")

    # -----------------------------------------------------
    # Cargar datos base
    # -----------------------------------------------------
    df_trab = listar_trabajadores()
    if df_trab.empty:
        st.warning("No hay trabajadores registrados.")
        st.stop()

    conn = get_conn()
    df_periodos = pd.read_sql_query("""
        SELECT p.*, 
               t.nombres AS trabajador,
               t.dni,
               t.cargo,
               a.nombre AS area,
               u.nombre AS unidad,
               d.nombre AS direccion,
               IFNULL((SELECT SUM(v.dias) FROM vacaciones v WHERE v.periodo_id = p.id), 0) AS dias_tomados
        FROM periodos p
        JOIN trabajadores t ON t.id = p.trabajador_id
        JOIN areas a ON a.id = t.area_id
        JOIN unidades u ON u.id = a.unidad_id
        JOIN direcciones d ON d.id = u.direccion_id
        ORDER BY d.nombre, u.nombre, a.nombre, t.nombres
    """, conn)

    df_vac = pd.read_sql_query("""
        SELECT v.*, 
               t.nombres AS trabajador,
               t.dni,
               a.nombre AS area,
               u.nombre AS unidad,
               d.nombre AS direccion,
               p.inicio_ciclo,
               p.fin_ciclo
        FROM vacaciones v
        JOIN trabajadores t ON t.id = v.trabajador_id
        JOIN periodos p ON p.id = v.periodo_id
        JOIN areas a ON a.id = t.area_id
        JOIN unidades u ON u.id = a.unidad_id
        JOIN direcciones d ON d.id = u.direccion_id
        ORDER BY v.fecha_inicio
    """, conn)

    df_res = pd.read_sql_query("""
        SELECT r.*, 
               t.nombres AS trabajador,
               t.dni,
               a.nombre AS area,
               u.nombre AS unidad,
               d.nombre AS direccion,
               p.inicio_ciclo,
               p.fin_ciclo
        FROM resoluciones r
        JOIN trabajadores t ON t.id = r.trabajador_id
        JOIN periodos p ON p.id = r.periodo_id
        JOIN areas a ON a.id = t.area_id
        JOIN unidades u ON u.id = a.unidad_id
        JOIN direcciones d ON d.id = u.direccion_id
        ORDER BY r.fecha_programada
    """, conn)
    conn.close()

    # -----------------------------------------------------
    # Filtros principales
    # -----------------------------------------------------
    st.subheader("Filtros")

    direcciones = sorted(df_periodos["direccion"].unique())
    direccion_sel = st.selectbox("Dirección", ["Todas"] + direcciones)

    if direccion_sel != "Todas":
        df_periodos = df_periodos[df_periodos["direccion"] == direccion_sel]
        df_vac = df_vac[df_vac["direccion"] == direccion_sel]
        df_res = df_res[df_res["direccion"] == direccion_sel]

    unidades = sorted(df_periodos["unidad"].unique())
    unidad_sel = st.selectbox("Unidad", ["Todas"] + unidades)

    if unidad_sel != "Todas":
        df_periodos = df_periodos[df_periodos["unidad"] == unidad_sel]
        df_vac = df_vac[df_vac["unidad"] == unidad_sel]
        df_res = df_res[df_res["unidad"] == unidad_sel]

    areas = sorted(df_periodos["area"].unique())
    area_sel = st.selectbox("Área", ["Todas"] + areas)

    if area_sel != "Todas":
        df_periodos = df_periodos[df_periodos["area"] == area_sel]
        df_vac = df_vac[df_vac["area"] == area_sel]
        df_res = df_res[df_res["area"] == area_sel]

    # -----------------------------------------------------
    # REPORTE 1: Periodos
    # -----------------------------------------------------
    st.markdown("---")
    st.subheader("Reporte de Periodos")

    df_periodos["dias_pendientes"] = 30 - df_periodos["dias_tomados"]

    st.dataframe(df_periodos[[
        "trabajador", "dni", "cargo",
        "direccion", "unidad", "area",
        "inicio_ciclo", "fin_ciclo",
        "goce_hasta", "acumulable_hasta",
        "dias_tomados", "dias_pendientes"
    ]])

    st.download_button(
        "Exportar Periodos (CSV)",
        df_periodos.to_csv(index=False).encode("utf-8"),
        "periodos.csv",
        "text/csv"
    )

    # -----------------------------------------------------
    # REPORTE 2: Vacaciones
    # -----------------------------------------------------
    st.markdown("---")
    st.subheader("Reporte de Vacaciones")

    if df_vac.empty:
        st.info("No hay vacaciones registradas con los filtros seleccionados.")
    else:
        st.dataframe(df_vac[[
            "trabajador", "dni",
            "direccion", "unidad", "area",
            "tipo", "fecha_inicio", "fecha_fin",
            "dias", "documento", "mad", "observaciones"
        ]])

        st.download_button(
            "Exportar Vacaciones (CSV)",
            df_vac.to_csv(index=False).encode("utf-8"),
            "vacaciones.csv",
            "text/csv"
        )

    # -----------------------------------------------------
    # REPORTE 3: Resoluciones
    # -----------------------------------------------------
    st.markdown("---")
    st.subheader("Reporte de Resoluciones")

    if df_res.empty:
        st.info("No hay resoluciones registradas con los filtros seleccionados.")
    else:
        st.dataframe(df_res[[
            "trabajador", "dni",
            "direccion", "unidad", "area",
            "numero_resolucion", "fecha_programada",
            "dias_autorizados", "mad", "observaciones"
        ]])

        st.download_button(
            "Exportar Resoluciones (CSV)",
            df_res.to_csv(index=False).encode("utf-8"),
            "resoluciones.csv",
            "text/csv"
        )

    # -----------------------------------------------------
    # REPORTE 4: Consolidado por Trabajador
    # -----------------------------------------------------
    st.markdown("---")
    st.subheader("Consolidado por Trabajador")

    trabajadores = sorted(df_periodos["trabajador"].unique())
    trab_sel = st.selectbox("Seleccione trabajador", trabajadores)

    df_p = df_periodos[df_periodos["trabajador"] == trab_sel]
    df_v = df_vac[df_vac["trabajador"] == trab_sel]
    df_r = df_res[df_res["trabajador"] == trab_sel]

    st.write("### Periodos")
    st.dataframe(df_p)

    st.write("### Vacaciones")
    st.dataframe(df_v)

    st.write("### Resoluciones")
    st.dataframe(df_r)

    st.download_button(
        "Exportar Consolidado (CSV)",
        pd.concat([df_p, df_v, df_r], axis=0).to_csv(index=False).encode("utf-8"),
        "consolidado_trabajador.csv",
        "text/csv"
    )
# =========================================================
# =====================  BLOQUE 8  ========================
# ================== PANEL DE CONTROL ADMIN ===============
# =========================================================

if menu == "Gestión de Usuarios":
    st.header("Gestión de Usuarios del Sistema")

    if ROL != "admin":
        st.error("Solo el administrador puede gestionar usuarios.")
        st.stop()

    st.subheader("Usuarios Registrados")

    df_users = listar_usuarios()
    st.dataframe(df_users.rename(columns={
        "id": "ID",
        "usuario": "Usuario",
        "rol": "Rol"
    }))

    st.markdown("---")

    # -----------------------------------------------------
    # CREAR NUEVO USUARIO
    # -----------------------------------------------------
    st.subheader("Crear Nuevo Usuario")

    nuevo_usuario = st.text_input("Nuevo usuario")
    nuevo_password = st.text_input("Contraseña", type="password")
    nuevo_rol = st.selectbox("Rol", ["admin", "responsable", "registrador"])

    if st.button("Registrar Usuario"):
        if not nuevo_usuario.strip() or not nuevo_password.strip():
            st.error("Debe ingresar usuario y contraseña.")
        else:
            try:
                crear_usuario(nuevo_usuario.strip(), nuevo_password.strip(), nuevo_rol)
                st.success("Usuario creado correctamente.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown("---")

    # -----------------------------------------------------
    # CAMBIAR ROL DE USUARIO
    # -----------------------------------------------------
    st.subheader("Cambiar Rol de Usuario")

    if not df_users.empty:
        mapa_users = {row["usuario"]: row["id"] for _, row in df_users.iterrows()}
        user_sel = st.selectbox("Seleccione usuario", list(mapa_users.keys()))
        user_id = mapa_users[user_sel]

        nuevo_rol_sel = st.selectbox("Nuevo rol", ["admin", "responsable", "registrador"])

        if st.button("Actualizar Rol"):
            actualizar_rol_usuario(user_id, nuevo_rol_sel)
            st.success("Rol actualizado correctamente.")
            st.experimental_rerun()

    st.markdown("---")

    # -----------------------------------------------------
    # ELIMINAR USUARIO
    # -----------------------------------------------------
    st.subheader("Eliminar Usuario")

    if not df_users.empty:
        user_del = st.selectbox("Seleccione usuario a eliminar", list(mapa_users.keys()))

        if st.button("Eliminar Usuario"):
            if user_del == "admin":
                st.error("No se puede eliminar el usuario administrador principal.")
            else:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("DELETE FROM usuarios WHERE usuario = ?", (user_del,))
                conn.commit()
                conn.close()
                st.success("Usuario eliminado.")
                st.experimental_rerun()

    st.markdown("---")

# ---------------------------------------------------------
# RESET DEL SISTEMA
# ---------------------------------------------------------

if menu == "Reset del Sistema":
    st.header("Reset Completo del Sistema")

    if ROL != "admin":
        st.error("Solo el administrador puede resetear el sistema.")
        st.stop()

    st.warning("⚠ Esta acción eliminará TODA la información del sistema.")
    st.warning("Incluye: trabajadores, periodos, vacaciones, resoluciones, jefes, áreas, unidades, direcciones y usuarios.")

    confirmar = st.checkbox("Confirmo que deseo resetear el sistema")

    if confirmar and st.button("RESET TOTAL"):
        resetear_todo()
        st.success("Sistema reseteado completamente. Usuario admin/admin restaurado.")
        st.experimental_rerun()
