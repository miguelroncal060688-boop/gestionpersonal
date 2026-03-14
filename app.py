import streamlit as st
import sqlite3
import pandas as pd
import datetime
import hashlib
import os
import json
from typing import Dict, Any, Tuple

DB_PATH = "vacaciones.db"

# =========================================================
# Rerun compatible
# =========================================================
def do_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    st.stop()

# =========================================================
# DB connection
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# =========================================================
# Utils
# =========================================================
def hash_password(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def to_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()

def from_date(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def table_exists(conn, name: str) -> bool:
    r = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return r is not None

def object_type(conn, name: str):
    r = conn.execute("SELECT type FROM sqlite_master WHERE name=?", (name,)).fetchone()
    return r["type"] if r else None

def safe_drop_object(conn, name: str):
    """
    Evita el error: DROP VIEW sobre una TABLE.
    """
    t = object_type(conn, name)
    if t == "view":
        conn.execute(f"DROP VIEW IF EXISTS {name}")
    elif t == "table":
        conn.execute(f"DROP TABLE IF EXISTS {name}")
    # si no existe, no hace nada

def table_info(conn, table: str):
    return conn.execute(f"PRAGMA table_info({table})").fetchall()

def table_columns(conn, table: str) -> Dict[str, Dict[str, Any]]:
    """
    Retorna dict: col -> {type, notnull, dflt_value}
    """
    cols = {}
    for r in table_info(conn, table):
        cols[r["name"]] = {
            "type": r["type"],
            "notnull": r["notnull"],
            "dflt_value": r["dflt_value"]
        }
    return cols

def add_column_if_missing(conn, table: str, col: str, col_def_sql: str):
    cols = set(table_columns(conn, table).keys())
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}")

# =========================================================
# Permisos por rol + por usuario (granular)
# =========================================================
PERM_KEYS = [
    "estructura_ver", "estructura_editar",
    "trabajadores_ver", "trabajadores_editar",
    "resoluciones_ver", "resoluciones_editar",
    "vacaciones_ver", "vacaciones_editar",
    "rrhh_aprobar",
    "dashboard_ver",
    "reportes_ver",
    "usuarios_admin",
    "backup_exportar",
    "reset_sistema",
    "adelantos_ver", "adelantos_editar",
]

ROLE_DEFAULT_PERMS = {
    "admin": {k: True for k in PERM_KEYS},
    "responsable": {
        "estructura_ver": True, "estructura_editar": True,
        "trabajadores_ver": True, "trabajadores_editar": True,
        "resoluciones_ver": True, "resoluciones_editar": True,
        "vacaciones_ver": True, "vacaciones_editar": True,
        "rrhh_aprobar": True,
        "dashboard_ver": True,
        "reportes_ver": True,
        "usuarios_admin": False,
        "backup_exportar": True,
        "reset_sistema": False,
        "adelantos_ver": True, "adelantos_editar": True,
    },
    "registrador": {
        "estructura_ver": False, "estructura_editar": False,
        "trabajadores_ver": True, "trabajadores_editar": True,
        "resoluciones_ver": True, "resoluciones_editar": True,
        "vacaciones_ver": True, "vacaciones_editar": True,
        "rrhh_aprobar": False,
        "dashboard_ver": False,
        "reportes_ver": True,
        "usuarios_admin": False,
        "backup_exportar": False,
        "reset_sistema": False,
        "adelantos_ver": True, "adelantos_editar": True,
    }
}

def get_user_perms(conn, usuario: str, rol: str) -> Dict[str, bool]:
    base = ROLE_DEFAULT_PERMS.get(rol, ROLE_DEFAULT_PERMS["registrador"]).copy()
    row = conn.execute("SELECT permisos_json FROM usuarios WHERE usuario=?", (usuario,)).fetchone()
    if row and row["permisos_json"]:
        try:
            d = json.loads(row["permisos_json"])
            if isinstance(d, dict):
                for k, v in d.items():
                    if k in base:
                        base[k] = bool(v)
        except Exception:
            pass
    return base

def can(perms: Dict[str,bool], key: str) -> bool:
    return bool(perms.get(key, False))

# =========================================================
# Esquema + migración robusta
# =========================================================
def ensure_schema():
    conn = get_conn()
    cur = conn.cursor()

    # Tablas base
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS usuarios(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        rol TEXT NOT NULL,
        permisos_json TEXT
    );

    CREATE TABLE IF NOT EXISTS direcciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS unidades(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        direccion_id INTEGER NOT NULL,
        nombre TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS areas(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unidad_id INTEGER NOT NULL,
        nombre TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS jefes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombres TEXT NOT NULL,
        cargo TEXT,
        area_id INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS trabajadores(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT,
        dni TEXT,
        nombres TEXT NOT NULL,
        cargo TEXT,
        regimen TEXT,
        fecha_ingreso TEXT NOT NULL,
        area_id INTEGER NOT NULL,
        jefe_id INTEGER
    );

    CREATE TABLE IF NOT EXISTS periodos(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        inicio_ciclo TEXT NOT NULL,
        fin_ciclo TEXT NOT NULL,
        goce_hasta TEXT NOT NULL,
        acumulable_hasta TEXT NOT NULL,
        UNIQUE(trabajador_id, inicio_ciclo)
    );

    CREATE TABLE IF NOT EXISTS resoluciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER,
        periodo_id INTEGER,
        numero TEXT,
        fecha_inicio TEXT,
        fecha_fin TEXT,
        dias_autorizados INTEGER,
        fraccionable INTEGER DEFAULT 0,
        mad TEXT,
        observaciones TEXT
    );

    CREATE TABLE IF NOT EXISTS vacaciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        periodo_id INTEGER,
        resolucion_id INTEGER,
        tipo TEXT NOT NULL,
        modo_resolucion TEXT,
        fecha_inicio TEXT NOT NULL,
        fecha_fin TEXT NOT NULL,
        dias INTEGER NOT NULL,
        documento TEXT,
        mad TEXT,
        observaciones TEXT,
        autorizado_rrhh INTEGER DEFAULT 0,
        rrhh_observacion TEXT,
        fecha_aprob_rrhh TEXT,
        usuario_rrhh TEXT
    );

    CREATE TABLE IF NOT EXISTS adelantos(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        fecha_solicitud TEXT NOT NULL,
        ciclo_inicio TEXT NOT NULL,
        ciclo_fin TEXT NOT NULL,
        dias_maximo INTEGER NOT NULL,
        dias_solicitados INTEGER NOT NULL,
        documento_acuerdo TEXT NOT NULL,
        observaciones TEXT,
        aprobado_rrhh INTEGER DEFAULT 0,
        rrhh_observacion TEXT,
        fecha_aprob_rrhh TEXT,
        usuario_rrhh TEXT
    );
    """)

    # Migración columnas faltantes (sin romper BD vieja)
    add_column_if_missing(conn, "usuarios", "permisos_json", "permisos_json TEXT")

    # resoluciones legacy support
    for coldef in [
        ("numero_resolucion", "numero_resolucion TEXT"),
        ("fecha_programada", "fecha_programada TEXT"),
        ("dias_autorizados", "dias_autorizados INTEGER"),
        ("numero", "numero TEXT"),
        ("fecha_inicio", "fecha_inicio TEXT"),
        ("fecha_fin", "fecha_fin TEXT"),
        ("fraccionable", "fraccionable INTEGER DEFAULT 0"),
        ("mad", "mad TEXT"),
        ("observaciones", "observaciones TEXT"),
        ("trabajador_id", "trabajador_id INTEGER"),
        ("periodo_id", "periodo_id INTEGER"),
    ]:
        add_column_if_missing(conn, "resoluciones", coldef[0], coldef[1])

    # vacaciones legacy support
    for coldef in [
        ("periodo_id", "periodo_id INTEGER"),
        ("resolucion_id", "resolucion_id INTEGER"),
        ("modo_resolucion", "modo_resolucion TEXT"),
        ("documento", "documento TEXT"),
        ("mad", "mad TEXT"),
        ("observaciones", "observaciones TEXT"),
        ("autorizado_rrhh", "autorizado_rrhh INTEGER DEFAULT 0"),
        ("rrhh_observacion", "rrhh_observacion TEXT"),
        ("fecha_aprob_rrhh", "fecha_aprob_rrhh TEXT"),
        ("usuario_rrhh", "usuario_rrhh TEXT"),
    ]:
        add_column_if_missing(conn, "vacaciones", coldef[0], coldef[1])

    conn.commit()

    # Admin inicial
    c = conn.execute("SELECT COUNT(*) AS c FROM usuarios").fetchone()["c"]
    if c == 0:
        conn.execute(
            "INSERT INTO usuarios(usuario,password_hash,rol,permisos_json) VALUES(?,?,?,?)",
            ("admin", hash_password("admin"), "admin", None)
        )
        conn.commit()

    # ---- Vistas de compatibilidad (SIN crash)
    # Si "resoluciones_norm" existe como TABLE, lo borramos como TABLE.
    safe_drop_object(conn, "resoluciones_norm")
    safe_drop_object(conn, "vacaciones_norm")

    res_cols = set(table_columns(conn, "resoluciones").keys())

    def pick(*names, fallback=None):
        for n in names:
            if n in res_cols:
                return n
        return fallback

    col_num = pick("numero", "numero_resolucion", fallback="numero")
    col_fi  = pick("fecha_inicio", "fecha_programada", fallback="fecha_inicio")
    col_ff  = pick("fecha_fin", fallback=col_fi)
    col_da  = pick("dias_autorizados", fallback="dias_autorizados")

    conn.execute(f"""
        CREATE VIEW resoluciones_norm AS
        SELECT
            id,
            trabajador_id,
            periodo_id,
            {col_num} AS numero,
            {col_fi} AS fecha_inicio,
            {col_ff} AS fecha_fin,
            {col_da} AS dias_autorizados,
            COALESCE(fraccionable,0) AS fraccionable,
            mad,
            observaciones
        FROM resoluciones
    """)

    conn.execute("""
        CREATE VIEW vacaciones_norm AS
        SELECT
            id,
            trabajador_id,
            periodo_id,
            resolucion_id,
            tipo,
            modo_resolucion,
            fecha_inicio,
            fecha_fin,
            dias,
            documento,
            mad,
            observaciones,
            COALESCE(autorizado_rrhh,0) AS autorizado_rrhh,
            rrhh_observacion,
            fecha_aprob_rrhh,
            usuario_rrhh
        FROM vacaciones
    """)

    conn.commit()
    conn.close()

# =========================================================
# Generación de periodos (regla correcta)
# =========================================================
def generar_periodos_para_trabajador(trabajador_id: int, fecha_ingreso_str: str):
    conn = get_conn()
    cur = conn.cursor()
    ingreso = to_date(fecha_ingreso_str)
    hoy = datetime.date.today()

    inicio = ingreso
    while True:
        fin_ciclo = inicio.replace(year=inicio.year + 1) - datetime.timedelta(days=1)
        if fin_ciclo >= hoy:
            break

        goce_hasta = fin_ciclo.replace(year=fin_ciclo.year + 1)
        acumulable_hasta = fin_ciclo.replace(year=fin_ciclo.year + 2)

        cur.execute("""
            INSERT OR IGNORE INTO periodos(trabajador_id,inicio_ciclo,fin_ciclo,goce_hasta,acumulable_hasta)
            VALUES(?,?,?,?,?)
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

def periodos_trabajador_df(trabajador_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT p.*,
               IFNULL((SELECT SUM(dias) FROM vacaciones_norm v WHERE v.periodo_id=p.id),0) AS dias_usados
        FROM periodos p
        WHERE p.trabajador_id=?
        ORDER BY p.inicio_ciclo
    """, conn, params=(trabajador_id,))
    conn.close()
    return df

def dias_periodo_restantes(periodo_id: int) -> int:
    conn = get_conn()
    r = conn.execute("""
        SELECT 30 - IFNULL((SELECT SUM(dias) FROM vacaciones_norm WHERE periodo_id=?),0) AS r
    """, (periodo_id,)).fetchone()
    conn.close()
    return max(0, safe_int(r["r"], 0))

def dias_resolucion_restantes(resolucion_id: int) -> int:
    conn = get_conn()
    r = conn.execute("SELECT dias_autorizados FROM resoluciones_norm WHERE id=?", (resolucion_id,)).fetchone()
    if not r or r["dias_autorizados"] is None:
        conn.close()
        return 0
    autorizados = safe_int(r["dias_autorizados"], 0)
    usados = conn.execute("SELECT IFNULL(SUM(dias),0) AS u FROM vacaciones_norm WHERE resolucion_id=?", (resolucion_id,)).fetchone()["u"]
    conn.close()
    return max(0, autorizados - safe_int(usados, 0))

# =========================================================
# INSERCIÓN ROBUSTA (resoluciones/vacaciones) para BD vieja
# =========================================================
def insert_resolucion_robusta(conn, trabajador_id, periodo_id, numero, fecha_inicio, fecha_fin, dias_aut, fraccionable, observaciones, mad=None):
    cols = table_columns(conn, "resoluciones")

    # Mapeo de campos según columnas existentes
    payload = {}

    def set_if_exists(colname, value):
        if colname in cols:
            payload[colname] = value

    set_if_exists("trabajador_id", trabajador_id)
    set_if_exists("periodo_id", periodo_id)

    # Número en columna nueva o vieja
    if "numero_resolucion" in cols:
        payload["numero_resolucion"] = numero
    if "numero" in cols:
        payload["numero"] = numero

    # Fechas
    if "fecha_inicio" in cols:
        payload["fecha_inicio"] = fecha_inicio
    if "fecha_fin" in cols:
        payload["fecha_fin"] = fecha_fin

    # legacy: fecha_programada (si existe y es NOT NULL, la rellenamos)
    if "fecha_programada" in cols:
        payload["fecha_programada"] = fecha_inicio

    # Días
    if "dias_autorizados" in cols:
        payload["dias_autorizados"] = dias_aut
    if "dias_aut" in cols:
        payload["dias_aut"] = dias_aut
    if "dias" in cols and "dias_autorizados" not in cols:
        payload["dias"] = dias_aut

    # Fraccionable
    if "fraccionable" in cols:
        payload["fraccionable"] = 1 if fraccionable else 0

    # Mad/Obs
    if "mad" in cols:
        payload["mad"] = mad
    if "observaciones" in cols:
        payload["observaciones"] = observaciones

    # Completar NOT NULL faltantes con defaults
    for c, meta in cols.items():
        if meta["notnull"] == 1 and c not in payload:
            dv = meta["dflt_value"]
            if dv is not None:
                # dflt_value viene como SQL literal, a veces con comillas. Lo dejamos tal cual usando SQL DEFAULT no es posible aquí.
                # Mejor: si es texto quoted, extraemos; si es num, usamos.
                dv2 = str(dv).strip("'").strip('"')
                payload[c] = dv2
            else:
                # fallback por tipo
                t = (meta["type"] or "").upper()
                payload[c] = 0 if "INT" in t else ""

    cols_sql = ", ".join(payload.keys())
    qs = ", ".join(["?"] * len(payload))
    conn.execute(f"INSERT INTO resoluciones({cols_sql}) VALUES({qs})", tuple(payload.values()))

def insert_vacacion_robusta(conn, trabajador_id, periodo_id, resolucion_id, tipo, modo_resolucion, fecha_inicio, fecha_fin, dias, documento, observaciones=None, mad=None):
    cols = table_columns(conn, "vacaciones")
    payload = {}

    def set_if_exists(colname, value):
        if colname in cols:
            payload[colname] = value

    set_if_exists("trabajador_id", trabajador_id)
    set_if_exists("periodo_id", periodo_id)
    set_if_exists("resolucion_id", resolucion_id)
    set_if_exists("tipo", tipo)
    set_if_exists("modo_resolucion", modo_resolucion)
    set_if_exists("fecha_inicio", fecha_inicio)
    set_if_exists("fecha_fin", fecha_fin)
    set_if_exists("dias", dias)
    set_if_exists("documento", documento)
    set_if_exists("observaciones", observaciones)
    set_if_exists("mad", mad)
    if "autorizado_rrhh" in cols and "autorizado_rrhh" not in payload:
        payload["autorizado_rrhh"] = 0

    # NOT NULL defaults
    for c, meta in cols.items():
        if meta["notnull"] == 1 and c not in payload:
            dv = meta["dflt_value"]
            if dv is not None:
                dv2 = str(dv).strip("'").strip('"')
                payload[c] = dv2
            else:
                t = (meta["type"] or "").upper()
                payload[c] = 0 if "INT" in t else ""

    cols_sql = ", ".join(payload.keys())
    qs = ", ".join(["?"] * len(payload))
    conn.execute(f"INSERT INTO vacaciones({cols_sql}) VALUES({qs})", tuple(payload.values()))

# =========================================================
# Adelanto proporcional
# =========================================================
def ciclo_en_curso(fecha_ingreso_str: str) -> Tuple[datetime.date, datetime.date, int]:
    ingreso = to_date(fecha_ingreso_str)
    hoy = datetime.date.today()
    inicio = ingreso
    while True:
        nxt = inicio.replace(year=inicio.year + 1)
        if nxt > hoy:
            break
        inicio = nxt
    fin = inicio.replace(year=inicio.year + 1) - datetime.timedelta(days=1)
    dias_trab = (hoy - inicio).days + 1
    dias_max = int((dias_trab / 365.0) * 30)
    dias_max = max(0, min(30, dias_max))
    return inicio, fin, dias_max

# =========================================================
# App init
# =========================================================
st.set_page_config(page_title="Vacaciones DRE Cajamarca", layout="wide")
ensure_schema()

# =========================================================
# Login
# =========================================================
if "user" not in st.session_state:
    st.session_state["user"] = None
if "rol" not in st.session_state:
    st.session_state["rol"] = None

def login():
    st.title("Sistema de Gestión de Vacaciones – DRE Cajamarca")
    u = st.text_input("Usuario")
    p = st.text_input("Contraseña", type="password")
    if st.button("Ingresar"):
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM usuarios WHERE usuario=? AND password_hash=?",
            (u, hash_password(p))
        ).fetchone()
        conn.close()
        if row:
            st.session_state["user"] = row["usuario"]
            st.session_state["rol"] = row["rol"]
            do_rerun()
        else:
            st.error("Usuario o contraseña incorrectos")

def logout():
    st.session_state["user"] = None
    st.session_state["rol"] = None
    do_rerun()

if st.session_state["user"] is None:
    login()
    st.stop()

USER = st.session_state["user"]
ROL = st.session_state["rol"]

conn_perm = get_conn()
PERMS = get_user_perms(conn_perm, USER, ROL)
conn_perm.close()

# =========================================================
# Menú filtrado por permisos
# =========================================================
MENU_ALL = [
    ("Direcciones / Unidades / Áreas / Jefes", "estructura_ver"),
    ("Registrar Trabajador", "trabajadores_ver"),
    ("Reporte de Trabajadores", "trabajadores_ver"),
    ("Resoluciones", "resoluciones_ver"),
    ("Registrar Vacaciones", "vacaciones_ver"),
    ("Adelanto de Vacaciones", "adelantos_ver"),
    ("Panel RRHH", "rrhh_aprobar"),
    ("Dashboard", "dashboard_ver"),
    ("Reportes", "reportes_ver"),
    ("Usuarios y Permisos", "usuarios_admin"),
    ("Exportar / Backup", "backup_exportar"),
    ("Reset del Sistema", "reset_sistema"),
]

menu_items = [name for name, perm in MENU_ALL if can(PERMS, perm)]
if not menu_items:
    st.error("Tu usuario no tiene permisos asignados. Contacta al administrador.")
    st.stop()

menu = st.sidebar.radio("Menú", menu_items)
st.sidebar.write(f"Usuario: {USER} ({ROL})")
st.sidebar.write("Permisos activos:", ", ".join([k for k,v in PERMS.items() if v]))
if st.sidebar.button("Cerrar sesión"):
    logout()

st.title("Sistema de Gestión de Vacaciones – DRE Cajamarca")

# =========================================================
# Estructura: direcciones/unidades/áreas/jefes (CRUD)
# =========================================================
if menu == "Direcciones / Unidades / Áreas / Jefes":
    if not can(PERMS, "estructura_ver"):
        st.error("Sin permiso.")
        st.stop()
    editable = can(PERMS, "estructura_editar")

    conn = get_conn()
    st.header("Direcciones / Unidades / Áreas / Jefes")

    # Direcciones
    st.subheader("Direcciones")
    df_dir = pd.read_sql("SELECT * FROM direcciones ORDER BY nombre", conn)
    st.dataframe(df_dir, use_container_width=True)

    if editable:
        with st.form("f_dir", clear_on_submit=True):
            nd = st.text_input("Nueva Dirección")
            if st.form_submit_button("Agregar Dirección"):
                if nd.strip():
                    conn.execute("INSERT INTO direcciones(nombre) VALUES(?)", (nd.strip(),))
                    conn.commit()
                    do_rerun()

        if not df_dir.empty:
            col1, col2 = st.columns(2)
            with col1:
                did = st.selectbox("Editar Dirección", df_dir["id"], format_func=lambda x: df_dir[df_dir["id"]==x]["nombre"].values[0])
                newname = st.text_input("Nuevo nombre dirección")
                if st.button("📝 Guardar dirección"):
                    if newname.strip():
                        conn.execute("UPDATE direcciones SET nombre=? WHERE id=?", (newname.strip(), did))
                        conn.commit()
                        do_rerun()
            with col2:
                did2 = st.selectbox("Eliminar Dirección", df_dir["id"], format_func=lambda x: df_dir[df_dir["id"]==x]["nombre"].values[0], key="del_dir")
                if st.button("🗑️ Eliminar dirección"):
                    conn.execute("DELETE FROM direcciones WHERE id=?", (did2,))
                    conn.commit()
                    do_rerun()

    st.divider()

    # Unidades
    st.subheader("Unidades")
    if df_dir.empty:
        st.info("Registra direcciones primero.")
    else:
        mapa_dir = dict(zip(df_dir["nombre"], df_dir["id"]))
        dsel = st.selectbox("Dirección", list(mapa_dir.keys()))
        df_uni = pd.read_sql("SELECT * FROM unidades WHERE direccion_id=? ORDER BY nombre", conn, params=(mapa_dir[dsel],))
        st.dataframe(df_uni, use_container_width=True)

        if editable:
            with st.form("f_uni", clear_on_submit=True):
                nu = st.text_input("Nueva Unidad")
                if st.form_submit_button("Agregar Unidad"):
                    if nu.strip():
                        conn.execute("INSERT INTO unidades(direccion_id,nombre) VALUES(?,?)", (mapa_dir[dsel], nu.strip()))
                        conn.commit()
                        do_rerun()

            if not df_uni.empty:
                col1, col2 = st.columns(2)
                with col1:
                    uid = st.selectbox("Editar Unidad", df_uni["id"], format_func=lambda x: df_uni[df_uni["id"]==x]["nombre"].values[0])
                    newu = st.text_input("Nuevo nombre unidad")
                    if st.button("📝 Guardar unidad"):
                        if newu.strip():
                            conn.execute("UPDATE unidades SET nombre=? WHERE id=?", (newu.strip(), uid))
                            conn.commit()
                            do_rerun()
                with col2:
                    uid2 = st.selectbox("Eliminar Unidad", df_uni["id"], format_func=lambda x: df_uni[df_uni["id"]==x]["nombre"].values[0], key="del_uni")
                    if st.button("🗑️ Eliminar unidad"):
                        conn.execute("DELETE FROM unidades WHERE id=?", (uid2,))
                        conn.commit()
                        do_rerun()

    st.divider()

    # Áreas
    st.subheader("Áreas")
    df_uni_all = pd.read_sql("""
        SELECT u.id,u.nombre,d.nombre AS direccion
        FROM unidades u JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre
    """, conn)
    if df_uni_all.empty:
        st.info("Registra unidades primero.")
    else:
        mapa_uni = {f"{r['direccion']} - {r['nombre']}": r["id"] for _, r in df_uni_all.iterrows()}
        usel = st.selectbox("Unidad", list(mapa_uni.keys()))
        df_area = pd.read_sql("SELECT * FROM areas WHERE unidad_id=? ORDER BY nombre", conn, params=(mapa_uni[usel],))
        st.dataframe(df_area, use_container_width=True)

        if editable:
            with st.form("f_area", clear_on_submit=True):
                na = st.text_input("Nueva Área")
                if st.form_submit_button("Agregar Área"):
                    if na.strip():
                        conn.execute("INSERT INTO areas(unidad_id,nombre) VALUES(?,?)", (mapa_uni[usel], na.strip()))
                        conn.commit()
                        do_rerun()

            if not df_area.empty:
                col1, col2 = st.columns(2)
                with col1:
                    aid = st.selectbox("Editar Área", df_area["id"], format_func=lambda x: df_area[df_area["id"]==x]["nombre"].values[0])
                    newa = st.text_input("Nuevo nombre área")
                    if st.button("📝 Guardar área"):
                        if newa.strip():
                            conn.execute("UPDATE areas SET nombre=? WHERE id=?", (newa.strip(), aid))
                            conn.commit()
                            do_rerun()
                with col2:
                    aid2 = st.selectbox("Eliminar Área", df_area["id"], format_func=lambda x: df_area[df_area["id"]==x]["nombre"].values[0], key="del_area")
                    if st.button("🗑️ Eliminar área"):
                        conn.execute("DELETE FROM areas WHERE id=?", (aid2,))
                        conn.commit()
                        do_rerun()

    st.divider()

    # Jefes
    st.subheader("Jefes")
    df_areas = pd.read_sql("""
        SELECT a.id,a.nombre,u.nombre AS unidad,d.nombre AS direccion
        FROM areas a
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre,a.nombre
    """, conn)
    if df_areas.empty:
        st.warning("Registra áreas antes de jefes.")
    else:
        mapa_area = {f"{r['direccion']} - {r['unidad']} - {r['nombre']}": r["id"] for _, r in df_areas.iterrows()}
        asel = st.selectbox("Área del jefe", list(mapa_area.keys()))
        df_j = pd.read_sql("""
            SELECT j.id,j.nombres,j.cargo,a.nombre AS area,u.nombre AS unidad,d.nombre AS direccion
            FROM jefes j
            JOIN areas a ON a.id=j.area_id
            JOIN unidades u ON u.id=a.unidad_id
            JOIN direcciones d ON d.id=u.direccion_id
            ORDER BY d.nombre,u.nombre,a.nombre,j.nombres
        """, conn)
        st.dataframe(df_j, use_container_width=True)

        if editable:
            with st.form("f_jefe", clear_on_submit=True):
                nj = st.text_input("Nombres del jefe")
                cj = st.text_input("Cargo")
                if st.form_submit_button("Guardar jefe"):
                    if nj.strip():
                        conn.execute("INSERT INTO jefes(nombres,cargo,area_id) VALUES(?,?,?)", (nj.strip(), cj.strip(), mapa_area[asel]))
                        conn.commit()
                        do_rerun()
                    else:
                        st.error("Nombre obligatorio")

            if not df_j.empty:
                col1, col2 = st.columns(2)
                with col1:
                    jid = st.selectbox("Editar Jefe", df_j["id"], format_func=lambda x: df_j[df_j["id"]==x]["nombres"].values[0])
                    newn = st.text_input("Nuevo nombre jefe")
                    newc = st.text_input("Nuevo cargo jefe")
                    if st.button("📝 Guardar jefe"):
                        if newn.strip():
                            conn.execute("UPDATE jefes SET nombres=?, cargo=? WHERE id=?", (newn.strip(), newc.strip(), jid))
                            conn.commit()
                            do_rerun()
                with col2:
                    jid2 = st.selectbox("Eliminar Jefe", df_j["id"], format_func=lambda x: df_j[df_j["id"]==x]["nombres"].values[0], key="del_jefe")
                    if st.button("🗑️ Eliminar jefe"):
                        conn.execute("DELETE FROM jefes WHERE id=?", (jid2,))
                        conn.commit()
                        do_rerun()

    conn.close()

# =========================================================
# Registrar Trabajador (con generación periodos) + editar/borrar
# =========================================================
elif menu == "Registrar Trabajador":
    if not can(PERMS, "trabajadores_ver"):
        st.error("Sin permiso.")
        st.stop()
    editable = can(PERMS, "trabajadores_editar")

    conn = get_conn()
    st.header("Trabajadores")

    # Alta
    if editable:
        df_area_ctx = pd.read_sql("""
            SELECT a.id,a.nombre AS area,u.nombre AS unidad,d.nombre AS direccion
            FROM areas a JOIN unidades u ON u.id=a.unidad_id
            JOIN direcciones d ON d.id=u.direccion_id
            ORDER BY d.nombre,u.nombre,a.nombre
        """, conn)
        df_jef = pd.read_sql("""
            SELECT j.id,j.nombres,a.nombre AS area,u.nombre AS unidad,d.nombre AS direccion
            FROM jefes j
            JOIN areas a ON a.id=j.area_id
            JOIN unidades u ON u.id=a.unidad_id
            JOIN direcciones d ON d.id=u.direccion_id
            ORDER BY d.nombre,u.nombre,a.nombre,j.nombres
        """, conn)

        if df_area_ctx.empty or df_jef.empty:
            st.warning("Primero registra Estructura y Jefes.")
        else:
            mapa_area = {f"{r['direccion']} - {r['unidad']} - {r['area']}": r["id"] for _, r in df_area_ctx.iterrows()}
            mapa_jef = {f"{r['direccion']} - {r['unidad']} - {r['area']} - {r['nombres']}": r["id"] for _, r in df_jef.iterrows()}

            with st.form("f_trab", clear_on_submit=True):
                numero = st.text_input("Número (opcional)")
                dni = st.text_input("DNI")
                nombres = st.text_input("Apellidos y Nombres")
                cargo = st.text_input("Cargo")
                regimen = st.selectbox("Régimen", ["DL 276","DL 728","DL 1057","Carrera Especial"])
                fi = st.date_input("Fecha ingreso")
                area_sel = st.selectbox("Área", list(mapa_area.keys()))
                jefe_sel = st.selectbox("Jefe", list(mapa_jef.keys()))
                if st.form_submit_button("Guardar"):
                    if not nombres.strip():
                        st.error("Nombre obligatorio.")
                    else:
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO trabajadores(numero,dni,nombres,cargo,regimen,fecha_ingreso,area_id,jefe_id)
                            VALUES(?,?,?,?,?,?,?,?)
                        """, (
                            numero.strip() if numero.strip() else None,
                            dni.strip() if dni.strip() else None,
                            nombres.strip(),
                            cargo.strip() if cargo.strip() else None,
                            regimen,
                            fi.strftime("%Y-%m-%d"),
                            mapa_area[area_sel],
                            mapa_jef[jefe_sel]
                        ))
                        tid = cur.lastrowid
                        conn.commit()
                        generar_periodos_para_trabajador(tid, fi.strftime("%Y-%m-%d"))
                        do_rerun()

    # Listado + búsqueda
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        qn = st.text_input("Buscar por nombre")
    with col2:
        qd = st.text_input("Buscar por DNI")

    df_trab = pd.read_sql("""
        SELECT t.id,t.numero,t.dni,t.nombres,t.cargo,t.regimen,t.fecha_ingreso,
               a.nombre AS area,u.nombre AS unidad,d.nombre AS direccion,
               j.nombres AS jefe
        FROM trabajadores t
        JOIN areas a ON a.id=t.area_id
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        LEFT JOIN jefes j ON j.id=t.jefe_id
        ORDER BY t.nombres
    """, conn)

    if qn:
        df_trab = df_trab[df_trab["nombres"].str.contains(qn, case=False, na=False)]
    if qd:
        df_trab = df_trab[df_trab["dni"].fillna("").str.contains(qd, na=False)]

    st.dataframe(df_trab, use_container_width=True)

    # Editar/borrar trabajador
    if editable and not df_trab.empty:
        st.subheader("Editar / Eliminar trabajador")
        tid = st.selectbox("Trabajador", df_trab["id"], format_func=lambda x: df_trab[df_trab["id"]==x]["nombres"].values[0])
        row = conn.execute("SELECT * FROM trabajadores WHERE id=?", (tid,)).fetchone()

        with st.form("edit_trab"):
            en_num = st.text_input("Número", value=row["numero"] or "")
            en_dni = st.text_input("DNI", value=row["dni"] or "")
            en_nom = st.text_input("Nombres", value=row["nombres"] or "")
            en_car = st.text_input("Cargo", value=row["cargo"] or "")
            en_reg = st.text_input("Régimen", value=row["regimen"] or "")
            en_fi = st.date_input("Fecha ingreso", value=to_date(row["fecha_ingreso"]))
            if st.form_submit_button("📝 Guardar cambios"):
                conn.execute("""
                    UPDATE trabajadores SET numero=?, dni=?, nombres=?, cargo=?, regimen=?, fecha_ingreso=?
                    WHERE id=?
                """, (
                    en_num.strip() if en_num.strip() else None,
                    en_dni.strip() if en_dni.strip() else None,
                    en_nom.strip(),
                    en_car.strip() if en_car.strip() else None,
                    en_reg.strip() if en_reg.strip() else None,
                    en_fi.strftime("%Y-%m-%d"),
                    tid
                ))
                conn.commit()
                generar_periodos_para_trabajador(tid, en_fi.strftime("%Y-%m-%d"))
                do_rerun()

        st.warning("Eliminar borrará periodos, resoluciones y vacaciones asociadas (por consistencia).")
        if st.button("🗑️ Eliminar trabajador"):
            conn.execute("DELETE FROM trabajadores WHERE id=?", (tid,))
            conn.commit()
            do_rerun()

        st.divider()
        st.subheader("Periodos del trabajador")
        dfp = periodos_trabajador_df(tid)
        if dfp.empty:
            st.info("Aún no tiene periodos completos.")
        else:
            hoy = datetime.date.today()
            dfp["dias_restantes"] = 30 - dfp["dias_usados"].astype(int)
            dfp["estado"] = dfp["acumulable_hasta"].apply(lambda x: "🔴 Vencido" if hoy > to_date(x) else ("🟡 Por vencer" if hoy > (to_date(x) - datetime.timedelta(days=60)) else "🟢 Vigente"))
            st.dataframe(dfp, use_container_width=True)

    conn.close()

# =========================================================
# Reporte trabajadores
# =========================================================
elif menu == "Reporte de Trabajadores":
    conn = get_conn()
    st.header("Reporte de Trabajadores")
    col1, col2 = st.columns(2)
    with col1:
        qn = st.text_input("Buscar por nombre", key="rep_nom")
    with col2:
        qd = st.text_input("Buscar por DNI", key="rep_dni")

    df = pd.read_sql("""
        SELECT t.nombres,t.dni,t.cargo,t.regimen,t.fecha_ingreso,
               a.nombre AS area,u.nombre AS unidad,d.nombre AS direccion,
               j.nombres AS jefe
        FROM trabajadores t
        JOIN areas a ON a.id=t.area_id
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        LEFT JOIN jefes j ON j.id=t.jefe_id
        ORDER BY t.nombres
    """, conn)

    if qn:
        df = df[df["nombres"].str.contains(qn, case=False, na=False)]
    if qd:
        df = df[df["dni"].fillna("").str.contains(qd, na=False)]

    st.dataframe(df, use_container_width=True)
    st.download_button("Descargar CSV", df.to_csv(index=False).encode("utf-8"), "trabajadores.csv", "text/csv")
    conn.close()

# =========================================================
# Resoluciones (con botones editar/borrar + inserción robusta)
# =========================================================
elif menu == "Resoluciones":
    if not can(PERMS, "resoluciones_ver"):
        st.error("Sin permiso.")
        st.stop()
    editable = can(PERMS, "resoluciones_editar")

    conn = get_conn()
    st.header("Resoluciones")

    df_t = pd.read_sql("SELECT id,nombres,fecha_ingreso FROM trabajadores ORDER BY nombres", conn)
    if df_t.empty:
        st.warning("No hay trabajadores.")
        conn.close()
        st.stop()

    mapa_t = dict(zip(df_t["nombres"], df_t["id"]))
    tsel = st.selectbox("Trabajador", list(mapa_t.keys()))
    tid = mapa_t[tsel]
    fecha_ing = df_t[df_t["id"]==tid]["fecha_ingreso"].values[0]
    generar_periodos_para_trabajador(tid, fecha_ing)

    dfp = periodos_trabajador_df(tid)
    if dfp.empty:
        st.info("Aún no tiene periodos completos.")
        conn.close()
        st.stop()

    mapa_p = {f"{r['inicio_ciclo']} a {r['fin_ciclo']} (saldo {30-int(r['dias_usados'])})": r["id"] for _, r in dfp.iterrows()}
    pid_label = st.selectbox("Periodo", list(mapa_p.keys()))
    pid = mapa_p[pid_label]

    if editable:
        st.subheader("Registrar resolución")
        with st.form("f_res", clear_on_submit=True):
            numero = st.text_input("Número")
            fi = st.date_input("Inicio autorizado")
            ff = st.date_input("Fin autorizado")
            da = st.number_input("Días autorizados", 1, 30, value=30)
            fr = st.checkbox("Fraccionable")
            mad = st.text_input("MAD (opcional)")
            ob = st.text_area("Observaciones")
            if st.form_submit_button("Registrar"):
                try:
                    insert_resolucion_robusta(
                        conn,
                        trabajador_id=tid,
                        periodo_id=pid,
                        numero=numero.strip(),
                        fecha_inicio=fi.strftime("%Y-%m-%d"),
                        fecha_fin=ff.strftime("%Y-%m-%d"),
                        dias_aut=int(da),
                        fraccionable=fr,
                        observaciones=ob.strip(),
                        mad=mad.strip() if mad.strip() else None
                    )
                    conn.commit()
                    st.success("Resolución registrada.")
                    do_rerun()
                except sqlite3.IntegrityError as e:
                    st.error("No se pudo registrar por restricciones de tu BD (NOT NULL). Ya lo corregimos para la mayoría de casos; si persiste, es porque hay otra columna obligatoria antigua.")
                    st.exception(e)

    st.divider()
    st.subheader("Resoluciones (vista compatible)")
    df_res = pd.read_sql("""
        SELECT * FROM resoluciones_norm
        WHERE trabajador_id=?
        ORDER BY date(COALESCE(fecha_inicio,'1900-01-01')) DESC
    """, conn, params=(tid,))
    st.dataframe(df_res, use_container_width=True)

    if editable and not df_res.empty:
        col1, col2 = st.columns(2)
        with col1:
            rid = st.selectbox("Editar resolución", df_res["id"], format_func=lambda x: df_res[df_res["id"]==x]["numero"].values[0])
            rrow = conn.execute("SELECT * FROM resoluciones WHERE id=?", (rid,)).fetchone()
            with st.form("edit_res"):
                en_num = st.text_input("Número", value=(rrow["numero"] or rrow.get("numero_resolucion") or ""))
                en_fi = st.date_input("Inicio", value=to_date(rrow["fecha_inicio"] or rrow.get("fecha_programada") or datetime.date.today().strftime("%Y-%m-%d")))
                en_ff = st.date_input("Fin", value=to_date(rrow["fecha_fin"] or rrow.get("fecha_programada") or datetime.date.today().strftime("%Y-%m-%d")))
                en_da = st.number_input("Días", 1, 30, value=safe_int(rrow["dias_autorizados"], 30))
                en_fr = st.checkbox("Fraccionable", value=bool(safe_int(rrow["fraccionable"], 0)))
                en_mad = st.text_input("MAD", value=(rrow["mad"] or ""))
                en_ob = st.text_area("Obs", value=(rrow["observaciones"] or ""))
                if st.form_submit_button("📝 Guardar cambios"):
                    # Actualizamos ambas columnas si existen
                    cols = table_columns(conn, "resoluciones")
                    if "numero" in cols:
                        conn.execute("UPDATE resoluciones SET numero=? WHERE id=?", (en_num.strip(), rid))
                    if "numero_resolucion" in cols:
                        conn.execute("UPDATE resoluciones SET numero_resolucion=? WHERE id=?", (en_num.strip(), rid))
                    if "fecha_inicio" in cols:
                        conn.execute("UPDATE resoluciones SET fecha_inicio=? WHERE id=?", (en_fi.strftime("%Y-%m-%d"), rid))
                    if "fecha_fin" in cols:
                        conn.execute("UPDATE resoluciones SET fecha_fin=? WHERE id=?", (en_ff.strftime("%Y-%m-%d"), rid))
                    if "fecha_programada" in cols:
                        conn.execute("UPDATE resoluciones SET fecha_programada=? WHERE id=?", (en_fi.strftime("%Y-%m-%d"), rid))
                    if "dias_autorizados" in cols:
                        conn.execute("UPDATE resoluciones SET dias_autorizados=? WHERE id=?", (int(en_da), rid))
                    if "fraccionable" in cols:
                        conn.execute("UPDATE resoluciones SET fraccionable=? WHERE id=?", (1 if en_fr else 0, rid))
                    if "mad" in cols:
                        conn.execute("UPDATE resoluciones SET mad=? WHERE id=?", (en_mad.strip() if en_mad.strip() else None, rid))
                    if "observaciones" in cols:
                        conn.execute("UPDATE resoluciones SET observaciones=? WHERE id=?", (en_ob.strip(), rid))
                    conn.commit()
                    do_rerun()

        with col2:
            rid2 = st.selectbox("Eliminar resolución", df_res["id"], format_func=lambda x: f"{df_res[df_res['id']==x]['numero'].values[0]} (ID {x})", key="del_res")
            st.warning("Si eliminas, las vacaciones quedarán sin resolucion_id (si existe FK se pondrá NULL o puede fallar si tu esquema antiguo lo impide).")
            if st.button("🗑️ Eliminar resolución"):
                conn.execute("DELETE FROM resoluciones WHERE id=?", (rid2,))
                conn.commit()
                do_rerun()

    conn.close()

# =========================================================
# Vacaciones (con edición/borrado + búsqueda en listados)
# =========================================================
elif menu == "Registrar Vacaciones":
    if not can(PERMS, "vacaciones_ver"):
        st.error("Sin permiso.")
        st.stop()
    editable = can(PERMS, "vacaciones_editar")

    conn = get_conn()
    st.header("Registrar Vacaciones")

    df_t = pd.read_sql("SELECT id,nombres,fecha_ingreso,dni FROM trabajadores ORDER BY nombres", conn)
    if df_t.empty:
        st.warning("No hay trabajadores.")
        conn.close()
        st.stop()

    mapa_t = {f"{r['nombres']} ({r['dni'] or ''})": r["id"] for _, r in df_t.iterrows()}
    tsel = st.selectbox("Trabajador", list(mapa_t.keys()))
    tid = mapa_t[tsel]

    fecha_ing = df_t[df_t["id"]==tid]["fecha_ingreso"].values[0]
    generar_periodos_para_trabajador(tid, fecha_ing)

    dfp = periodos_trabajador_df(tid)
    if dfp.empty:
        st.warning("No hay periodos completos.")
        conn.close()
        st.stop()

    # filtrar periodos no vencidos
    hoy = datetime.date.today()
    dfp["vigente"] = dfp["acumulable_hasta"].apply(lambda x: hoy <= to_date(x))
    dfp = dfp[dfp["vigente"] == True]
    if dfp.empty:
        st.warning("Todos los periodos están vencidos.")
        conn.close()
        st.stop()

    mapa_p = {f"{r['inicio_ciclo']} a {r['fin_ciclo']} | saldo {30-int(r['dias_usados'])} | acum {r['acumulable_hasta']}": r["id"] for _, r in dfp.iterrows()}
    psel = st.selectbox("Periodo", list(mapa_p.keys()))
    pid = mapa_p[psel]

    usar_res = st.checkbox("Usar resolución", value=True)
    resolucion_id = None
    modo_res = None

    if usar_res:
        df_res = pd.read_sql("""
            SELECT * FROM resoluciones_norm
            WHERE trabajador_id=?
            ORDER BY date(COALESCE(fecha_inicio,'1900-01-01')) DESC
        """, conn, params=(tid,))
        if df_res.empty:
            st.error("No hay resoluciones para este trabajador.")
            conn.close()
            st.stop()

        mapa_res = {f"{r['numero']} | {r['dias_autorizados']} días | Fracc: {'Sí' if int(r['fraccionable'] or 0)==1 else 'No'}": r["id"] for _, r in df_res.iterrows()}
        rsel = st.selectbox("Resolución", list(mapa_res.keys()))
        resolucion_id = mapa_res[rsel]
        saldo_res = dias_resolucion_restantes(resolucion_id)
        st.info(f"Saldo resolución: {saldo_res} días")

        fracc_ok = int(df_res[df_res["id"]==resolucion_id]["fraccionable"].values[0] or 0) == 1
        modo_res = st.radio("Ejecución", ["Íntegra"] + (["Fraccionada"] if fracc_ok else []), horizontal=True)
        tipo = "Resolución" if modo_res == "Íntegra" else st.selectbox("Sustento fraccionamiento", ["Memorando","Solicitud"])
    else:
        tipo = st.selectbox("Tipo", ["Memorando","Solicitud"])

    documento = st.text_input("Documento (N°)")
    dias = st.number_input("Días", 1, 30, value=7)
    fi = st.date_input("Inicio")
    ff = fi + datetime.timedelta(days=int(dias)-1)
    st.caption(f"Fin calculado: {ff}")

    saldo_p = dias_periodo_restantes(pid)
    st.info(f"Saldo periodo: {saldo_p} días")

    if editable:
        if st.button("Registrar Vacación"):
            if int(dias) > saldo_p:
                st.error("Excede saldo del periodo.")
            elif usar_res and resolucion_id is not None and int(dias) > dias_resolucion_restantes(resolucion_id):
                st.error("Excede saldo de resolución.")
            else:
                insert_vacacion_robusta(
                    conn,
                    trabajador_id=tid,
                    periodo_id=pid,
                    resolucion_id=resolucion_id,
                    tipo=tipo,
                    modo_resolucion=("Integra" if usar_res and modo_res=="Íntegra" else ("Fraccionada" if usar_res else None)),
                    fecha_inicio=fi.strftime("%Y-%m-%d"),
                    fecha_fin=ff.strftime("%Y-%m-%d"),
                    dias=int(dias),
                    documento=documento.strip() if documento.strip() else None,
                    observaciones=None
                )
                conn.commit()
                do_rerun()

    st.divider()
    st.subheader("Vacaciones registradas (búsqueda + editar/borrar)")

    col1, col2, col3 = st.columns(3)
    with col1:
        b_doc = st.text_input("Buscar por documento", key="b_doc")
    with col2:
        b_tipo = st.selectbox("Filtrar tipo", ["Todos","Solicitud","Memorando","Resolución"], key="b_tipo")
    with col3:
        b_rrhh = st.selectbox("Filtrar RRHH", ["Todos","Pendientes","Aprobadas"], key="b_rrhh")

    df_v = pd.read_sql("""
        SELECT v.*, r.numero AS resolucion_num
        FROM vacaciones_norm v
        LEFT JOIN resoluciones_norm r ON r.id=v.resolucion_id
        WHERE v.trabajador_id=? AND v.periodo_id=?
        ORDER BY date(v.fecha_inicio) DESC
    """, conn, params=(tid, pid))

    if b_doc:
        df_v = df_v[df_v["documento"].fillna("").str.contains(b_doc, case=False, na=False)]
    if b_tipo != "Todos":
        df_v = df_v[df_v["tipo"] == b_tipo]
    if b_rrhh == "Pendientes":
        df_v = df_v[df_v["autorizado_rrhh"] == 0]
    elif b_rrhh == "Aprobadas":
        df_v = df_v[df_v["autorizado_rrhh"] == 1]

    st.dataframe(df_v, use_container_width=True)

    st.download_button("Descargar CSV (Vacaciones del periodo)", df_v.to_csv(index=False).encode("utf-8"), "vacaciones_periodo.csv", "text/csv")

    if editable and not df_v.empty:
        col1, col2 = st.columns(2)
        with col1:
            vid = st.selectbox("Editar vacación", df_v["id"], format_func=lambda x: f"ID {x} - {df_v[df_v['id']==x]['fecha_inicio'].values[0]}", key="edit_v")
            vrow = conn.execute("SELECT * FROM vacaciones WHERE id=?", (vid,)).fetchone()
            with st.form("edit_vac"):
                e_fi = st.date_input("Inicio", value=to_date(vrow["fecha_inicio"]))
                e_d = st.number_input("Días", 1, 30, value=safe_int(vrow["dias"], 1))
                e_ff = e_fi + datetime.timedelta(days=int(e_d)-1)
                e_doc = st.text_input("Documento", value=vrow["documento"] or "")
                e_obs = st.text_area("Obs", value=vrow["observaciones"] or "")
                if st.form_submit_button("📝 Guardar cambios"):
                    conn.execute("""
                        UPDATE vacaciones SET fecha_inicio=?, fecha_fin=?, dias=?, documento=?, observaciones=?
                        WHERE id=?
                    """, (e_fi.strftime("%Y-%m-%d"), e_ff.strftime("%Y-%m-%d"), int(e_d), e_doc.strip() if e_doc.strip() else None, e_obs.strip(), vid))
                    conn.commit()
                    do_rerun()
        with col2:
            vid2 = st.selectbox("Eliminar vacación", df_v["id"], format_func=lambda x: f"ID {x}", key="del_v")
            if st.button("🗑️ Eliminar vacación"):
                conn.execute("DELETE FROM vacaciones WHERE id=?", (vid2,))
                conn.commit()
                do_rerun()

    conn.close()

# =========================================================
# Adelanto de Vacaciones (CRUD + RRHH)
# =========================================================
elif menu == "Adelanto de Vacaciones":
    if not can(PERMS, "adelantos_ver"):
        st.error("Sin permiso.")
        st.stop()
    editable = can(PERMS, "adelantos_editar")

    conn = get_conn()
    st.header("Adelanto de Vacaciones (con acuerdo)")

    df_t = pd.read_sql("SELECT id,nombres,fecha_ingreso,dni FROM trabajadores ORDER BY nombres", conn)
    if df_t.empty:
        st.warning("No hay trabajadores.")
        conn.close()
        st.stop()

    mapa_t = {f"{r['nombres']} ({r['dni'] or ''})": r["id"] for _, r in df_t.iterrows()}
    tsel = st.selectbox("Trabajador", list(mapa_t.keys()))
    tid = mapa_t[tsel]
    fecha_ing = df_t[df_t["id"]==tid]["fecha_ingreso"].values[0]

    ini, fin, max_dias = ciclo_en_curso(fecha_ing)
    st.info(f"Ciclo en curso: {ini} a {fin} | Máximo proporcional: {max_dias} días")

    if editable:
        with st.form("f_adel", clear_on_submit=True):
            dias_sol = st.number_input("Días solicitados", 0, 30, value=min(5, max_dias))
            doc = st.text_input("Documento de acuerdo (obligatorio)")
            obs = st.text_area("Observaciones")
            if st.form_submit_button("Registrar solicitud"):
                if not doc.strip():
                    st.error("Documento obligatorio.")
                elif int(dias_sol) > max_dias:
                    st.error("Excede máximo proporcional.")
                else:
                    conn.execute("""
                        INSERT INTO adelantos(trabajador_id,fecha_solicitud,ciclo_inicio,ciclo_fin,dias_maximo,dias_solicitados,documento_acuerdo,observaciones,aprobado_rrhh)
                        VALUES(?,?,?,?,?,?,?,?,0)
                    """, (
                        tid,
                        datetime.date.today().strftime("%Y-%m-%d"),
                        ini.strftime("%Y-%m-%d"),
                        fin.strftime("%Y-%m-%d"),
                        int(max_dias),
                        int(dias_sol),
                        doc.strip(),
                        obs.strip(),
                    ))
                    conn.commit()
                    do_rerun()

    st.divider()
    st.subheader("Solicitudes registradas")
    df_a = pd.read_sql("""
        SELECT * FROM adelantos
        WHERE trabajador_id=?
        ORDER BY date(fecha_solicitud) DESC
    """, conn, params=(tid,))
    st.dataframe(df_a, use_container_width=True)

    if editable and not df_a.empty:
        col1, col2 = st.columns(2)
        with col1:
            aid = st.selectbox("Editar solicitud", df_a["id"], format_func=lambda x: f"ID {x} - {df_a[df_a['id']==x]['fecha_solicitud'].values[0]}")
            arow = conn.execute("SELECT * FROM adelantos WHERE id=?", (aid,)).fetchone()
            with st.form("edit_adel"):
                e_d = st.number_input("Días solicitados", 0, 30, value=safe_int(arow["dias_solicitados"], 0))
                e_doc = st.text_input("Documento acuerdo", value=arow["documento_acuerdo"] or "")
                e_obs = st.text_area("Obs", value=arow["observaciones"] or "")
                if st.form_submit_button("📝 Guardar cambios"):
                    conn.execute("""
                        UPDATE adelantos SET dias_solicitados=?, documento_acuerdo=?, observaciones=?
                        WHERE id=?
                    """, (int(e_d), e_doc.strip(), e_obs.strip(), aid))
                    conn.commit()
                    do_rerun()
        with col2:
            aid2 = st.selectbox("Eliminar solicitud", df_a["id"], format_func=lambda x: f"ID {x}", key="del_adel")
            if st.button("🗑️ Eliminar solicitud"):
                conn.execute("DELETE FROM adelantos WHERE id=?", (aid2,))
                conn.commit()
                do_rerun()

    conn.close()

# =========================================================
# Panel RRHH (vacaciones + adelantos)
# =========================================================
elif menu == "Panel RRHH":
    if not can(PERMS, "rrhh_aprobar"):
        st.error("Sin permiso.")
        st.stop()

    conn = get_conn()
    st.header("Panel RRHH")

    st.subheader("Vacaciones pendientes")
    df_p = pd.read_sql("""
        SELECT v.id, t.nombres AS trabajador, v.tipo, v.fecha_inicio, v.fecha_fin, v.dias, v.documento, v.rrhh_observacion
        FROM vacaciones_norm v
        JOIN trabajadores t ON t.id=v.trabajador_id
        WHERE v.autorizado_rrhh=0
        ORDER BY date(v.fecha_inicio) DESC
    """, conn)
    st.dataframe(df_p, use_container_width=True)

    if not df_p.empty:
        vid = st.selectbox("Vacación", df_p["id"], format_func=lambda x: f"ID {x} - {df_p[df_p['id']==x]['trabajador'].values[0]}")
        obs = st.text_area("Observación RRHH", key="obs_rrhh_v")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Aprobar vacación"):
                conn.execute("""
                    UPDATE vacaciones SET autorizado_rrhh=1, rrhh_observacion=?, fecha_aprob_rrhh=?, usuario_rrhh=?
                    WHERE id=?
                """, (obs.strip(), datetime.date.today().strftime("%Y-%m-%d"), USER, vid))
                conn.commit()
                do_rerun()
        with col2:
            if st.button("📝 Guardar observación"):
                conn.execute("UPDATE vacaciones SET rrhh_observacion=? WHERE id=?", (obs.strip(), vid))
                conn.commit()
                do_rerun()

    st.divider()
    st.subheader("Adelantos pendientes")
    df_ap = pd.read_sql("""
        SELECT a.id, t.nombres AS trabajador, a.fecha_solicitud, a.ciclo_inicio, a.ciclo_fin, a.dias_solicitados, a.documento_acuerdo, a.rrhh_observacion
        FROM adelantos a
        JOIN trabajadores t ON t.id=a.trabajador_id
        WHERE a.aprobado_rrhh=0
        ORDER BY date(a.fecha_solicitud) DESC
    """, conn)
    st.dataframe(df_ap, use_container_width=True)

    if not df_ap.empty:
        aid = st.selectbox("Adelanto", df_ap["id"], format_func=lambda x: f"ID {x} - {df_ap[df_ap['id']==x]['trabajador'].values[0]}")
        obs2 = st.text_area("Obs RRHH (adelanto)", key="obs_rrhh_a")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Aprobar adelanto"):
                conn.execute("""
                    UPDATE adelantos SET aprobado_rrhh=1, rrhh_observacion=?, fecha_aprob_rrhh=?, usuario_rrhh=?
                    WHERE id=?
                """, (obs2.strip(), datetime.date.today().strftime("%Y-%m-%d"), USER, aid))
                conn.commit()
                do_rerun()
        with col2:
            if st.button("📝 Guardar observación (adelanto)"):
                conn.execute("UPDATE adelantos SET rrhh_observacion=? WHERE id=?", (obs2.strip(), aid))
                conn.commit()
                do_rerun()

    conn.close()

# =========================================================
# Dashboard (semaforizado)
# =========================================================
elif menu == "Dashboard":
    conn = get_conn()
    st.header("Dashboard")
    hoy = datetime.date.today()
    inicio_mes = hoy.replace(day=1)
    fin_mes = (inicio_mes.replace(month=inicio_mes.month % 12 + 1, year=inicio_mes.year + (1 if inicio_mes.month == 12 else 0)) - datetime.timedelta(days=1))

    df_goce = pd.read_sql("""
        SELECT t.nombres AS trabajador, v.fecha_inicio, v.fecha_fin, v.dias, v.tipo, v.autorizado_rrhh
        FROM vacaciones_norm v
        JOIN trabajadores t ON t.id=v.trabajador_id
        WHERE date(v.fecha_inicio) <= date(?) AND date(v.fecha_fin) >= date(?)
        ORDER BY date(v.fecha_inicio)
    """, conn, params=(fin_mes.strftime("%Y-%m-%d"), inicio_mes.strftime("%Y-%m-%d")))

    dfp = pd.read_sql("""
        SELECT p.*, t.nombres AS trabajador,
               IFNULL((SELECT SUM(dias) FROM vacaciones_norm v WHERE v.periodo_id=p.id),0) AS dias_usados
        FROM periodos p
        JOIN trabajadores t ON t.id=p.trabajador_id
        ORDER BY t.nombres, p.inicio_ciclo
    """, conn)

    if not dfp.empty:
        dfp["estado"] = dfp["acumulable_hasta"].apply(lambda x: "🔴 Vencido" if hoy > to_date(x) else ("🟡 Por vencer" if hoy > (to_date(x) - datetime.timedelta(days=60)) else "🟢 Vigente"))
        dfp["dias_restantes"] = 30 - dfp["dias_usados"].astype(int)

    c1, c2, c3 = st.columns(3)
    c1.metric("🟢 Vacaciones en goce (mes)", int(df_goce.shape[0]))
    c2.metric("🟡 Periodos por vencer", int((dfp["estado"]=="🟡 Por vencer").sum()) if not dfp.empty else 0)
    c3.metric("🔴 Periodos vencidos", int((dfp["estado"]=="🔴 Vencido").sum()) if not dfp.empty else 0)

    st.divider()
    st.subheader("Vacaciones en goce del mes")
    st.dataframe(df_goce, use_container_width=True)

    st.divider()
    st.subheader("Semáforo de periodos")
    st.dataframe(dfp, use_container_width=True)

    conn.close()

# =========================================================
# Reportes (filtros y búsqueda restaurados)
# =========================================================
elif menu == "Reportes":
    conn = get_conn()
    st.header("Reportes")

    colA, colB, colC = st.columns(3)
    with colA:
        b_nom = st.text_input("Buscar trabajador por nombre")
    with colB:
        b_dni = st.text_input("Buscar por DNI")
    with colC:
        tipo_sel = st.selectbox("Tipo", ["Todos","Solicitud","Memorando","Resolución"])

    rrhh_sel = st.selectbox("Estado RRHH", ["Todos","Pendientes","Aprobadas"])

    q = """
        SELECT v.*, t.nombres AS trabajador, t.dni, r.numero AS resolucion_num,
               p.inicio_ciclo, p.fin_ciclo
        FROM vacaciones_norm v
        JOIN trabajadores t ON t.id=v.trabajador_id
        LEFT JOIN resoluciones_norm r ON r.id=v.resolucion_id
        LEFT JOIN periodos p ON p.id=v.periodo_id
        WHERE 1=1
    """
    params = []

    if tipo_sel != "Todos":
        q += " AND v.tipo=?"
        params.append(tipo_sel)

    if rrhh_sel == "Pendientes":
        q += " AND v.autorizado_rrhh=0"
    elif rrhh_sel == "Aprobadas":
        q += " AND v.autorizado_rrhh=1"

    df_v = pd.read_sql(q + " ORDER BY date(v.fecha_inicio) DESC", conn, params=params)

    if b_nom:
        df_v = df_v[df_v["trabajador"].str.contains(b_nom, case=False, na=False)]
    if b_dni:
        df_v = df_v[df_v["dni"].fillna("").str.contains(b_dni, na=False)]

    st.dataframe(df_v, use_container_width=True)
    st.download_button("Descargar CSV (Vacaciones)", df_v.to_csv(index=False).encode("utf-8"), "reporte_vacaciones.csv", "text/csv")

    st.divider()
    st.subheader("Resoluciones")
    df_r = pd.read_sql("""
        SELECT r.*, t.nombres AS trabajador
        FROM resoluciones_norm r
        LEFT JOIN trabajadores t ON t.id=r.trabajador_id
        ORDER BY date(COALESCE(r.fecha_inicio,'1900-01-01')) DESC
    """, conn)
    if b_nom:
        df_r = df_r[df_r["trabajador"].fillna("").str.contains(b_nom, case=False, na=False)]
    st.dataframe(df_r, use_container_width=True)
    st.download_button("Descargar CSV (Resoluciones)", df_r.to_csv(index=False).encode("utf-8"), "reporte_resoluciones.csv", "text/csv")

    st.divider()
    st.subheader("Adelantos")
    df_a = pd.read_sql("""
        SELECT a.*, t.nombres AS trabajador, t.dni
        FROM adelantos a
        JOIN trabajadores t ON t.id=a.trabajador_id
        ORDER BY date(a.fecha_solicitud) DESC
    """, conn)
    if b_nom:
        df_a = df_a[df_a["trabajador"].str.contains(b_nom, case=False, na=False)]
    if b_dni:
        df_a = df_a[df_a["dni"].fillna("").str.contains(b_dni, na=False)]
    st.dataframe(df_a, use_container_width=True)
    st.download_button("Descargar CSV (Adelantos)", df_a.to_csv(index=False).encode("utf-8"), "reporte_adelantos.csv", "text/csv")

    conn.close()

# =========================================================
# Usuarios y permisos (granular)
# =========================================================
elif menu == "Usuarios y Permisos":
    if not can(PERMS, "usuarios_admin"):
        st.error("Sin permiso.")
        st.stop()

    conn = get_conn()
    st.header("Usuarios y Permisos")

    st.subheader("Permisos por rol (referencia)")
    st.write("- **admin**: todo.")
    st.write("- **responsable**: opera RRHH y gestión (sin administrar usuarios/reset).")
    st.write("- **registrador**: registra (sin RRHH, sin dashboard).")

    df_u = pd.read_sql("SELECT id,usuario,rol,permisos_json FROM usuarios ORDER BY usuario", conn)
    st.dataframe(df_u[["id","usuario","rol"]], use_container_width=True)

    st.divider()
    st.subheader("Crear usuario")
    with st.form("f_user", clear_on_submit=True):
        u = st.text_input("Usuario")
        p = st.text_input("Contraseña", type="password")
        r = st.selectbox("Rol", ["admin","responsable","registrador"])
        if st.form_submit_button("Crear"):
            if not u.strip() or not p.strip():
                st.error("Usuario y contraseña obligatorios.")
            else:
                conn.execute("INSERT INTO usuarios(usuario,password_hash,rol,permisos_json) VALUES(?,?,?,?)",
                             (u.strip(), hash_password(p.strip()), r, None))
                conn.commit()
                do_rerun()

    st.divider()
    st.subheader("Editar permisos por usuario (granular)")
    uid = st.selectbox("Seleccione usuario", df_u["id"], format_func=lambda x: df_u[df_u["id"]==x]["usuario"].values[0])
    urow = conn.execute("SELECT * FROM usuarios WHERE id=?", (uid,)).fetchone()

    base = ROLE_DEFAULT_PERMS.get(urow["rol"], ROLE_DEFAULT_PERMS["registrador"]).copy()
    current = base.copy()
    if urow["permisos_json"]:
        try:
            ov = json.loads(urow["permisos_json"])
            if isinstance(ov, dict):
                for k in current:
                    if k in ov:
                        current[k] = bool(ov[k])
        except Exception:
            pass

    st.caption("Marca/desmarca permisos. Se guardan en permisos_json para ese usuario.")
    new_perm = {}
    cols = st.columns(3)
    for i, k in enumerate(PERM_KEYS):
        with cols[i % 3]:
            new_perm[k] = st.checkbox(k, value=current.get(k, False), key=f"perm_{k}")

    if st.button("💾 Guardar permisos"):
        conn.execute("UPDATE usuarios SET permisos_json=? WHERE id=?", (json.dumps(new_perm), uid))
        conn.commit()
        st.success("Permisos guardados.")
        do_rerun()

    st.divider()
    st.subheader("Reset password / Cambiar rol / Eliminar")
    col1, col2, col3 = st.columns(3)
    with col1:
        newpass = st.text_input("Nueva contraseña", type="password")
        if st.button("Reset password"):
            if not newpass.strip():
                st.error("Contraseña obligatoria.")
            else:
                conn.execute("UPDATE usuarios SET password_hash=? WHERE id=?", (hash_password(newpass.strip()), uid))
                conn.commit()
                do_rerun()
    with col2:
        newrol = st.selectbox("Nuevo rol", ["admin","responsable","registrador"], index=["admin","responsable","registrador"].index(urow["rol"]))
        if st.button("Cambiar rol"):
            conn.execute("UPDATE usuarios SET rol=? WHERE id=?", (newrol, uid))
            conn.commit()
            do_rerun()
    with col3:
        if st.button("Eliminar usuario"):
            if urow["usuario"] == "admin":
                st.error("No se puede eliminar admin principal.")
            else:
                conn.execute("DELETE FROM usuarios WHERE id=?", (uid,))
                conn.commit()
                do_rerun()

    conn.close()

# =========================================================
# Exportar / Backup + debug esquema
# =========================================================
elif menu == "Exportar / Backup":
    if not can(PERMS, "backup_exportar"):
        st.error("Sin permiso.")
        st.stop()

    conn = get_conn()
    st.header("Exportar / Backup")

    tablas = ["usuarios","direcciones","unidades","areas","jefes","trabajadores","periodos","resoluciones","vacaciones","adelantos"]
    tsel = st.selectbox("Tabla", tablas)
    df = pd.read_sql(f"SELECT * FROM {tsel}", conn)
    st.dataframe(df, use_container_width=True)
    st.download_button(f"Descargar {tsel}.csv", df.to_csv(index=False).encode("utf-8"), f"{tsel}.csv", "text/csv")

    st.divider()
    st.subheader("Descargar Base de Datos (SQLite)")
    if os.path.exists(DB_PATH):
        with open(DB_PATH, "rb") as f:
            data = f.read()
        st.download_button("Descargar vacaciones.db", data, "vacaciones.db", "application/x-sqlite3")

    st.divider()
    st.subheader("Diagnóstico de esquema (para depurar errores)")
    for t in ["resoluciones","vacaciones"]:
        if table_exists(conn, t):
            info = table_info(conn, t)
            st.write(f"**{t}** columnas:")
            st.json([{ "name": r["name"], "type": r["type"], "notnull": r["notnull"], "dflt_value": r["dflt_value"] } for r in info])

    conn.close()

# =========================================================
# Reset con confirmación escrita
# =========================================================
elif menu == "Reset del Sistema":
    if not can(PERMS, "reset_sistema"):
        st.error("Sin permiso.")
        st.stop()

    st.header("Reset del Sistema (PELIGRO)")
    st.warning("Esta acción eliminará TODA la información (tablas principales).")
    st.warning("Recomendado: primero usa Exportar/Backup para descargar la DB.")

    confirm = st.text_input("Escriba exactamente: RESET TOTAL")
    check = st.checkbox("Confirmo que deseo resetear el sistema")

    if st.button("RESET TOTAL"):
        if confirm.strip() != "RESET TOTAL" or not check:
            st.error("Confirmación incorrecta.")
        else:
            conn = get_conn()
            cur = conn.cursor()
            # borrado seguro
            for obj in ["vacaciones_norm","resoluciones_norm"]:
                safe_drop_object(conn, obj)
            for t in ["vacaciones","resoluciones","periodos","trabajadores","jefes","areas","unidades","direcciones","adelantos","usuarios"]:
                cur.execute(f"DROP TABLE IF EXISTS {t}")
            conn.commit()
            conn.close()
            ensure_schema()
            st.success("Sistema reseteado. Usuario admin/admin restaurado.")
            do_rerun()
