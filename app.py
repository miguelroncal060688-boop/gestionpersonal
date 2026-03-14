import streamlit as st
import sqlite3
import pandas as pd
import datetime
import hashlib
import os
import json
from typing import Dict, Any

DB_PATH = "vacaciones.db"

# =========================================================
# Streamlit rerun compatible
# =========================================================
def do_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    st.stop()

# =========================================================
# DB
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def table_info(conn, table: str):
    return conn.execute(f"PRAGMA table_info({table})").fetchall()

def table_columns(conn, table: str) -> Dict[str, Dict[str, Any]]:
    cols = {}
    for r in table_info(conn, table):
        cols[r["name"]] = {"type": r["type"], "notnull": r["notnull"], "dflt_value": r["dflt_value"]}
    return cols

def add_column_if_missing(conn, table: str, col: str, col_def_sql: str):
    cols = set(table_columns(conn, table).keys())
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}")

def object_type(conn, name: str):
    r = conn.execute("SELECT type FROM sqlite_master WHERE name=?", (name,)).fetchone()
    return r["type"] if r else None

def safe_drop_object(conn, name: str):
    t = object_type(conn, name)
    if t == "view":
        conn.execute(f"DROP VIEW IF EXISTS {name}")
    elif t == "table":
        conn.execute(f"DROP TABLE IF EXISTS {name}")

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

def days_inclusive(d1: datetime.date, d2: datetime.date) -> int:
    if d2 < d1:
        return 0
    return (d2 - d1).days + 1

# =========================================================
# Permisos granulares
# =========================================================
PERM_KEYS = [
    "estructura_ver","estructura_editar",
    "trabajadores_ver","trabajadores_editar",
    "resoluciones_ver","resoluciones_editar",
    "vacaciones_ver","vacaciones_editar",
    "adelantos_ver","adelantos_editar",
    "rrhh_aprobar",
    "dashboard_ver",
    "reportes_ver",
    "usuarios_admin",
    "backup_exportar",
    "reset_sistema",
]

ROLE_DEFAULT_PERMS = {
    "admin": {k: True for k in PERM_KEYS},
    "responsable": {
        "estructura_ver": True, "estructura_editar": True,
        "trabajadores_ver": True, "trabajadores_editar": True,
        "resoluciones_ver": True, "resoluciones_editar": True,
        "vacaciones_ver": True, "vacaciones_editar": True,
        "adelantos_ver": True, "adelantos_editar": True,
        "rrhh_aprobar": True,
        "dashboard_ver": True,
        "reportes_ver": True,
        "usuarios_admin": False,
        "backup_exportar": True,
        "reset_sistema": False,
    },
    "registrador": {
        "estructura_ver": False, "estructura_editar": False,
        "trabajadores_ver": True, "trabajadores_editar": True,
        "resoluciones_ver": True, "resoluciones_editar": True,
        "vacaciones_ver": True, "vacaciones_editar": True,
        "adelantos_ver": True, "adelantos_editar": True,
        "rrhh_aprobar": False,
        "dashboard_ver": False,
        "reportes_ver": True,
        "usuarios_admin": False,
        "backup_exportar": False,
        "reset_sistema": False,
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
# Periodos (regla correcta)
# =========================================================
def generar_periodos_para_trabajador(trabajador_id: int, fecha_ingreso_str: str):
    """
    Ciclo: ingreso -> +1 año -1 día
    Goce hasta: fin_ciclo +1 año
    Acumulable hasta: fin_ciclo +2 años
    Solo ciclos completados (fin_ciclo < hoy)
    """
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
        """, (trabajador_id, from_date(inicio), from_date(fin_ciclo), from_date(goce_hasta), from_date(acumulable_hasta)))

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

# =========================================================
# Adelanto proporcional (ciclo en curso)
# =========================================================
def ciclo_en_curso(fecha_ingreso_str: str):
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
# Migración + vistas + normalización de NULLs
# =========================================================
def ensure_schema():
    conn = get_conn()
    cur = conn.cursor()

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
        nombre TEXT NOT NULL,
        FOREIGN KEY(direccion_id) REFERENCES direcciones(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS areas(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unidad_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        FOREIGN KEY(unidad_id) REFERENCES unidades(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS jefes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombres TEXT NOT NULL,
        cargo TEXT,
        area_id INTEGER NOT NULL,
        FOREIGN KEY(area_id) REFERENCES areas(id) ON DELETE CASCADE
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
        jefe_id INTEGER,
        FOREIGN KEY(area_id) REFERENCES areas(id) ON DELETE CASCADE,
        FOREIGN KEY(jefe_id) REFERENCES jefes(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS periodos(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        inicio_ciclo TEXT NOT NULL,
        fin_ciclo TEXT NOT NULL,
        goce_hasta TEXT NOT NULL,
        acumulable_hasta TEXT NOT NULL,
        UNIQUE(trabajador_id, inicio_ciclo),
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE
    );

    -- NUEVO MODELO RESOLUCIONES: cabecera + detalle
    CREATE TABLE IF NOT EXISTS resolucion_cab(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT NOT NULL,
        fecha_emision TEXT,
        fraccionable INTEGER DEFAULT 0,
        mad TEXT,
        observaciones TEXT
    );

    CREATE TABLE IF NOT EXISTS resolucion_det(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cab_id INTEGER NOT NULL,
        trabajador_id INTEGER NOT NULL,
        periodo_id INTEGER NOT NULL,
        fecha_inicio TEXT NOT NULL,
        fecha_fin TEXT NOT NULL,
        dias_autorizados INTEGER NOT NULL,
        mad TEXT,
        observaciones TEXT,
        FOREIGN KEY(cab_id) REFERENCES resolucion_cab(id) ON DELETE CASCADE,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE,
        FOREIGN KEY(periodo_id) REFERENCES periodos(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS vacaciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        periodo_id INTEGER,
        resolucion_det_id INTEGER,
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
        usuario_rrhh TEXT,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE,
        FOREIGN KEY(periodo_id) REFERENCES periodos(id) ON DELETE CASCADE,
        FOREIGN KEY(resolucion_det_id) REFERENCES resolucion_det(id) ON DELETE SET NULL
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
        usuario_rrhh TEXT,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE
    );
    """)

    # columnas legacy si venías de versiones anteriores
    add_column_if_missing(conn, "usuarios", "permisos_json", "permisos_json TEXT")
    add_column_if_missing(conn, "trabajadores", "numero", "numero TEXT")
    add_column_if_missing(conn, "vacaciones", "resolucion_det_id", "resolucion_det_id INTEGER")

    # Normalizar NULLs RRHH → 0 para que “Aprobadas” funcione
    conn.execute("UPDATE vacaciones SET autorizado_rrhh=0 WHERE autorizado_rrhh IS NULL")

    conn.commit()

    # Admin inicial
    c = conn.execute("SELECT COUNT(*) AS c FROM usuarios").fetchone()["c"]
    if c == 0:
        conn.execute("INSERT INTO usuarios(usuario,password_hash,rol,permisos_json) VALUES(?,?,?,?)",
                     ("admin", hash_password("admin"), "admin", None))
        conn.commit()

    # Vistas robustas
    safe_drop_object(conn, "vacaciones_norm")
    safe_drop_object(conn, "resoluciones_norm")

    conn.execute("""
        CREATE VIEW vacaciones_norm AS
        SELECT
            id, trabajador_id, periodo_id, resolucion_det_id,
            tipo, modo_resolucion, fecha_inicio, fecha_fin, dias,
            documento, mad, observaciones,
            COALESCE(autorizado_rrhh,0) AS autorizado_rrhh,
            rrhh_observacion, fecha_aprob_rrhh, usuario_rrhh
        FROM vacaciones
    """)

    # Vista resoluciones_norm: detalle + cabecera
    conn.execute("""
        CREATE VIEW resoluciones_norm AS
        SELECT
            d.id AS det_id,
            c.id AS cab_id,
            c.numero,
            c.fecha_emision,
            c.fraccionable,
            d.trabajador_id,
            d.periodo_id,
            d.fecha_inicio,
            d.fecha_fin,
            d.dias_autorizados,
            COALESCE(d.mad, c.mad) AS mad,
            COALESCE(d.observaciones, c.observaciones) AS observaciones
        FROM resolucion_det d
        JOIN resolucion_cab c ON c.id=d.cab_id
    """)

    conn.commit()
    conn.close()

# =========================================================
# Numero automático trabajador
# =========================================================
def siguiente_numero_trabajador() -> str:
    conn = get_conn()
    r = conn.execute("SELECT IFNULL(MAX(CAST(numero AS INTEGER)),0)+1 AS n FROM trabajadores").fetchone()
    conn.close()
    return str(r["n"])

# =========================================================
# Saldo resolución detalle
# =========================================================
def dias_res_det_restantes(det_id: int) -> int:
    conn = get_conn()
    r = conn.execute("SELECT dias_autorizados FROM resolucion_det WHERE id=?", (det_id,)).fetchone()
    if not r:
        conn.close()
        return 0
    autorizados = safe_int(r["dias_autorizados"], 0)
    usados = conn.execute("SELECT IFNULL(SUM(dias),0) AS u FROM vacaciones_norm WHERE resolucion_det_id=?", (det_id,)).fetchone()["u"]
    conn.close()
    return max(0, autorizados - safe_int(usados, 0))

# =========================================================
# UI helpers (búsqueda de opciones)
# =========================================================
def filter_options(options, q: str):
    if not q:
        return options
    q2 = q.lower().strip()
    return [o for o in options if q2 in o.lower()]

# =========================================================
# INIT APP
# =========================================================
st.set_page_config(page_title="Vacaciones DRE Cajamarca", layout="wide")
ensure_schema()

# =========================================================
# LOGIN
# =========================================================
if "user" not in st.session_state:
    st.session_state["user"] = None
if "rol" not in st.session_state:
    st.session_state["rol"] = None

def login():
    st.title("Sistema de Gestión de Vacaciones – DRE Cajamarca")
    u = st.text_input("Usuario", key="login_usuario")
    p = st.text_input("Contraseña", type="password", key="login_password")
    if st.button("Ingresar", key="login_btn"):
        conn = get_conn()
        row = conn.execute("SELECT * FROM usuarios WHERE usuario=? AND password_hash=?",
                           (u, hash_password(p))).fetchone()
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

connp = get_conn()
PERMS = get_user_perms(connp, USER, ROL)
connp.close()

# =========================================================
# MENU por permisos
# =========================================================
MENU_ALL = [
    ("Organización (Direcciones/Unidades/Áreas/Jefes)", "estructura_ver"),
    ("Trabajadores", "trabajadores_ver"),
    ("Resoluciones", "resoluciones_ver"),
    ("Vacaciones", "vacaciones_ver"),
    ("Adelanto de Vacaciones", "adelantos_ver"),
    ("Panel RRHH", "rrhh_aprobar"),
    ("Dashboard", "dashboard_ver"),
    ("Reportes", "reportes_ver"),
    ("Usuarios y Permisos", "usuarios_admin"),
    ("Exportar / Backup", "backup_exportar"),
    ("Reset del Sistema", "reset_sistema"),
]
menu_items = [name for name, perm in MENU_ALL if can(PERMS, perm)]
menu = st.sidebar.radio("Menú", menu_items)
st.sidebar.write(f"Usuario: {USER} ({ROL})")
if st.sidebar.button("Cerrar sesión"):
    logout()

st.title("Sistema de Gestión de Vacaciones – DRE Cajamarca")

# =========================================================
# 1) ESTRUCTURA (tabs)
# =========================================================
if menu == "Organización (Direcciones/Unidades/Áreas/Jefes)":
    editable = can(PERMS, "estructura_editar")
    conn = get_conn()
    st.header("Organización (Direcciones, Unidades, Áreas y Jefes)")

    tab_reg, tab_edit, tab_del = st.tabs(["➕ Registrar", "📝 Editar", "🗑️ Eliminar"])

    # ---------- Registrar ----------
    with tab_reg:
        st.subheader("Direcciones")
        with st.form("reg_dir", clear_on_submit=True):
            nd = st.text_input("Nombre dirección")
            if st.form_submit_button("Guardar dirección") and editable:
                if nd.strip():
                    conn.execute("INSERT INTO direcciones(nombre) VALUES(?)", (nd.strip(),))
                    conn.commit()
                    do_rerun()

        st.subheader("Unidades")
        df_dir = pd.read_sql("SELECT * FROM direcciones ORDER BY nombre", conn)
        if df_dir.empty:
            st.info("Primero registra Direcciones.")
        else:
            mapa_dir = dict(zip(df_dir["nombre"], df_dir["id"]))
            dsel = st.selectbox("Dirección", list(mapa_dir.keys()))
            with st.form("reg_uni", clear_on_submit=True):
                nu = st.text_input("Nombre unidad")
                if st.form_submit_button("Guardar unidad") and editable:
                    if nu.strip():
                        conn.execute("INSERT INTO unidades(direccion_id,nombre) VALUES(?,?)", (mapa_dir[dsel], nu.strip()))
                        conn.commit()
                        do_rerun()

        st.subheader("Áreas")
        df_uni = pd.read_sql("""
            SELECT u.id,u.nombre,d.nombre AS direccion
            FROM unidades u JOIN direcciones d ON d.id=u.direccion_id
            ORDER BY d.nombre,u.nombre
        """, conn)
        if df_uni.empty:
            st.info("Primero registra Unidades.")
        else:
            mapa_uni = {f"{r['direccion']} - {r['nombre']}": r["id"] for _, r in df_uni.iterrows()}
            usel = st.selectbox("Unidad", list(mapa_uni.keys()))
            with st.form("reg_area", clear_on_submit=True):
                na = st.text_input("Nombre área")
                if st.form_submit_button("Guardar área") and editable:
                    if na.strip():
                        conn.execute("INSERT INTO areas(unidad_id,nombre) VALUES(?,?)", (mapa_uni[usel], na.strip()))
                        conn.commit()
                        do_rerun()

        # 👇 JEFES SIEMPRE VISIBLE: si no hay áreas, te avisa, pero no desaparece
        st.subheader("Jefes")
        df_areas = pd.read_sql("""
            SELECT a.id,a.nombre,u.nombre AS unidad,d.nombre AS direccion
            FROM areas a
            JOIN unidades u ON u.id=a.unidad_id
            JOIN direcciones d ON d.id=u.direccion_id
            ORDER BY d.nombre,u.nombre,a.nombre
        """, conn)

        if df_areas.empty:
            st.warning("Aún no hay Áreas. Registra Áreas y vuelve aquí para registrar Jefes.")
        else:
            mapa_area = {f"{r['direccion']} - {r['unidad']} - {r['nombre']}": r["id"] for _, r in df_areas.iterrows()}
            asel = st.selectbox("Área del jefe", list(mapa_area.keys()))
            with st.form("reg_jefe", clear_on_submit=True):
                nj = st.text_input("Nombres del jefe")
                cj = st.text_input("Cargo")
                if st.form_submit_button("Guardar jefe") and editable:
                    if nj.strip():
                        conn.execute("INSERT INTO jefes(nombres,cargo,area_id) VALUES(?,?,?)", (nj.strip(), cj.strip(), mapa_area[asel]))
                        conn.commit()
                        do_rerun()
                    else:
                        st.error("Nombre obligatorio.")

        st.divider()
        st.subheader("Listado rápido")
        st.dataframe(pd.read_sql("SELECT * FROM direcciones ORDER BY nombre", conn), use_container_width=True)
        st.dataframe(pd.read_sql("SELECT * FROM unidades ORDER BY nombre", conn), use_container_width=True)
        st.dataframe(pd.read_sql("SELECT * FROM areas ORDER BY nombre", conn), use_container_width=True)
        st.dataframe(pd.read_sql("SELECT * FROM jefes ORDER BY nombres", conn), use_container_width=True)

    # ---------- Editar ----------
    with tab_edit:
        if not editable:
            st.info("No tienes permiso de edición.")
        else:
            st.subheader("Editar Dirección")
            df_dir = pd.read_sql("SELECT * FROM direcciones ORDER BY nombre", conn)
            if not df_dir.empty:
                did = st.selectbox("Dirección", df_dir["id"], format_func=lambda x: df_dir[df_dir["id"]==x]["nombre"].values[0])
                nuevo = st.text_input("Nuevo nombre", key="edir_dir")
                if st.button("Guardar cambios (Dirección)"):
                    if nuevo.strip():
                        conn.execute("UPDATE direcciones SET nombre=? WHERE id=?", (nuevo.strip(), did))
                        conn.commit()
                        do_rerun()

            st.subheader("Editar Unidad")
            df_uni = pd.read_sql("SELECT * FROM unidades ORDER BY nombre", conn)
            if not df_uni.empty:
                uid = st.selectbox("Unidad", df_uni["id"], format_func=lambda x: df_uni[df_uni["id"]==x]["nombre"].values[0])
                nuevo = st.text_input("Nuevo nombre unidad", key="edir_uni")
                if st.button("Guardar cambios (Unidad)"):
                    if nuevo.strip():
                        conn.execute("UPDATE unidades SET nombre=? WHERE id=?", (nuevo.strip(), uid))
                        conn.commit()
                        do_rerun()

            st.subheader("Editar Área")
            df_area = pd.read_sql("SELECT * FROM areas ORDER BY nombre", conn)
            if not df_area.empty:
                aid = st.selectbox("Área", df_area["id"], format_func=lambda x: df_area[df_area["id"]==x]["nombre"].values[0])
                nuevo = st.text_input("Nuevo nombre área", key="edir_area")
                if st.button("Guardar cambios (Área)"):
                    if nuevo.strip():
                        conn.execute("UPDATE areas SET nombre=? WHERE id=?", (nuevo.strip(), aid))
                        conn.commit()
                        do_rerun()

            st.subheader("Editar Jefe")
            df_j = pd.read_sql("SELECT * FROM jefes ORDER BY nombres", conn)
            if not df_j.empty:
                jid = st.selectbox("Jefe", df_j["id"], format_func=lambda x: df_j[df_j["id"]==x]["nombres"].values[0])
                nuevo_n = st.text_input("Nuevo nombre jefe", key="edir_jefe_n")
                nuevo_c = st.text_input("Nuevo cargo", key="edir_jefe_c")
                if st.button("Guardar cambios (Jefe)"):
                    if nuevo_n.strip():
                        conn.execute("UPDATE jefes SET nombres=?, cargo=? WHERE id=?", (nuevo_n.strip(), nuevo_c.strip(), jid))
                        conn.commit()
                        do_rerun()

    # ---------- Eliminar ----------
    with tab_del:
        if not editable:
            st.info("No tienes permiso de eliminación.")
        else:
            st.warning("Eliminar estructura puede afectar registros relacionados.")
            st.subheader("Eliminar Dirección")
            df_dir = pd.read_sql("SELECT * FROM direcciones ORDER BY nombre", conn)
            if not df_dir.empty:
                did2 = st.selectbox("Dirección", df_dir["id"], format_func=lambda x: df_dir[df_dir["id"]==x]["nombre"].values[0], key="del_dir")
                if st.button("Eliminar Dirección"):
                    conn.execute("DELETE FROM direcciones WHERE id=?", (did2,))
                    conn.commit()
                    do_rerun()

            st.subheader("Eliminar Unidad")
            df_uni = pd.read_sql("SELECT * FROM unidades ORDER BY nombre", conn)
            if not df_uni.empty:
                uid2 = st.selectbox("Unidad", df_uni["id"], format_func=lambda x: df_uni[df_uni["id"]==x]["nombre"].values[0], key="del_uni")
                if st.button("Eliminar Unidad"):
                    conn.execute("DELETE FROM unidades WHERE id=?", (uid2,))
                    conn.commit()
                    do_rerun()

            st.subheader("Eliminar Área")
            df_area = pd.read_sql("SELECT * FROM areas ORDER BY nombre", conn)
            if not df_area.empty:
                aid2 = st.selectbox("Área", df_area["id"], format_func=lambda x: df_area[df_area["id"]==x]["nombre"].values[0], key="del_area")
                if st.button("Eliminar Área"):
                    conn.execute("DELETE FROM areas WHERE id=?", (aid2,))
                    conn.commit()
                    do_rerun()

            st.subheader("Eliminar Jefe")
            df_j = pd.read_sql("SELECT * FROM jefes ORDER BY nombres", conn)
            if not df_j.empty:
                jid2 = st.selectbox("Jefe", df_j["id"], format_func=lambda x: df_j[df_j["id"]==x]["nombres"].values[0], key="del_jefe")
                if st.button("Eliminar Jefe"):
                    conn.execute("DELETE FROM jefes WHERE id=?", (jid2,))
                    conn.commit()
                    do_rerun()

    conn.close()

# =========================================================
# 2) TRABAJADORES (tabs + numero automático)
# =========================================================
elif menu == "Trabajadores":
    editable = can(PERMS, "trabajadores_editar")
    conn = get_conn()
    st.header("Trabajadores")

    tab_reg, tab_bus, tab_edit, tab_del = st.tabs(["➕ Registrar", "🔎 Buscar", "📝 Editar", "🗑️ Eliminar"])

    # Datos para alta
    df_area = pd.read_sql("""
        SELECT a.id, a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion
        FROM areas a
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre,a.nombre
    """, conn)

    df_jef = pd.read_sql("""
        SELECT j.id, j.nombres, a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion
        FROM jefes j
        JOIN areas a ON a.id=j.area_id
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre,a.nombre,j.nombres
    """, conn)

    # ---------- Registrar ----------
    with tab_reg:
        if not editable:
            st.info("No tienes permiso para registrar.")
        elif df_area.empty or df_jef.empty:
            st.warning("Primero registra estructura (Áreas) y Jefes.")
        else:
            mapa_area = {f"{r['direccion']} - {r['unidad']} - {r['area']}": r["id"] for _, r in df_area.iterrows()}
            mapa_jef = {f"{r['direccion']} - {r['unidad']} - {r['area']} - {r['nombres']}": r["id"] for _, r in df_jef.iterrows()}

            num_auto = siguiente_numero_trabajador()
            st.info(f"N° automático: {num_auto}")

            with st.form("reg_trab", clear_on_submit=True):
                numero = st.text_input("Número", value=num_auto)
                dni = st.text_input("DNI")
                nombres = st.text_input("Apellidos y Nombres")
                cargo = st.text_input("Cargo")
                regimen = st.selectbox("Régimen", ["DL 276","DL 728","DL 1057","Carrera Especial"])
                fi = st.date_input("Fecha ingreso")
                area_sel = st.selectbox("Área", list(mapa_area.keys()))
                jefe_sel = st.selectbox("Jefe", list(mapa_jef.keys()))
                if st.form_submit_button("Guardar trabajador"):
                    if not nombres.strip():
                        st.error("Nombre obligatorio.")
                    else:
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO trabajadores(numero,dni,nombres,cargo,regimen,fecha_ingreso,area_id,jefe_id)
                            VALUES(?,?,?,?,?,?,?,?)
                        """, (numero.strip(), dni.strip() if dni.strip() else None, nombres.strip(), cargo.strip() if cargo.strip() else None,
                              regimen, fi.strftime("%Y-%m-%d"), mapa_area[area_sel], mapa_jef[jefe_sel]))
                        tid = cur.lastrowid
                        conn.commit()
                        generar_periodos_para_trabajador(tid, fi.strftime("%Y-%m-%d"))
                        do_rerun()

    # Base listado
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

    # ---------- Buscar ----------
    with tab_bus:
        col1, col2 = st.columns(2)
        with col1:
            qn = st.text_input("Buscar por nombre", key="bus_nom")
        with col2:
            qd = st.text_input("Buscar por DNI", key="bus_dni")
        dfv = df_trab.copy()
        if qn:
            dfv = dfv[dfv["nombres"].str.contains(qn, case=False, na=False)]
        if qd:
            dfv = dfv[dfv["dni"].fillna("").str.contains(qd, na=False)]
        st.dataframe(dfv, use_container_width=True)

    # ---------- Editar ----------
    with tab_edit:
        if not editable:
            st.info("No tienes permiso para editar.")
        elif df_trab.empty:
            st.info("No hay trabajadores.")
        else:
            tid = st.selectbox("Trabajador", df_trab["id"], format_func=lambda x: df_trab[df_trab["id"]==x]["nombres"].values[0])
            row = conn.execute("SELECT * FROM trabajadores WHERE id=?", (tid,)).fetchone()
            with st.form("edit_trab"):
                en_num = st.text_input("Número", value=row["numero"] or "")
                en_dni = st.text_input("DNI", value=row["dni"] or "")
                en_nom = st.text_input("Nombres", value=row["nombres"] or "")
                en_car = st.text_input("Cargo", value=row["cargo"] or "")
                en_reg = st.text_input("Régimen", value=row["regimen"] or "")
                en_fi = st.date_input("Fecha ingreso", value=to_date(row["fecha_ingreso"]))
                if st.form_submit_button("Guardar cambios"):
                    conn.execute("""
                        UPDATE trabajadores SET numero=?, dni=?, nombres=?, cargo=?, regimen=?, fecha_ingreso=?
                        WHERE id=?
                    """, (en_num.strip(), en_dni.strip() if en_dni.strip() else None, en_nom.strip(), en_car.strip() if en_car.strip() else None,
                          en_reg.strip(), en_fi.strftime("%Y-%m-%d"), tid))
                    conn.commit()
                    generar_periodos_para_trabajador(tid, en_fi.strftime("%Y-%m-%d"))
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

    # ---------- Eliminar ----------
    with tab_del:
        if not editable:
            st.info("No tienes permiso para eliminar.")
        elif df_trab.empty:
            st.info("No hay trabajadores.")
        else:
            st.warning("Eliminar borra periodos, resoluciones detalle y vacaciones del trabajador.")
            tid2 = st.selectbox("Trabajador a eliminar", df_trab["id"], format_func=lambda x: df_trab[df_trab["id"]==x]["nombres"].values[0], key="del_trab")
            if st.button("Eliminar trabajador"):
                conn.execute("DELETE FROM trabajadores WHERE id=?", (tid2,))
                conn.commit()
                do_rerun()

    conn.close()

# =========================================================
# 3) RESOLUCIONES (cabecera + detalle, tabs, multi-trabajador)
# =========================================================
elif menu == "Resoluciones":
    editable = can(PERMS, "resoluciones_editar")
    conn = get_conn()
    st.header("Resoluciones (Cabecera + Detalle por trabajador)")

    tab_cab, tab_det, tab_edit, tab_del = st.tabs(["➕ Cabecera", "➕ Agregar Trabajadores", "📝 Editar", "🗑️ Eliminar"])

    # ---------- Cabecera ----------
    with tab_cab:
        if not editable:
            st.info("No tienes permiso.")
        else:
            with st.form("reg_cab", clear_on_submit=True):
                num = st.text_input("Número de Resolución")
                fecha_em = st.date_input("Fecha emisión", value=datetime.date.today())
                fracc = st.checkbox("Fraccionable")
                mad = st.text_input("MAD (opcional)")
                obs = st.text_area("Observaciones")
                if st.form_submit_button("Crear Cabecera"):
                    if not num.strip():
                        st.error("Número obligatorio.")
                    else:
                        conn.execute("""
                            INSERT INTO resolucion_cab(numero,fecha_emision,fraccionable,mad,observaciones)
                            VALUES(?,?,?,?,?)
                        """, (num.strip(), fecha_em.strftime("%Y-%m-%d"), 1 if fracc else 0, mad.strip() if mad.strip() else None, obs.strip()))
                        conn.commit()
                        do_rerun()

        st.divider()
        st.subheader("Cabeceras registradas")
        df_cab = pd.read_sql("SELECT * FROM resolucion_cab ORDER BY id DESC", conn)
        st.dataframe(df_cab, use_container_width=True)

    # ---------- Detalle: agregar trabajadores ----------
    with tab_det:
        df_cab = pd.read_sql("SELECT * FROM resolucion_cab ORDER BY id DESC", conn)
        if df_cab.empty:
            st.info("Primero crea una cabecera.")
        else:
            # buscador cabecera
            qcab = st.text_input("Buscar resolución por número", key="qcab")
            cab_opts = [f"ID {r['id']} | {r['numero']} | {r['fecha_emision'] or ''}" for _, r in df_cab.iterrows()]
            cab_opts_f = filter_options(cab_opts, qcab)
            if not cab_opts_f:
                st.warning("No hay coincidencias.")
            else:
                cab_sel = st.selectbox("Resolución (cabecera)", cab_opts_f)
                cab_id = int(cab_sel.split("|")[0].replace("ID","").strip())

                # trabajador
                df_t = pd.read_sql("SELECT id,nombres,dni,fecha_ingreso FROM trabajadores ORDER BY nombres", conn)
                if df_t.empty:
                    st.warning("No hay trabajadores.")
                else:
                    qtrab = st.text_input("Buscar trabajador (nombre o DNI)", key="qtrab_res")
                    options = [f"{r['nombres']} | DNI:{r['dni'] or ''} | ID:{r['id']}" for _, r in df_t.iterrows()]
                    options_f = filter_options(options, qtrab)
                    tsel = st.selectbox("Trabajador", options_f if options_f else options)
                    tid = int(tsel.split("ID:")[-1].strip())

                    fecha_ing = df_t[df_t["id"]==tid]["fecha_ingreso"].values[0]
                    generar_periodos_para_trabajador(tid, fecha_ing)

                    dfp = periodos_trabajador_df(tid)
                    if dfp.empty:
                        st.warning("Trabajador aún no tiene periodos completos.")
                    else:
                        # elegir periodo
                        hoy = datetime.date.today()
                        dfp["vigente"] = dfp["acumulable_hasta"].apply(lambda x: hoy <= to_date(x))
                        dfp = dfp[dfp["vigente"]==True]
                        mapa_p = {f"{r['inicio_ciclo']} a {r['fin_ciclo']} | acum {r['acumulable_hasta']}": r["id"] for _, r in dfp.iterrows()}
                        pid_label = st.selectbox("Periodo", list(mapa_p.keys()))
                        pid = mapa_p[pid_label]

                        # fechas y días calculados
                        fi = st.date_input("Inicio autorizado", key="res_fi")
                        ff = st.date_input("Fin autorizado", key="res_ff")
                        dias_auto = days_inclusive(fi, ff)
                        st.info(f"Días calculados automáticamente (calendario): {dias_auto}")

                        mad = st.text_input("MAD (detalle)", key="res_mad_det")
                        obs = st.text_area("Obs (detalle)", key="res_obs_det")

                        if st.button("Agregar trabajador a resolución") and editable:
                            if dias_auto <= 0:
                                st.error("Rango de fechas inválido.")
                            else:
                                conn.execute("""
                                    INSERT INTO resolucion_det(cab_id,trabajador_id,periodo_id,fecha_inicio,fecha_fin,dias_autorizados,mad,observaciones)
                                    VALUES(?,?,?,?,?,?,?,?)
                                """, (cab_id, tid, pid, fi.strftime("%Y-%m-%d"), ff.strftime("%Y-%m-%d"),
                                      int(dias_auto), mad.strip() if mad.strip() else None, obs.strip()))
                                conn.commit()
                                do_rerun()

                st.divider()
                st.subheader("Detalle de la resolución")
                df_det = pd.read_sql("""
                    SELECT d.id, c.numero, t.nombres AS trabajador, t.dni, p.inicio_ciclo, p.fin_ciclo,
                           d.fecha_inicio, d.fecha_fin, d.dias_autorizados
                    FROM resolucion_det d
                    JOIN resolucion_cab c ON c.id=d.cab_id
                    JOIN trabajadores t ON t.id=d.trabajador_id
                    JOIN periodos p ON p.id=d.periodo_id
                    WHERE d.cab_id=?
                    ORDER BY t.nombres
                """, conn, params=(cab_id,))
                st.dataframe(df_det, use_container_width=True)

    # ---------- Editar (cabecera o detalle) ----------
    with tab_edit:
        if not editable:
            st.info("No tienes permiso.")
        else:
            st.subheader("Editar cabecera")
            df_cab = pd.read_sql("SELECT * FROM resolucion_cab ORDER BY id DESC", conn)
            if df_cab.empty:
                st.info("No hay cabeceras.")
            else:
                cab_id = st.selectbox("Cabecera", df_cab["id"], format_func=lambda x: df_cab[df_cab["id"]==x]["numero"].values[0], key="edit_cab")
                row = conn.execute("SELECT * FROM resolucion_cab WHERE id=?", (cab_id,)).fetchone()
                with st.form("edit_cab_form"):
                    en_num = st.text_input("Número", value=row["numero"] or "")
                    en_fe = st.date_input("Fecha emisión", value=to_date(row["fecha_emision"]) if row["fecha_emision"] else datetime.date.today())
                    en_fr = st.checkbox("Fraccionable", value=bool(safe_int(row["fraccionable"],0)))
                    en_mad = st.text_input("MAD", value=row["mad"] or "")
                    en_obs = st.text_area("Obs", value=row["observaciones"] or "")
                    if st.form_submit_button("Guardar cambios cabecera"):
                        conn.execute("""
                            UPDATE resolucion_cab
                            SET numero=?, fecha_emision=?, fraccionable=?, mad=?, observaciones=?
                            WHERE id=?
                        """, (en_num.strip(), en_fe.strftime("%Y-%m-%d"), 1 if en_fr else 0,
                              en_mad.strip() if en_mad.strip() else None, en_obs.strip(), cab_id))
                        conn.commit()
                        do_rerun()

            st.divider()
            st.subheader("Editar detalle (trabajador)")
            df_det_all = pd.read_sql("""
                SELECT d.id, c.numero, t.nombres AS trabajador, d.fecha_inicio, d.fecha_fin, d.dias_autorizados
                FROM resolucion_det d
                JOIN resolucion_cab c ON c.id=d.cab_id
                JOIN trabajadores t ON t.id=d.trabajador_id
                ORDER BY d.id DESC
            """, conn)
            if df_det_all.empty:
                st.info("No hay detalle.")
            else:
                det_id = st.selectbox("Detalle", df_det_all["id"], format_func=lambda x: f"ID {x} | {df_det_all[df_det_all['id']==x]['numero'].values[0]} | {df_det_all[df_det_all['id']==x]['trabajador'].values[0]}")
                drow = conn.execute("SELECT * FROM resolucion_det WHERE id=?", (det_id,)).fetchone()
                with st.form("edit_det_form"):
                    fi = st.date_input("Inicio", value=to_date(drow["fecha_inicio"]))
                    ff = st.date_input("Fin", value=to_date(drow["fecha_fin"]))
                    dias_auto = days_inclusive(fi, ff)
                    st.info(f"Días recalculados: {dias_auto}")
                    mad = st.text_input("MAD", value=drow["mad"] or "")
                    obs = st.text_area("Obs", value=drow["observaciones"] or "")
                    if st.form_submit_button("Guardar cambios detalle"):
                        conn.execute("""
                            UPDATE resolucion_det
                            SET fecha_inicio=?, fecha_fin=?, dias_autorizados=?, mad=?, observaciones=?
                            WHERE id=?
                        """, (fi.strftime("%Y-%m-%d"), ff.strftime("%Y-%m-%d"), int(dias_auto),
                              mad.strip() if mad.strip() else None, obs.strip(), det_id))
                        conn.commit()
                        do_rerun()

    # ---------- Eliminar ----------
    with tab_del:
        if not editable:
            st.info("No tienes permiso.")
        else:
            st.warning("Eliminar cabecera borrará todo su detalle.")
            df_cab = pd.read_sql("SELECT * FROM resolucion_cab ORDER BY id DESC", conn)
            if not df_cab.empty:
                cab_id = st.selectbox("Cabecera a eliminar", df_cab["id"], format_func=lambda x: df_cab[df_cab["id"]==x]["numero"].values[0], key="del_cab")
                if st.button("Eliminar cabecera"):
                    conn.execute("DELETE FROM resolucion_cab WHERE id=?", (cab_id,))
                    conn.commit()
                    do_rerun()

            st.divider()
            st.warning("Eliminar solo un detalle (trabajador) no borra la cabecera.")
            df_det_all = pd.read_sql("""
                SELECT d.id, c.numero, t.nombres AS trabajador
                FROM resolucion_det d
                JOIN resolucion_cab c ON c.id=d.cab_id
                JOIN trabajadores t ON t.id=d.trabajador_id
                ORDER BY d.id DESC
            """, conn)
            if not df_det_all.empty:
                det_id = st.selectbox("Detalle a eliminar", df_det_all["id"], format_func=lambda x: f"ID {x} | {df_det_all[df_det_all['id']==x]['numero'].values[0]} | {df_det_all[df_det_all['id']==x]['trabajador'].values[0]}")
                if st.button("Eliminar detalle"):
                    conn.execute("DELETE FROM resolucion_det WHERE id=?", (det_id,))
                    conn.commit()
                    do_rerun()

    conn.close()

# =========================================================
# 4) VACACIONES (tabs, usa resolución detalle)
# =========================================================
elif menu == "Vacaciones":
    editable = can(PERMS, "vacaciones_editar")
    conn = get_conn()
    st.header("Vacaciones")

    tab_reg, tab_bus, tab_edit, tab_del = st.tabs(["➕ Registrar", "🔎 Buscar", "📝 Editar", "🗑️ Eliminar"])

    df_t = pd.read_sql("SELECT id,nombres,dni,fecha_ingreso FROM trabajadores ORDER BY nombres", conn)
    if df_t.empty:
        st.warning("No hay trabajadores.")
        conn.close()
        st.stop()

    # ---------- Registrar ----------
    with tab_reg:
        if not editable:
            st.info("No tienes permiso.")
        else:
            qtrab = st.text_input("Buscar trabajador (nombre/DNI)", key="qtrab_vac")
            options = [f"{r['nombres']} | DNI:{r['dni'] or ''} | ID:{r['id']}" for _, r in df_t.iterrows()]
            options_f = filter_options(options, qtrab)
            tsel = st.selectbox("Trabajador", options_f if options_f else options)
            tid = int(tsel.split("ID:")[-1].strip())

            fecha_ing = df_t[df_t["id"]==tid]["fecha_ingreso"].values[0]
            generar_periodos_para_trabajador(tid, fecha_ing)

            dfp = periodos_trabajador_df(tid)
            if dfp.empty:
                st.warning("No hay periodos completos.")
            else:
                hoy = datetime.date.today()
                dfp["vigente"] = dfp["acumulable_hasta"].apply(lambda x: hoy <= to_date(x))
                dfp = dfp[dfp["vigente"]==True]
                if dfp.empty:
                    st.warning("Todos los periodos están vencidos.")
                else:
                    mapa_p = {f"{r['inicio_ciclo']} a {r['fin_ciclo']} | saldo {30-int(r['dias_usados'])} | acum {r['acumulable_hasta']}": r["id"] for _, r in dfp.iterrows()}
                    psel = st.selectbox("Periodo", list(mapa_p.keys()))
                    pid = mapa_p[psel]

                    saldo_p = dias_periodo_restantes(pid)
                    st.info(f"Saldo periodo: {saldo_p} días")

                    usar_res = st.checkbox("Usar Resolución", value=True)
                    det_id = None
                    tipo = None
                    modo_res = None
                    dias_default = 7

                    if usar_res:
                        # Elegir cabecera por número + luego detalle por trabajador
                        df_cab = pd.read_sql("SELECT * FROM resolucion_cab ORDER BY id DESC", conn)
                        if df_cab.empty:
                            st.error("No hay resoluciones. Registra una cabecera primero.")
                        else:
                            qres = st.text_input("Buscar resolución por número", key="qres_vac")
                            cab_opts = [f"ID {r['id']} | {r['numero']} | {r['fecha_emision'] or ''}" for _, r in df_cab.iterrows()]
                            cab_opts_f = filter_options(cab_opts, qres)
                            cab_sel = st.selectbox("Resolución", cab_opts_f if cab_opts_f else cab_opts)
                            cab_id = int(cab_sel.split("|")[0].replace("ID","").strip())

                            # detalle del trabajador en ese cab
                            df_det = pd.read_sql("""
                                SELECT d.id, d.fecha_inicio, d.fecha_fin, d.dias_autorizados, c.fraccionable
                                FROM resolucion_det d
                                JOIN resolucion_cab c ON c.id=d.cab_id
                                WHERE d.cab_id=? AND d.trabajador_id=? AND d.periodo_id=?
                                ORDER BY d.id DESC
                            """, conn, params=(cab_id, tid, pid))
                            if df_det.empty:
                                st.error("Esta resolución no tiene detalle para este trabajador y periodo. Agrégalo en Resoluciones → Agregar Trabajadores.")
                            else:
                                det_id = int(df_det.iloc[0]["id"])
                                saldo_res = dias_res_det_restantes(det_id)
                                fraccionable = bool(safe_int(df_det.iloc[0]["fraccionable"],0))

                                st.info(f"Saldo autorizado (detalle resolución): {saldo_res} días")

                                modo_res = st.radio("Ejecución", ["Íntegra"] + (["Fraccionada"] if fraccionable else []), horizontal=True)
                                if modo_res == "Íntegra":
                                    tipo = "Resolución"
                                    # ✅ aquí corregimos tu queja: por defecto debe ser saldo_res, no 7
                                    dias_default = max(1, min(30, saldo_res))
                                else:
                                    tipo = st.selectbox("Sustento fraccionamiento", ["Memorando","Solicitud"])
                                    dias_default = min(7, max(1, min(30, saldo_res)))
                    else:
                        tipo = st.selectbox("Tipo", ["Memorando","Solicitud"])
                        dias_default = 7

                    documento = st.text_input("Documento (N°)")
                    dias = st.number_input("Días", 1, 30, value=int(dias_default))
                    fi = st.date_input("Inicio")
                    ff = fi + datetime.timedelta(days=int(dias)-1)
                    st.caption(f"Fin calculado: {ff}")

                    if st.button("Registrar Vacación"):
                        if int(dias) > saldo_p:
                            st.error("Excede saldo del periodo.")
                        elif usar_res and det_id is not None and int(dias) > dias_res_det_restantes(det_id):
                            st.error("Excede saldo autorizado de la resolución.")
                        else:
                            conn.execute("""
                                INSERT INTO vacaciones(trabajador_id,periodo_id,resolucion_det_id,tipo,modo_resolucion,fecha_inicio,fecha_fin,dias,documento,autorizado_rrhh)
                                VALUES(?,?,?,?,?,?,?,?,?,0)
                            """, (tid, pid, det_id, tipo, ("Integra" if usar_res and modo_res=="Íntegra" else ("Fraccionada" if usar_res else None)),
                                  fi.strftime("%Y-%m-%d"), ff.strftime("%Y-%m-%d"), int(dias),
                                  documento.strip() if documento.strip() else None))
                            conn.commit()
                            do_rerun()

    # Dataset vacaciones global (para tabs)
    df_v_all = pd.read_sql("""
        SELECT v.*, t.nombres AS trabajador, t.dni,
               p.inicio_ciclo, p.fin_ciclo,
               c.numero AS resolucion_num
        FROM vacaciones_norm v
        JOIN trabajadores t ON t.id=v.trabajador_id
        LEFT JOIN periodos p ON p.id=v.periodo_id
        LEFT JOIN resolucion_det d ON d.id=v.resolucion_det_id
        LEFT JOIN resolucion_cab c ON c.id=d.cab_id
        ORDER BY date(v.fecha_inicio) DESC
    """, conn)

    # ---------- Buscar ----------
    with tab_bus:
        col1, col2, col3 = st.columns(3)
        with col1:
            qn = st.text_input("Buscar trabajador por nombre")
        with col2:
            qd = st.text_input("Buscar por DNI")
        with col3:
            rr = st.selectbox("Estado RRHH", ["Todos","Pendientes","Aprobadas"])
        dfv = df_v_all.copy()
        if qn:
            dfv = dfv[dfv["trabajador"].str.contains(qn, case=False, na=False)]
        if qd:
            dfv = dfv[dfv["dni"].fillna("").str.contains(qd, na=False)]
        if rr == "Pendientes":
            dfv = dfv[dfv["autorizado_rrhh"]==0]
        elif rr == "Aprobadas":
            dfv = dfv[dfv["autorizado_rrhh"]==1]
        st.dataframe(dfv, use_container_width=True)
        st.download_button("Descargar CSV (Vacaciones)", dfv.to_csv(index=False).encode("utf-8"), "vacaciones.csv", "text/csv")

    # ---------- Editar ----------
    with tab_edit:
        if not editable:
            st.info("No tienes permiso.")
        elif df_v_all.empty:
            st.info("No hay vacaciones.")
        else:
            vid = st.selectbox("Vacación", df_v_all["id"], format_func=lambda x: f"ID {x} | {df_v_all[df_v_all['id']==x]['trabajador'].values[0]} | {df_v_all[df_v_all['id']==x]['fecha_inicio'].values[0]}")
            row = conn.execute("SELECT * FROM vacaciones WHERE id=?", (vid,)).fetchone()
            with st.form("edit_vac"):
                e_fi = st.date_input("Inicio", value=to_date(row["fecha_inicio"]))
                e_d = st.number_input("Días", 1, 30, value=safe_int(row["dias"],1))
                e_ff = e_fi + datetime.timedelta(days=int(e_d)-1)
                st.caption(f"Fin calculado: {e_ff}")
                e_doc = st.text_input("Documento", value=row["documento"] or "")
                e_obs = st.text_area("Observaciones", value=row["observaciones"] or "")
                if st.form_submit_button("Guardar cambios"):
                    conn.execute("""
                        UPDATE vacaciones SET fecha_inicio=?, fecha_fin=?, dias=?, documento=?, observaciones=?
                        WHERE id=?
                    """, (e_fi.strftime("%Y-%m-%d"), e_ff.strftime("%Y-%m-%d"), int(e_d),
                          e_doc.strip() if e_doc.strip() else None, e_obs.strip(), vid))
                    conn.commit()
                    do_rerun()

    # ---------- Eliminar ----------
    with tab_del:
        if not editable:
            st.info("No tienes permiso.")
        elif df_v_all.empty:
            st.info("No hay vacaciones.")
        else:
            vid2 = st.selectbox("Vacación a eliminar", df_v_all["id"], format_func=lambda x: f"ID {x} | {df_v_all[df_v_all['id']==x]['trabajador'].values[0]}", key="del_vac")
            if st.button("Eliminar vacación"):
                conn.execute("DELETE FROM vacaciones WHERE id=?", (vid2,))
                conn.commit()
                do_rerun()

    conn.close()

# =========================================================
# 5) ADELANTO (tabs + buscador + edit/del aparte)
# =========================================================
elif menu == "Adelanto de Vacaciones":
    editable = can(PERMS, "adelantos_editar")
    conn = get_conn()
    st.header("Adelanto de Vacaciones")

    tab_reg, tab_bus, tab_edit, tab_del = st.tabs(["➕ Registrar", "🔎 Buscar", "📝 Editar", "🗑️ Eliminar"])

    df_t = pd.read_sql("SELECT id,nombres,dni,fecha_ingreso FROM trabajadores ORDER BY nombres", conn)

    with tab_reg:
        if not editable:
            st.info("No tienes permiso.")
        elif df_t.empty:
            st.info("No hay trabajadores.")
        else:
            q = st.text_input("Buscar trabajador (nombre/DNI)", key="adel_q")
            opts = [f"{r['nombres']} | DNI:{r['dni'] or ''} | ID:{r['id']}" for _, r in df_t.iterrows()]
            opts_f = filter_options(opts, q)
            tsel = st.selectbox("Trabajador", opts_f if opts_f else opts)
            tid = int(tsel.split("ID:")[-1].strip())

            fecha_ing = df_t[df_t["id"]==tid]["fecha_ingreso"].values[0]
            ini, fin, max_d = ciclo_en_curso(fecha_ing)
            st.info(f"Ciclo en curso: {ini} a {fin} | Máximo proporcional: {max_d} días")

            with st.form("adel_form", clear_on_submit=True):
                dias_sol = st.number_input("Días solicitados", 0, 30, value=min(5, max_d))
                doc = st.text_input("Documento de acuerdo (obligatorio)")
                obs = st.text_area("Observaciones")
                if st.form_submit_button("Registrar solicitud"):
                    if not doc.strip():
                        st.error("Documento obligatorio.")
                    elif int(dias_sol) > max_d:
                        st.error("Excede máximo proporcional.")
                    else:
                        conn.execute("""
                            INSERT INTO adelantos(trabajador_id,fecha_solicitud,ciclo_inicio,ciclo_fin,dias_maximo,dias_solicitados,documento_acuerdo,observaciones,aprobado_rrhh)
                            VALUES(?,?,?,?,?,?,?,?,0)
                        """, (tid, datetime.date.today().strftime("%Y-%m-%d"), ini.strftime("%Y-%m-%d"), fin.strftime("%Y-%m-%d"),
                              int(max_d), int(dias_sol), doc.strip(), obs.strip()))
                        conn.commit()
                        do_rerun()

    df_a_all = pd.read_sql("""
        SELECT a.*, t.nombres AS trabajador, t.dni
        FROM adelantos a
        JOIN trabajadores t ON t.id=a.trabajador_id
        ORDER BY date(a.fecha_solicitud) DESC
    """, conn)

    with tab_bus:
        col1, col2, col3 = st.columns(3)
        with col1:
            qn = st.text_input("Buscar por nombre", key="adel_bn")
        with col2:
            qd = st.text_input("Buscar por DNI", key="adel_bd")
        with col3:
            rr = st.selectbox("RRHH", ["Todos","Pendientes","Aprobadas"], key="adel_rr")
        dfv = df_a_all.copy()
        if qn:
            dfv = dfv[dfv["trabajador"].str.contains(qn, case=False, na=False)]
        if qd:
            dfv = dfv[dfv["dni"].fillna("").str.contains(qd, na=False)]
        if rr == "Pendientes":
            dfv = dfv[dfv["aprobado_rrhh"]==0]
        elif rr == "Aprobadas":
            dfv = dfv[dfv["aprobado_rrhh"]==1]
        st.dataframe(dfv, use_container_width=True)
        st.download_button("Descargar CSV (Adelantos)", dfv.to_csv(index=False).encode("utf-8"), "adelantos.csv", "text/csv")

    with tab_edit:
        if not editable:
            st.info("No tienes permiso.")
        elif df_a_all.empty:
            st.info("No hay adelantos.")
        else:
            aid = st.selectbox("Solicitud", df_a_all["id"], format_func=lambda x: f"ID {x} | {df_a_all[df_a_all['id']==x]['trabajador'].values[0]} | {df_a_all[df_a_all['id']==x]['fecha_solicitud'].values[0]}")
            row = conn.execute("SELECT * FROM adelantos WHERE id=?", (aid,)).fetchone()
            with st.form("edit_adel"):
                e_d = st.number_input("Días solicitados", 0, 30, value=safe_int(row["dias_solicitados"],0))
                e_doc = st.text_input("Documento acuerdo", value=row["documento_acuerdo"] or "")
                e_obs = st.text_area("Obs", value=row["observaciones"] or "")
                if st.form_submit_button("Guardar cambios"):
                    conn.execute("UPDATE adelantos SET dias_solicitados=?, documento_acuerdo=?, observaciones=? WHERE id=?",
                                 (int(e_d), e_doc.strip(), e_obs.strip(), aid))
                    conn.commit()
                    do_rerun()

    with tab_del:
        if not editable:
            st.info("No tienes permiso.")
        elif df_a_all.empty:
            st.info("No hay adelantos.")
        else:
            aid2 = st.selectbox("Solicitud a eliminar", df_a_all["id"], format_func=lambda x: f"ID {x} | {df_a_all[df_a_all['id']==x]['trabajador'].values[0]}", key="del_adel")
            if st.button("Eliminar solicitud"):
                conn.execute("DELETE FROM adelantos WHERE id=?", (aid2,))
                conn.commit()
                do_rerun()

    conn.close()

# =========================================================
# 6) RRHH (pendientes y aprobadas visibles)
# =========================================================
elif menu == "Panel RRHH":
    conn = get_conn()
    st.header("Panel RRHH")

    tab_v, tab_a = st.tabs(["Vacaciones", "Adelantos"])

    with tab_v:
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
            obs = st.text_area("Obs RRHH", key="rrhh_v_obs")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Aprobar vacación"):
                    conn.execute("UPDATE vacaciones SET autorizado_rrhh=1, rrhh_observacion=?, fecha_aprob_rrhh=?, usuario_rrhh=? WHERE id=?",
                                 (obs.strip(), datetime.date.today().strftime("%Y-%m-%d"), USER, vid))
                    conn.commit()
                    do_rerun()
            with col2:
                if st.button("📝 Guardar observación"):
                    conn.execute("UPDATE vacaciones SET rrhh_observacion=? WHERE id=?", (obs.strip(), vid))
                    conn.commit()
                    do_rerun()

        st.divider()
        st.subheader("Vacaciones aprobadas")
        df_ok = pd.read_sql("""
            SELECT v.id, t.nombres AS trabajador, v.tipo, v.fecha_inicio, v.fecha_fin, v.dias, v.documento, v.fecha_aprob_rrhh, v.usuario_rrhh
            FROM vacaciones_norm v
            JOIN trabajadores t ON t.id=v.trabajador_id
            WHERE v.autorizado_rrhh=1
            ORDER BY date(v.fecha_inicio) DESC
        """, conn)
        st.dataframe(df_ok, use_container_width=True)

    with tab_a:
        st.subheader("Adelantos pendientes")
        df_ap = pd.read_sql("""
            SELECT a.id, t.nombres AS trabajador, a.fecha_solicitud, a.dias_solicitados, a.documento_acuerdo, a.rrhh_observacion
            FROM adelantos a
            JOIN trabajadores t ON t.id=a.trabajador_id
            WHERE a.aprobado_rrhh=0
            ORDER BY date(a.fecha_solicitud) DESC
        """, conn)
        st.dataframe(df_ap, use_container_width=True)

        if not df_ap.empty:
            aid = st.selectbox("Adelanto", df_ap["id"], format_func=lambda x: f"ID {x} - {df_ap[df_ap['id']==x]['trabajador'].values[0]}")
            obs = st.text_area("Obs RRHH (adelanto)", key="rrhh_a_obs")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Aprobar adelanto"):
                    conn.execute("UPDATE adelantos SET aprobado_rrhh=1, rrhh_observacion=?, fecha_aprob_rrhh=?, usuario_rrhh=? WHERE id=?",
                                 (obs.strip(), datetime.date.today().strftime("%Y-%m-%d"), USER, aid))
                    conn.commit()
                    do_rerun()
            with col2:
                if st.button("📝 Guardar observación (adelanto)"):
                    conn.execute("UPDATE adelantos SET rrhh_observacion=? WHERE id=?", (obs.strip(), aid))
                    conn.commit()
                    do_rerun()

        st.divider()
        st.subheader("Adelantos aprobados")
        df_ok = pd.read_sql("""
            SELECT a.id, t.nombres AS trabajador, a.fecha_solicitud, a.dias_solicitados, a.documento_acuerdo, a.fecha_aprob_rrhh, a.usuario_rrhh
            FROM adelantos a
            JOIN trabajadores t ON t.id=a.trabajador_id
            WHERE a.aprobado_rrhh=1
            ORDER BY date(a.fecha_solicitud) DESC
        """, conn)
        st.dataframe(df_ok, use_container_width=True)

    conn.close()

# =========================================================
# 7) Dashboard
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
        dfp["dias_restantes"] = 30 - dfp["dias_usados"].astype(int)
        dfp["estado"] = dfp["acumulable_hasta"].apply(lambda x: "🔴 Vencido" if hoy > to_date(x) else ("🟡 Por vencer" if hoy > (to_date(x) - datetime.timedelta(days=60)) else "🟢 Vigente"))

    c1,c2,c3 = st.columns(3)
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
# 8) Reportes amigables (historial por trabajador y por área)
# =========================================================
elif menu == "Reportes":
    conn = get_conn()
    st.header("Reportes")

    tab_v, tab_hist_t, tab_hist_a = st.tabs(["Vacaciones/Resoluciones/Adelantos", "Historial por Trabajador", "Historial por Área"])

    with tab_v:
        col1,col2,col3 = st.columns(3)
        with col1:
            qn = st.text_input("Buscar nombre")
        with col2:
            qd = st.text_input("Buscar DNI")
        with col3:
            rr = st.selectbox("RRHH", ["Todos","Pendientes","Aprobadas"])
        tipo_sel = st.selectbox("Tipo", ["Todos","Solicitud","Memorando","Resolución"])

        df_v = pd.read_sql("""
            SELECT v.*, t.nombres AS trabajador, t.dni,
                   c.numero AS resolucion_num
            FROM vacaciones_norm v
            JOIN trabajadores t ON t.id=v.trabajador_id
            LEFT JOIN resolucion_det d ON d.id=v.resolucion_det_id
            LEFT JOIN resolucion_cab c ON c.id=d.cab_id
            ORDER BY date(v.fecha_inicio) DESC
        """, conn)

        if qn:
            df_v = df_v[df_v["trabajador"].str.contains(qn, case=False, na=False)]
        if qd:
            df_v = df_v[df_v["dni"].fillna("").str.contains(qd, na=False)]
        if rr == "Pendientes":
            df_v = df_v[df_v["autorizado_rrhh"]==0]
        elif rr == "Aprobadas":
            df_v = df_v[df_v["autorizado_rrhh"]==1]
        if tipo_sel != "Todos":
            df_v = df_v[df_v["tipo"]==tipo_sel]

        st.subheader("Vacaciones")
        st.dataframe(df_v, use_container_width=True)
        st.download_button("Descargar CSV Vacaciones", df_v.to_csv(index=False).encode("utf-8"), "reporte_vacaciones.csv", "text/csv")

        st.divider()
        st.subheader("Resoluciones (detalle)")
        df_r = pd.read_sql("""
            SELECT r.*, t.nombres AS trabajador, t.dni
            FROM resoluciones_norm r
            JOIN trabajadores t ON t.id=r.trabajador_id
            ORDER BY date(r.fecha_inicio) DESC
        """, conn)
        if qn:
            df_r = df_r[df_r["trabajador"].str.contains(qn, case=False, na=False)]
        if qd:
            df_r = df_r[df_r["dni"].fillna("").str.contains(qd, na=False)]
        st.dataframe(df_r, use_container_width=True)
        st.download_button("Descargar CSV Resoluciones", df_r.to_csv(index=False).encode("utf-8"), "reporte_resoluciones.csv", "text/csv")

        st.divider()
        st.subheader("Adelantos")
        df_a = pd.read_sql("""
            SELECT a.*, t.nombres AS trabajador, t.dni
            FROM adelantos a
            JOIN trabajadores t ON t.id=a.trabajador_id
            ORDER BY date(a.fecha_solicitud) DESC
        """, conn)
        if qn:
            df_a = df_a[df_a["trabajador"].str.contains(qn, case=False, na=False)]
        if qd:
            df_a = df_a[df_a["dni"].fillna("").str.contains(qd, na=False)]
        st.dataframe(df_a, use_container_width=True)
        st.download_button("Descargar CSV Adelantos", df_a.to_csv(index=False).encode("utf-8"), "reporte_adelantos.csv", "text/csv")

    with tab_hist_t:
        st.subheader("Historial completo por trabajador")
        df_t = pd.read_sql("SELECT id,nombres,dni FROM trabajadores ORDER BY nombres", conn)
        if df_t.empty:
            st.info("No hay trabajadores.")
        else:
            q = st.text_input("Buscar (nombre/DNI)", key="hist_t_q")
            opts = [f"{r['nombres']} | DNI:{r['dni'] or ''} | ID:{r['id']}" for _, r in df_t.iterrows()]
            opts_f = filter_options(opts, q)
            sel = st.selectbox("Trabajador", opts_f if opts_f else opts)
            tid = int(sel.split("ID:")[-1].strip())

            st.markdown("### Periodos")
            dfp = pd.read_sql("""
                SELECT p.*, IFNULL((SELECT SUM(dias) FROM vacaciones_norm v WHERE v.periodo_id=p.id),0) AS dias_usados
                FROM periodos p WHERE p.trabajador_id=?
                ORDER BY p.inicio_ciclo
            """, conn, params=(tid,))
            st.dataframe(dfp, use_container_width=True)

            st.markdown("### Resoluciones")
            dfr = pd.read_sql("""
                SELECT r.* FROM resoluciones_norm r
                WHERE r.trabajador_id=?
                ORDER BY date(r.fecha_inicio) DESC
            """, conn, params=(tid,))
            st.dataframe(dfr, use_container_width=True)

            st.markdown("### Vacaciones")
            dfv = pd.read_sql("""
                SELECT v.*, c.numero AS resolucion_num
                FROM vacaciones_norm v
                LEFT JOIN resolucion_det d ON d.id=v.resolucion_det_id
                LEFT JOIN resolucion_cab c ON c.id=d.cab_id
                WHERE v.trabajador_id=?
                ORDER BY date(v.fecha_inicio) DESC
            """, conn, params=(tid,))
            st.dataframe(dfv, use_container_width=True)

            st.markdown("### Adelantos")
            dfa = pd.read_sql("SELECT * FROM adelantos WHERE trabajador_id=? ORDER BY date(fecha_solicitud) DESC", conn, params=(tid,))
            st.dataframe(dfa, use_container_width=True)

            st.download_button("Descargar historial (CSV combinado)",
                               pd.concat([dfp.assign(seccion="periodos"),
                                          dfr.assign(seccion="resoluciones"),
                                          dfv.assign(seccion="vacaciones"),
                                          dfa.assign(seccion="adelantos")], axis=0, ignore_index=True).to_csv(index=False).encode("utf-8"),
                               "historial_trabajador.csv", "text/csv")

    with tab_hist_a:
        st.subheader("Historial por Área")
        df_area = pd.read_sql("""
            SELECT a.id, a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion
            FROM areas a
            JOIN unidades u ON u.id=a.unidad_id
            JOIN direcciones d ON d.id=u.direccion_id
            ORDER BY d.nombre,u.nombre,a.nombre
        """, conn)
        if df_area.empty:
            st.info("No hay áreas.")
        else:
            opts = [f"{r['direccion']} - {r['unidad']} - {r['area']} | ID:{r['id']}" for _, r in df_area.iterrows()]
            sel = st.selectbox("Área", opts)
            aid = int(sel.split("ID:")[-1].strip())

            df_tr = pd.read_sql("""
                SELECT t.id,t.nombres,t.dni FROM trabajadores t
                WHERE t.area_id=?
                ORDER BY t.nombres
            """, conn, params=(aid,))
            st.markdown("### Trabajadores del área")
            st.dataframe(df_tr, use_container_width=True)

            dfv = pd.read_sql("""
                SELECT v.*, t.nombres AS trabajador, t.dni
                FROM vacaciones_norm v
                JOIN trabajadores t ON t.id=v.trabajador_id
                WHERE t.area_id=?
                ORDER BY date(v.fecha_inicio) DESC
            """, conn, params=(aid,))
            st.markdown("### Vacaciones del área")
            st.dataframe(dfv, use_container_width=True)
            st.download_button("Descargar CSV vacaciones área", dfv.to_csv(index=False).encode("utf-8"), "vacaciones_area.csv", "text/csv")

    conn.close()

# =========================================================
# 9) Usuarios y Permisos (tabs separados)
# =========================================================
elif menu == "Usuarios y Permisos":
    conn = get_conn()
    st.header("Usuarios y Permisos")

    tab_users, tab_perms = st.tabs(["👤 Usuarios", "🔐 Permisos por usuario"])

    with tab_users:
        df_u = pd.read_sql("SELECT id,usuario,rol FROM usuarios ORDER BY usuario", conn)
        st.dataframe(df_u, use_container_width=True)

        st.subheader("Crear usuario")
        with st.form("crear_user", clear_on_submit=True):
            u = st.text_input("Usuario", key="login_usuario")
            p = st.text_input("Contraseña", type="password", key="login_password")
            r = st.selectbox("Rol", ["admin","responsable","registrador"])
            if st.form_submit_button("Crear"):
                if u.strip() and p.strip():
                    conn.execute("INSERT INTO usuarios(usuario,password_hash,rol,permisos_json) VALUES(?,?,?,?)",
                                 (u.strip(), hash_password(p.strip()), r, None))
                    conn.commit()
                    do_rerun()

        st.subheader("Reset password / Cambiar rol / Eliminar")
        if not df_u.empty:
            uid = st.selectbox("Usuario", df_u["id"], format_func=lambda x: df_u[df_u["id"]==x]["usuario"].values[0])
            urow = conn.execute("SELECT * FROM usuarios WHERE id=?", (uid,)).fetchone()

            col1,col2,col3 = st.columns(3)
            with col1:
                np = st.text_input("Nueva contraseña", type="password")
                if st.button("Reset password"):
                    if np.strip():
                        conn.execute("UPDATE usuarios SET password_hash=? WHERE id=?", (hash_password(np.strip()), uid))
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

    with tab_perms:
        df_u = pd.read_sql("SELECT id,usuario,rol,permisos_json FROM usuarios ORDER BY usuario", conn)
        uid = st.selectbox("Seleccione usuario", df_u["id"], format_func=lambda x: df_u[df_u["id"]==x]["usuario"].values[0], key="perm_uid")
        urow = conn.execute("SELECT * FROM usuarios WHERE id=?", (uid,)).fetchone()

        base = ROLE_DEFAULT_PERMS.get(urow["rol"], ROLE_DEFAULT_PERMS["registrador"]).copy()
        curp = base.copy()
        if urow["permisos_json"]:
            try:
                d = json.loads(urow["permisos_json"])
                if isinstance(d, dict):
                    for k in curp:
                        if k in d:
                            curp[k] = bool(d[k])
            except Exception:
                pass

        st.info(f"Rol base: {urow['rol']}. Aquí puedes ajustar permisos finamente para este usuario.")
        cols = st.columns(3)
        new_perm = {}
        for i, k in enumerate(PERM_KEYS):
            with cols[i % 3]:
                new_perm[k] = st.checkbox(k, value=curp.get(k, False), key=f"perm_{k}")

        if st.button("Guardar permisos"):
            conn.execute("UPDATE usuarios SET permisos_json=? WHERE id=?", (json.dumps(new_perm), uid))
            conn.commit()
            do_rerun()

    conn.close()

# =========================================================
# 10) Exportar/Backup
# =========================================================
elif menu == "Exportar / Backup":
    conn = get_conn()
    st.header("Exportar / Backup")
    tablas = ["usuarios","direcciones","unidades","areas","jefes","trabajadores","periodos","resolucion_cab","resolucion_det","vacaciones","adelantos"]
    tsel = st.selectbox("Tabla", tablas)
    df = pd.read_sql(f"SELECT * FROM {tsel}", conn)
    st.dataframe(df, use_container_width=True)
    st.download_button(f"Descargar {tsel}.csv", df.to_csv(index=False).encode("utf-8"), f"{tsel}.csv", "text/csv")

    st.divider()
    st.subheader("Descargar base completa")
    if os.path.exists(DB_PATH):
        with open(DB_PATH, "rb") as f:
            data = f.read()
        st.download_button("Descargar vacaciones.db", data, "vacaciones.db", "application/x-sqlite3")

    conn.close()

# =========================================================
# 11) Reset con confirmación escrita
# =========================================================
elif menu == "Reset del Sistema":
    st.header("Reset del Sistema")
    st.warning("⚠ Eliminará TODO. Descarga backup antes.")
    confirm = st.text_input("Escriba: RESET TOTAL")
    check = st.checkbox("Confirmo que deseo resetear")
    if st.button("RESET TOTAL"):
        if confirm.strip() != "RESET TOTAL" or not check:
            st.error("Confirmación incorrecta.")
        else:
            conn = get_conn()
            # Drop views first if exist
            safe_drop_object(conn, "vacaciones_norm")
            safe_drop_object(conn, "resoluciones_norm")
            # Drop tables
            for t in ["vacaciones","resolucion_det","resolucion_cab","adelantos","periodos","trabajadores","jefes","areas","unidades","direcciones","usuarios"]:
                conn.execute(f"DROP TABLE IF EXISTS {t}")
            conn.commit()
            conn.close()
            ensure_schema()
            st.success("Sistema reseteado. admin/admin restaurado.")
            do_rerun()
