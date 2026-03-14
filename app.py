import streamlit as st
import sqlite3
import pandas as pd
import datetime
import hashlib
import os

DB_PATH = "vacaciones.db"

# =========================================================
# Helpers base
# =========================================================
def do_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.stop()

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def hash_password(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def to_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()

def from_date(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")

def table_columns(conn, table: str) -> set:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

def add_column_if_missing(conn, table: str, col: str, col_def_sql: str):
    cols = table_columns(conn, table)
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}")

def table_exists(conn, table: str) -> bool:
    r = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return r is not None

def view_exists(conn, view: str) -> bool:
    r = conn.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view,)).fetchone()
    return r is not None

# =========================================================
# Esquema + Migración + Vistas de compatibilidad
# =========================================================
def ensure_schema():
    conn = get_conn()
    cur = conn.cursor()

    # --- Tablas base (no destruye datos)
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS usuarios(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        rol TEXT NOT NULL
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

    CREATE TABLE IF NOT EXISTS resoluciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER,
        periodo_id INTEGER,
        numero TEXT,
        fecha_inicio TEXT,
        fecha_fin TEXT,
        dias_autorizados INTEGER,
        fraccionable INTEGER DEFAULT 0,
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
        usuario_rrhh TEXT,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE
    );
    """)

    # --- Migraciones de columnas (para BD viejas)
    # usuarios
    add_column_if_missing(conn, "usuarios", "rol", "rol TEXT NOT NULL DEFAULT 'registrador'")
    add_column_if_missing(conn, "usuarios", "password_hash", "password_hash TEXT NOT NULL DEFAULT ''")

    # resoluciones: soportar nombres antiguos
    for coldef in [
        ("trabajador_id", "trabajador_id INTEGER"),
        ("periodo_id", "periodo_id INTEGER"),
        ("numero", "numero TEXT"),
        ("fecha_inicio", "fecha_inicio TEXT"),
        ("fecha_fin", "fecha_fin TEXT"),
        ("dias_autorizados", "dias_autorizados INTEGER"),
        ("fraccionable", "fraccionable INTEGER DEFAULT 0"),
        ("observaciones", "observaciones TEXT"),
    ]:
        add_column_if_missing(conn, "resoluciones", coldef[0], coldef[1])

    # vacaciones
    for coldef in [
        ("periodo_id", "periodo_id INTEGER"),
        ("resolucion_id", "resolucion_id INTEGER"),
        ("modo_resolucion", "modo_resolucion TEXT"),
        ("documento", "documento TEXT"),
        ("observaciones", "observaciones TEXT"),
        ("autorizado_rrhh", "autorizado_rrhh INTEGER DEFAULT 0"),
        ("rrhh_observacion", "rrhh_observacion TEXT"),
        ("fecha_aprob_rrhh", "fecha_aprob_rrhh TEXT"),
        ("usuario_rrhh", "usuario_rrhh TEXT"),
    ]:
        add_column_if_missing(conn, "vacaciones", coldef[0], coldef[1])

    conn.commit()

    # --- Admin inicial
    c = conn.execute("SELECT COUNT(*) AS c FROM usuarios").fetchone()["c"]
    if c == 0:
        conn.execute(
            "INSERT INTO usuarios(usuario,password_hash,rol) VALUES(?,?,?)",
            ("admin", hash_password("admin"), "admin")
        )
        conn.commit()

    # --- Crear / recrear VISTA de compatibilidad para resoluciones
    # Nota: la vista arma campos "numero/fecha_inicio/fecha_fin/dias_autorizados" usando columnas alternativas si existen.
    # Si tu BD vieja tiene: numero_resolucion, fecha_programada, etc., esta vista los "mapea" sin reventar.
    # (Construimos dinámicamente para no referir columnas inexistentes)
    res_cols = table_columns(conn, "resoluciones")
    def pick(*names):
        for n in names:
            if n in res_cols:
                return n
        return None

    col_num = pick("numero", "numero_resolucion", "num_resolucion")
    col_fi  = pick("fecha_inicio", "fecha_programada", "fecha")
    col_ff  = pick("fecha_fin", "fecha_programada", "fecha")
    col_dias= pick("dias_autorizados", "dias", "dias_aut")

    # asegurar que existan (ya añadimos los nuevos), entonces col_num/col_fi/col_ff/col_dias deben existir
    # pero si venían vacíos, igual existe como columna añadida.
    if col_num is None: col_num = "numero"
    if col_fi  is None: col_fi  = "fecha_inicio"
    if col_ff  is None: col_ff  = "fecha_fin"
    if col_dias is None: col_dias = "dias_autorizados"

    # dropear vista si existe para recrearla
    if view_exists(conn, "resoluciones_norm"):
        conn.execute("DROP VIEW IF EXISTS resoluciones_norm")

    conn.execute(f"""
        CREATE VIEW resoluciones_norm AS
        SELECT
            id,
            trabajador_id,
            periodo_id,
            {col_num} AS numero,
            {col_fi} AS fecha_inicio,
            {col_ff} AS fecha_fin,
            {col_dias} AS dias_autorizados,
            COALESCE(fraccionable,0) AS fraccionable,
            observaciones
        FROM resoluciones
    """)

    # --- Vista vacaciones_norm para reportes robustos
    vac_cols = table_columns(conn, "vacaciones")
    if view_exists(conn, "vacaciones_norm"):
        conn.execute("DROP VIEW IF EXISTS vacaciones_norm")

    # columnas siempre presentes (por migración)
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
# Periodos correctos (regla)
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
    row = conn.execute("""
        SELECT 30 - IFNULL((SELECT SUM(dias) FROM vacaciones_norm WHERE periodo_id=?),0) AS r
    """, (periodo_id,)).fetchone()
    conn.close()
    return max(0, int(row["r"]))

def dias_resolucion_restantes(resolucion_id: int) -> int:
    conn = get_conn()
    r = conn.execute("SELECT dias_autorizados FROM resoluciones_norm WHERE id=?", (resolucion_id,)).fetchone()
    if not r or r["dias_autorizados"] is None:
        conn.close()
        return 0
    autorizados = int(r["dias_autorizados"])
    usados = conn.execute("SELECT IFNULL(SUM(dias),0) AS u FROM vacaciones_norm WHERE resolucion_id=?", (resolucion_id,)).fetchone()["u"]
    conn.close()
    return max(0, autorizados - int(usados))

def periodo_vigente(periodo_row) -> bool:
    hoy = datetime.date.today()
    try:
        return hoy <= to_date(periodo_row["acumulable_hasta"])
    except Exception:
        return True

# =========================================================
# Adelanto: cálculo proporcional (ciclo en curso)
# =========================================================
def ciclo_en_curso(fecha_ingreso_str: str) -> tuple[datetime.date, datetime.date, int]:
    """
    Devuelve (inicio_ciclo, fin_ciclo, dias_max_adelanto)
    - inicio_ciclo: aniversario más reciente (incluye fecha_ingreso como base)
    - fin_ciclo: inicio + 1 año - 1 día
    - días max: floor((días trabajados en el ciclo / 365) * 30)
    """
    ingreso = to_date(fecha_ingreso_str)
    hoy = datetime.date.today()

    # encontrar inicio del ciclo actual
    inicio = ingreso
    while True:
        nxt = inicio.replace(year=inicio.year + 1)
        if nxt > hoy:
            break
        inicio = nxt

    fin = inicio.replace(year=inicio.year + 1) - datetime.timedelta(days=1)
    dias_trab = (hoy - inicio).days + 1
    dias_max = int((dias_trab / 365.0) * 30)
    if dias_max < 0:
        dias_max = 0
    if dias_max > 30:
        dias_max = 30
    return inicio, fin, dias_max

# =========================================================
# App start
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
            st.success("Acceso concedido")
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

# =========================================================
# Menú por rol
# =========================================================
MENU = {
    "admin": [
        "Direcciones / Unidades / Áreas / Jefes",
        "Registrar Trabajador",
        "Reporte de Trabajadores",
        "Resoluciones",
        "Registrar Vacaciones",
        "Adelanto de Vacaciones",
        "Panel RRHH",
        "Dashboard",
        "Reportes",
        "Usuarios y Permisos",
        "Exportar / Backup"
    ],
    "responsable": [
        "Direcciones / Unidades / Áreas / Jefes",
        "Registrar Trabajador",
        "Reporte de Trabajadores",
        "Resoluciones",
        "Registrar Vacaciones",
        "Adelanto de Vacaciones",
        "Panel RRHH",
        "Dashboard",
        "Reportes",
        "Exportar / Backup"
    ],
    "registrador": [
        "Registrar Trabajador",
        "Reporte de Trabajadores",
        "Resoluciones",
        "Registrar Vacaciones",
        "Adelanto de Vacaciones",
        "Reportes"
    ]
}

menu = st.sidebar.radio("Menú", MENU.get(ROL, MENU["registrador"]))
st.sidebar.write(f"Usuario: {USER} ({ROL})")
if st.sidebar.button("Cerrar sesión"):
    logout()

st.title("Sistema de Gestión de Vacaciones – DRE Cajamarca")

# =========================================================
# Módulo: Direcciones / Unidades / Áreas / Jefes (CRUD)
# =========================================================
if menu == "Direcciones / Unidades / Áreas / Jefes":
    st.header("Direcciones, Unidades, Áreas y Jefes")
    conn = get_conn()

    st.subheader("Direcciones")
    df_dir = pd.read_sql("SELECT * FROM direcciones ORDER BY nombre", conn)
    st.dataframe(df_dir, use_container_width=True)

    with st.form("f_dir", clear_on_submit=True):
        nd = st.text_input("Nueva Dirección")
        if st.form_submit_button("Agregar Dirección"):
            if nd.strip():
                conn.execute("INSERT INTO direcciones(nombre) VALUES(?)", (nd.strip(),))
                conn.commit()
                st.success("Dirección registrada.")
                do_rerun()

    st.divider()
    st.subheader("Unidades")
    if df_dir.empty:
        st.info("Crea una Dirección primero.")
    else:
        mapa_dir = dict(zip(df_dir["nombre"], df_dir["id"]))
        dsel = st.selectbox("Dirección", list(mapa_dir.keys()))
        df_uni = pd.read_sql("SELECT * FROM unidades WHERE direccion_id=? ORDER BY nombre", conn, params=(mapa_dir[dsel],))
        st.dataframe(df_uni, use_container_width=True)

        with st.form("f_uni", clear_on_submit=True):
            nu = st.text_input("Nueva Unidad")
            if st.form_submit_button("Agregar Unidad"):
                if nu.strip():
                    conn.execute("INSERT INTO unidades(direccion_id,nombre) VALUES(?,?)", (mapa_dir[dsel], nu.strip()))
                    conn.commit()
                    st.success("Unidad registrada.")
                    do_rerun()

    st.divider()
    st.subheader("Áreas")
    df_uni_all = pd.read_sql("""
        SELECT u.id,u.nombre, d.nombre AS direccion
        FROM unidades u JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre
    """, conn)
    if df_uni_all.empty:
        st.info("Crea una Unidad primero.")
    else:
        mapa_uni = {f"{r['direccion']} - {r['nombre']}": r["id"] for _, r in df_uni_all.iterrows()}
        usel = st.selectbox("Unidad", list(mapa_uni.keys()))
        df_area = pd.read_sql("SELECT * FROM areas WHERE unidad_id=? ORDER BY nombre", conn, params=(mapa_uni[usel],))
        st.dataframe(df_area, use_container_width=True)

        with st.form("f_area", clear_on_submit=True):
            na = st.text_input("Nueva Área")
            if st.form_submit_button("Agregar Área"):
                if na.strip():
                    conn.execute("INSERT INTO areas(unidad_id,nombre) VALUES(?,?)", (mapa_uni[usel], na.strip()))
                    conn.commit()
                    st.success("Área registrada.")
                    do_rerun()

    st.divider()
    st.subheader("Jefes")
    df_areas = pd.read_sql("""
        SELECT a.id,a.nombre, u.nombre AS unidad, d.nombre AS direccion
        FROM areas a
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre,a.nombre
    """, conn)
    if df_areas.empty:
        st.warning("Registra Áreas antes de registrar Jefes.")
    else:
        mapa_area = {f"{r['direccion']} - {r['unidad']} - {r['nombre']}": r["id"] for _, r in df_areas.iterrows()}
        asel = st.selectbox("Área del jefe", list(mapa_area.keys()))
        with st.form("f_jefe", clear_on_submit=True):
            nj = st.text_input("Nombres del jefe")
            cj = st.text_input("Cargo")
            if st.form_submit_button("Guardar Jefe"):
                if nj.strip():
                    conn.execute("INSERT INTO jefes(nombres,cargo,area_id) VALUES(?,?,?)", (nj.strip(), cj.strip(), mapa_area[asel]))
                    conn.commit()
                    st.success("Jefe registrado.")
                    do_rerun()
                else:
                    st.error("Nombre obligatorio.")

        df_j = pd.read_sql("""
            SELECT j.id,j.nombres,j.cargo,a.nombre AS area,u.nombre AS unidad,d.nombre AS direccion
            FROM jefes j
            JOIN areas a ON a.id=j.area_id
            JOIN unidades u ON u.id=a.unidad_id
            JOIN direcciones d ON d.id=u.direccion_id
            ORDER BY d.nombre,u.nombre,a.nombre,j.nombres
        """, conn)
        st.dataframe(df_j, use_container_width=True)

    conn.close()

# =========================================================
# Módulo: Registrar Trabajador (CRUD + periodos)
# =========================================================
elif menu == "Registrar Trabajador":
    st.header("Trabajadores")
    conn = get_conn()

    df_area_ctx = pd.read_sql("""
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

    if df_area_ctx.empty:
        st.warning("Primero registra Direcciones → Unidades → Áreas.")
        conn.close()
        st.stop()
    if df_jef.empty:
        st.warning("Primero registra Jefes.")
        conn.close()
        st.stop()

    mapa_area = {f"{r['direccion']} - {r['unidad']} - {r['area']}": r["id"] for _, r in df_area_ctx.iterrows()}
    mapa_jef = {f"{r['direccion']} - {r['unidad']} - {r['area']} - {r['nombres']}": r["id"] for _, r in df_jef.iterrows()}

    with st.form("f_trab", clear_on_submit=True):
        numero = st.text_input("Número (opcional)")
        dni = st.text_input("DNI")
        nombres = st.text_input("Apellidos y Nombres")
        cargo = st.text_input("Cargo")
        regimen = st.selectbox("Régimen", ["DL 276","DL 728","DL 1057","Carrera Especial"])
        fecha_ing = st.date_input("Fecha de ingreso")
        area_sel = st.selectbox("Área", list(mapa_area.keys()))
        jefe_sel = st.selectbox("Jefe inmediato", list(mapa_jef.keys()))
        if st.form_submit_button("Guardar trabajador"):
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
                    fecha_ing.strftime("%Y-%m-%d"),
                    mapa_area[area_sel],
                    mapa_jef[jefe_sel]
                ))
                tid = cur.lastrowid
                conn.commit()
                conn.close()
                generar_periodos_para_trabajador(tid, fecha_ing.strftime("%Y-%m-%d"))
                st.success("Trabajador registrado y periodos generados.")
                do_rerun()

    # Listado + buscador
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        f_nom = st.text_input("Buscar por nombre")
    with col2:
        f_dni = st.text_input("Buscar por DNI")

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

    if f_nom:
        df_trab = df_trab[df_trab["nombres"].str.contains(f_nom, case=False, na=False)]
    if f_dni:
        df_trab = df_trab[df_trab["dni"].fillna("").str.contains(f_dni, na=False)]
    st.dataframe(df_trab, use_container_width=True)

    conn.close()

# =========================================================
# Reporte de Trabajadores
# =========================================================
elif menu == "Reporte de Trabajadores":
    st.header("Reporte de Trabajadores")
    conn = get_conn()
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
    st.dataframe(df, use_container_width=True)
    st.download_button("Descargar CSV (Trabajadores)", df.to_csv(index=False).encode("utf-8"), "trabajadores.csv", "text/csv")
    conn.close()

# =========================================================
# Resoluciones (usa resoluciones_norm para evitar DatabaseError)
# =========================================================
elif menu == "Resoluciones":
    st.header("Resoluciones")
    conn = get_conn()

    df_t = pd.read_sql("SELECT id,nombres,fecha_ingreso FROM trabajadores ORDER BY nombres", conn)
    if df_t.empty:
        st.warning("No hay trabajadores.")
        conn.close()
        st.stop()

    mapa_t = dict(zip(df_t["nombres"], df_t["id"]))
    tsel = st.selectbox("Trabajador", list(mapa_t.keys()))
    tid = mapa_t[tsel]

    fin_ing = df_t[df_t["id"]==tid]["fecha_ingreso"].values[0]
    generar_periodos_para_trabajador(tid, fin_ing)

    dfp = periodos_trabajador_df(tid)
    if dfp.empty:
        st.info("Aún no tiene periodos completos.")
        conn.close()
        st.stop()

    mapa_p = {f"{r['inicio_ciclo']} a {r['fin_ciclo']} (restantes {30-int(r['dias_usados'])})": r["id"] for _, r in dfp.iterrows()}
    pid_label = st.selectbox("Periodo", list(mapa_p.keys()))
    pid = mapa_p[pid_label]

    st.subheader("Registrar resolución")
    with st.form("f_res", clear_on_submit=True):
        numero = st.text_input("Número")
        fi = st.date_input("Fecha inicio autorizada")
        ff = st.date_input("Fecha fin autorizada")
        da = st.number_input("Días autorizados", 1, 30, value=30)
        fr = st.checkbox("Fraccionable")
        ob = st.text_area("Observaciones")
        if st.form_submit_button("Registrar"):
            if not numero.strip():
                st.error("Número obligatorio.")
            else:
                conn.execute("""
                    INSERT INTO resoluciones(trabajador_id,periodo_id,numero,fecha_inicio,fecha_fin,dias_autorizados,fraccionable,observaciones)
                    VALUES(?,?,?,?,?,?,?,?)
                """, (tid, pid, numero.strip(), fi.strftime("%Y-%m-%d"), ff.strftime("%Y-%m-%d"), int(da), 1 if fr else 0, ob.strip()))
                conn.commit()
                st.success("Resolución registrada.")
                do_rerun()

    st.divider()
    st.subheader("Resoluciones del trabajador (compatibles)")
    # 👇 Esto evita el error que reportaste
    df_res = pd.read_sql("""
        SELECT * FROM resoluciones_norm
        WHERE trabajador_id=?
        ORDER BY date(COALESCE(fecha_inicio,'1900-01-01')) DESC
    """, conn, params=(tid,))
    st.dataframe(df_res, use_container_width=True)

    conn.close()

# =========================================================
# Registrar Vacaciones (usa resoluciones_norm + vacaciones_norm)
# =========================================================
elif menu == "Registrar Vacaciones":
    st.header("Registrar Vacaciones")
    conn = get_conn()

    df_t = pd.read_sql("SELECT id,nombres,fecha_ingreso FROM trabajadores ORDER BY nombres", conn)
    if df_t.empty:
        st.warning("No hay trabajadores.")
        conn.close()
        st.stop()

    mapa_t = dict(zip(df_t["nombres"], df_t["id"]))
    tsel = st.selectbox("Trabajador", list(mapa_t.keys()))
    tid = mapa_t[tsel]

    fin_ing = df_t[df_t["id"]==tid]["fecha_ingreso"].values[0]
    generar_periodos_para_trabajador(tid, fin_ing)

    dfp = periodos_trabajador_df(tid)
    dfp = dfp[dfp.apply(periodo_vigente, axis=1)]
    if dfp.empty:
        st.warning("No hay periodos vigentes para registrar.")
        conn.close()
        st.stop()

    mapa_p = {f"{r['inicio_ciclo']} a {r['fin_ciclo']} | saldo {30-int(r['dias_usados'])} | acumulable {r['acumulable_hasta']}": r["id"] for _, r in dfp.iterrows()}
    pid_label = st.selectbox("Periodo", list(mapa_p.keys()))
    pid = mapa_p[pid_label]

    usar_res = st.checkbox("Usar resolución", value=True)

    resolucion_id = None
    modo_res = None

    if usar_res:
        # 👇 ESTE ERA TU ERROR: ahora consultamos la vista resoluciones_norm (no falla aunque falten columnas)
        df_res = pd.read_sql("""
            SELECT * FROM resoluciones_norm
            WHERE trabajador_id=?
            ORDER BY date(COALESCE(fecha_inicio,'1900-01-01')) DESC
        """, conn, params=(tid,))

        if df_res.empty:
            st.error("No hay resoluciones para este trabajador (o no están completas).")
            conn.close()
            st.stop()

        mapa_res = {f"{r['numero']} | {r['dias_autorizados']} días | Fracc: {'Sí' if int(r['fraccionable'] or 0)==1 else 'No'}": r["id"] for _, r in df_res.iterrows()}
        rsel = st.selectbox("Resolución", list(mapa_res.keys()))
        resolucion_id = mapa_res[rsel]
        saldo_res = dias_resolucion_restantes(resolucion_id)
        st.info(f"Saldo resolución: {saldo_res}")

        fracc_ok = int(df_res[df_res["id"]==resolucion_id]["fraccionable"].values[0] or 0) == 1
        modo_res = st.radio("Ejecución", ["Íntegra"] + (["Fraccionada"] if fracc_ok else []), horizontal=True)

        if modo_res == "Íntegra":
            tipo = "Resolución"
        else:
            tipo = st.selectbox("Sustento fraccionamiento", ["Memorando","Solicitud"])
    else:
        tipo = st.selectbox("Tipo", ["Memorando","Solicitud"])

    documento = st.text_input("Documento (N°)")
    dias = st.number_input("Días", 1, 30, value=7)
    fi = st.date_input("Inicio")
    ff = fi + datetime.timedelta(days=int(dias)-1)
    st.caption(f"Fin calculado: {ff}")

    saldo_p = dias_periodo_restantes(pid)
    st.info(f"Saldo periodo: {saldo_p}")

    if st.button("Registrar Vacación"):
        # validaciones
        if int(dias) > saldo_p:
            st.error("Excede saldo del periodo.")
        elif usar_res and resolucion_id is not None and int(dias) > dias_resolucion_restantes(resolucion_id):
            st.error("Excede saldo de la resolución.")
        else:
            conn.execute("""
                INSERT INTO vacaciones(trabajador_id,periodo_id,resolucion_id,tipo,modo_resolucion,fecha_inicio,fecha_fin,dias,documento,observaciones,autorizado_rrhh)
                VALUES(?,?,?,?,?,?,?,?,?,?,0)
            """, (
                tid, pid, resolucion_id, tipo,
                ("Integra" if usar_res and modo_res=="Íntegra" else ("Fraccionada" if usar_res else None)),
                fi.strftime("%Y-%m-%d"), ff.strftime("%Y-%m-%d"),
                int(dias),
                documento.strip() if documento.strip() else None,
                None
            ))
            conn.commit()
            st.success("Registrado.")
            do_rerun()

    st.divider()
    st.subheader("Vacaciones registradas (vista robusta)")
    df_v = pd.read_sql("""
        SELECT v.*, t.nombres AS trabajador, r.numero AS resolucion_num
        FROM vacaciones_norm v
        JOIN trabajadores t ON t.id=v.trabajador_id
        LEFT JOIN resoluciones_norm r ON r.id=v.resolucion_id
        WHERE v.trabajador_id=? AND v.periodo_id=?
        ORDER BY date(v.fecha_inicio) DESC
    """, conn, params=(tid, pid))
    st.dataframe(df_v, use_container_width=True)

    conn.close()

# =========================================================
# Adelanto de Vacaciones (Acuerdo + solicitud proporcional)
# =========================================================
elif menu == "Adelanto de Vacaciones":
    st.header("Adelanto de Vacaciones (Acuerdo)")

    conn = get_conn()
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
    st.info(f"Ciclo en curso: {ini} a {fin} | Máximo adelanto proporcional: {max_dias} días")

    with st.form("f_adel", clear_on_submit=True):
        dias_sol = st.number_input("Días solicitados (adelanto)", 0, 30, value=min(5, max_dias))
        doc = st.text_input("Documento de acuerdo (obligatorio)")
        obs = st.text_area("Observaciones")
        if st.form_submit_button("Registrar Solicitud de Adelanto"):
            if not doc.strip():
                st.error("Documento de acuerdo es obligatorio.")
            elif int(dias_sol) > max_dias:
                st.error("Excede el máximo proporcional permitido.")
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
                    obs.strip()
                ))
                conn.commit()
                st.success("Solicitud registrada (pendiente RRHH).")
                do_rerun()

    st.divider()
    st.subheader("Solicitudes de adelanto")
    df_a = pd.read_sql("""
        SELECT a.*, t.nombres AS trabajador
        FROM adelantos a
        JOIN trabajadores t ON t.id=a.trabajador_id
        WHERE a.trabajador_id=?
        ORDER BY date(a.fecha_solicitud) DESC
    """, conn, params=(tid,))
    st.dataframe(df_a, use_container_width=True)
    conn.close()

# =========================================================
# Panel RRHH (vacaciones + adelantos)
# =========================================================
elif menu == "Panel RRHH":
    st.header("Panel RRHH")

    if ROL not in ("admin","responsable"):
        st.error("No tienes permisos para RRHH.")
        st.stop()

    conn = get_conn()

    st.subheader("Vacaciones pendientes RRHH")
    df_p = pd.read_sql("""
        SELECT v.id, t.nombres AS trabajador, v.tipo, v.modo_resolucion, v.fecha_inicio, v.fecha_fin, v.dias,
               v.documento, v.rrhh_observacion
        FROM vacaciones_norm v
        JOIN trabajadores t ON t.id=v.trabajador_id
        WHERE v.autorizado_rrhh=0
        ORDER BY date(v.fecha_inicio) DESC
    """, conn)
    st.dataframe(df_p, use_container_width=True)

    if not df_p.empty:
        vid = st.selectbox("Vacación a evaluar", df_p["id"], format_func=lambda x: f"ID {x} - {df_p[df_p['id']==x]['trabajador'].values[0]}")
        obs = st.text_area("Observación RRHH", key="obs_rrhh_vac")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Aprobar Vacación"):
                conn.execute("""
                    UPDATE vacaciones
                    SET autorizado_rrhh=1, rrhh_observacion=?, fecha_aprob_rrhh=?, usuario_rrhh=?
                    WHERE id=?
                """, (obs.strip(), datetime.date.today().strftime("%Y-%m-%d"), USER, vid))
                conn.commit()
                st.success("Aprobado.")
                do_rerun()
        with col2:
            if st.button("📝 Guardar observación (sin aprobar)"):
                conn.execute("UPDATE vacaciones SET rrhh_observacion=? WHERE id=?", (obs.strip(), vid))
                conn.commit()
                st.success("Observación guardada.")
                do_rerun()

    st.divider()
    st.subheader("Adelantos pendientes RRHH")
    df_ap = pd.read_sql("""
        SELECT a.id, t.nombres AS trabajador, a.fecha_solicitud, a.ciclo_inicio, a.ciclo_fin,
               a.dias_maximo, a.dias_solicitados, a.documento_acuerdo, a.rrhh_observacion
        FROM adelantos a
        JOIN trabajadores t ON t.id=a.trabajador_id
        WHERE a.aprobado_rrhh=0
        ORDER BY date(a.fecha_solicitud) DESC
    """, conn)
    st.dataframe(df_ap, use_container_width=True)

    if not df_ap.empty:
        aid = st.selectbox("Adelanto a evaluar", df_ap["id"], format_func=lambda x: f"ID {x} - {df_ap[df_ap['id']==x]['trabajador'].values[0]}")
        obs2 = st.text_area("Observación RRHH (adelanto)", key="obs_rrhh_adel")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Aprobar Adelanto"):
                conn.execute("""
                    UPDATE adelantos
                    SET aprobado_rrhh=1, rrhh_observacion=?, fecha_aprob_rrhh=?, usuario_rrhh=?
                    WHERE id=?
                """, (obs2.strip(), datetime.date.today().strftime("%Y-%m-%d"), USER, aid))
                conn.commit()
                st.success("Adelanto aprobado.")
                do_rerun()
        with col2:
            if st.button("📝 Guardar observación (adelanto)"):
                conn.execute("UPDATE adelantos SET rrhh_observacion=? WHERE id=?", (obs2.strip(), aid))
                conn.commit()
                st.success("Observación guardada.")
                do_rerun()

    conn.close()

# =========================================================
# Dashboard (semaforización)
# =========================================================
elif menu == "Dashboard":
    st.header("Dashboard")
    conn = get_conn()
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
        dfp["acumulable_hasta_date"] = dfp["acumulable_hasta"].apply(to_date)
        dfp["estado"] = dfp["acumulable_hasta_date"].apply(
            lambda d: "🔴 Vencido" if hoy > d else ("🟡 Por vencer" if hoy > (d - datetime.timedelta(days=60)) else "🟢 Vigente")
        )
        dfp["dias_restantes"] = 30 - dfp["dias_usados"].astype(int)

    col1, col2, col3 = st.columns(3)
    col1.metric("🟢 Vacaciones en goce (mes)", int(df_goce.shape[0]))
    col2.metric("🟡 Periodos por vencer", int((dfp["estado"]=="🟡 Por vencer").sum()) if not dfp.empty else 0)
    col3.metric("🔴 Periodos vencidos", int((dfp["estado"]=="🔴 Vencido").sum()) if not dfp.empty else 0)

    st.divider()
    st.subheader("🟢 Vacaciones en goce durante el mes")
    st.dataframe(df_goce, use_container_width=True)

    st.divider()
    st.subheader("Semáforo de periodos")
    if dfp.empty:
        st.info("Sin periodos aún.")
    else:
        st.dataframe(dfp[[
            "trabajador","inicio_ciclo","fin_ciclo","goce_hasta","acumulable_hasta",
            "dias_usados","dias_restantes","estado"
        ]], use_container_width=True)

    conn.close()

# =========================================================
# Reportes (robustos con vistas) + CSV
# =========================================================
elif menu == "Reportes":
    st.header("Reportes")
    conn = get_conn()

    df_t = pd.read_sql("SELECT id,nombres,dni FROM trabajadores ORDER BY nombres", conn)
    mapa_t = {"Todos": None}
    for _, r in df_t.iterrows():
        mapa_t[f"{r['nombres']} ({r['dni'] or ''})"] = r["id"]
    tsel = st.selectbox("Trabajador", list(mapa_t.keys()))
    tid = mapa_t[tsel]

    tipo_sel = st.selectbox("Tipo", ["Todos","Solicitud","Memorando","Resolución"])
    rrhh_sel = st.selectbox("RRHH", ["Todos","Pendientes","Aprobadas"])

    q = """
        SELECT v.*, t.nombres AS trabajador, t.dni,
               r.numero AS resolucion_num,
               p.inicio_ciclo, p.fin_ciclo
        FROM vacaciones_norm v
        JOIN trabajadores t ON t.id=v.trabajador_id
        LEFT JOIN resoluciones_norm r ON r.id=v.resolucion_id
        LEFT JOIN periodos p ON p.id=v.periodo_id
        WHERE 1=1
    """
    params = []
    if tid is not None:
        q += " AND v.trabajador_id=?"
        params.append(tid)
    if tipo_sel != "Todos":
        q += " AND v.tipo=?"
        params.append(tipo_sel)
    if rrhh_sel == "Pendientes":
        q += " AND v.autorizado_rrhh=0"
    elif rrhh_sel == "Aprobadas":
        q += " AND v.autorizado_rrhh=1"
    q += " ORDER BY date(v.fecha_inicio) DESC"

    # 👇 ESTA CONSULTA YA NO REVIENTA por diferencias de columnas
    df_v = pd.read_sql(q, conn, params=params)
    st.dataframe(df_v, use_container_width=True)

    st.download_button("Descargar CSV (Vacaciones)", df_v.to_csv(index=False).encode("utf-8"), "vacaciones.csv", "text/csv")

    st.divider()
    st.subheader("Adelantos")
    df_a = pd.read_sql("""
        SELECT a.*, t.nombres AS trabajador, t.dni
        FROM adelantos a
        JOIN trabajadores t ON t.id=a.trabajador_id
        ORDER BY date(a.fecha_solicitud) DESC
    """, conn)
    if tid is not None:
        df_a = df_a[df_a["trabajador_id"] == tid]
    st.dataframe(df_a, use_container_width=True)
    st.download_button("Descargar CSV (Adelantos)", df_a.to_csv(index=False).encode("utf-8"), "adelantos.csv", "text/csv")

    conn.close()

# =========================================================
# Usuarios y Permisos (admin) + explicación de permisos
# =========================================================
elif menu == "Usuarios y Permisos":
    st.header("Usuarios y Permisos")

    permisos = {
        "admin": "Acceso total: configuración, RRHH, reportes, usuarios, backups.",
        "responsable": "Puede registrar/editar datos y aprobar RRHH, ver reportes y dashboard.",
        "registrador": "Puede registrar trabajadores, resoluciones, vacaciones y ver reportes básicos."
    }
    st.subheader("Permisos por rol")
    st.write("- **admin**: " + permisos["admin"])
    st.write("- **responsable**: " + permisos["responsable"])
    st.write("- **registrador**: " + permisos["registrador"])

    if ROL != "admin":
        st.error("Solo admin puede gestionar usuarios.")
        st.stop()

    conn = get_conn()
    df_u = pd.read_sql("SELECT id,usuario,rol FROM usuarios ORDER BY usuario", conn)
    st.dataframe(df_u, use_container_width=True)

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
                try:
                    conn.execute("INSERT INTO usuarios(usuario,password_hash,rol) VALUES(?,?,?)", (u.strip(), hash_password(p.strip()), r))
                    conn.commit()
                    st.success("Usuario creado.")
                    do_rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

    st.divider()
    st.subheader("Actualizar rol / Reset password / Eliminar")
    if not df_u.empty:
        uid = st.selectbox("Usuario", df_u["id"], format_func=lambda x: df_u[df_u["id"]==x]["usuario"].values[0])
        urow = conn.execute("SELECT * FROM usuarios WHERE id=?", (uid,)).fetchone()

        col1, col2, col3 = st.columns(3)
        with col1:
            nuevo_rol = st.selectbox("Nuevo rol", ["admin","responsable","registrador"], index=["admin","responsable","registrador"].index(urow["rol"]))
            if st.button("Actualizar rol"):
                conn.execute("UPDATE usuarios SET rol=? WHERE id=?", (nuevo_rol, uid))
                conn.commit()
                st.success("Rol actualizado.")
                do_rerun()
        with col2:
            np = st.text_input("Nueva contraseña", type="password", key="np")
            if st.button("Reset password"):
                if not np.strip():
                    st.error("Contraseña obligatoria.")
                else:
                    conn.execute("UPDATE usuarios SET password_hash=? WHERE id=?", (hash_password(np.strip()), uid))
                    conn.commit()
                    st.success("Contraseña actualizada.")
                    do_rerun()
        with col3:
            if st.button("Eliminar usuario"):
                if urow["usuario"] == "admin":
                    st.error("No se puede eliminar admin principal.")
                else:
                    conn.execute("DELETE FROM usuarios WHERE id=?", (uid,))
                    conn.commit()
                    st.success("Usuario eliminado.")
                    do_rerun()

    conn.close()

# =========================================================
# Exportar / Backup (CSV + descargar SQLite)
# =========================================================
elif menu == "Exportar / Backup":
    st.header("Exportar / Backup")
    conn = get_conn()
    tablas = ["usuarios","direcciones","unidades","areas","jefes","trabajadores","periodos","resoluciones","vacaciones","adelantos"]
    tsel = st.selectbox("Tabla", tablas)
    df = pd.read_sql(f"SELECT * FROM {tsel}", conn)
    st.dataframe(df, use_container_width=True)
    st.download_button(f"Descargar {tsel}.csv", df.to_csv(index=False).encode("utf-8"), f"{tsel}.csv", "text/csv")

    st.divider()
    st.subheader("Backup completo (SQLite)")
    if os.path.exists(DB_PATH):
        with open(DB_PATH, "rb") as f:
            data = f.read()
        st.download_button("Descargar vacaciones.db", data, "vacaciones.db", "application/x-sqlite3")
    else:
        st.warning("No se encontró el archivo de base de datos.")

    conn.close()
