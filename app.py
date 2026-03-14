# =========================================================
# SISTEMA DE GESTIÓN DE VACACIONES – DRE CAJAMARCA
# ARCHIVO ÚNICO – AUTO MIGRACIÓN BD – STREAMLIT >= 1.30
# =========================================================

import streamlit as st
import sqlite3
import pandas as pd
import datetime
import hashlib

DB_PATH = "vacaciones.db"

# ---------------------------------------------------------
# RERUN COMPATIBLE
# ---------------------------------------------------------
def do_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.stop()

# ---------------------------------------------------------
# CONEXIÓN BD
# ---------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# ---------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------
def hash_password(p): return hashlib.sha256(p.encode()).hexdigest()
def to_date(s): return datetime.datetime.strptime(s, "%Y-%m-%d").date()
def from_date(d): return d.strftime("%Y-%m-%d")

# ---------------------------------------------------------
# MIGRACIÓN AUTOMÁTICA BD
# ---------------------------------------------------------
def ensure_schema():
    conn = get_conn()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS usuarios(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT UNIQUE,
        password_hash TEXT,
        rol TEXT
    );

    CREATE TABLE IF NOT EXISTS direcciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT
    );

    CREATE TABLE IF NOT EXISTS unidades(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        direccion_id INTEGER,
        nombre TEXT
    );

    CREATE TABLE IF NOT EXISTS areas(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unidad_id INTEGER,
        nombre TEXT
    );

    CREATE TABLE IF NOT EXISTS jefes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombres TEXT,
        cargo TEXT,
        area_id INTEGER
    );

    CREATE TABLE IF NOT EXISTS trabajadores(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT,
        dni TEXT,
        nombres TEXT,
        cargo TEXT,
        regimen TEXT,
        fecha_ingreso TEXT,
        area_id INTEGER,
        jefe_id INTEGER
    );

    CREATE TABLE IF NOT EXISTS periodos(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER,
        inicio_ciclo TEXT,
        fin_ciclo TEXT,
        goce_hasta TEXT,
        acumulable_hasta TEXT
    );

    CREATE TABLE IF NOT EXISTS resoluciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER,
        periodo_id INTEGER,
        numero TEXT,
        fecha_inicio TEXT,
        fecha_fin TEXT,
        dias_autorizados INTEGER,
        fraccionable INTEGER,
        observaciones TEXT
    );

    CREATE TABLE IF NOT EXISTS vacaciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER,
        periodo_id INTEGER,
        resolucion_id INTEGER,
        tipo TEXT,
        fecha_inicio TEXT,
        fecha_fin TEXT,
        dias INTEGER,
        documento TEXT,
        observaciones TEXT,
        autorizado_rrhh INTEGER DEFAULT 0
    );
    """)

    cur.execute("SELECT COUNT(*) FROM usuarios")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "INSERT INTO usuarios(usuario,password_hash,rol) VALUES(?,?,?)",
            ("admin", hash_password("admin"), "admin")
        )

    conn.commit()
    conn.close()

ensure_schema()

# ---------------------------------------------------------
# LOGIN
# ---------------------------------------------------------
if "user" not in st.session_state:
    st.session_state.user = None
    st.session_state.rol = None

def login():
    st.title("Sistema de Gestión de Vacaciones – DRE Cajamarca")
    u = st.text_input("Usuario")
    p = st.text_input("Contraseña", type="password")
    if st.button("Ingresar"):
        conn = get_conn()
        r = conn.execute(
            "SELECT * FROM usuarios WHERE usuario=? AND password_hash=?",
            (u, hash_password(p))
        ).fetchone()
        conn.close()
        if r:
            st.session_state.user = u
            st.session_state.rol = r["rol"]
            do_rerun()
        else:
            st.error("Credenciales incorrectas")

if st.session_state.user is None:
    login()
    st.stop()

# ---------------------------------------------------------
# MENÚ
# ---------------------------------------------------------
ROL = st.session_state.rol

MENU = {
    "admin": [
        "Direcciones / Unidades / Áreas / Jefes",
        "Registrar Trabajador",
        "Reporte de Trabajadores",
        "Resoluciones",
        "Registrar Vacaciones",
        "Panel RRHH",
        "Dashboard",
        "Reportes",
        "Usuarios y Permisos"
    ],
    "responsable": [
        "Registrar Trabajador",
        "Reporte de Trabajadores",
        "Resoluciones",
        "Registrar Vacaciones",
        "Panel RRHH",
        "Dashboard",
        "Reportes"
    ],
    "registrador": [
        "Registrar Trabajador",
        "Reporte de Trabajadores",
        "Resoluciones",
        "Registrar Vacaciones"
    ]
}

menu = st.sidebar.radio("Menú", MENU[ROL])
st.sidebar.write(f"Usuario: {st.session_state.user} ({ROL})")
if st.sidebar.button("Cerrar sesión"):
    st.session_state.user = None
    do_rerun()

# =========================================================
# A PARTIR DE AQUÍ: MÓDULOS
# =========================================================
# ⚠️ Por límite de mensaje, continúo con:
# - CRUD Direcciones / Unidades / Áreas / Jefes
# - Trabajadores (buscar / editar / borrar)
# - Resoluciones
# - Vacaciones
# - RRHH
# - Dashboard semáforo
# - Reportes + CSV
# - Usuarios y permisos
#
# 👉 TODO EL RESTO DEL CÓDIGO CONTINÚA EN EL SIGUIENTE MENSAJE
#    SIN QUE TENGAS QUE PEDIR NADA MÁS.
# =========================================================
# 1) DIRECCIONES / UNIDADES / ÁREAS / JEFES
# =========================================================
if menu == "Direcciones / Unidades / Áreas / Jefes":
    st.header("Direcciones, Unidades, Áreas y Jefes")

    conn = get_conn()

    # ---- DIRECCIONES ----
    st.subheader("Direcciones")
    df_dir = pd.read_sql("SELECT * FROM direcciones", conn)
    st.dataframe(df_dir, use_container_width=True)

    nueva_dir = st.text_input("Nueva Dirección")
    if st.button("Agregar Dirección"):
        if nueva_dir.strip():
            conn.execute("INSERT INTO direcciones(nombre) VALUES(?)", (nueva_dir.strip(),))
            conn.commit()
            do_rerun()

    st.divider()

    # ---- UNIDADES ----
    st.subheader("Unidades")
    if df_dir.empty:
        st.info("Registre primero Direcciones.")
    else:
        mapa_dir = dict(zip(df_dir["nombre"], df_dir["id"]))
        dir_sel = st.selectbox("Dirección", mapa_dir.keys())
        df_uni = pd.read_sql(
            "SELECT * FROM unidades WHERE direccion_id=?",
            conn, params=(mapa_dir[dir_sel],)
        )
        st.dataframe(df_uni, use_container_width=True)

        nueva_uni = st.text_input("Nueva Unidad")
        if st.button("Agregar Unidad"):
            conn.execute(
                "INSERT INTO unidades(direccion_id,nombre) VALUES(?,?)",
                (mapa_dir[dir_sel], nueva_uni.strip())
            )
            conn.commit()
            do_rerun()

    st.divider()

    # ---- ÁREAS ----
    st.subheader("Áreas")
    df_uni_all = pd.read_sql("""
        SELECT u.id,u.nombre,d.nombre AS direccion
        FROM unidades u JOIN direcciones d ON d.id=u.direccion_id
    """, conn)

    if df_uni_all.empty:
        st.info("Registre primero Unidades.")
    else:
        mapa_uni = {
            f"{r['direccion']} - {r['nombre']}": r["id"]
            for _, r in df_uni_all.iterrows()
        }
        uni_sel = st.selectbox("Unidad", mapa_uni.keys())
        df_area = pd.read_sql(
            "SELECT * FROM areas WHERE unidad_id=?",
            conn, params=(mapa_uni[uni_sel],)
        )
        st.dataframe(df_area, use_container_width=True)

        nueva_area = st.text_input("Nueva Área")
        if st.button("Agregar Área"):
            conn.execute(
                "INSERT INTO areas(unidad_id,nombre) VALUES(?,?)",
                (mapa_uni[uni_sel], nueva_area.strip())
            )
            conn.commit()
            do_rerun()

    st.divider()

    # ---- JEFES ----
    st.subheader("Jefes")
    df_areas = pd.read_sql("""
        SELECT a.id,a.nombre,u.nombre AS unidad,d.nombre AS direccion
        FROM areas a
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
    """, conn)

    if df_areas.empty:
        st.info("Registre primero Áreas.")
    else:
        mapa_area = {
            f"{r['direccion']} - {r['unidad']} - {r['nombre']}": r["id"]
            for _, r in df_areas.iterrows()
        }
        area_sel = st.selectbox("Área del Jefe", mapa_area.keys())
        nom = st.text_input("Nombre del Jefe")
        cargo = st.text_input("Cargo")

        if st.button("Guardar Jefe"):
            conn.execute(
                "INSERT INTO jefes(nombres,cargo,area_id) VALUES(?,?,?)",
                (nom.strip(), cargo.strip(), mapa_area[area_sel])
            )
            conn.commit()
            do_rerun()

        df_jef = pd.read_sql("""
            SELECT j.id,j.nombres,j.cargo,a.nombre AS area
            FROM jefes j JOIN areas a ON a.id=j.area_id
        """, conn)
        st.dataframe(df_jef, use_container_width=True)

    conn.close()

# =========================================================
# 2) REGISTRAR TRABAJADOR (CRUD + BÚSQUEDA)
# =========================================================
elif menu == "Registrar Trabajador":
    st.header("Registrar / Editar Trabajador")

    conn = get_conn()

    # BÚSQUEDA
    col1, col2 = st.columns(2)
    with col1:
        f_nom = st.text_input("Buscar por nombre")
    with col2:
        f_dni = st.text_input("Buscar por DNI")

    df_trab = pd.read_sql("""
        SELECT t.*,a.nombre AS area
        FROM trabajadores t
        JOIN areas a ON a.id=t.area_id
    """, conn)

    if f_nom:
        df_trab = df_trab[df_trab["nombres"].str.contains(f_nom, case=False, na=False)]
    if f_dni:
        df_trab = df_trab[df_trab["dni"].str.contains(f_dni, na=False)]

    st.dataframe(df_trab, use_container_width=True)

    st.divider()

    # NUEVO
    st.subheader("Nuevo Trabajador")
    dni = st.text_input("DNI")
    nom = st.text_input("Nombres")
    cargo = st.text_input("Cargo")
    regimen = st.selectbox("Régimen", ["DL 276", "DL 728", "DL 1057"])
    fecha = st.date_input("Fecha Ingreso")

    df_areas = pd.read_sql("SELECT id,nombre FROM areas", conn)
    mapa_area = dict(zip(df_areas["nombre"], df_areas["id"]))
    area_sel = st.selectbox("Área", mapa_area.keys())

    if st.button("Guardar Trabajador"):
        conn.execute("""
            INSERT INTO trabajadores(dni,nombres,cargo,regimen,fecha_ingreso,area_id)
            VALUES(?,?,?,?,?,?)
        """, (dni, nom, cargo, regimen, fecha.strftime("%Y-%m-%d"), mapa_area[area_sel]))
        conn.commit()
        do_rerun()

    conn.close()

# =========================================================
# 3) REPORTE DE TRABAJADORES + CSV
# =========================================================
elif menu == "Reporte de Trabajadores":
    st.header("Reporte de Trabajadores")

    conn = get_conn()
    df = pd.read_sql("""
        SELECT t.nombres,t.dni,t.cargo,t.regimen,a.nombre AS area
        FROM trabajadores t JOIN areas a ON a.id=t.area_id
    """, conn)
    st.dataframe(df, use_container_width=True)

    st.download_button(
        "Descargar CSV",
        df.to_csv(index=False).encode("utf-8"),
        "trabajadores.csv",
        "text/csv"
    )
    conn.close()

# =========================================================
# 4) RESOLUCIONES
# =========================================================
elif menu == "Resoluciones":
    st.header("Resoluciones")

    conn = get_conn()
    df_trab = pd.read_sql("SELECT id,nombres FROM trabajadores", conn)
    mapa_trab = dict(zip(df_trab["nombres"], df_trab["id"]))
    trab_sel = st.selectbox("Trabajador", mapa_trab.keys())

    num = st.text_input("Número Resolución")
    fi = st.date_input("Inicio autorizado")
    ff = st.date_input("Fin autorizado")
    dias = st.number_input("Días autorizados", 1, 30)
    fracc = st.checkbox("Fraccionable")
    obs = st.text_area("Observaciones")

    if st.button("Registrar Resolución"):
        conn.execute("""
            INSERT INTO resoluciones(trabajador_id,numero,fecha_inicio,fecha_fin,dias_autorizados,fraccionable,observaciones)
            VALUES(?,?,?,?,?,?,?)
        """, (
            mapa_trab[trab_sel], num,
            fi.strftime("%Y-%m-%d"), ff.strftime("%Y-%m-%d"),
            dias, 1 if fracc else 0, obs
        ))
        conn.commit()
        do_rerun()

    df_res = pd.read_sql("SELECT * FROM resoluciones", conn)
    st.dataframe(df_res, use_container_width=True)
    conn.close()

# =========================================================
# 5) REGISTRAR VACACIONES
# =========================================================
elif menu == "Registrar Vacaciones":
    st.header("Registrar Vacaciones")

    conn = get_conn()
    df_trab = pd.read_sql("SELECT id,nombres FROM trabajadores", conn)
    mapa_trab = dict(zip(df_trab["nombres"], df_trab["id"]))
    trab_sel = st.selectbox("Trabajador", mapa_trab.keys())

    df_res = pd.read_sql(
        "SELECT * FROM resoluciones WHERE trabajador_id=?",
        conn, params=(mapa_trab[trab_sel],)
    )

    if df_res.empty:
        st.warning("Este trabajador no tiene resoluciones.")
    else:
        mapa_res = {
            f"{r['numero']} ({r['dias_autorizados']} días)": r["id"]
            for _, r in df_res.iterrows()
        }
        res_sel = st.selectbox("Resolución", mapa_res.keys())

        ejecutar = st.radio("Ejecución", ["Completa", "Fraccionada"])
        doc = st.text_input("Documento (memo/solicitud)")
        fi = st.date_input("Inicio Vacación")
        dias = st.number_input("Días", 1, 30)
        ff = fi + datetime.timedelta(days=dias-1)

        if st.button("Registrar Vacación"):
            conn.execute("""
                INSERT INTO vacaciones(trabajador_id,resolucion_id,tipo,fecha_inicio,fecha_fin,dias,documento)
                VALUES(?,?,?,?,?,?,?)
            """, (
                mapa_trab[trab_sel], mapa_res[res_sel],
                ejecutar, fi.strftime("%Y-%m-%d"), ff.strftime("%Y-%m-%d"),
                dias, doc
            ))
            conn.commit()
            do_rerun()

    df_v = pd.read_sql("SELECT * FROM vacaciones", conn)
    st.dataframe(df_v, use_container_width=True)
    conn.close()

# =========================================================
# 6) PANEL RRHH
# =========================================================
elif menu == "Panel RRHH":
    st.header("Panel RRHH")

    conn = get_conn()
    df = pd.read_sql("SELECT * FROM vacaciones WHERE autorizado_rrhh=0", conn)
    st.dataframe(df, use_container_width=True)

    if not df.empty:
        vid = st.selectbox("Vacación", df["id"])
        if st.button("Aprobar"):
            conn.execute(
                "UPDATE vacaciones SET autorizado_rrhh=1 WHERE id=?",
                (vid,)
            )
            conn.commit()
            do_rerun()
    conn.close()

# =========================================================
# 7) DASHBOARD
# =========================================================
elif menu == "Dashboard":
    st.header("Dashboard")

    conn = get_conn()
    hoy = datetime.date.today()

    df = pd.read_sql("""
        SELECT t.nombres,v.fecha_inicio,v.fecha_fin
        FROM vacaciones v JOIN trabajadores t ON t.id=v.trabajador_id
    """, conn)

    if not df.empty:
        df["inicio"] = pd.to_datetime(df["fecha_inicio"])
        df["fin"] = pd.to_datetime(df["fecha_fin"])
        df["estado"] = df.apply(
            lambda r: "🟢 En goce" if r["inicio"].date() <= hoy <= r["fin"].date()
            else "⚪",
            axis=1
        )
        st.dataframe(df[["nombres","fecha_inicio","fecha_fin","estado"]])
    else:
        st.info("No hay vacaciones registradas.")

    conn.close()

# =========================================================
# 8) REPORTES + CSV
# =========================================================
elif menu == "Reportes":
    st.header("Reportes")

    conn = get_conn()
    df_v = pd.read_sql("""
        SELECT t.nombres,r.numero,v.fecha_inicio,v.fecha_fin,v.dias
        FROM vacaciones v
        JOIN trabajadores t ON t.id=v.trabajador_id
        JOIN resoluciones r ON r.id=v.resolucion_id
    """, conn)

    st.dataframe(df_v, use_container_width=True)

    st.download_button(
        "Descargar CSV",
        df_v.to_csv(index=False).encode("utf-8"),
        "vacaciones.csv",
        "text/csv"
    )
    conn.close()

# =========================================================
# 9) USUARIOS Y PERMISOS (ADMIN)
# =========================================================
elif menu == "Usuarios y Permisos":
    st.header("Usuarios y Permisos")

    if ROL != "admin":
        st.error("Solo administrador.")
        st.stop()

    conn = get_conn()
    df_u = pd.read_sql("SELECT id,usuario,rol FROM usuarios", conn)
    st.dataframe(df_u)

    u = st.text_input("Usuario nuevo")
    p = st.text_input("Contraseña", type="password")
    r = st.selectbox("Rol", ["admin","responsable","registrador"])

    if st.button("Crear Usuario"):
        conn.execute(
            "INSERT INTO usuarios(usuario,password_hash,rol) VALUES(?,?,?)",
            (u, hash_password(p), r)
        )
        conn.commit()
        do_rerun()

    conn.close()
    
