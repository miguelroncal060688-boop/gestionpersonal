import streamlit as st
import sqlite3
import pandas as pd
import datetime
import hashlib

DB_PATH = "vacaciones.db"

# =========================
# Helpers generales
# =========================
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

def add_column_if_missing(conn, table, col, col_def_sql):
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}")

# =========================
# Esquema + Migración
# =========================
def ensure_schema():
    conn = get_conn()
    cur = conn.cursor()

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
        trabajador_id INTEGER NOT NULL,
        periodo_id INTEGER NOT NULL,
        numero TEXT NOT NULL,
        fecha_inicio TEXT NOT NULL,
        fecha_fin TEXT NOT NULL,
        dias_autorizados INTEGER NOT NULL,
        fraccionable INTEGER DEFAULT 0,
        observaciones TEXT,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE,
        FOREIGN KEY(periodo_id) REFERENCES periodos(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS vacaciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER NOT NULL,
        periodo_id INTEGER NOT NULL,
        resolucion_id INTEGER,
        tipo TEXT NOT NULL,               -- Solicitud/Memorando/Resolución
        modo_resolucion TEXT,             -- Integra/Fraccionada (si hay resolución)
        fecha_inicio TEXT NOT NULL,
        fecha_fin TEXT NOT NULL,
        dias INTEGER NOT NULL,
        documento TEXT,
        observaciones TEXT,
        autorizado_rrhh INTEGER DEFAULT 0,
        rrhh_observacion TEXT,
        fecha_aprob_rrhh TEXT,
        usuario_rrhh TEXT,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE,
        FOREIGN KEY(periodo_id) REFERENCES periodos(id) ON DELETE CASCADE,
        FOREIGN KEY(resolucion_id) REFERENCES resoluciones(id) ON DELETE SET NULL
    );
    """)

    # Migraciones suaves (por si vienes de una BD previa)
    add_column_if_missing(conn, "vacaciones", "modo_resolucion", "modo_resolucion TEXT")
    add_column_if_missing(conn, "vacaciones", "rrhh_observacion", "rrhh_observacion TEXT")
    add_column_if_missing(conn, "vacaciones", "fecha_aprob_rrhh", "fecha_aprob_rrhh TEXT")
    add_column_if_missing(conn, "vacaciones", "usuario_rrhh", "usuario_rrhh TEXT")
    add_column_if_missing(conn, "resoluciones", "fraccionable", "fraccionable INTEGER DEFAULT 0")

    # Admin inicial
    c = conn.execute("SELECT COUNT(*) AS c FROM usuarios").fetchone()["c"]
    if c == 0:
        conn.execute(
            "INSERT INTO usuarios(usuario,password_hash,rol) VALUES(?,?,?)",
            ("admin", hash_password("admin"), "admin")
        )
    conn.commit()
    conn.close()

# =========================
# Periodos correctos (norma)
# =========================
def generar_periodos_para_trabajador(trabajador_id: int, fecha_ingreso_str: str):
    """
    Regla:
    - Ciclo: ingreso -> +1 año -1 día
    - Goce hasta: fin_ciclo + 1 año
    - Acumulable hasta: fin_ciclo + 2 años
    Solo se generan ciclos COMPLETADOS (fin_ciclo < hoy).
    """
    conn = get_conn()
    cur = conn.cursor()

    fecha_ingreso = to_date(fecha_ingreso_str)
    hoy = datetime.date.today()

    inicio = fecha_ingreso
    while True:
        fin_ciclo = inicio.replace(year=inicio.year + 1) - datetime.timedelta(days=1)
        if fin_ciclo >= hoy:
            break  # ciclo aún no completado

        goce_hasta = fin_ciclo.replace(year=fin_ciclo.year + 1)
        acumulable_hasta = fin_ciclo.replace(year=fin_ciclo.year + 2)

        cur.execute("""
            INSERT OR IGNORE INTO periodos(trabajador_id,inicio_ciclo,fin_ciclo,goce_hasta,acumulable_hasta)
            VALUES(?,?,?,?,?)
        """, (trabajador_id, from_date(inicio), from_date(fin_ciclo), from_date(goce_hasta), from_date(acumulable_hasta)))

        inicio = inicio.replace(year=inicio.year + 1)

    conn.commit()
    conn.close()

def periodos_trabajador(trabajador_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT p.*,
               IFNULL((SELECT SUM(dias) FROM vacaciones v WHERE v.periodo_id=p.id),0) AS dias_usados
        FROM periodos p
        WHERE p.trabajador_id=?
        ORDER BY p.inicio_ciclo
    """, conn, params=(trabajador_id,))
    conn.close()
    return df

def dias_periodo_restantes(periodo_id: int) -> int:
    conn = get_conn()
    row = conn.execute("""
        SELECT 30 - IFNULL((SELECT SUM(dias) FROM vacaciones WHERE periodo_id=?),0) AS r
    """, (periodo_id,)).fetchone()
    conn.close()
    return max(0, int(row["r"]))

def dias_resolucion_restantes(resolucion_id: int) -> int:
    conn = get_conn()
    r = conn.execute("SELECT dias_autorizados FROM resoluciones WHERE id=?", (resolucion_id,)).fetchone()
    if not r:
        conn.close()
        return 0
    autorizados = int(r["dias_autorizados"])
    usados = conn.execute("""
        SELECT IFNULL(SUM(dias),0) AS u FROM vacaciones WHERE resolucion_id=?
    """, (resolucion_id,)).fetchone()["u"]
    conn.close()
    return max(0, autorizados - int(usados))

def periodo_vigente_para_registro(periodo_row) -> bool:
    """Permite registro si hoy <= acumulable_hasta y aún quedan días."""
    hoy = datetime.date.today()
    return hoy <= to_date(periodo_row["acumulable_hasta"]) and (30 - int(periodo_row["dias_usados"])) > 0

# =========================
# LOGIN
# =========================
st.set_page_config(page_title="Vacaciones DRE Cajamarca", layout="wide")
ensure_schema()

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

ROL = st.session_state["rol"]
USER = st.session_state["user"]

# =========================
# Menú por rol
# =========================
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
        "Usuarios y Permisos",
        "Exportar Tablas"
    ],
    "responsable": [
        "Direcciones / Unidades / Áreas / Jefes",
        "Registrar Trabajador",
        "Reporte de Trabajadores",
        "Resoluciones",
        "Registrar Vacaciones",
        "Panel RRHH",
        "Dashboard",
        "Reportes",
        "Exportar Tablas"
    ],
    "registrador": [
        "Registrar Trabajador",
        "Reporte de Trabajadores",
        "Resoluciones",
        "Registrar Vacaciones",
        "Reportes"
    ]
}

menu = st.sidebar.radio("Menú", MENU.get(ROL, MENU["registrador"]))
st.sidebar.write(f"Usuario: {USER} ({ROL})")
if st.sidebar.button("Cerrar sesión"):
    logout()

st.title("Sistema de Gestión de Vacaciones – DRE Cajamarca")

# =========================================================
# MÓDULO: Direcciones / Unidades / Áreas / Jefes (CRUD)
# =========================================================
if menu == "Direcciones / Unidades / Áreas / Jefes":
    st.header("Direcciones, Unidades, Áreas y Jefes")

    conn = get_conn()

    # ---- DIRECCIONES CRUD ----
    st.subheader("Direcciones")
    df_dir = pd.read_sql("SELECT * FROM direcciones ORDER BY nombre", conn)
    st.dataframe(df_dir, use_container_width=True)

    with st.form("form_dir", clear_on_submit=True):
        nueva_dir = st.text_input("Nueva Dirección")
        crear = st.form_submit_button("Agregar Dirección")
        if crear and nueva_dir.strip():
            conn.execute("INSERT INTO direcciones(nombre) VALUES(?)", (nueva_dir.strip(),))
            conn.commit()
            st.success("Dirección registrada.")
            do_rerun()

    if not df_dir.empty:
        colE, colD = st.columns(2)
        with colE:
            did = st.selectbox("Editar Dirección", df_dir["id"], format_func=lambda x: df_dir[df_dir["id"]==x]["nombre"].values[0], key="edir_dir")
            nuevo = st.text_input("Nuevo nombre", key="edir_dir_name")
            if st.button("📝 Guardar cambios (Dirección)"):
                if nuevo.strip():
                    conn.execute("UPDATE direcciones SET nombre=? WHERE id=?", (nuevo.strip(), did))
                    conn.commit()
                    st.success("Dirección actualizada.")
                    do_rerun()
        with colD:
            did2 = st.selectbox("Eliminar Dirección", df_dir["id"], format_func=lambda x: df_dir[df_dir["id"]==x]["nombre"].values[0], key="del_dir")
            if st.button("🗑️ Eliminar (Dirección)"):
                conn.execute("DELETE FROM direcciones WHERE id=?", (did2,))
                conn.commit()
                st.success("Dirección eliminada.")
                do_rerun()

    st.divider()

    # ---- UNIDADES CRUD ----
    st.subheader("Unidades")
    if df_dir.empty:
        st.info("Registra al menos una Dirección para crear Unidades.")
    else:
        mapa_dir = dict(zip(df_dir["nombre"], df_dir["id"]))
        dir_sel = st.selectbox("Dirección para unidades", list(mapa_dir.keys()))
        df_uni = pd.read_sql("SELECT * FROM unidades WHERE direccion_id=? ORDER BY nombre", conn, params=(mapa_dir[dir_sel],))
        st.dataframe(df_uni, use_container_width=True)

        with st.form("form_uni", clear_on_submit=True):
            nueva_uni = st.text_input("Nueva Unidad")
            crear_u = st.form_submit_button("Agregar Unidad")
            if crear_u and nueva_uni.strip():
                conn.execute("INSERT INTO unidades(direccion_id,nombre) VALUES(?,?)", (mapa_dir[dir_sel], nueva_uni.strip()))
                conn.commit()
                st.success("Unidad registrada.")
                do_rerun()

        if not df_uni.empty:
            colE, colD = st.columns(2)
            with colE:
                uid = st.selectbox("Editar Unidad", df_uni["id"], format_func=lambda x: df_uni[df_uni["id"]==x]["nombre"].values[0], key="edir_uni")
                nuevo = st.text_input("Nuevo nombre unidad", key="edir_uni_name")
                if st.button("📝 Guardar cambios (Unidad)"):
                    if nuevo.strip():
                        conn.execute("UPDATE unidades SET nombre=? WHERE id=?", (nuevo.strip(), uid))
                        conn.commit()
                        st.success("Unidad actualizada.")
                        do_rerun()
            with colD:
                uid2 = st.selectbox("Eliminar Unidad", df_uni["id"], format_func=lambda x: df_uni[df_uni["id"]==x]["nombre"].values[0], key="del_uni")
                if st.button("🗑️ Eliminar (Unidad)"):
                    conn.execute("DELETE FROM unidades WHERE id=?", (uid2,))
                    conn.commit()
                    st.success("Unidad eliminada.")
                    do_rerun()

    st.divider()

    # ---- ÁREAS CRUD ----
    st.subheader("Áreas")
    df_uni_all = pd.read_sql("""
        SELECT u.id,u.nombre, d.nombre AS direccion
        FROM unidades u JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre
    """, conn)

    if df_uni_all.empty:
        st.info("Registra Unidades para crear Áreas.")
    else:
        mapa_uni = {f"{r['direccion']} - {r['nombre']}": r["id"] for _, r in df_uni_all.iterrows()}
        uni_sel = st.selectbox("Unidad para áreas", list(mapa_uni.keys()))
        df_area = pd.read_sql("SELECT * FROM areas WHERE unidad_id=? ORDER BY nombre", conn, params=(mapa_uni[uni_sel],))
        st.dataframe(df_area, use_container_width=True)

        with st.form("form_area", clear_on_submit=True):
            nueva_area = st.text_input("Nueva Área")
            crear_a = st.form_submit_button("Agregar Área")
            if crear_a and nueva_area.strip():
                conn.execute("INSERT INTO areas(unidad_id,nombre) VALUES(?,?)", (mapa_uni[uni_sel], nueva_area.strip()))
                conn.commit()
                st.success("Área registrada.")
                do_rerun()

        if not df_area.empty:
            colE, colD = st.columns(2)
            with colE:
                aid = st.selectbox("Editar Área", df_area["id"], format_func=lambda x: df_area[df_area["id"]==x]["nombre"].values[0], key="edir_area")
                nuevo = st.text_input("Nuevo nombre área", key="edir_area_name")
                if st.button("📝 Guardar cambios (Área)"):
                    if nuevo.strip():
                        conn.execute("UPDATE areas SET nombre=? WHERE id=?", (nuevo.strip(), aid))
                        conn.commit()
                        st.success("Área actualizada.")
                        do_rerun()
            with colD:
                aid2 = st.selectbox("Eliminar Área", df_area["id"], format_func=lambda x: df_area[df_area["id"]==x]["nombre"].values[0], key="del_area")
                if st.button("🗑️ Eliminar (Área)"):
                    conn.execute("DELETE FROM areas WHERE id=?", (aid2,))
                    conn.commit()
                    st.success("Área eliminada.")
                    do_rerun()

    st.divider()

    # ---- JEFES CRUD ----
    st.subheader("Jefes")
    df_areas = pd.read_sql("""
        SELECT a.id,a.nombre, u.nombre AS unidad, d.nombre AS direccion
        FROM areas a
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre,a.nombre
    """, conn)

    if df_areas.empty:
        st.warning("Aún no hay Áreas. Primero registra Áreas para poder registrar Jefes.")
    else:
        mapa_area = {f"{r['direccion']} - {r['unidad']} - {r['nombre']}": r["id"] for _, r in df_areas.iterrows()}
        area_sel = st.selectbox("Área del jefe", list(mapa_area.keys()))
        with st.form("form_jefe", clear_on_submit=True):
            nom = st.text_input("Nombres del jefe")
            cargo = st.text_input("Cargo del jefe")
            crear_j = st.form_submit_button("Guardar Jefe")
            if crear_j:
                if not nom.strip():
                    st.error("Nombre obligatorio.")
                else:
                    conn.execute("INSERT INTO jefes(nombres,cargo,area_id) VALUES(?,?,?)", (nom.strip(), cargo.strip(), mapa_area[area_sel]))
                    conn.commit()
                    st.success("Jefe registrado.")
                    do_rerun()

        df_jef = pd.read_sql("""
            SELECT j.id,j.nombres,j.cargo,
                   a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion
            FROM jefes j
            JOIN areas a ON a.id=j.area_id
            JOIN unidades u ON u.id=a.unidad_id
            JOIN direcciones d ON d.id=u.direccion_id
            ORDER BY d.nombre,u.nombre,a.nombre,j.nombres
        """, conn)
        st.dataframe(df_jef, use_container_width=True)

        if not df_jef.empty:
            colE, colD = st.columns(2)
            with colE:
                jid = st.selectbox("Editar Jefe", df_jef["id"], format_func=lambda x: df_jef[df_jef["id"]==x]["nombres"].values[0], key="edir_jefe")
                nuevo_nom = st.text_input("Nuevo nombre jefe", key="edir_jefe_nom")
                nuevo_car = st.text_input("Nuevo cargo jefe", key="edir_jefe_car")
                if st.button("📝 Guardar cambios (Jefe)"):
                    if nuevo_nom.strip():
                        conn.execute("UPDATE jefes SET nombres=?, cargo=? WHERE id=?", (nuevo_nom.strip(), nuevo_car.strip(), jid))
                        conn.commit()
                        st.success("Jefe actualizado.")
                        do_rerun()
            with colD:
                jid2 = st.selectbox("Eliminar Jefe", df_jef["id"], format_func=lambda x: df_jef[df_jef["id"]==x]["nombres"].values[0], key="del_jefe")
                if st.button("🗑️ Eliminar (Jefe)"):
                    conn.execute("DELETE FROM jefes WHERE id=?", (jid2,))
                    conn.commit()
                    st.success("Jefe eliminado.")
                    do_rerun()

    conn.close()

# =========================================================
# MÓDULO: Registrar Trabajador (CRUD + periodos)
# =========================================================
elif menu == "Registrar Trabajador":
    st.header("Trabajadores")

    conn = get_conn()

    # Buscador
    col1, col2 = st.columns(2)
    with col1:
        f_nom = st.text_input("Buscar por nombre")
    with col2:
        f_dni = st.text_input("Buscar por DNI")

    df_trab = pd.read_sql("""
        SELECT t.id,t.numero,t.dni,t.nombres,t.cargo,t.regimen,t.fecha_ingreso,
               a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion,
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

    st.divider()
    st.subheader("Nuevo trabajador")

    df_area_ctx = pd.read_sql("""
        SELECT a.id, a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion
        FROM areas a JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre,a.nombre
    """, conn)
    if df_area_ctx.empty:
        st.warning("Primero registra Direcciones → Unidades → Áreas.")
        conn.close()
        st.stop()

    df_jef = pd.read_sql("""
        SELECT j.id, j.nombres, a.nombre AS area, u.nombre AS unidad, d.nombre AS direccion
        FROM jefes j
        JOIN areas a ON a.id=j.area_id
        JOIN unidades u ON u.id=a.unidad_id
        JOIN direcciones d ON d.id=u.direccion_id
        ORDER BY d.nombre,u.nombre,a.nombre,j.nombres
    """, conn)
    if df_jef.empty:
        st.warning("Primero registra Jefes.")
        conn.close()
        st.stop()

    mapa_area = {f"{r['direccion']} - {r['unidad']} - {r['area']}": r["id"] for _, r in df_area_ctx.iterrows()}
    mapa_jef = {f"{r['direccion']} - {r['unidad']} - {r['area']} - {r['nombres']}": r["id"] for _, r in df_jef.iterrows()}

    with st.form("form_trab", clear_on_submit=True):
        numero = st.text_input("Número (opcional)")
        dni = st.text_input("DNI")
        nombres = st.text_input("Apellidos y Nombres")
        cargo = st.text_input("Cargo")
        regimen = st.selectbox("Régimen", ["DL 276", "DL 728", "DL 1057", "Carrera Especial"])
        fecha_ing = st.date_input("Fecha de ingreso")
        area_sel = st.selectbox("Área", list(mapa_area.keys()))
        jefe_sel = st.selectbox("Jefe inmediato", list(mapa_jef.keys()))
        guardar = st.form_submit_button("Guardar trabajador")

        if guardar:
            if not nombres.strip():
                st.error("El nombre es obligatorio.")
            else:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO trabajadores(numero,dni,nombres,cargo,regimen,fecha_ingreso,area_id,jefe_id)
                    VALUES(?,?,?,?,?,?,?,?)
                """, (
                    numero.strip() if numero.strip() else None,
                    dni.strip() if dni.strip() else None,
                    nombres.strip(),
                    cargo.strip(),
                    regimen,
                    fecha_ing.strftime("%Y-%m-%d"),
                    mapa_area[area_sel],
                    mapa_jef[jefe_sel]
                ))
                tid = cur.lastrowid
                conn.commit()
                conn.close()

                generar_periodos_para_trabajador(tid, fecha_ing.strftime("%Y-%m-%d"))
                st.success("Trabajador registrado. Periodos generados automáticamente.")
                do_rerun()

    # Editar / eliminar trabajador
    st.divider()
    st.subheader("Editar / eliminar trabajador")

    if df_trab.empty:
        st.info("No hay trabajadores aún.")
        conn.close()
        st.stop()

    tid = st.selectbox("Seleccione trabajador", df_trab["id"], format_func=lambda x: df_trab[df_trab["id"]==x]["nombres"].values[0])
    row = conn.execute("SELECT * FROM trabajadores WHERE id=?", (tid,)).fetchone()

    colE, colD = st.columns(2)
    with colE:
        with st.form("form_edit_trab"):
            en_num = st.text_input("Número", value=row["numero"] or "")
            en_dni = st.text_input("DNI", value=row["dni"] or "")
            en_nom = st.text_input("Nombres", value=row["nombres"] or "")
            en_car = st.text_input("Cargo", value=row["cargo"] or "")
            en_reg = st.text_input("Régimen", value=row["regimen"] or "")
            en_fi = st.date_input("Fecha ingreso", value=to_date(row["fecha_ingreso"]))
            area_sel2 = st.selectbox("Área (editar)", list(mapa_area.keys()), index=0)
            jefe_sel2 = st.selectbox("Jefe (editar)", list(mapa_jef.keys()), index=0)
            save = st.form_submit_button("📝 Guardar cambios")
            if save:
                conn.execute("""
                    UPDATE trabajadores
                    SET numero=?, dni=?, nombres=?, cargo=?, regimen=?, fecha_ingreso=?, area_id=?, jefe_id=?
                    WHERE id=?
                """, (
                    en_num.strip() if en_num.strip() else None,
                    en_dni.strip() if en_dni.strip() else None,
                    en_nom.strip(),
                    en_car.strip(),
                    en_reg.strip(),
                    en_fi.strftime("%Y-%m-%d"),
                    mapa_area[area_sel2],
                    mapa_jef[jefe_sel2],
                    tid
                ))
                conn.commit()

                # regenerar periodos (idempotente por UNIQUE)
                generar_periodos_para_trabajador(tid, en_fi.strftime("%Y-%m-%d"))
                st.success("Actualizado.")
                do_rerun()

    with colD:
        st.warning("Eliminar borra también periodos / resoluciones / vacaciones del trabajador.")
        if st.button("🗑️ Eliminar trabajador"):
            conn.execute("DELETE FROM trabajadores WHERE id=?", (tid,))
            conn.commit()
            st.success("Trabajador eliminado.")
            do_rerun()

    # Ver periodos del trabajador
    st.divider()
    st.subheader("Periodos del trabajador (calculados)")
    dfp = periodos_trabajador(tid)
    if dfp.empty:
        st.info("Aún no tiene periodos completos.")
    else:
        hoy = datetime.date.today()
        dfp["estado"] = dfp.apply(lambda r: "Vencido" if hoy > to_date(r["acumulable_hasta"])
                                  else ("Por vencer" if hoy > (to_date(r["acumulable_hasta"]) - datetime.timedelta(days=60))
                                        else "Vigente"), axis=1)
        dfp["dias_restantes"] = dfp.apply(lambda r: 30 - int(r["dias_usados"]), axis=1)
        st.dataframe(dfp, use_container_width=True)

    conn.close()

# =========================================================
# MÓDULO: Reporte de Trabajadores + CSV
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

    col1, col2 = st.columns(2)
    with col1:
        f_nom = st.text_input("Buscar por nombre", key="rep_nom")
    with col2:
        f_dni = st.text_input("Buscar por DNI", key="rep_dni")

    if f_nom:
        df = df[df["nombres"].str.contains(f_nom, case=False, na=False)]
    if f_dni:
        df = df[df["dni"].fillna("").str.contains(f_dni, na=False)]

    st.dataframe(df, use_container_width=True)

    st.download_button(
        "Descargar CSV (Trabajadores)",
        df.to_csv(index=False).encode("utf-8"),
        "trabajadores.csv",
        "text/csv"
    )
    conn.close()

# =========================================================
# MÓDULO: Resoluciones (con periodo + CRUD)
# =========================================================
elif menu == "Resoluciones":
    st.header("Resoluciones")

    conn = get_conn()
    df_trab = pd.read_sql("SELECT id,nombres,fecha_ingreso FROM trabajadores ORDER BY nombres", conn)
    if df_trab.empty:
        st.warning("No hay trabajadores.")
        conn.close()
        st.stop()

    mapa_trab = dict(zip(df_trab["nombres"], df_trab["id"]))
    tsel = st.selectbox("Trabajador", list(mapa_trab.keys()))
    tid = mapa_trab[tsel]

    # garantizar periodos para ese trabajador
    fi_ing = df_trab[df_trab["id"]==tid]["fecha_ingreso"].values[0]
    generar_periodos_para_trabajador(tid, fi_ing)

    dfp = periodos_trabajador(tid)
    dfp_valid = dfp[dfp.apply(periodo_vigente_para_registro, axis=1)]
    if dfp_valid.empty:
        st.info("No hay periodos vigentes con días disponibles (o aún no completa 1 año).")
        conn.close()
        st.stop()

    mapa_p = {f"{r['inicio_ciclo']} a {r['fin_ciclo']} | Restantes: {30-int(r['dias_usados'])}": r["id"] for _, r in dfp_valid.iterrows()}
    pid_label = st.selectbox("Periodo (vigente y con saldo)", list(mapa_p.keys()))
    pid = mapa_p[pid_label]

    st.subheader("Registrar Resolución")
    with st.form("form_res", clear_on_submit=True):
        numero = st.text_input("Número de Resolución")
        fecha_ini = st.date_input("Inicio autorizado")
        fecha_fin = st.date_input("Fin autorizado")
        dias_aut = st.number_input("Días autorizados", 1, 30, value=30)
        fracc = st.checkbox("Fraccionable")
        obs = st.text_area("Observaciones")
        guardar = st.form_submit_button("Registrar Resolución")
        if guardar:
            if not numero.strip():
                st.error("Número obligatorio.")
            else:
                conn.execute("""
                    INSERT INTO resoluciones(trabajador_id,periodo_id,numero,fecha_inicio,fecha_fin,dias_autorizados,fraccionable,observaciones)
                    VALUES(?,?,?,?,?,?,?,?)
                """, (
                    tid, pid, numero.strip(),
                    fecha_ini.strftime("%Y-%m-%d"),
                    fecha_fin.strftime("%Y-%m-%d"),
                    int(dias_aut),
                    1 if fracc else 0,
                    obs.strip()
                ))
                conn.commit()
                st.success("Resolución registrada.")
                do_rerun()

    st.divider()
    st.subheader("Resoluciones registradas (del trabajador)")
    df_res = pd.read_sql("""
        SELECT r.*,
               p.inicio_ciclo,p.fin_ciclo
        FROM resoluciones r
        JOIN periodos p ON p.id=r.periodo_id
        WHERE r.trabajador_id=?
        ORDER BY date(r.fecha_inicio) DESC
    """, conn, params=(tid,))
    st.dataframe(df_res, use_container_width=True)

    if not df_res.empty:
        colE, colD = st.columns(2)
        with colE:
            rid = st.selectbox("Editar Resolución", df_res["id"], format_func=lambda x: df_res[df_res["id"]==x]["numero"].values[0], key="edir_res")
            rrow = conn.execute("SELECT * FROM resoluciones WHERE id=?", (rid,)).fetchone()
            with st.form("form_edit_res"):
                en_num = st.text_input("Número", value=rrow["numero"])
                en_fi = st.date_input("Inicio", value=to_date(rrow["fecha_inicio"]))
                en_ff = st.date_input("Fin", value=to_date(rrow["fecha_fin"]))
                en_da = st.number_input("Días autorizados", 1, 30, value=int(rrow["dias_autorizados"]))
                en_fr = st.checkbox("Fraccionable", value=bool(rrow["fraccionable"]))
                en_ob = st.text_area("Obs", value=rrow["observaciones"] or "")
                save = st.form_submit_button("📝 Guardar cambios")
                if save:
                    conn.execute("""
                        UPDATE resoluciones
                        SET numero=?, fecha_inicio=?, fecha_fin=?, dias_autorizados=?, fraccionable=?, observaciones=?
                        WHERE id=?
                    """, (
                        en_num.strip(),
                        en_fi.strftime("%Y-%m-%d"),
                        en_ff.strftime("%Y-%m-%d"),
                        int(en_da),
                        1 if en_fr else 0,
                        en_ob.strip(),
                        rid
                    ))
                    conn.commit()
                    st.success("Resolución actualizada.")
                    do_rerun()

        with colD:
            rid2 = st.selectbox("Eliminar Resolución", df_res["id"], format_func=lambda x: df_res[df_res["id"]==x]["numero"].values[0], key="del_res")
            st.warning("Si eliminas, las vacaciones asociadas quedarán sin resolucion_id (se pondrá NULL).")
            if st.button("🗑️ Eliminar Resolución"):
                conn.execute("DELETE FROM resoluciones WHERE id=?", (rid2,))
                conn.commit()
                st.success("Resolución eliminada.")
                do_rerun()

    conn.close()

# =========================================================
# MÓDULO: Registrar Vacaciones (con resolución y fraccionamiento)
# =========================================================
elif menu == "Registrar Vacaciones":
    st.header("Registrar Vacaciones")

    conn = get_conn()
    df_trab = pd.read_sql("SELECT id,nombres,fecha_ingreso FROM trabajadores ORDER BY nombres", conn)
    if df_trab.empty:
        st.warning("No hay trabajadores.")
        conn.close()
        st.stop()

    mapa_trab = dict(zip(df_trab["nombres"], df_trab["id"]))
    tsel = st.selectbox("Trabajador", list(mapa_trab.keys()))
    tid = mapa_trab[tsel]

    # garantizar periodos
    fi_ing = df_trab[df_trab["id"]==tid]["fecha_ingreso"].values[0]
    generar_periodos_para_trabajador(tid, fi_ing)

    dfp = periodos_trabajador(tid)
    dfp_valid = dfp[dfp.apply(periodo_vigente_para_registro, axis=1)]
    if dfp_valid.empty:
        st.warning("No hay periodos vigentes con saldo (o aún no completa 1 año).")
        conn.close()
        st.stop()

    mapa_p = {f"{r['inicio_ciclo']} a {r['fin_ciclo']} | Restantes: {30-int(r['dias_usados'])} | Acumulable hasta: {r['acumulable_hasta']}": r["id"] for _, r in dfp_valid.iterrows()}
    pid_label = st.selectbox("Periodo (vigente y con saldo)", list(mapa_p.keys()))
    pid = mapa_p[pid_label]

    # Selección: con resolución o sin resolución
    usar_res = st.checkbox("Registrar con Resolución", value=True)

    resolucion_id = None
    modo_res = None
    tipo = None

    if usar_res:
        df_res = pd.read_sql("""
            SELECT * FROM resoluciones
            WHERE trabajador_id=? AND periodo_id=?
            ORDER BY date(fecha_inicio) DESC
        """, conn, params=(tid, pid))

        if df_res.empty:
            st.error("No hay resoluciones para este trabajador en este periodo.")
            conn.close()
            st.stop()

        mapa_res = {f"{r['numero']} | Aut: {r['dias_autorizados']} | Fracc: {'Sí' if r['fraccionable'] else 'No'}": r["id"] for _, r in df_res.iterrows()}
        res_label = st.selectbox("Resolución", list(mapa_res.keys()))
        resolucion_id = mapa_res[res_label]

        row_res = conn.execute("SELECT * FROM resoluciones WHERE id=?", (resolucion_id,)).fetchone()
        saldo_res = dias_resolucion_restantes(resolucion_id)
        st.info(f"Saldo de días en Resolución: {saldo_res}")

        if saldo_res <= 0:
            st.error("Esta resolución ya no tiene saldo.")
            conn.close()
            st.stop()

        opciones_modo = ["Íntegra", "Fraccionada"] if row_res["fraccionable"] else ["Íntegra"]
        modo_res = st.radio("Ejecución de resolución", opciones_modo, horizontal=True)
        tipo = "Resolución"

        if modo_res == "Íntegra":
            dias = st.number_input("Días a ejecutar", 1, min(30, saldo_res), value=min(30, saldo_res))
        else:
            st.warning("Fraccionada: debe adjuntar Memo/Solicitud.")
            tipo = st.selectbox("Tipo de sustento", ["Memorando", "Solicitud"])
            dias = st.number_input("Días fraccionados", 1, min(30, saldo_res), value=min(7, min(30, saldo_res)))

        documento = st.text_input("Documento (N° memo/solicitud o constancia)")
    else:
        tipo = st.selectbox("Tipo", ["Solicitud", "Memorando"])
        documento = st.text_input("Documento (N°)")
        dias = st.number_input("Días", 1, 30, value=7)

    fecha_inicio = st.date_input("Fecha de inicio")
    fecha_fin = fecha_inicio + datetime.timedelta(days=int(dias)-1)
    st.write(f"Fecha fin: **{fecha_fin}**")

    saldo_periodo = dias_periodo_restantes(pid)
    st.info(f"Saldo de días del periodo: {saldo_periodo}")

    # Validaciones fuertes
    if int(dias) > saldo_periodo:
        st.error("Los días exceden el saldo del periodo.")
    if usar_res and resolucion_id is not None:
        if int(dias) > dias_resolucion_restantes(resolucion_id):
            st.error("Los días exceden el saldo de la resolución.")

    # validar que periodo no esté vencido
    per = conn.execute("SELECT * FROM periodos WHERE id=?", (pid,)).fetchone()
    if datetime.date.today() > to_date(per["acumulable_hasta"]):
        st.error("El periodo está vencido (superó acumulable_hasta). No se puede registrar.")

    observaciones = st.text_area("Observaciones (opcional)")

    if st.button("Registrar Vacación"):
        # reevaluar validaciones al guardar
        if int(dias) <= 0:
            st.error("Días inválidos.")
            conn.close()
            st.stop()
        if int(dias) > dias_periodo_restantes(pid):
            st.error("Excede saldo de periodo.")
            conn.close()
            st.stop()
        if usar_res and resolucion_id is not None:
            if int(dias) > dias_resolucion_restantes(resolucion_id):
                st.error("Excede saldo de resolución.")
                conn.close()
                st.stop()

        conn.execute("""
            INSERT INTO vacaciones(trabajador_id,periodo_id,resolucion_id,tipo,modo_resolucion,fecha_inicio,fecha_fin,dias,documento,observaciones,autorizado_rrhh)
            VALUES(?,?,?,?,?,?,?,?,?,?,0)
        """, (
            tid, pid, resolucion_id,
            tipo,
            ("Integra" if usar_res and modo_res == "Íntegra" else ("Fraccionada" if usar_res else None)),
            fecha_inicio.strftime("%Y-%m-%d"),
            fecha_fin.strftime("%Y-%m-%d"),
            int(dias),
            documento.strip() if documento.strip() else None,
            observaciones.strip()
        ))
        conn.commit()
        st.success("Vacación registrada.")
        do_rerun()

    st.divider()
    st.subheader("Vacaciones del trabajador (en el periodo)")
    df_vac = pd.read_sql("""
        SELECT v.*, r.numero AS resolucion_num
        FROM vacaciones v
        LEFT JOIN resoluciones r ON r.id=v.resolucion_id
        WHERE v.trabajador_id=? AND v.periodo_id=?
        ORDER BY date(v.fecha_inicio) DESC
    """, conn, params=(tid, pid))
    st.dataframe(df_vac, use_container_width=True)

    if not df_vac.empty:
        colE, colD = st.columns(2)
        with colE:
            vid = st.selectbox("Editar Vacación", df_vac["id"], format_func=lambda x: f"ID {x} | {df_vac[df_vac['id']==x]['fecha_inicio'].values[0]} ({df_vac[df_vac['id']==x]['dias'].values[0]} días)", key="edir_vac")
            vrow = conn.execute("SELECT * FROM vacaciones WHERE id=?", (vid,)).fetchone()
            with st.form("form_edit_vac"):
                e_fi = st.date_input("Inicio", value=to_date(vrow["fecha_inicio"]))
                e_dias = st.number_input("Días", 1, 30, value=int(vrow["dias"]))
                e_ff = e_fi + datetime.timedelta(days=int(e_dias)-1)
                st.caption(f"Fin calculado: {e_ff}")
                e_doc = st.text_input("Documento", value=vrow["documento"] or "")
                e_obs = st.text_area("Obs", value=vrow["observaciones"] or "")
                save = st.form_submit_button("📝 Guardar cambios")
                if save:
                    # valida saldos al editar
                    if int(e_dias) > dias_periodo_restantes(vrow["periodo_id"]) + int(vrow["dias"]):
                        st.error("Excede saldo de periodo (considerando el registro actual).")
                    else:
                        if vrow["resolucion_id"] is not None:
                            saldo = dias_resolucion_restantes(vrow["resolucion_id"]) + int(vrow["dias"])
                            if int(e_dias) > saldo:
                                st.error("Excede saldo de resolución (considerando el registro actual).")
                            else:
                                conn.execute("""
                                    UPDATE vacaciones
                                    SET fecha_inicio=?, fecha_fin=?, dias=?, documento=?, observaciones=?
                                    WHERE id=?
                                """, (
                                    e_fi.strftime("%Y-%m-%d"),
                                    e_ff.strftime("%Y-%m-%d"),
                                    int(e_dias),
                                    e_doc.strip() if e_doc.strip() else None,
                                    e_obs.strip(),
                                    vid
                                ))
                                conn.commit()
                                st.success("Vacación actualizada.")
                                do_rerun()
                        else:
                            conn.execute("""
                                UPDATE vacaciones
                                SET fecha_inicio=?, fecha_fin=?, dias=?, documento=?, observaciones=?
                                WHERE id=?
                            """, (
                                e_fi.strftime("%Y-%m-%d"),
                                e_ff.strftime("%Y-%m-%d"),
                                int(e_dias),
                                e_doc.strip() if e_doc.strip() else None,
                                e_obs.strip(),
                                vid
                            ))
                            conn.commit()
                            st.success("Vacación actualizada.")
                            do_rerun()

        with colD:
            vid2 = st.selectbox("Eliminar Vacación", df_vac["id"], format_func=lambda x: f"ID {x}", key="del_vac")
            if st.button("🗑️ Eliminar Vacación"):
                conn.execute("DELETE FROM vacaciones WHERE id=?", (vid2,))
                conn.commit()
                st.success("Vacación eliminada.")
                do_rerun()

    conn.close()

# =========================================================
# MÓDULO: Panel RRHH (aprobar/observar + export)
# =========================================================
elif menu == "Panel RRHH":
    st.header("Panel RRHH")

    if ROL not in ("admin", "responsable"):
        st.error("No tienes permisos para RRHH.")
        st.stop()

    conn = get_conn()

    st.subheader("Pendientes de aprobación")
    df_p = pd.read_sql("""
        SELECT v.id, t.nombres AS trabajador, v.tipo, v.modo_resolucion, v.fecha_inicio, v.fecha_fin, v.dias,
               v.documento, v.autorizado_rrhh, v.rrhh_observacion
        FROM vacaciones v
        JOIN trabajadores t ON t.id=v.trabajador_id
        WHERE v.autorizado_rrhh=0
        ORDER BY date(v.fecha_inicio) DESC
    """, conn)
    st.dataframe(df_p, use_container_width=True)

    if not df_p.empty:
        vid = st.selectbox("Selecciona vacación", df_p["id"], format_func=lambda x: f"ID {x} - {df_p[df_p['id']==x]['trabajador'].values[0]}")
        rrhh_obs = st.text_area("Observación RRHH (opcional)", key="rrhh_obs")

        colA, colR = st.columns(2)
        with colA:
            if st.button("✅ Aprobar RRHH"):
                conn.execute("""
                    UPDATE vacaciones
                    SET autorizado_rrhh=1, rrhh_observacion=?, fecha_aprob_rrhh=?, usuario_rrhh=?
                    WHERE id=?
                """, (rrhh_obs.strip(), datetime.date.today().strftime("%Y-%m-%d"), USER, vid))
                conn.commit()
                st.success("Aprobado.")
                do_rerun()
        with colR:
            if st.button("📝 Registrar observación (sin aprobar)"):
                conn.execute("UPDATE vacaciones SET rrhh_observacion=? WHERE id=?", (rrhh_obs.strip(), vid))
                conn.commit()
                st.success("Observación guardada.")
                do_rerun()

    st.divider()
    st.subheader("Aprobadas RRHH")
    df_ok = pd.read_sql("""
        SELECT v.id, t.nombres AS trabajador, v.tipo, v.fecha_inicio, v.fecha_fin, v.dias,
               v.documento, v.fecha_aprob_rrhh, v.usuario_rrhh
        FROM vacaciones v
        JOIN trabajadores t ON t.id=v.trabajador_id
        WHERE v.autorizado_rrhh=1
        ORDER BY date(v.fecha_inicio) DESC
    """, conn)
    st.dataframe(df_ok, use_container_width=True)

    st.download_button(
        "Descargar CSV (Pendientes RRHH)",
        df_p.to_csv(index=False).encode("utf-8"),
        "rrhh_pendientes.csv",
        "text/csv"
    )
    st.download_button(
        "Descargar CSV (Aprobadas RRHH)",
        df_ok.to_csv(index=False).encode("utf-8"),
        "rrhh_aprobadas.csv",
        "text/csv"
    )

    conn.close()

# =========================================================
# MÓDULO: Dashboard con semáforos
# =========================================================
elif menu == "Dashboard":
    st.header("Dashboard")

    conn = get_conn()
    hoy = datetime.date.today()
    inicio_mes = hoy.replace(day=1)
    fin_mes = (inicio_mes.replace(month=inicio_mes.month % 12 + 1, year=inicio_mes.year + (1 if inicio_mes.month == 12 else 0)) - datetime.timedelta(days=1))

    # Vacaciones en goce este mes
    df_goce = pd.read_sql("""
        SELECT t.nombres AS trabajador, v.fecha_inicio, v.fecha_fin, v.dias, v.tipo, v.autorizado_rrhh
        FROM vacaciones v
        JOIN trabajadores t ON t.id=v.trabajador_id
        WHERE date(v.fecha_inicio) <= date(?) AND date(v.fecha_fin) >= date(?)
        ORDER BY date(v.fecha_inicio)
    """, conn, params=(fin_mes.strftime("%Y-%m-%d"), inicio_mes.strftime("%Y-%m-%d")))

    # Periodos con estado
    dfp = pd.read_sql("""
        SELECT p.*, t.nombres AS trabajador,
               IFNULL((SELECT SUM(dias) FROM vacaciones v WHERE v.periodo_id=p.id),0) AS dias_usados
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
    if not dfp.empty:
        col2.metric("🟡 Periodos por vencer", int((dfp["estado"]=="🟡 Por vencer").sum()))
        col3.metric("🔴 Periodos vencidos", int((dfp["estado"]=="🔴 Vencido").sum()))
    else:
        col2.metric("🟡 Periodos por vencer", 0)
        col3.metric("🔴 Periodos vencidos", 0)

    st.divider()
    st.subheader("🟢 Vacaciones en goce durante el mes")
    if df_goce.empty:
        st.info("No hay vacaciones cruzando el mes actual.")
    else:
        st.dataframe(df_goce, use_container_width=True)

    st.divider()
    st.subheader("Estado de periodos (semaforizado)")
    if dfp.empty:
        st.info("No hay periodos generados aún.")
    else:
        st.dataframe(dfp[[
            "trabajador","inicio_ciclo","fin_ciclo","goce_hasta","acumulable_hasta",
            "dias_usados","dias_restantes","estado"
        ]], use_container_width=True)

    conn.close()

# =========================================================
# MÓDULO: Reportes (filtros + descargas)
# =========================================================
elif menu == "Reportes":
    st.header("Reportes")

    conn = get_conn()

    df_t = pd.read_sql("SELECT id,nombres,dni FROM trabajadores ORDER BY nombres", conn)
    mapa_t = {"Todos": None}
    for _, r in df_t.iterrows():
        mapa_t[f"{r['nombres']} ({r['dni'] or ''})"] = r["id"]

    trabajador_sel = st.selectbox("Filtrar por trabajador", list(mapa_t.keys()))
    tid = mapa_t[trabajador_sel]

    tipo_sel = st.selectbox("Filtrar por tipo", ["Todos", "Solicitud", "Memorando", "Resolución"])
    rrhh_sel = st.selectbox("Filtrar RRHH", ["Todos", "Pendientes", "Aprobadas"])

    q = """
        SELECT v.*, t.nombres AS trabajador, t.dni, r.numero AS resolucion_num,
               p.inicio_ciclo, p.fin_ciclo
        FROM vacaciones v
        JOIN trabajadores t ON t.id=v.trabajador_id
        LEFT JOIN resoluciones r ON r.id=v.resolucion_id
        JOIN periodos p ON p.id=v.periodo_id
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

    df_v = pd.read_sql(q, conn, params=params)
    st.dataframe(df_v, use_container_width=True)

    st.download_button(
        "Descargar CSV (Vacaciones filtradas)",
        df_v.to_csv(index=False).encode("utf-8"),
        "vacaciones_filtradas.csv",
        "text/csv"
    )

    # Resoluciones
    st.divider()
    st.subheader("Resoluciones (filtro trabajador)")
    q2 = """
        SELECT r.*, t.nombres AS trabajador, p.inicio_ciclo, p.fin_ciclo
        FROM resoluciones r
        JOIN trabajadores t ON t.id=r.trabajador_id
        JOIN periodos p ON p.id=r.periodo_id
        WHERE 1=1
        ORDER BY date(r.fecha_inicio) DESC
    """
    params2 = []
    if tid is not None:
        q2 = q2.replace("WHERE 1=1", "WHERE r.trabajador_id=?")
        params2 = [tid]
    df_r = pd.read_sql(q2, conn, params=params2)
    st.dataframe(df_r, use_container_width=True)

    st.download_button(
        "Descargar CSV (Resoluciones)",
        df_r.to_csv(index=False).encode("utf-8"),
        "resoluciones.csv",
        "text/csv"
    )

    conn.close()

# =========================================================
# MÓDULO: Usuarios y permisos (admin)
# =========================================================
elif menu == "Usuarios y Permisos":
    st.header("Usuarios y Permisos")

    if ROL != "admin":
        st.error("Solo admin puede gestionar usuarios.")
        st.stop()

    conn = get_conn()
    df_u = pd.read_sql("SELECT id,usuario,rol FROM usuarios ORDER BY usuario", conn)
    st.dataframe(df_u, use_container_width=True)

    st.divider()
    st.subheader("Crear usuario")
    with st.form("form_user", clear_on_submit=True):
        u = st.text_input("Usuario")
        p = st.text_input("Contraseña", type="password")
        r = st.selectbox("Rol", ["admin","responsable","registrador"])
        create = st.form_submit_button("Crear")
        if create:
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
    st.subheader("Cambiar rol / reset password / eliminar")
    if not df_u.empty:
        uid = st.selectbox("Seleccione usuario", df_u["id"], format_func=lambda x: df_u[df_u["id"]==x]["usuario"].values[0])
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
            newpass = st.text_input("Nueva contraseña", type="password", key="reset_pass")
            if st.button("Reset password"):
                if not newpass.strip():
                    st.error("Contraseña obligatoria.")
                else:
                    conn.execute("UPDATE usuarios SET password_hash=? WHERE id=?", (hash_password(newpass.strip()), uid))
                    conn.commit()
                    st.success("Contraseña actualizada.")
                    do_rerun()

        with col3:
            if st.button("Eliminar usuario"):
                if urow["usuario"] == "admin":
                    st.error("No se puede eliminar el admin principal.")
                else:
                    conn.execute("DELETE FROM usuarios WHERE id=?", (uid,))
                    conn.commit()
                    st.success("Usuario eliminado.")
                    do_rerun()

    conn.close()

# =========================================================
# Exportar tablas (backup CSV rápido)
# =========================================================
elif menu == "Exportar Tablas":
    st.header("Exportar tablas (CSV)")

    conn = get_conn()
    tablas = ["usuarios","direcciones","unidades","areas","jefes","trabajadores","periodos","resoluciones","vacaciones"]
    tsel = st.selectbox("Tabla", tablas)
    df = pd.read_sql(f"SELECT * FROM {tsel}", conn)
    st.dataframe(df, use_container_width=True)
    st.download_button(
        f"Descargar {tsel}.csv",
        df.to_csv(index=False).encode("utf-8"),
        f"{tsel}.csv",
        "text/csv"
    )
    conn.close()
