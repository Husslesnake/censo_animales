"""
ETL completo: Microsoft Access 2003 (.mdb) → MariaDB
=====================================================
Fases:
  1. EXTRACT   — Lectura y diagnóstico de la BD origen
  2. TRANSFORM — Limpieza, validación y mapeo de tipos
  3. LOAD      — Creación de esquema y carga en MariaDB
  4. SEED      — Inserción de datos de ejemplo en tablas vacías
  5. RELATIONS — Aplicación de claves foráneas post-carga
  6. VERIFY    — Validación post-carga y reporte final
"""

import pyodbc
import mysql.connector
import sys
import os
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
MDB_FILE = r"C:\Users\practicas\Desktop\Adrián Prácticas\CENSO_ANIMALES - copia.mdb"

MARIADB = {
    "host":     "localhost",
    "port":     3307,
    "user":     "root",
    "password": "123",
    "database": "censo_animales",
}

LOG_FILE   = "etl.log"
BATCH_SIZE = 500
# ──────────────────────────────────────────────────────────────────────────────


# ─── DDL SIN FK (las FK se añaden después de cargar los datos) ────────────────

DDL_TABLAS = {

"PROPIETARIOS": """
CREATE TABLE IF NOT EXISTS `PROPIETARIOS` (
    `DNI`              VARCHAR(50)  NOT NULL,
    `PRIMER_APELLIDO`  VARCHAR(100),
    `SEGUNDO_APELLIDO` VARCHAR(100),
    `NOMBRE`           VARCHAR(100),
    `TELEFONO1`        VARCHAR(50),
    `TELEFONO2`        VARCHAR(50),
    PRIMARY KEY (`DNI`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",

"PROPIETARIO_DIRECCION": """
CREATE TABLE IF NOT EXISTS `PROPIETARIO_DIRECCION` (
    `CODIGO`    INT(11)      NOT NULL AUTO_INCREMENT,
    `DNI`       VARCHAR(50),
    `DOMICILIO` VARCHAR(100),
    `CP`        VARCHAR(50),
    `MINICIPIO` VARCHAR(100),
    PRIMARY KEY (`CODIGO`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",

"LICENCIAS": """
CREATE TABLE IF NOT EXISTS `LICENCIAS` (
    `N_LICENCIA_ANIMALES_PELIGROSOS` VARCHAR(50)  NOT NULL,
    `LUGAR_EXPEDICION_LICENCIA`      VARCHAR(50),
    `FECHA_EXPEDICION_LICENCIA`      DATETIME,
    `N_REGISTRO`                     VARCHAR(50),
    `COD_REGISTRO`                   INT(11),
    `DNI_PROPIETARIO`                VARCHAR(50),
    PRIMARY KEY (`N_LICENCIA_ANIMALES_PELIGROSOS`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",

"ADIESTRADORES": """
CREATE TABLE IF NOT EXISTS `ADIESTRADORES` (
    `ID_ADIESTRAMIENTO`       INT(11)     NOT NULL AUTO_INCREMENT,
    `N_CHIP`                  VARCHAR(50),
    `ADIESTRAMIENTO`          TINYINT(1),
    `LUGAR`                   VARCHAR(50),
    `CERTIFICADO_ADIESTRADOR` VARCHAR(50),
    PRIMARY KEY (`ID_ADIESTRAMIENTO`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",

"SEGUROS": """
CREATE TABLE IF NOT EXISTS `SEGUROS` (
    `ID_SEGUROS`      INT(11)     NOT NULL AUTO_INCREMENT,
    `N_CHIP`          VARCHAR(50),
    `SEGURO_COMPANIA` VARCHAR(50),
    `SEGURO_POLIZA`   VARCHAR(50),
    PRIMARY KEY (`ID_SEGUROS`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",

"ESTADOS_HISTORICO": """
CREATE TABLE IF NOT EXISTS `ESTADOS_HISTORICO` (
    `ID_ESTADO` INT(11)     NOT NULL AUTO_INCREMENT,
    `ESTADO`    VARCHAR(50),
    PRIMARY KEY (`ID_ESTADO`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",

"ANIMALES": """
CREATE TABLE IF NOT EXISTS `ANIMALES` (
    `N_CHIP`                          VARCHAR(50)  NOT NULL,
    `ESPECIE`                         VARCHAR(50),
    `RAZA`                            VARCHAR(50),
    `SEXO`                            VARCHAR(50),
    `NOMBRE`                          VARCHAR(50),
    `COLOR`                           VARCHAR(50),
    `FECHA_NACIMIENTO`                DATETIME,
    `FECHA_ULTIMA_VACUNA_ANTIRRABICA` DATETIME,
    `ESTERILIZADO`                    TINYINT(1),
    `DNI_PROPIETARIO`                 VARCHAR(50),
    `N_CENSO`                         VARCHAR(50),
    `DOMICILIO_HABITUAL_ANIMAL`       VARCHAR(50),
    `PELIGROSO`                       TINYINT(1),
    `ID_ADIESTRAMIENTO`               INT(11),
    `ID_SEGUROS`                      INT(11),
    PRIMARY KEY (`N_CHIP`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",

"CENSO": """
CREATE TABLE IF NOT EXISTS `CENSO` (
    `N_CHIP`       VARCHAR(50),
    `N_CENSO`      VARCHAR(50),
    `CODIGO_CENSO` INT(11)  NOT NULL AUTO_INCREMENT,
    `FECHA_ALTA`   DATETIME,
    PRIMARY KEY (`CODIGO_CENSO`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",

"HISTORICO_MASCOTAS": """
CREATE TABLE IF NOT EXISTS `HISTORICO_MASCOTAS` (
    `ID_HISTORICO`    INT(11)     NOT NULL AUTO_INCREMENT,
    `N_CHIP`          VARCHAR(50),
    `FECHA`           DATETIME,
    `ID_ESTADO`       INT(11),
    `N_CENSO`         VARCHAR(50),
    `DNI_PROPIETARIO` VARCHAR(50),
    `OBSERVACIONES`   VARCHAR(50),
    PRIMARY KEY (`ID_HISTORICO`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""",
}


# ─── FK: se aplican DESPUÉS de cargar todos los datos ─────────────────────────
# Se omiten silenciosamente si ya existen

ALTER_FK = [
    # PROPIETARIO_DIRECCION → PROPIETARIOS
    """ALTER TABLE `PROPIETARIO_DIRECCION`
       ADD CONSTRAINT `fk_propdir_propietario`
       FOREIGN KEY (`DNI`) REFERENCES `PROPIETARIOS`(`DNI`)
       ON UPDATE CASCADE ON DELETE SET NULL""",

    # LICENCIAS → PROPIETARIOS
    """ALTER TABLE `LICENCIAS`
       ADD CONSTRAINT `fk_licencias_propietario`
       FOREIGN KEY (`DNI_PROPIETARIO`) REFERENCES `PROPIETARIOS`(`DNI`)
       ON UPDATE CASCADE ON DELETE SET NULL""",

    # ANIMALES → PROPIETARIOS
    """ALTER TABLE `ANIMALES`
       ADD CONSTRAINT `fk_animales_propietario`
       FOREIGN KEY (`DNI_PROPIETARIO`) REFERENCES `PROPIETARIOS`(`DNI`)
       ON UPDATE CASCADE ON DELETE SET NULL""",

    # ANIMALES → ADIESTRADORES
    """ALTER TABLE `ANIMALES`
       ADD CONSTRAINT `fk_animales_adiestrador`
       FOREIGN KEY (`ID_ADIESTRAMIENTO`) REFERENCES `ADIESTRADORES`(`ID_ADIESTRAMIENTO`)
       ON UPDATE CASCADE ON DELETE SET NULL""",

    # ANIMALES → SEGUROS
    """ALTER TABLE `ANIMALES`
       ADD CONSTRAINT `fk_animales_seguro`
       FOREIGN KEY (`ID_SEGUROS`) REFERENCES `SEGUROS`(`ID_SEGUROS`)
       ON UPDATE CASCADE ON DELETE SET NULL""",

    # CENSO → ANIMALES
    """ALTER TABLE `CENSO`
       ADD CONSTRAINT `fk_censo_animal`
       FOREIGN KEY (`N_CHIP`) REFERENCES `ANIMALES`(`N_CHIP`)
       ON UPDATE CASCADE ON DELETE SET NULL""",

    # HISTORICO_MASCOTAS → ANIMALES
    """ALTER TABLE `HISTORICO_MASCOTAS`
       ADD CONSTRAINT `fk_historico_animal`
       FOREIGN KEY (`N_CHIP`) REFERENCES `ANIMALES`(`N_CHIP`)
       ON UPDATE CASCADE ON DELETE SET NULL""",

    # HISTORICO_MASCOTAS → ESTADOS_HISTORICO
    """ALTER TABLE `HISTORICO_MASCOTAS`
       ADD CONSTRAINT `fk_historico_estado`
       FOREIGN KEY (`ID_ESTADO`) REFERENCES `ESTADOS_HISTORICO`(`ID_ESTADO`)
       ON UPDATE CASCADE ON DELETE SET NULL""",
]


# ─── DATOS DE EJEMPLO (se insertan solo si la tabla está vacía) ───────────────

DATOS_EJEMPLO = {

    "PROPIETARIOS": {
        "cols": ["DNI", "PRIMER_APELLIDO", "SEGUNDO_APELLIDO", "NOMBRE", "TELEFONO1", "TELEFONO2"],
        "rows": [
            ("12345678A", "García",    "López",    "Carlos",   "600111222", "910111222"),
            ("87654321B", "Martínez",  "Fernández","Ana",      "600333444", "910333444"),
            ("11223344C", "Sánchez",   "Ruiz",     "Pedro",    "600555666", None),
            ("44332211D", "González",  "Díaz",     "Laura",    "600777888", "910777888"),
            ("55667788E", "Rodríguez", "Moreno",   "Isabel",   "600999000", None),
        ]
    },

    "PROPIETARIO_DIRECCION": {
        "cols": ["DNI", "DOMICILIO", "CP", "MINICIPIO"],
        "rows": [
            ("12345678A", "Calle Mayor 1",       "28001", "Madrid"),
            ("87654321B", "Avenida del Parque 5","28002", "Madrid"),
            ("11223344C", "Calle del Sol 12",    "28003", "Madrid"),
            ("44332211D", "Plaza España 3",      "28004", "Madrid"),
            ("55667788E", "Calle Luna 8",        "28005", "Madrid"),
        ]
    },

    "LICENCIAS": {
        "cols": ["N_LICENCIA_ANIMALES_PELIGROSOS", "LUGAR_EXPEDICION_LICENCIA",
                 "FECHA_EXPEDICION_LICENCIA", "N_REGISTRO", "COD_REGISTRO", "DNI_PROPIETARIO"],
        "rows": [
            ("LIC-001", "Madrid", datetime(2022, 3, 15), "REG-001", 1, "12345678A"),
            ("LIC-002", "Madrid", datetime(2021, 7, 20), "REG-002", 2, "44332211D"),
            ("LIC-003", "Madrid", datetime(2023, 1, 10), "REG-003", 3, "55667788E"),
        ]
    },

    "ADIESTRADORES": {
        "cols": ["N_CHIP", "ADIESTRAMIENTO", "LUGAR", "CERTIFICADO_ADIESTRADOR"],
        "rows": [
            ("CHIP001", 1, "Centro Canino Madrid",   "CERT-2021-001"),
            ("CHIP002", 1, "Escuela Canina Sur",     "CERT-2022-002"),
            ("CHIP003", 0, "Centro Canino Norte",    None),
            ("CHIP004", 1, "Academia Mascotas",      "CERT-2020-003"),
            ("CHIP005", 0, None,                     None),
        ]
    },

    "SEGUROS": {
        "cols": ["N_CHIP", "SEGURO_COMPANIA", "SEGURO_POLIZA"],
        "rows": [
            ("CHIP001", "Mapfre",      "POL-2023-001"),
            ("CHIP002", "AXA",         "POL-2022-002"),
            ("CHIP003", "Allianz",     "POL-2023-003"),
            ("CHIP004", "Generali",    "POL-2021-004"),
            ("CHIP005", "Zurich",      "POL-2022-005"),
        ]
    },

    "ESTADOS_HISTORICO": {
        "cols": ["ESTADO"],
        "rows": [
            ("Alta",),
            ("Baja",),
            ("Fallecido",),
            ("Transferido",),
            ("Extraviado",),
            ("Recuperado",),
        ]
    },

    "ANIMALES": {
        "cols": ["N_CHIP", "ESPECIE", "RAZA", "SEXO", "NOMBRE", "COLOR",
                 "FECHA_NACIMIENTO", "FECHA_ULTIMA_VACUNA_ANTIRRABICA",
                 "ESTERILIZADO", "DNI_PROPIETARIO", "N_CENSO",
                 "DOMICILIO_HABITUAL_ANIMAL", "PELIGROSO",
                 "ID_ADIESTRAMIENTO", "ID_SEGUROS"],
        "rows": [
            ("CHIP001","Perro","Labrador","Macho","Toby","Amarillo",
             datetime(2019,4,10), datetime(2023,4,10),
             1,"12345678A","CEN-001","Calle Mayor 1",0,1,1),
            ("CHIP002","Perro","Pastor Alemán","Hembra","Luna","Negro",
             datetime(2020,8,22), datetime(2023,8,22),
             0,"87654321B","CEN-002","Avenida del Parque 5",1,2,2),
            ("CHIP003","Gato","Siamés","Macho","Misi","Blanco",
             datetime(2021,1,5), datetime(2023,1,5),
             1,"11223344C","CEN-003","Calle del Sol 12",0,3,3),
            ("CHIP004","Perro","Rottweiler","Macho","Rex","Negro",
             datetime(2018,6,15), datetime(2022,6,15),
             0,"44332211D","CEN-004","Plaza España 3",1,4,4),
            ("CHIP005","Gato","Persa","Hembra","Nieve","Blanco",
             datetime(2022,3,20), datetime(2023,3,20),
             1,"55667788E","CEN-005","Calle Luna 8",0,5,5),
        ]
    },

    "CENSO": {
        "cols": ["N_CHIP", "N_CENSO", "FECHA_ALTA"],
        "rows": [
            ("CHIP001", "CEN-001", datetime(2019, 4, 15)),
            ("CHIP002", "CEN-002", datetime(2020, 8, 25)),
            ("CHIP003", "CEN-003", datetime(2021, 1, 10)),
            ("CHIP004", "CEN-004", datetime(2018, 6, 20)),
            ("CHIP005", "CEN-005", datetime(2022, 3, 25)),
        ]
    },

    "HISTORICO_MASCOTAS": {
        "cols": ["N_CHIP", "FECHA", "ID_ESTADO", "N_CENSO",
                 "DNI_PROPIETARIO", "OBSERVACIONES"],
        "rows": [
            ("CHIP001", datetime(2019, 4, 15), 1, "CEN-001", "12345678A", "Alta inicial"),
            ("CHIP002", datetime(2020, 8, 25), 1, "CEN-002", "87654321B", "Alta inicial"),
            ("CHIP003", datetime(2021, 1, 10), 1, "CEN-003", "11223344C", "Alta inicial"),
            ("CHIP004", datetime(2018, 6, 20), 1, "CEN-004", "44332211D", "Alta inicial"),
            ("CHIP004", datetime(2023, 6, 20), 2, "CEN-004", "44332211D", "Baja voluntaria"),
            ("CHIP005", datetime(2022, 3, 25), 1, "CEN-005", "55667788E", "Alta inicial"),
        ]
    },
}


# ─── ORDEN ────────────────────────────────────────────────────────────────────
ORDEN_CARGA = [
    "PROPIETARIOS",
    "PROPIETARIO_DIRECCION",
    "LICENCIAS",
    "ADIESTRADORES",
    "SEGUROS",
    "ESTADOS_HISTORICO",
    "ANIMALES",
    "CENSO",
    "HISTORICO_MASCOTAS",
]

TIPOS_COLUMNAS = {
    "PROPIETARIOS":          {"DNI": "str"},
    "PROPIETARIO_DIRECCION": {"CODIGO": "int", "DNI": "str"},
    "LICENCIAS":             {"FECHA_EXPEDICION_LICENCIA": "datetime", "COD_REGISTRO": "int"},
    "ADIESTRADORES":         {"ID_ADIESTRAMIENTO": "int", "ADIESTRAMIENTO": "bool"},
    "SEGUROS":               {"ID_SEGUROS": "int"},
    "ESTADOS_HISTORICO":     {"ID_ESTADO": "int"},
    "ANIMALES": {
        "FECHA_NACIMIENTO":                "datetime",
        "FECHA_ULTIMA_VACUNA_ANTIRRABICA": "datetime",
        "ESTERILIZADO":                    "bool",
        "PELIGROSO":                       "bool",
        "ID_ADIESTRAMIENTO":               "int",
        "ID_SEGUROS":                      "int",
    },
    "CENSO":              {"CODIGO_CENSO": "int", "FECHA_ALTA": "datetime"},
    "HISTORICO_MASCOTAS": {"ID_HISTORICO": "int", "FECHA": "datetime", "ID_ESTADO": "int"},
}
# ──────────────────────────────────────────────────────────────────────────────


# ─── LOGGING ──────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ]
    )

log = logging.getLogger("ETL")
SEP  = "=" * 65
SEP2 = "-" * 65

def titulo(texto):
    log.info("")
    log.info(SEP)
    log.info(f"  {texto}")
    log.info(SEP)

def subtitulo(texto):
    log.info("")
    log.info(f"  ── {texto}")
    log.info(SEP2)


# ══════════════════════════════════════════════════════════════════
#  FASE 1: EXTRACT
# ══════════════════════════════════════════════════════════════════

def conectar_access():
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={MDB_FILE};"
    )
    return pyodbc.connect(conn_str)


def extraer_tablas(conn):
    cur = conn.cursor()
    return [
        r.table_name for r in cur.tables(tableType="TABLE")
        if not r.table_name.startswith("MSys")
    ]


def extraer_datos_tabla(conn, tabla):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM [{tabla}]")
    cols  = [c[0] for c in cur.description]
    filas = [dict(zip(cols, f)) for f in cur.fetchall()]
    return cols, filas


def diagnostico_access(conn, tablas):
    titulo("FASE 1 — EXTRACT: Diagnóstico de la BD Access")
    log.info(f"  Archivo : {MDB_FILE}")
    log.info(f"  Tamaño  : {os.path.getsize(MDB_FILE) / 1024:.1f} KB")
    log.info(f"  Tablas  : {len(tablas)}")

    resumen = {}
    for tabla in tablas:
        cur = conn.cursor()
        cur.execute(f"SELECT TOP 1 * FROM [{tabla}]")
        cols = [c[0] for c in (cur.description or [])]
        cur.execute(f"SELECT COUNT(*) FROM [{tabla}]")
        n = cur.fetchone()[0]
        resumen[tabla] = {"filas": n, "columnas": cols}
        log.info(f"\n  Tabla: {tabla}  ({n} filas, {len(cols)} columnas)")
        for c in cols:
            log.info(f"    · {c}")

    total = sum(v["filas"] for v in resumen.values())
    log.info(f"\n  TOTAL FILAS ORIGEN: {total}")
    return resumen


# ══════════════════════════════════════════════════════════════════
#  FASE 2: TRANSFORM
# ══════════════════════════════════════════════════════════════════

FORMATOS_FECHA = [
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
    "%m/%d/%Y", "%d-%m-%Y",
]

def parsear_fecha(v):
    if v is None: return None
    if isinstance(v, datetime): return v
    s = str(v).strip()
    if not s: return None
    for fmt in FORMATOS_FECHA:
        try: return datetime.strptime(s, fmt)
        except ValueError: pass
    return None

def parsear_bool(v):
    if v is None: return None
    if isinstance(v, bool): return int(v)
    if isinstance(v, int): return 1 if v else 0
    s = str(v).strip().lower()
    if s in ("1","true","si","sí","yes","s"): return 1
    if s in ("0","false","no","n"): return 0
    return None

def limpiar_texto(v, max_len=None):
    if v is None: return None
    s = str(v).strip()
    try: s = s.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError): pass
    if max_len and len(s) > max_len:
        log.warning(f"    Truncado {len(s)}→{max_len}: '{s[:30]}...'")
        s = s[:max_len]
    return s or None

def transformar_valor(v, tipo):
    if v is None: return None
    try:
        if tipo == "int":
            return int(float(str(v).strip())) if not isinstance(v, bool) else int(v)
        if tipo == "float":   return float(str(v).strip())
        if tipo == "decimal": return Decimal(str(v).strip())
        if tipo == "bool":    return parsear_bool(v)
        if tipo == "datetime":return parsear_fecha(v)
        return limpiar_texto(v)
    except (ValueError, TypeError, InvalidOperation):
        return None

def transformar_tabla(tabla, columnas, filas_raw):
    subtitulo(f"Transformando: {tabla}")
    tipos = TIPOS_COLUMNAS.get(tabla, {})
    pks   = {"DNI","N_CHIP","ID_ADIESTRAMIENTO","ID_SEGUROS","ID_ESTADO",
             "CODIGO_CENSO","ID_HISTORICO","N_LICENCIA_ANIMALES_PELIGROSOS","CODIGO"}
    pk    = next((c for c in columnas if c in pks), None)

    filas_ok, vistos, omitidas, advert = [], set(), 0, 0

    for i, fila in enumerate(filas_raw):
        fila_limpia = {}
        for col in columnas:
            v_orig = fila.get(col)
            tipo   = tipos.get(col, "str")
            v_new  = transformar_valor(v_orig, tipo)
            if v_orig is not None and v_new is None and str(v_orig).strip():
                log.debug(f"    Fila {i+1} '{col}': '{v_orig}' → NULL")
                advert += 1
            fila_limpia[col] = v_new

        if pk:
            pk_val = fila_limpia.get(pk)
            if pk_val is not None:
                if pk_val in vistos:
                    log.warning(f"    Fila {i+1}: duplicado {pk}='{pk_val}', omitida.")
                    omitidas += 1
                    continue
                vistos.add(pk_val)

        filas_ok.append(fila_limpia)

    log.info(f"  Procesadas: {len(filas_raw)}  Válidas: {len(filas_ok)}  "
             f"Omitidas: {omitidas}  Advertencias: {advert}")
    return filas_ok


# ══════════════════════════════════════════════════════════════════
#  FASE 3: LOAD
# ══════════════════════════════════════════════════════════════════

def conectar_mariadb():
    conn = mysql.connector.connect(**MARIADB)
    cur  = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    cur.execute("SET NAMES utf8mb4")
    conn.commit()
    return conn

def crear_esquema(conn):
    subtitulo("Creando tablas en MariaDB (sin FK)")
    cur = conn.cursor()
    for tabla in ORDEN_CARGA:
        ddl = DDL_TABLAS.get(tabla, "")
        try:
            cur.execute(ddl)
            log.info(f"  OK  {tabla}")
        except Exception as e:
            log.error(f"  ERROR {tabla}: {e}")
    conn.commit()

def cargar_tabla(conn, tabla, columnas, filas):
    if not filas:
        log.info("  Sin datos.")
        return 0

    cur          = conn.cursor()
    cols_str     = ", ".join(f"`{c}`" for c in columnas)
    placeholders = ", ".join(["%s"] * len(columnas))
    sql          = f"INSERT IGNORE INTO `{tabla}` ({cols_str}) VALUES ({placeholders})"
    insertadas   = 0
    errores      = 0

    for inicio in range(0, len(filas), BATCH_SIZE):
        batch = filas[inicio:inicio + BATCH_SIZE]
        datos = [tuple(f.get(c) for c in columnas) for f in batch]
        try:
            cur.executemany(sql, datos)
            conn.commit()
            insertadas += cur.rowcount if cur.rowcount >= 0 else len(batch)
        except Exception as e:
            log.error(f"  Batch {inicio}: {e}")
            conn.rollback()
            for j, d in enumerate(datos):
                try:
                    cur.execute(sql, d)
                    conn.commit()
                    insertadas += 1
                except Exception as e2:
                    log.warning(f"    Fila {inicio+j+1} omitida: {e2}")
                    errores += 1
                    conn.rollback()

    log.info(f"  Insertadas: {insertadas}  Errores: {errores}")
    return insertadas


# ══════════════════════════════════════════════════════════════════
#  FASE 4: SEED — datos de ejemplo en tablas vacías
# ══════════════════════════════════════════════════════════════════

def seed_datos_ejemplo(conn):
    titulo("FASE 4 — SEED: Datos de ejemplo en tablas vacías")
    cur = conn.cursor()

    for tabla in ORDEN_CARGA:
        seed = DATOS_EJEMPLO.get(tabla)
        if not seed:
            continue

        cur.execute(f"SELECT COUNT(*) FROM `{tabla}`")
        n = cur.fetchone()[0]

        if n > 0:
            log.info(f"  {tabla}: {n} filas existentes, se omite el seed.")
            continue

        cols_str     = ", ".join(f"`{c}`" for c in seed["cols"])
        placeholders = ", ".join(["%s"] * len(seed["cols"]))
        sql          = f"INSERT IGNORE INTO `{tabla}` ({cols_str}) VALUES ({placeholders})"

        try:
            cur.executemany(sql, seed["rows"])
            conn.commit()
            log.info(f"  {tabla}: {len(seed['rows'])} filas de ejemplo insertadas.")
        except Exception as e:
            log.error(f"  ERROR seed {tabla}: {e}")
            conn.rollback()


# ══════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════
#  FASE 5: RELATIONS — limpiar huerfanos y aplicar FK
# ══════════════════════════════════════════════════════════════════

# (tabla_hijo, columna_fk, tabla_padre, columna_pk)
FK_DEPENDENCIAS = [
    ("PROPIETARIO_DIRECCION", "DNI",               "PROPIETARIOS",      "DNI"),
    ("LICENCIAS",             "DNI_PROPIETARIO",   "PROPIETARIOS",      "DNI"),
    ("ANIMALES",              "DNI_PROPIETARIO",   "PROPIETARIOS",      "DNI"),
    ("ANIMALES",              "ID_ADIESTRAMIENTO", "ADIESTRADORES",     "ID_ADIESTRAMIENTO"),
    ("ANIMALES",              "ID_SEGUROS",        "SEGUROS",           "ID_SEGUROS"),
    ("CENSO",                 "N_CHIP",            "ANIMALES",          "N_CHIP"),
    ("HISTORICO_MASCOTAS",    "N_CHIP",            "ANIMALES",          "N_CHIP"),
    ("HISTORICO_MASCOTAS",    "ID_ESTADO",         "ESTADOS_HISTORICO", "ID_ESTADO"),
]


def columna_existe(cur, tabla, columna):
    """Comprueba si una columna existe en una tabla de MariaDB."""
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() "
        "AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (tabla, columna)
    )
    return cur.fetchone()[0] > 0


def limpiar_huerfanos(conn):
    subtitulo("Limpiando registros huerfanos")
    cur = conn.cursor()
    total = 0

    for (tabla_h, col_fk, tabla_p, col_pk) in FK_DEPENDENCIAS:
        # Saltar si la columna no existe en la tabla (datos de Access incompletos)
        if not columna_existe(cur, tabla_h, col_fk):
            log.warning(f"  OMITIDO  {tabla_h}.{col_fk}: columna no existe en MariaDB")
            continue
        if not columna_existe(cur, tabla_p, col_pk):
            log.warning(f"  OMITIDO  {tabla_p}.{col_pk}: columna no existe en MariaDB")
            continue

        sql_count = (
            f"SELECT COUNT(*) FROM `{tabla_h}` "
            f"WHERE `{col_fk}` IS NOT NULL "
            f"AND `{col_fk}` NOT IN (SELECT `{col_pk}` FROM `{tabla_p}`)"
        )
        cur.execute(sql_count)
        n = cur.fetchone()[0]

        if n == 0:
            log.info(f"  OK  {tabla_h}.{col_fk} -> {tabla_p}.{col_pk}")
            continue

        sql_fix = (
            f"UPDATE `{tabla_h}` SET `{col_fk}` = NULL "
            f"WHERE `{col_fk}` IS NOT NULL "
            f"AND `{col_fk}` NOT IN (SELECT `{col_pk}` FROM `{tabla_p}`)"
        )
        cur.execute(sql_fix)
        conn.commit()
        total += n
        log.warning(f"  LIMPIADO  {tabla_h}.{col_fk}: {n} valores huerfanos -> NULL")

    log.info(f"  Total huerfanos limpiados: {total}")



def aplicar_relaciones(conn):
    titulo("FASE 5 - RELATIONS: Aplicando claves foraneas")

    limpiar_huerfanos(conn)

    subtitulo("Aplicando ALTER TABLE ADD CONSTRAINT")
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()

    ok = omitida = error = 0

    for alter in ALTER_FK:
        nombre = next(
            (p.strip("`") for p in alter.split() if p.strip("`").startswith("fk_")),
            alter[:40]
        )
        try:
            cur.execute(alter)
            conn.commit()
            log.info(f"  OK  {nombre}")
            ok += 1
        except mysql.connector.Error as e:
            if e.errno in (1061, 1826) or "Duplicate" in str(e):
                log.info(f"  YA EXISTE  {nombre}")
                omitida += 1
            else:
                log.error(f"  ERROR  {nombre}: {e}")
                error += 1

    log.info(f"  Aplicadas: {ok}  Ya existian: {omitida}  Errores: {error}")


# ══════════════════════════════════════════════════════════════════
#  FASE 6: VERIFY
# ══════════════════════════════════════════════════════════════════

def verificar(conn, resumen_origen):
    titulo("FASE 6 — VERIFY: Validación post-carga")
    cur = conn.cursor()

    log.info(f"\n  {'Tabla':<30} {'Origen':>8} {'Destino':>9} {'Estado':>10}")
    log.info(f"  {'-'*30} {'-'*8} {'-'*9} {'-'*10}")

    todo_ok = True
    for tabla in ORDEN_CARGA:
        origen = resumen_origen.get(tabla, {}).get("filas", "N/A")
        try:
            cur.execute(f"SELECT COUNT(*) FROM `{tabla}`")
            destino = cur.fetchone()[0]
        except Exception:
            destino = "ERROR"

        if isinstance(origen, int) and isinstance(destino, int):
            estado = "OK" if destino >= origen else "REVISAR"
            if estado != "OK": todo_ok = False
        else:
            estado = "?"

        log.info(f"  {tabla:<30} {str(origen):>8} {str(destino):>9} {estado:>10}")

    log.info("")
    if todo_ok:
        log.info("  RESULTADO: migración completada correctamente.")
    else:
        log.warning("  RESULTADO: algunas tablas tienen discrepancias. Revise etl.log.")


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    setup_logging()
    inicio = datetime.now()

    log.info("")
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║        ETL: Microsoft Access 2003 → MariaDB                  ║")
    log.info(f"║        Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}                           ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    # ── 1. EXTRACT ───────────────────────────────────────────────
    try:
        access_conn = conectar_access()
        log.info(f"\n  Conexión Access OK")
    except Exception as e:
        log.error(f"No se pudo abrir el .mdb: {e}")
        sys.exit(1)

    tablas_access  = extraer_tablas(access_conn)
    resumen_origen = diagnostico_access(access_conn, tablas_access)

    datos_extraidos = {}
    for tabla in tablas_access:
        try:
            cols, filas = extraer_datos_tabla(access_conn, tabla)
            datos_extraidos[tabla] = (cols, filas)
        except Exception as e:
            log.error(f"  No se pudo extraer {tabla}: {e}")

    access_conn.close()
    log.info("\n  Extracción completada.")

    # ── 2. TRANSFORM ─────────────────────────────────────────────
    titulo("FASE 2 — TRANSFORM: Limpieza y validación")

    datos_transformados = {}
    for tabla in ORDEN_CARGA:
        if tabla not in datos_extraidos:
            log.warning(f"  {tabla}: no encontrada en Access.")
            continue
        cols, filas_raw = datos_extraidos[tabla]
        datos_transformados[tabla] = (cols, transformar_tabla(tabla, cols, filas_raw))

    # ── 3. LOAD ──────────────────────────────────────────────────
    titulo("FASE 3 — LOAD: Carga en MariaDB")

    try:
        maria_conn = conectar_mariadb()
        log.info(f"  Conexión MariaDB OK: {MARIADB['host']}:{MARIADB['port']}/{MARIADB['database']}")
    except Exception as e:
        log.error(f"No se pudo conectar a MariaDB: {e}")
        sys.exit(1)

    crear_esquema(maria_conn)

    subtitulo("Cargando datos de Access")
    total_filas = 0
    for i, tabla in enumerate(ORDEN_CARGA):
        log.info(f"\n  [{i+1}/{len(ORDEN_CARGA)}] {tabla}")
        if tabla not in datos_transformados:
            log.warning("  Sin datos para esta tabla.")
            continue
        cols, filas = datos_transformados[tabla]
        total_filas += cargar_tabla(maria_conn, tabla, cols, filas)

    # ── 4. SEED ──────────────────────────────────────────────────
    seed_datos_ejemplo(maria_conn)

    # ── 5. RELATIONS ─────────────────────────────────────────────
    aplicar_relaciones(maria_conn)

    # ── 6. VERIFY ────────────────────────────────────────────────
    verificar(maria_conn, resumen_origen)
    maria_conn.close()

    # ── RESUMEN FINAL ────────────────────────────────────────────
    fin      = datetime.now()
    duracion = (fin - inicio).seconds

    titulo("RESUMEN FINAL")
    log.info(f"  Inicio    : {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Fin       : {fin.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Duración  : {duracion}s")
    log.info(f"  Filas cargadas desde Access : {total_filas}")
    log.info(f"  Log completo: {os.path.abspath(LOG_FILE)}")
    log.info(SEP)
    log.info("  ETL completado.")
    log.info(SEP)


if __name__ == "__main__":
    main()