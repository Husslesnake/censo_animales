"""
relaciones.py — v3
==================
1. Convierte TEXT -> VARCHAR(100) en columnas FK
2. Añade índice en columnas padre si no lo tienen
3. Iguala charset/collation entre columnas relacionadas
4. Limpia huérfanos
5. Aplica FK
"""

import mysql.connector
import sys
import logging
from datetime import datetime

MARIADB = {
    "host":     "localhost",
    "port":     3307,
    "user":     "root",
    "password": "123",
    "database": "censo_animales",
}
LOG_FILE = "relaciones.log"

RELACIONES = [
    ("PROPIETARIO_DIRECCION", "DNI",             "PROPIETARIOS",      "DNI",       "fk_propdir_propietario"),
    ("LICENCIAS",             "DNI_PROPIETARIO", "PROPIETARIOS",      "DNI",       "fk_licencias_propietario"),
    ("ANIMALES",              "DNI_PROPIETARIO", "PROPIETARIOS",      "DNI",       "fk_animales_propietario"),
    ("ANIMALES",              "SEXO",            "SEXO",              "CLAVE",     "fk_animales_sexo"),
    ("HISTORICO_MASCOTAS",    "ID_ESTADO",       "ESTADOS_HISTORICO", "ID_ESTADO", "fk_historico_estado"),
    ("CENSO",                 None,              "ANIMALES",          None,        "fk_censo_animal"),
    ("HISTORICO_MASCOTAS",    "N_CHIP",          "ANIMALES",          None,        "fk_historico_animal"),
    ("SEGUROS",               "N_CHIP",          "ANIMALES",          None,        "fk_seguros_animal"),
    ("ADIESTRADORES",         "N_CHIP",          "ANIMALES",          None,        "fk_adiestradores_animal"),
    ("ALTA_ANIMAL",           None,              "ANIMALES",          None,        "fk_alta_animal"),
    ("BAJA_ANIMAL",           None,              "ANIMALES",          None,        "fk_baja_animal"),
    ("BAJA_ANIMAL",           "MOTIVO",          "MOTIVO_BAJA",       "CLAVE",     "fk_baja_motivo"),
    ("ANIMALES_PELIGROSOS",   None,              "ANIMALES",          None,        "fk_anipeligrosos_animal"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log  = logging.getLogger("FK")
SEP  = "=" * 65
SEP2 = "-" * 65


# ─── HELPERS ──────────────────────────────────────────────────────

def q(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()

def q1(cur, sql, params=()):
    cur.execute(sql, params)
    r = cur.fetchone()
    return r[0] if r else None

def tabla_existe(cur, t):
    return q1(cur,
        "SELECT COUNT(*) FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s", (t,)) > 0

def col_info(cur, tabla, col):
    rows = q(cur,
        "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
        "CHARACTER_SET_NAME, COLLATION_NAME "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() "
        "AND TABLE_NAME=%s AND COLUMN_NAME=%s",
        (tabla, col))
    return rows[0] if rows else None

def tiene_indice(cur, tabla, col):
    """Comprueba si una columna tiene PRIMARY KEY, UNIQUE o INDEX."""
    n = q1(cur,
        "SELECT COUNT(*) FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() "
        "AND TABLE_NAME=%s AND COLUMN_NAME=%s",
        (tabla, col))
    return (n or 0) > 0

def fk_existe(cur, nombre):
    n = q1(cur,
        "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS "
        "WHERE TABLE_SCHEMA=DATABASE() "
        "AND CONSTRAINT_NAME=%s AND CONSTRAINT_TYPE='FOREIGN KEY'",
        (nombre,))
    return (n or 0) > 0

def pk_chip(cur, tabla):
    for candidato in ("N_CHIP", "Nº_CHIP", "NÃº_CHIP"):
        info = col_info(cur, tabla, candidato)
        if info:
            return candidato
    row = q(cur,
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s "
        "AND COLUMN_NAME LIKE '%CHIP%' LIMIT 1", (tabla,))
    return row[0][0] if row else None


# ─── PASO 1: Convertir TEXT → VARCHAR ─────────────────────────────

def asegurar_varchar(conn, tabla, col, longitud=100):
    cur = conn.cursor()
    info = col_info(cur, tabla, col)
    if info is None:
        log.warning(f"  OMITIDO  {tabla}.{col}: columna no encontrada")
        return False
    tipo = info[0].upper()
    if tipo not in ("TEXT","LONGTEXT","MEDIUMTEXT","TINYTEXT"):
        return True  # ya es VARCHAR u otro tipo indexable
    try:
        cur.execute(
            f"ALTER TABLE `{tabla}` MODIFY COLUMN `{col}` "
            f"VARCHAR({longitud}) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        conn.commit()
        log.info(f"  CONVERTIDO  {tabla}.{col}: {tipo} -> VARCHAR({longitud})")
        return True
    except mysql.connector.Error as e:
        log.error(f"  ERROR convirtiendo {tabla}.{col}: {e}")
        return False


# ─── PASO 2: Añadir índice en columna padre ───────────────────────

def asegurar_indice(conn, tabla, col):
    cur = conn.cursor()
    if tiene_indice(cur, tabla, col):
        return True
    idx_name = f"idx_{tabla}_{col}"
    try:
        cur.execute(
            f"ALTER TABLE `{tabla}` ADD INDEX `{idx_name}` (`{col}`)"
        )
        conn.commit()
        log.info(f"  INDICE AÑADIDO  {tabla}.{col}")
        return True
    except mysql.connector.Error as e:
        log.error(f"  ERROR añadiendo indice {tabla}.{col}: {e}")
        return False


# ─── PASO 3: Igualar collation ────────────────────────────────────

def igualar_collation(conn, tabla_h, col_fk, tabla_p, col_pk):
    cur = conn.cursor()
    info_p = col_info(cur, tabla_p, col_pk)
    info_h = col_info(cur, tabla_h, col_fk)
    if not info_p or not info_h:
        return
    tipo_p, long_p, cs_p, coll_p = info_p
    tipo_h, long_h, cs_h, coll_h = info_h

    # Si ya coinciden no hacer nada
    if coll_h == coll_p and tipo_h.upper() == tipo_p.upper():
        return

    tipo_p_up = tipo_p.upper()
    if tipo_p_up not in ("VARCHAR","CHAR","INT","TINYINT","SMALLINT","BIGINT"):
        return  # no es indexable, se habrá convertido antes

    long_usar = long_p or 100
    try:
        cur.execute(
            f"ALTER TABLE `{tabla_h}` MODIFY COLUMN `{col_fk}` "
            f"VARCHAR({long_usar}) CHARACTER SET {cs_p} COLLATE {coll_p}"
        )
        conn.commit()
        log.info(
            f"  COLLATION IGUALADA  {tabla_h}.{col_fk} "
            f"-> {cs_p}/{coll_p} (igual que {tabla_p}.{col_pk})"
        )
    except mysql.connector.Error as e:
        log.error(f"  ERROR igualando collation {tabla_h}.{col_fk}: {e}")


# ─── PASO 4: Limpiar huérfanos ────────────────────────────────────

def limpiar_huerfanos(conn, th, cfk, tp, cpk):
    cur = conn.cursor()
    if not col_info(cur, th, cfk) or not col_info(cur, tp, cpk):
        return
    cur.execute(
        f"SELECT COUNT(*) FROM `{th}` "
        f"WHERE `{cfk}` IS NOT NULL "
        f"AND `{cfk}` NOT IN (SELECT `{cpk}` FROM `{tp}`)"
    )
    n = cur.fetchone()[0]
    if n == 0:
        log.info(f"  OK  sin huerfanos: {th}.{cfk} -> {tp}.{cpk}")
        return
    cur.execute(
        f"UPDATE `{th}` SET `{cfk}` = NULL "
        f"WHERE `{cfk}` IS NOT NULL "
        f"AND `{cfk}` NOT IN (SELECT `{cpk}` FROM `{tp}`)"
    )
    conn.commit()
    log.warning(f"  LIMPIADO  {th}.{cfk}: {n} huerfanos -> NULL")


# ─── MAIN ─────────────────────────────────────────────────────────

def main():
    inicio = datetime.now()
    log.info("")
    log.info(SEP)
    log.info("  APLICAR RELACIONES FK — censo_animales  v3")
    log.info(SEP)

    try:
        conn = mysql.connector.connect(**MARIADB)
        cur  = conn.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        cur.execute("SET NAMES utf8mb4")
        conn.commit()
        log.info("  Conexion OK\n")
    except Exception as e:
        log.error(f"  No se pudo conectar: {e}")
        sys.exit(1)

    chip = pk_chip(cur, "ANIMALES")
    log.info(f"  Columna chip detectada en ANIMALES: '{chip}'\n")

    # Resolver Nones
    relaciones = []
    for (th, cfk, tp, cpk, nombre) in RELACIONES:
        if cfk is None:
            cfk = pk_chip(cur, th) or chip
        if cpk is None and tp == "ANIMALES":
            cpk = chip
        if cfk and cpk:
            relaciones.append((th, cfk, tp, cpk, nombre))
        else:
            log.warning(f"  OMITIDO  {nombre}: columnas no resueltas")

    # ── PASO 1: TEXT → VARCHAR ────────────────────────────────────
    log.info(SEP2)
    log.info("  PASO 1 — Convirtiendo TEXT -> VARCHAR")
    log.info(SEP2)
    for (th, cfk, tp, cpk, _) in relaciones:
        if not tabla_existe(cur, th) or not tabla_existe(cur, tp):
            continue
        asegurar_varchar(conn, tp, cpk)   # padre primero
        asegurar_varchar(conn, th, cfk)   # luego hijo

    # ── PASO 2: Añadir índices en columnas padre ──────────────────
    log.info("")
    log.info(SEP2)
    log.info("  PASO 2 — Añadiendo indices en columnas padre")
    log.info(SEP2)
    padres_vistos = set()
    for (th, cfk, tp, cpk, _) in relaciones:
        if not tabla_existe(cur, tp):
            continue
        clave = (tp, cpk)
        if clave not in padres_vistos:
            asegurar_indice(conn, tp, cpk)
            padres_vistos.add(clave)

    # ── PASO 3: Igualar collation ─────────────────────────────────
    log.info("")
    log.info(SEP2)
    log.info("  PASO 3 — Igualando charset/collation")
    log.info(SEP2)
    for (th, cfk, tp, cpk, _) in relaciones:
        if tabla_existe(cur, th) and tabla_existe(cur, tp):
            igualar_collation(conn, th, cfk, tp, cpk)

    # ── PASO 4: Limpiar huérfanos ─────────────────────────────────
    log.info("")
    log.info(SEP2)
    log.info("  PASO 4 — Limpiando huerfanos")
    log.info(SEP2)
    for (th, cfk, tp, cpk, _) in relaciones:
        if tabla_existe(cur, th) and tabla_existe(cur, tp):
            limpiar_huerfanos(conn, th, cfk, tp, cpk)

    # ── PASO 5: Aplicar FK ────────────────────────────────────────
    log.info("")
    log.info(SEP2)
    log.info("  PASO 5 — Aplicando claves foraneas")
    log.info(SEP2)
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()

    ok = omitida = error = 0
    for (th, cfk, tp, cpk, nombre) in relaciones:
        if not tabla_existe(cur, th) or not tabla_existe(cur, tp):
            log.warning(f"  OMITIDO  {nombre}: tabla no existe")
            continue
        if fk_existe(cur, nombre):
            log.info(f"  YA EXISTE  {nombre}")
            omitida += 1
            continue
        alter = (
            f"ALTER TABLE `{th}` ADD CONSTRAINT `{nombre}` "
            f"FOREIGN KEY (`{cfk}`) REFERENCES `{tp}`(`{cpk}`) "
            f"ON UPDATE CASCADE ON DELETE SET NULL"
        )
        try:
            cur.execute(alter)
            conn.commit()
            log.info(f"  OK  {nombre}  ({th}.{cfk} -> {tp}.{cpk})")
            ok += 1
        except mysql.connector.Error as e:
            log.error(f"  ERROR  {nombre}: {e}")
            error += 1

    log.info("")
    log.info(SEP)
    log.info(f"  Aplicadas  : {ok}")
    log.info(f"  Ya existian: {omitida}")
    log.info(f"  Errores    : {error}")
    log.info(f"  Duracion   : {(datetime.now()-inicio).seconds}s")
    log.info(SEP)
    conn.close()

if __name__ == "__main__":
    main()