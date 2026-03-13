"""
rellenar_nulos.py
=================
Rellena los campos NULL de todas las tablas con valores de ejemplo
coherentes con el tipo y nombre de cada columna.
"""

import mysql.connector
import sys
import logging
from datetime import datetime
import random

MARIADB = {
    "host":     "localhost",
    "port":     3307,
    "user":     "root",
    "password": "123",
    "database": "censo_animales",
}
LOG_FILE = "rellenar_nulos.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("SEED")
SEP  = "=" * 65
SEP2 = "-" * 65

# ─── DATOS DE EJEMPLO POR TIPO DE COLUMNA ─────────────────────────────────────
# Se elige el valor según el nombre de la columna (case-insensitive)

NOMBRES     = ["Carlos", "Ana", "Pedro", "Laura", "Isabel", "Miguel", "Sofia", "Javier", "Maria", "Luis"]
APELLIDOS   = ["Garcia", "Martinez", "Lopez", "Sanchez", "Gonzalez", "Rodriguez", "Fernandez", "Diaz", "Moreno", "Ruiz"]
CALLES      = ["Calle Mayor", "Av. del Parque", "Calle del Sol", "Plaza Espana", "Calle Luna", "Av. Constitucion", "Calle Ancha", "Calle Real"]
MUNICIPIOS  = ["Madrid", "Navalcarnero", "Mostoles", "Alcorcon", "Leganes", "Getafe", "Alcala de Henares", "Fuenlabrada"]
ESPECIES    = ["Perro", "Gato", "Conejo", "Hamster", "Ave"]
RAZAS_PERRO = ["Labrador", "Pastor Aleman", "Bulldog", "Caniche", "Chihuahua", "Boxer", "Beagle", "Husky"]
RAZAS_GATO  = ["Siames", "Persa", "Maine Coon", "British Shorthair", "Bengala"]
COLORES     = ["Negro", "Blanco", "Marron", "Gris", "Naranja", "Atigrado", "Bicolor", "Dorado"]
COMPANIAS   = ["Mapfre", "AXA", "Allianz", "Generali", "Zurich", "Mutua Madrilena", "Helvetia"]
ESTADOS     = ["Alta", "Baja", "Fallecido", "Transferido", "Extraviado"]
MOTIVOS     = ["Fallecimiento", "Cesion", "Abandono", "Mudanza", "Otros"]
LUGARES     = ["Madrid", "Mostoles", "Navalcarnero", "Alcorcon", "Leganes"]

def valor_para_columna(col, tipo, idx):
    """Genera un valor de ejemplo según el nombre y tipo de columna."""
    c = col.upper()
    i = idx % 10

    # Fechas
    if tipo in ("datetime", "date", "timestamp") or "FECHA" in c or "DATE" in c or "ALTA" in c or "EXP" in c:
        anio  = 2018 + (idx % 6)
        mes   = (idx % 12) + 1
        dia   = (idx % 28) + 1
        return f"{anio}-{mes:02d}-{dia:02d} 00:00:00"

    # Booleanos
    if tipo in ("tinyint",) or "ESTERILIZADO" in c or "PELIGROSO" in c or "ADIESTRAMIENTO" in c or "ESPECIE_PROTEGIDA" in c:
        return idx % 2

    # Enteros numéricos
    if tipo in ("int", "bigint", "smallint"):
        return (idx % 5) + 1

    # Textos especiales por nombre de columna
    if "DNI" in c:                         return f"{10000000 + idx * 7 % 90000000:08d}{chr(65 + idx % 26)}"
    if "CHIP" in c:                        return f"CHIP{idx+1:04d}"
    if "CENSO" in c:                       return f"CEN-{idx+1:04d}"
    if "TELEFONO" in c or "PHONE" in c:    return f"6{idx*13 % 100000000:08d}"
    if "CP" in c or "POSTAL" in c:         return f"{28000 + idx % 900}"
    if "MUNICIPIO" in c or "MINICIPIO" in c or "CIUDAD" in c: return MUNICIPIOS[i]
    if "DOMICILIO" in c or "DIRECCION" in c or "CALLE" in c:  return f"{CALLES[i % len(CALLES)]} {idx+1}"
    if "NOMBRE" in c:                      return NOMBRES[i]
    if "PRIMER_APELLIDO" in c:             return APELLIDOS[i]
    if "SEGUNDO_APELLIDO" in c:            return APELLIDOS[(i+1) % 10]
    if "ESPECIE" in c:                     return ESPECIES[i % len(ESPECIES)]
    if "RAZA" in c:                        return RAZAS_PERRO[i % len(RAZAS_PERRO)]
    if "SEXO" in c or "CLAVE" in c:        return "Macho" if idx % 2 == 0 else "Hembra"
    if "COLOR" in c:                       return COLORES[i % len(COLORES)]
    if "COMPANIA" in c or "SEGURO_C" in c: return COMPANIAS[i % len(COMPANIAS)]
    if "POLIZA" in c:                      return f"POL-{2020 + idx % 5}-{idx+1:04d}"
    if "LICENCIA" in c:                    return f"LIC-{idx+1:04d}"
    if "REGISTRO" in c:                    return f"REG-{idx+1:04d}"
    if "LUGAR" in c or "EXPEDICION" in c:  return LUGARES[i % len(LUGARES)]
    if "CERTIFICADO" in c:                 return f"CERT-{2020 + idx % 5}-{idx+1:03d}"
    if "ESTADO" in c:                      return ESTADOS[i % len(ESTADOS)]
    if "MOTIVO" in c:                      return MOTIVOS[i % len(MOTIVOS)]
    if "OBSERVACION" in c:                 return "Sin observaciones"
    if "IDENTIFICACION" in c:              return f"ID-{idx+1:06d}"
    if "HABITACULO" in c:                  return "Domicilio particular"
    if "VENDEDOR" in c:                    return f"Vendedor {idx+1}"
    if "DESTINO" in c:                     return "Perro" if idx % 2 == 0 else "Gato"
    if "FAMILIA" in c:                     return "No"
    if "PROTEGIDA" in c:                   return "No"
    if "PESA" in c or "KG" in c:           return "No" if idx % 2 == 0 else "Si"
    if "CITES" in c:                       return f"CITES-{idx+1:04d}"
    if "BAJA" in c:                        return f"BAJA-{idx+1:04d}"
    if "ALTA" in c:                        return f"ALTA-{idx+1:04d}"
    if "COD" in c:                         return f"COD-{idx+1:04d}"

    # Fallback texto genérico
    return f"Valor-{col[:8]}-{idx+1}"


def obtener_columnas(cur, tabla):
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
        "ORDER BY ORDINAL_POSITION",
        (tabla,)
    )
    return cur.fetchall()  # (nombre, tipo, nullable, key)


def obtener_tablas(cur):
    cur.execute(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' "
        "ORDER BY TABLE_NAME"
    )
    return [r[0] for r in cur.fetchall()]


def rellenar_tabla(conn, tabla):
    cur = conn.cursor()
    columnas = obtener_columnas(cur, tabla)

    # Columnas nullables que no son PK ni AUTO_INCREMENT
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
        "AND IS_NULLABLE = 'YES' AND COLUMN_KEY != 'PRI' "
        "AND EXTRA NOT LIKE '%auto_increment%'",
        (tabla,)
    )
    cols_nullable = [r[0] for r in cur.fetchall()]

    if not cols_nullable:
        log.info(f"  {tabla}: sin columnas nullables, omitida.")
        return 0

    # Obtener PK para poder hacer el UPDATE fila por fila
    pk_col = None
    for (nombre, tipo, nullable, key) in columnas:
        if key == "PRI":
            pk_col = nombre
            break

    total_actualizadas = 0

    for col_null in cols_nullable:
        # Ver cuántas filas tienen NULL en esta columna
        cur.execute(f"SELECT COUNT(*) FROM `{tabla}` WHERE `{col_null}` IS NULL")
        n_nulos = cur.fetchone()[0]
        if n_nulos == 0:
            continue

        # Obtener tipo de la columna
        tipo_col = next(
            (t for (n, t, _, _) in columnas if n == col_null),
            "varchar"
        )

        if pk_col:
            # Actualizar fila a fila para que cada registro tenga un valor distinto
            cur.execute(
                f"SELECT `{pk_col}` FROM `{tabla}` WHERE `{col_null}` IS NULL"
            )
            pks = [r[0] for r in cur.fetchall()]

            for idx, pk_val in enumerate(pks):
                valor = valor_para_columna(col_null, tipo_col, idx)
                try:
                    cur.execute(
                        f"UPDATE `{tabla}` SET `{col_null}` = %s "
                        f"WHERE `{pk_col}` = %s",
                        (valor, pk_val)
                    )
                except Exception as e:
                    log.warning(f"    Fila {pk_val} col {col_null}: {e}")
        else:
            # Sin PK: UPDATE masivo con valor único
            valor = valor_para_columna(col_null, tipo_col, 0)
            cur.execute(
                f"UPDATE `{tabla}` SET `{col_null}` = %s WHERE `{col_null}` IS NULL",
                (valor,)
            )

        conn.commit()
        total_actualizadas += n_nulos
        log.info(f"    {col_null}: {n_nulos} NULL -> valores de ejemplo")

    return total_actualizadas


def main():
    inicio = datetime.now()
    log.info("")
    log.info(SEP)
    log.info("  RELLENAR CAMPOS NULL — censo_animales")
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

    tablas = obtener_tablas(cur)
    log.info(f"  Tablas encontradas: {len(tablas)}")
    log.info("")

    total_global = 0
    for tabla in tablas:
        log.info(f"{SEP2}")
        log.info(f"  Tabla: {tabla}")
        n = rellenar_tabla(conn, tabla)
        log.info(f"  Campos actualizados: {n}")
        total_global += n

    log.info("")
    log.info(SEP)
    log.info(f"  COMPLETADO")
    log.info(f"  Total campos actualizados: {total_global}")
    log.info(f"  Duracion: {(datetime.now()-inicio).seconds}s")
    log.info(f"  Log: {LOG_FILE}")
    log.info(SEP)

    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
