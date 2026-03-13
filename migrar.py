import pyodbc
import mysql.connector
import sys
import os

# ─── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
MDB_FILE = r"C:\Users\practicas\Desktop\Adrián Prácticas\CENSO_ANIMALES - copia.mdb"  # <── cambie esto

MARIADB = {
    "host":     "localhost",
    "port":     3307,           # verifique con: docker ps
    "user":     "root",         # su usuario de MariaDB
    "password": "123",     # su contraseña
    "database": "censo_animales",        # nombre de la base de datos destino
}
# ───────────────────────────────────────────────────────────────────────────────


def conectar_access(mdb_file):
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={mdb_file};"
    )
    return pyodbc.connect(conn_str)


def conectar_mariadb(cfg):
    return mysql.connector.connect(**cfg)


def obtener_tablas(access_conn):
    cursor = access_conn.cursor()
    tablas = [
        row.table_name
        for row in cursor.tables(tableType="TABLE")
        if not row.table_name.startswith("MSys")
    ]
    return tablas


def mapear_tipo(type_code):
    mapa = {
        -11: "CHAR(36)",
        -10: "LONGTEXT",
        -9:  "VARCHAR(255)",
        -8:  "CHAR(10)",
        -7:  "TINYINT(1)",
        -6:  "TINYINT",
        -5:  "BIGINT",
        -4:  "LONGBLOB",
        -3:  "BLOB",
        -2:  "BLOB",
        -1:  "LONGTEXT",
         1:  "CHAR(255)",
         2:  "DECIMAL(18,4)",
         3:  "DECIMAL(18,4)",
         4:  "INT",
         5:  "SMALLINT",
         6:  "FLOAT",
         7:  "FLOAT",
         8:  "DOUBLE",
         9:  "DATE",
        10:  "TIME",
        11:  "DATETIME",
        12:  "VARCHAR(255)",
        93:  "DATETIME",
    }
    return mapa.get(type_code, "TEXT")


def nombre_tipo(type_code):
    mapa = {
        -11: "GUID",
        -10: "Memo",
        -9:  "Texto",
        -8:  "Char",
        -7:  "Si/No",
        -6:  "Entero pequeno",
        -5:  "Entero largo",
        -4:  "OLE/Binario",
        -3:  "Binario",
        -2:  "Binario",
        -1:  "Memo",
         1:  "Texto",
         2:  "Numerico",
         3:  "Numerico",
         4:  "Entero largo",
         5:  "Entero",
         6:  "Decimal",
         7:  "Decimal",
         8:  "Decimal",
         9:  "Fecha",
        10:  "Hora",
        11:  "Fecha/Hora",
        12:  "Texto",
        93:  "Fecha/Hora",
    }
    return mapa.get(type_code, "Desconocido")


def mostrar_info_access(access_conn, tablas):
    sep = "=" * 60
    print(sep)
    print("  INFORMACION DE LA BASE DE DATOS ACCESS")
    print(sep)
    print(f"  Archivo : {MDB_FILE}")
    print(f"  Tamanio : {os.path.getsize(MDB_FILE) / 1024:.1f} KB")
    print(f"  Tablas  : {len(tablas)}")
    print(sep)

    total_filas = 0

    for tabla in tablas:
        cursor = access_conn.cursor()

        cursor.execute(f"SELECT TOP 1 * FROM [{tabla}]")
        columnas = cursor.description or []

        cursor.execute(f"SELECT COUNT(*) FROM [{tabla}]")
        n_filas = cursor.fetchone()[0]
        total_filas += n_filas

        print(f"\n  Tabla: {tabla}")
        print(f"  Filas: {n_filas}  |  Columnas: {len(columnas)}")
        print(f"  {'Columna':<30} {'Tipo Access':<18} {'Tipo MariaDB'}")
        print(f"  {'-'*30} {'-'*18} {'-'*15}")

        for col in columnas:
            nombre = col[0]
            tipo_a = nombre_tipo(col[1])
            tipo_m = mapear_tipo(col[1])
            print(f"  {nombre:<30} {tipo_a:<18} {tipo_m}")

    print(f"\n{sep}")
    print(f"  TOTAL FILAS EN ACCESS: {total_filas}")
    print(sep)
    print()


def crear_tabla(maria_cursor, access_cursor, tabla):
    access_cursor.execute(f"SELECT TOP 1 * FROM [{tabla}]")
    columnas = access_cursor.description

    col_defs = []
    for col in columnas:
        nombre = col[0].replace(" ", "_")
        tipo   = mapear_tipo(col[1])
        col_defs.append(f"  `{nombre}` {tipo}")

    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{tabla}` (\n"
        + ",\n".join(col_defs)
        + "\n) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    maria_cursor.execute(ddl)


def migrar_tabla(access_conn, maria_conn, tabla):
    a_cur = access_conn.cursor()
    m_cur = maria_conn.cursor()

    crear_tabla(m_cur, a_cur, tabla)

    a_cur.execute(f"SELECT * FROM [{tabla}]")
    columnas = [col[0].replace(" ", "_") for col in a_cur.description]
    filas    = a_cur.fetchall()

    if not filas:
        return 0

    cols_str     = ", ".join(f"`{c}`" for c in columnas)
    placeholders = ", ".join(["%s"] * len(columnas))
    sql          = f"INSERT IGNORE INTO `{tabla}` ({cols_str}) VALUES ({placeholders})"

    datos = []
    for fila in filas:
        datos.append(tuple(
            str(v) if not isinstance(v, (int, float, type(None))) else v
            for v in fila
        ))

    m_cur.executemany(sql, datos)
    maria_conn.commit()
    return len(datos)


def main():
    print("\nConectando a Access...")
    try:
        access_conn = conectar_access(MDB_FILE)
    except Exception as e:
        print(f"\nERROR: No se pudo abrir el archivo .mdb")
        print(f"Detalle: {e}")
        print("Verifique que el driver de Access esta instalado.")
        sys.exit(1)

    tablas = obtener_tablas(access_conn)

    # Mostrar resumen de Access antes de migrar
    mostrar_info_access(access_conn, tablas)

    print("Conectando a MariaDB...")
    try:
        maria_conn = conectar_mariadb(MARIADB)
    except Exception as e:
        print(f"\nERROR: No se pudo conectar a MariaDB")
        print(f"Detalle: {e}")
        print("Verifique que Docker esta corriendo y el puerto 3306 esta expuesto.")
        sys.exit(1)

    sep = "=" * 60
    print(sep)
    print("  INICIANDO MIGRACION")
    print(sep)

    total = 0
    for i, tabla in enumerate(tablas):
        print(f"\n  [{i+1}/{len(tablas)}] {tabla}")
        try:
            n = migrar_tabla(access_conn, maria_conn, tabla)
            print(f"  OK -- {n} filas insertadas.")
            total += n
        except Exception as e:
            print(f"  ERROR: {e}")

    access_conn.close()
    maria_conn.close()

    print(f"\n{sep}")
    print(f"  MIGRACION COMPLETADA")
    print(f"  Total filas migradas: {total}")
    print(sep)


if __name__ == "__main__":
    main()