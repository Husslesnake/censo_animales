"""
app.py — API REST Flask para CENSO_ANIMALES
Lee la configuración de variables de entorno (Docker Compose).
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import os
from datetime import datetime, date

app = Flask(__name__)
CORS(app)

MARIADB = {
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     int(os.environ.get("DB_PORT", "3306")),
    "user":     os.environ.get("DB_USER",     "root"),
    "password": os.environ.get("DB_PASSWORD", "123"),
    "database": os.environ.get("DB_NAME",     "censo_animales"),
}

def get_conn():
    conn = mysql.connector.connect(**MARIADB)
    conn.cursor().execute("SET NAMES utf8mb4")
    return conn

def serializar(val):
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return val

def fila_a_dict(cursor, fila):
    cols = [d[0] for d in cursor.description]
    return {c: serializar(v) for c, v in zip(cols, fila)}

def detectar_chip(cur):
    for c in ("N_CHIP", "Nº_CHIP", "NÃº_CHIP"):
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='ANIMALES' AND COLUMN_NAME=%s", (c,)
        )
        if cur.fetchone()[0]:
            return c
    return "N_CHIP"

# ── Propietarios ──────────────────────────────────────────────────

@app.route("/api/propietarios", methods=["GET"])
def listar_propietarios():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM PROPIETARIOS ORDER BY PRIMER_APELLIDO, NOMBRE")
        rows = [fila_a_dict(cur, r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"ok": True, "datos": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/propietarios/<dni>", methods=["GET"])
def obtener_propietario(dni):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM PROPIETARIOS WHERE DNI = %s", (dni,))
        row = cur.fetchone(); conn.close()
        if not row:
            return jsonify({"ok": False, "error": "No encontrado"}), 404
        return jsonify({"ok": True, "datos": fila_a_dict(cur, row)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/propietarios", methods=["POST"])
def insertar_propietario():
    d = request.get_json()
    if not d.get("DNI"):
        return jsonify({"ok": False, "error": "Campo requerido: DNI"}), 400
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO PROPIETARIOS
              (DNI, PRIMER_APELLIDO, SEGUNDO_APELLIDO, NOMBRE,
               TELEFONO1, TELEFONO2, DOMICILIO, CP, MINICIPIO, CODIGO)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            d.get("DNI"), d.get("PRIMER_APELLIDO"), d.get("SEGUNDO_APELLIDO"),
            d.get("NOMBRE"), d.get("TELEFONO1"), d.get("TELEFONO2"),
            d.get("DOMICILIO"), d.get("CP"), d.get("MINICIPIO"), d.get("CODIGO"),
        ))
        conn.commit(); conn.close()
        return jsonify({"ok": True, "mensaje": "Propietario registrado correctamente."})
    except mysql.connector.IntegrityError:
        return jsonify({"ok": False, "error": "El DNI ya existe en la base de datos."}), 409
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ── Animales ──────────────────────────────────────────────────────

@app.route("/api/animales", methods=["GET"])
def listar_animales():
    try:
        conn = get_conn(); cur = conn.cursor()
        chip_col = detectar_chip(cur)
        cur.execute(f"SELECT * FROM ANIMALES ORDER BY `{chip_col}`")
        rows = [fila_a_dict(cur, r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"ok": True, "datos": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/animales/<chip>", methods=["GET"])
def obtener_animal(chip):
    try:
        conn = get_conn(); cur = conn.cursor()
        chip_col = detectar_chip(cur)
        cur.execute(f"SELECT * FROM ANIMALES WHERE `{chip_col}` = %s", (chip,))
        row = cur.fetchone(); conn.close()
        if not row:
            return jsonify({"ok": False, "error": "No encontrado"}), 404
        return jsonify({"ok": True, "datos": fila_a_dict(cur, row)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/animales", methods=["POST"])
def insertar_animal():
    d = request.get_json()
    try:
        conn = get_conn(); cur = conn.cursor()
        chip_col = detectar_chip(cur)
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='ANIMALES' "
            "ORDER BY ORDINAL_POSITION"
        )
        cols_tabla = [r[0] for r in cur.fetchall()]

        mapa = {
            "N_CHIP":                         chip_col,
            "ESPECIE":                        "ESPECIE",
            "RAZA":                           "RAZA",
            "SEXO":                           "SEXO",
            "NOMBRE":                         "NOMBRE",
            "COLOR":                          "COLOR",
            "FECHA_NACIMIENTO":               "FECHA_NACIMIENTO" if "FECHA_NACIMIENTO" in cols_tabla else "AÑO_DE_NACIMIENTO",
            "FECHA_ULTIMA_VACUNA_ANTIRRABICA": "FECHA_ULTIMA_VACUNA_ANTIRRABICA",
            "ESTERILIZADO":                   "ESTERILIZADO",
            "DNI_PROPIETARIO":                "DNI_PROPIETARIO",
            "PELIGROSO":                      "PELIGROSO",
        }

        insert_cols, insert_vals = [], []
        for campo_form, col_real in mapa.items():
            if col_real in cols_tabla and d.get(campo_form) is not None and str(d.get(campo_form)).strip():
                insert_cols.append(f"`{col_real}`")
                val = d[campo_form]
                if campo_form in ("ESTERILIZADO", "PELIGROSO"):
                    val = 1 if str(val).lower() in ("1","true","si","sí","yes") else 0
                insert_vals.append(val)

        if not insert_cols:
            return jsonify({"ok": False, "error": "No se proporcionaron datos."}), 400

        sql = (f"INSERT INTO ANIMALES ({','.join(insert_cols)}) "
               f"VALUES ({','.join(['%s']*len(insert_vals))})")
        cur.execute(sql, insert_vals)
        conn.commit(); conn.close()
        return jsonify({"ok": True, "mensaje": "Animal registrado correctamente."})
    except mysql.connector.IntegrityError as e:
        if e.errno == 1062:
            return jsonify({"ok": False, "error": f"El número de chip ya existe. Detalle: {e}"}), 409
        elif e.errno == 1452:
            return jsonify({"ok": False, "error": f"El DNI del propietario no existe. Regístrelo primero. Detalle: {e}"}), 409
        else:
            return jsonify({"ok": False, "error": f"Error de integridad ({e.errno}): {e}"}), 409
    except Exception as e:
        return jsonify({"ok": False, "error": f"Error inesperado: {e}"}), 500

# ── Sexos y estados ───────────────────────────────────────────────

@app.route("/api/sexos", methods=["GET"])
def listar_sexos():
    try:
        conn = get_conn(); cur = conn.cursor()
        try:
            cur.execute("SELECT CLAVE, SEXO FROM SEXO")
            rows = [fila_a_dict(cur, r) for r in cur.fetchall()]
        except Exception:
            rows = [{"CLAVE":"Macho","SEXO":"Macho"},{"CLAVE":"Hembra","SEXO":"Hembra"}]
        conn.close()
        return jsonify({"ok": True, "datos": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/estados", methods=["GET"])
def listar_estados():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT ID_ESTADO, ESTADO FROM ESTADOS_HISTORICO ORDER BY ESTADO")
        rows = [fila_a_dict(cur, r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"ok": True, "datos": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ── Seguros ───────────────────────────────────────────────────────

@app.route("/api/seguros", methods=["GET"])
def listar_seguros():
    try:
        conn = get_conn(); cur = conn.cursor()
        chip_col = detectar_chip(cur)
        cur.execute(f"""
            SELECT s.ID_SEGUROS, s.`{chip_col}` AS N_CHIP,
                   s.SEGURO_COMPANIA, s.SEGURO_POLIZA,
                   a.NOMBRE AS NOMBRE_ANIMAL, a.ESPECIE,
                   a.DNI_PROPIETARIO
            FROM SEGUROS s
            LEFT JOIN ANIMALES a ON s.`{chip_col}` = a.`{chip_col}`
            ORDER BY s.ID_SEGUROS
        """)
        rows = [fila_a_dict(cur, r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"ok": True, "datos": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/seguros/<int:id_seguro>", methods=["GET"])
def obtener_seguro(id_seguro):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM SEGUROS WHERE ID_SEGUROS = %s", (id_seguro,))
        row = cur.fetchone(); conn.close()
        if not row:
            return jsonify({"ok": False, "error": "No encontrado"}), 404
        return jsonify({"ok": True, "datos": fila_a_dict(cur, row)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/seguros", methods=["POST"])
def insertar_seguro():
    d = request.get_json()
    if not d.get("N_CHIP"):
        return jsonify({"ok": False, "error": "Campo requerido: N.º chip"}), 400
    if not d.get("SEGURO_COMPANIA"):
        return jsonify({"ok": False, "error": "Campo requerido: Compañía"}), 400
    if not d.get("SEGURO_POLIZA"):
        return jsonify({"ok": False, "error": "Campo requerido: N.º póliza"}), 400
    try:
        conn = get_conn(); cur = conn.cursor()
        chip_col = detectar_chip(cur)
        cur.execute(
            f"INSERT INTO SEGUROS (`{chip_col}`, SEGURO_COMPANIA, SEGURO_POLIZA) "
            "VALUES (%s, %s, %s)",
            (d["N_CHIP"], d["SEGURO_COMPANIA"], d["SEGURO_POLIZA"])
        )
        conn.commit()
        nuevo_id = cur.lastrowid
        conn.close()
        return jsonify({"ok": True, "mensaje": "Seguro registrado correctamente.", "id": nuevo_id})
    except mysql.connector.IntegrityError as e:
        if e.errno == 1452:
            return jsonify({"ok": False, "error": "El número de chip no existe en el censo de animales."}), 409
        return jsonify({"ok": False, "error": f"Error de integridad: {e}"}), 409
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/seguros/<int:id_seguro>", methods=["DELETE"])
def eliminar_seguro(id_seguro):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM SEGUROS WHERE ID_SEGUROS = %s", (id_seguro,))
        conn.commit(); conn.close()
        return jsonify({"ok": True, "mensaje": "Seguro eliminado."})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    print(f"API conectando a {MARIADB['host']}:{MARIADB['port']}/{MARIADB['database']}")
    app.run(debug=False, host="0.0.0.0", port=5000)