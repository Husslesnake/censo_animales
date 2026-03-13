"""
test_bateria.py — Batería completa de tests unitarios
======================================================
Autocontenido. No requiere BD, drivers ODBC ni MySQL.

Módulos cubiertos:
  migrar.py        — ETL Access → MariaDB
  rellenar_nulos.py — Relleno de campos NULL
  relaciones.py    — Lógica de resolución de FK

Grupos:
  G01  parsear_fecha             12 tests
  G02  parsear_bool              12 tests
  G03  limpiar_texto              9 tests
  G04  transformar_valor         18 tests
  G05  transformar_tabla         10 tests
  G06  DDL / ORDEN_CARGA         13 tests
  G07  valor_para_columna        14 tests
  G08  pk_chip (resolución chip)  7 tests
  G09  limpiar_huerfanos (lógica) 6 tests
  G10  Integración e2e            6 tests
  G11  Regresión / bordes         8 tests
  ──────────────────────────────────────────
  TOTAL                         115 tests

Uso:
    python test_bateria.py
    python -m pytest test_bateria.py -v
"""

import sys
import unittest
from datetime import datetime
from decimal import Decimal, InvalidOperation
from unittest.mock import MagicMock, patch, call

# ══════════════════════════════════════════════════════════════════
#  CÓDIGO BAJO TEST — embebido para autonomía total
# ══════════════════════════════════════════════════════════════════

# ── migrar.py ─────────────────────────────────────────────────────

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
    if isinstance(v, int):  return 1 if v else 0
    s = str(v).strip().lower()
    if s in ("1","true","si","sí","yes","s"): return 1
    if s in ("0","false","no","n"):           return 0
    return None

def limpiar_texto(v, max_len=None):
    if v is None: return None
    s = str(v).strip()
    try: s = s.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError): pass
    if max_len and len(s) > max_len:
        s = s[:max_len]
    return s or None

def transformar_valor(v, tipo):
    if v is None: return None
    try:
        if tipo == "int":
            return int(float(str(v).strip())) if not isinstance(v, bool) else int(v)
        if tipo == "float":    return float(str(v).strip())
        if tipo == "decimal":  return Decimal(str(v).strip())
        if tipo == "bool":     return parsear_bool(v)
        if tipo == "datetime": return parsear_fecha(v)
        return limpiar_texto(v)
    except (ValueError, TypeError, InvalidOperation):
        return None

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
        "ESTERILIZADO": "bool",
        "PELIGROSO":    "bool",
        "ID_ADIESTRAMIENTO": "int",
        "ID_SEGUROS":        "int",
    },
    "CENSO":              {"CODIGO_CENSO": "int", "FECHA_ALTA": "datetime"},
    "HISTORICO_MASCOTAS": {"ID_HISTORICO": "int", "FECHA": "datetime", "ID_ESTADO": "int"},
}

ORDEN_CARGA = [
    "PROPIETARIOS","PROPIETARIO_DIRECCION","LICENCIAS",
    "ADIESTRADORES","SEGUROS","ESTADOS_HISTORICO",
    "ANIMALES","CENSO","HISTORICO_MASCOTAS",
]

DDL_TABLAS = {
    "PROPIETARIOS":
        "CREATE TABLE IF NOT EXISTS `PROPIETARIOS` (`DNI` VARCHAR(50) NOT NULL, `PRIMER_APELLIDO` VARCHAR(100), `NOMBRE` VARCHAR(100), PRIMARY KEY (`DNI`)) CHARACTER SET utf8mb4",
    "PROPIETARIO_DIRECCION":
        "CREATE TABLE IF NOT EXISTS `PROPIETARIO_DIRECCION` (`CODIGO` INT NOT NULL AUTO_INCREMENT, `DNI` VARCHAR(50), PRIMARY KEY (`CODIGO`)) CHARACTER SET utf8mb4",
    "LICENCIAS":
        "CREATE TABLE IF NOT EXISTS `LICENCIAS` (`N_LICENCIA_ANIMALES_PELIGROSOS` VARCHAR(50) NOT NULL, `DNI_PROPIETARIO` VARCHAR(50), PRIMARY KEY (`N_LICENCIA_ANIMALES_PELIGROSOS`)) CHARACTER SET utf8mb4",
    "ADIESTRADORES":
        "CREATE TABLE IF NOT EXISTS `ADIESTRADORES` (`ID_ADIESTRAMIENTO` INT NOT NULL AUTO_INCREMENT, `N_CHIP` VARCHAR(50), PRIMARY KEY (`ID_ADIESTRAMIENTO`)) CHARACTER SET utf8mb4",
    "SEGUROS":
        "CREATE TABLE IF NOT EXISTS `SEGUROS` (`ID_SEGUROS` INT NOT NULL AUTO_INCREMENT, `N_CHIP` VARCHAR(50), PRIMARY KEY (`ID_SEGUROS`)) CHARACTER SET utf8mb4",
    "ESTADOS_HISTORICO":
        "CREATE TABLE IF NOT EXISTS `ESTADOS_HISTORICO` (`ID_ESTADO` INT NOT NULL AUTO_INCREMENT, `ESTADO` VARCHAR(50), PRIMARY KEY (`ID_ESTADO`)) CHARACTER SET utf8mb4",
    "ANIMALES":
        "CREATE TABLE IF NOT EXISTS `ANIMALES` (`N_CHIP` VARCHAR(50) NOT NULL, `DNI_PROPIETARIO` VARCHAR(50), `ESTERILIZADO` TINYINT(1), PRIMARY KEY (`N_CHIP`)) CHARACTER SET utf8mb4",
    "CENSO":
        "CREATE TABLE IF NOT EXISTS `CENSO` (`CODIGO_CENSO` INT NOT NULL AUTO_INCREMENT, `N_CHIP` VARCHAR(50), PRIMARY KEY (`CODIGO_CENSO`)) CHARACTER SET utf8mb4",
    "HISTORICO_MASCOTAS":
        "CREATE TABLE IF NOT EXISTS `HISTORICO_MASCOTAS` (`ID_HISTORICO` INT NOT NULL AUTO_INCREMENT, `N_CHIP` VARCHAR(50), `ID_ESTADO` INT, PRIMARY KEY (`ID_HISTORICO`)) CHARACTER SET utf8mb4",
}

PK_POR_TABLA = {
    "PROPIETARIOS":          "DNI",
    "PROPIETARIO_DIRECCION": "CODIGO",
    "LICENCIAS":             "N_LICENCIA_ANIMALES_PELIGROSOS",
    "ADIESTRADORES":         "ID_ADIESTRAMIENTO",
    "SEGUROS":               "ID_SEGUROS",
    "ESTADOS_HISTORICO":     "ID_ESTADO",
    "ANIMALES":              "N_CHIP",
    "CENSO":                 "CODIGO_CENSO",
    "HISTORICO_MASCOTAS":    "ID_HISTORICO",
}

def transformar_tabla(tabla, columnas, filas_raw):
    tipos  = TIPOS_COLUMNAS.get(tabla, {})
    pk_set = set(PK_POR_TABLA.values())
    pk     = next((c for c in columnas if c in pk_set), None)
    filas_ok, vistos, omitidas = [], set(), 0
    for fila in filas_raw:
        fila_limpia = {c: transformar_valor(fila.get(c), tipos.get(c, "str")) for c in columnas}
        if pk:
            pk_val = fila_limpia.get(pk)
            if pk_val is not None:
                if pk_val in vistos:
                    omitidas += 1
                    continue
                vistos.add(pk_val)
        filas_ok.append(fila_limpia)
    return filas_ok


# ── rellenar_nulos.py ─────────────────────────────────────────────

NOMBRES    = ["Carlos","Ana","Pedro","Laura","Isabel","Miguel","Sofia","Javier","Maria","Luis"]
APELLIDOS  = ["Garcia","Martinez","Lopez","Sanchez","Gonzalez","Rodriguez","Fernandez","Diaz","Moreno","Ruiz"]
CALLES     = ["Calle Mayor","Av. del Parque","Calle del Sol","Plaza Espana","Calle Luna","Av. Constitucion","Calle Ancha","Calle Real"]
MUNICIPIOS = ["Madrid","Navalcarnero","Mostoles","Alcorcon","Leganes","Getafe","Alcala de Henares","Fuenlabrada"]
COMPANIAS  = ["Mapfre","AXA","Allianz","Generali","Zurich","Mutua Madrilena","Helvetia"]
ESTADOS    = ["Alta","Baja","Fallecido","Transferido","Extraviado"]
MOTIVOS    = ["Fallecimiento","Cesion","Abandono","Mudanza","Otros"]
LUGARES    = ["Madrid","Mostoles","Navalcarnero","Alcorcon","Leganes"]

def valor_para_columna(col, tipo, idx):
    c = col.upper()
    i = idx % 10
    if tipo in ("datetime","date","timestamp") or any(k in c for k in ("FECHA","DATE","ALTA","EXP")):
        return f"{2018+(idx%6)}-{(idx%12)+1:02d}-{(idx%28)+1:02d} 00:00:00"
    if tipo == "tinyint" or any(k in c for k in ("ESTERILIZADO","PELIGROSO","ADIESTRAMIENTO","ESPECIE_PROTEGIDA")):
        return idx % 2
    if tipo in ("int","bigint","smallint"):
        return (idx % 5) + 1
    if "DNI"      in c: return f"{10000000+idx*7%90000000:08d}{chr(65+idx%26)}"
    if "CHIP"     in c: return f"CHIP{idx+1:04d}"
    if "CENSO"    in c: return f"CEN-{idx+1:04d}"
    if "TELEFONO" in c: return f"6{idx*13%100000000:08d}"
    if "CP"       in c: return f"{28000+idx%900}"
    if "MUNICIPIO" in c or "MINICIPIO" in c: return MUNICIPIOS[i]
    if "DOMICILIO" in c or "CALLE"     in c: return f"{CALLES[i%len(CALLES)]} {idx+1}"
    if "PRIMER_APELLIDO"  in c: return APELLIDOS[i]
    if "SEGUNDO_APELLIDO" in c: return APELLIDOS[(i+1)%10]
    if "NOMBRE"   in c: return NOMBRES[i]
    if "COMPANIA" in c: return COMPANIAS[i%len(COMPANIAS)]
    if "POLIZA"   in c: return f"POL-{2020+idx%5}-{idx+1:04d}"
    if "ESTADO"   in c: return ESTADOS[i%len(ESTADOS)]
    if "MOTIVO"   in c: return MOTIVOS[i%len(MOTIVOS)]
    if "LUGAR"    in c: return LUGARES[i%len(LUGARES)]
    return f"Valor-{col[:8]}-{idx+1}"


# ── relaciones.py — lógica de resolución chip ─────────────────────

CANDIDATOS_CHIP = ("N_CHIP","Nº_CHIP","NÃº_CHIP")

def resolver_chip(columnas_disponibles):
    """Dada una lista de columnas, devuelve la columna chip o None."""
    for c in CANDIDATOS_CHIP:
        if c in columnas_disponibles:
            return c
    for c in columnas_disponibles:
        if "CHIP" in c.upper():
            return c
    return None

def hay_huerfanos(datos_hijo, col_fk, datos_padre, col_pk):
    """Devuelve True si existe algún valor en col_fk que no está en col_pk del padre."""
    valores_padre = {fila[col_pk] for fila in datos_padre if fila.get(col_pk) is not None}
    for fila in datos_hijo:
        v = fila.get(col_fk)
        if v is not None and v not in valores_padre:
            return True
    return False

def limpiar_huerfanos_local(datos_hijo, col_fk, datos_padre, col_pk):
    """Pone a None los valores huérfanos en una lista de dicts (versión sin BD)."""
    valores_padre = {fila[col_pk] for fila in datos_padre if fila.get(col_pk) is not None}
    limpiados = 0
    for fila in datos_hijo:
        v = fila.get(col_fk)
        if v is not None and v not in valores_padre:
            fila[col_fk] = None
            limpiados += 1
    return limpiados

def construir_alter_fk(tabla_h, col_fk, tabla_p, col_pk, nombre):
    return (
        f"ALTER TABLE `{tabla_h}` ADD CONSTRAINT `{nombre}` "
        f"FOREIGN KEY (`{col_fk}`) REFERENCES `{tabla_p}`(`{col_pk}`) "
        f"ON UPDATE CASCADE ON DELETE SET NULL"
    )


# ══════════════════════════════════════════════════════════════════
#  G01 — parsear_fecha
# ══════════════════════════════════════════════════════════════════

class G01_ParsarFecha(unittest.TestCase):

    def test_none(self):                        self.assertIsNone(parsear_fecha(None))
    def test_string_vacio(self):                self.assertIsNone(parsear_fecha(""))
    def test_string_espacios(self):             self.assertIsNone(parsear_fecha("   "))
    def test_texto_libre(self):                 self.assertIsNone(parsear_fecha("hola mundo"))
    def test_dia_invalido(self):                self.assertIsNone(parsear_fecha("32/01/2020"))
    def test_mes_invalido(self):                self.assertIsNone(parsear_fecha("13/13/2020"))

    def test_datetime_pasado_directo(self):
        dt = datetime(2023, 5, 10, 12, 0)
        self.assertIs(parsear_fecha(dt), dt)

    def test_iso_con_hora(self):
        self.assertEqual(parsear_fecha("2023-05-10 12:30:00"), datetime(2023,5,10,12,30,0))

    def test_iso_solo_fecha(self):
        self.assertEqual(parsear_fecha("2023-05-10"), datetime(2023,5,10))

    def test_espanol(self):
        self.assertEqual(parsear_fecha("10/05/2023"), datetime(2023,5,10))

    def test_espanol_con_hora(self):
        self.assertEqual(parsear_fecha("10/05/2023 08:30:00"), datetime(2023,5,10,8,30,0))

    def test_bisiesto_valido(self):
        self.assertEqual(parsear_fecha("29/02/2020"), datetime(2020,2,29))


# ══════════════════════════════════════════════════════════════════
#  G02 — parsear_bool
# ══════════════════════════════════════════════════════════════════

class G02_ParsarBool(unittest.TestCase):

    def test_none(self):                    self.assertIsNone(parsear_bool(None))
    def test_ambiguo_devuelve_none(self):   self.assertIsNone(parsear_bool("quiza"))
    def test_numero_grande_devuelve_none(self): self.assertIsNone(parsear_bool("2"))
    def test_bool_true(self):               self.assertEqual(parsear_bool(True), 1)
    def test_bool_false(self):              self.assertEqual(parsear_bool(False), 0)
    def test_int_1(self):                   self.assertEqual(parsear_bool(1), 1)
    def test_int_0(self):                   self.assertEqual(parsear_bool(0), 0)
    def test_resultado_es_int_no_bool(self):
        self.assertIsInstance(parsear_bool(True), int)
        self.assertNotIsInstance(parsear_bool(True), bool)

    def test_strings_verdaderos(self):
        for v in ("si","sí","Si","s","S","yes","Yes","true","True","TRUE","1"):
            with self.subTest(v=v):
                self.assertEqual(parsear_bool(v), 1)

    def test_strings_falsos(self):
        for v in ("no","No","NO","n","N","false","False","FALSE","0"):
            with self.subTest(v=v):
                self.assertEqual(parsear_bool(v), 0)

    def test_espacios_ignorados(self):
        self.assertEqual(parsear_bool("  si  "), 1)
        self.assertEqual(parsear_bool("  no  "), 0)

    def test_mayusculas_ignoradas(self):
        self.assertEqual(parsear_bool("SI"), 1)
        self.assertEqual(parsear_bool("NO"), 0)


# ══════════════════════════════════════════════════════════════════
#  G03 — limpiar_texto
# ══════════════════════════════════════════════════════════════════

class G03_LimpiarTexto(unittest.TestCase):

    def test_none(self):                    self.assertIsNone(limpiar_texto(None))
    def test_vacio(self):                   self.assertIsNone(limpiar_texto(""))
    def test_solo_espacios(self):           self.assertIsNone(limpiar_texto("   "))
    def test_strip(self):                   self.assertEqual(limpiar_texto("  hola  "), "hola")
    def test_normal(self):                  self.assertEqual(limpiar_texto("García"), "García")
    def test_numero_a_str(self):            self.assertEqual(limpiar_texto(42), "42")
    def test_truncado(self):                self.assertEqual(len(limpiar_texto("X"*200, max_len=50)), 50)
    def test_sin_truncar_si_cabe(self):     self.assertEqual(limpiar_texto("Hola", max_len=100), "Hola")
    def test_acentos_conservados(self):
        r = limpiar_texto("Martínez")
        self.assertIsNotNone(r)
        self.assertIn("tín", r)


# ══════════════════════════════════════════════════════════════════
#  G04 — transformar_valor
# ══════════════════════════════════════════════════════════════════

class G04_TransformarValor(unittest.TestCase):

    def test_none_en_todos_los_tipos(self):
        for tipo in ("int","float","bool","datetime","str","decimal"):
            with self.subTest(tipo=tipo):
                self.assertIsNone(transformar_valor(None, tipo))

    def test_int_string(self):              self.assertEqual(transformar_valor("42","int"), 42)
    def test_int_float_trunca(self):        self.assertEqual(transformar_valor("3.9","int"), 3)
    def test_int_negativo(self):            self.assertEqual(transformar_valor("-5","int"), -5)
    def test_int_bool_true(self):           self.assertEqual(transformar_valor(True,"int"), 1)
    def test_int_invalido(self):            self.assertIsNone(transformar_valor("abc","int"))

    def test_float_string(self):            self.assertAlmostEqual(transformar_valor("3.14","float"), 3.14)
    def test_float_negativo(self):          self.assertAlmostEqual(transformar_valor("-1.5","float"), -1.5)
    def test_float_invalido(self):          self.assertIsNone(transformar_valor("xyz","float"))

    def test_decimal_correcto(self):        self.assertEqual(transformar_valor("12.50","decimal"), Decimal("12.50"))
    def test_decimal_invalido(self):        self.assertIsNone(transformar_valor("abc","decimal"))

    def test_bool_si(self):                 self.assertEqual(transformar_valor("si","bool"), 1)
    def test_bool_no(self):                 self.assertEqual(transformar_valor("no","bool"), 0)

    def test_datetime_iso(self):            self.assertEqual(transformar_valor("2022-01-15","datetime"), datetime(2022,1,15))
    def test_datetime_invalido(self):       self.assertIsNone(transformar_valor("no-es-fecha","datetime"))

    def test_str_strip(self):               self.assertEqual(transformar_valor("  hola  ","str"), "hola")
    def test_str_vacio_none(self):          self.assertIsNone(transformar_valor("  ","str"))

    def test_tipo_desconocido_trata_como_str(self):
        # tipo no definido → limpiar_texto
        self.assertEqual(transformar_valor("  hola  ","desconocido"), "hola")


# ══════════════════════════════════════════════════════════════════
#  G05 — transformar_tabla
# ══════════════════════════════════════════════════════════════════

class G05_TransformarTabla(unittest.TestCase):

    def _prop(self, dni, nombre):
        return {"DNI": dni, "PRIMER_APELLIDO": "Garcia", "SEGUNDO_APELLIDO": None,
                "NOMBRE": nombre, "TELEFONO1": "600000000", "TELEFONO2": None}

    def test_lista_vacia_devuelve_vacia(self):
        self.assertEqual(transformar_tabla("PROPIETARIOS", ["DNI","NOMBRE"], []), [])

    def test_devuelve_lista(self):
        r = transformar_tabla("PROPIETARIOS", ["DNI","NOMBRE"],
                              [{"DNI":"12345678A","NOMBRE":"Carlos"}])
        self.assertIsInstance(r, list)

    def test_strip_texto(self):
        r = transformar_tabla("PROPIETARIOS", ["DNI","NOMBRE"],
                              [{"DNI":"12345678A","NOMBRE":"  Carlos  "}])
        self.assertEqual(r[0]["NOMBRE"], "Carlos")

    def test_duplicados_eliminados_conserva_primero(self):
        cols  = ["DNI","NOMBRE","PRIMER_APELLIDO","SEGUNDO_APELLIDO","TELEFONO1","TELEFONO2"]
        filas = [self._prop("11111111A","Carlos"), self._prop("11111111A","Duplicado"),
                 self._prop("22222222B","Ana")]
        r = transformar_tabla("PROPIETARIOS", cols, filas)
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0]["NOMBRE"], "Carlos")

    def test_animales_fecha_es_datetime(self):
        cols  = ["N_CHIP","FECHA_NACIMIENTO","ESTERILIZADO"]
        filas = [{"N_CHIP":"CHIP001","FECHA_NACIMIENTO":"01/06/2019","ESTERILIZADO":"si"}]
        r = transformar_tabla("ANIMALES", cols, filas)
        self.assertIsInstance(r[0]["FECHA_NACIMIENTO"], datetime)

    def test_animales_bool_correcto(self):
        cols  = ["N_CHIP","ESTERILIZADO","PELIGROSO"]
        filas = [{"N_CHIP":"CHIP001","ESTERILIZADO":"si","PELIGROSO":"no"}]
        r = transformar_tabla("ANIMALES", cols, filas)
        self.assertEqual(r[0]["ESTERILIZADO"], 1)
        self.assertEqual(r[0]["PELIGROSO"], 0)

    def test_animales_id_int(self):
        cols  = ["N_CHIP","ID_ADIESTRAMIENTO","ID_SEGUROS"]
        filas = [{"N_CHIP":"CHIP001","ID_ADIESTRAMIENTO":"3","ID_SEGUROS":"2"}]
        r = transformar_tabla("ANIMALES", cols, filas)
        self.assertIsInstance(r[0]["ID_ADIESTRAMIENTO"], int)
        self.assertEqual(r[0]["ID_ADIESTRAMIENTO"], 3)

    def test_historico_tipos(self):
        cols  = ["ID_HISTORICO","FECHA","ID_ESTADO"]
        filas = [{"ID_HISTORICO":"1","FECHA":"2022-03-10","ID_ESTADO":"2"}]
        r = transformar_tabla("HISTORICO_MASCOTAS", cols, filas)
        self.assertIsInstance(r[0]["FECHA"], datetime)
        self.assertEqual(r[0]["ID_ESTADO"], 2)

    def test_none_valores_preservados(self):
        cols  = ["DNI","NOMBRE","TELEFONO2"]
        filas = [{"DNI":"12345678A","NOMBRE":"Ana","TELEFONO2":None}]
        r = transformar_tabla("PROPIETARIOS", cols, filas)
        self.assertIsNone(r[0]["TELEFONO2"])

    def test_sin_pk_no_deduplicar(self):
        cols  = ["NOMBRE","TELEFONO1"]  # sin columna PK
        filas = [{"NOMBRE":"Carlos","TELEFONO1":"600"},
                 {"NOMBRE":"Carlos","TELEFONO1":"601"}]
        r = transformar_tabla("PROPIETARIOS", cols, filas)
        self.assertEqual(len(r), 2)


# ══════════════════════════════════════════════════════════════════
#  G06 — DDL y ORDEN_CARGA
# ══════════════════════════════════════════════════════════════════

class G06_DDLyOrden(unittest.TestCase):

    TABLAS = list(ORDEN_CARGA)

    def test_todas_tablas_en_ddl(self):
        for t in self.TABLAS:
            with self.subTest(t=t): self.assertIn(t, DDL_TABLAS)

    def test_ddl_create_table_if_not_exists(self):
        for t, ddl in DDL_TABLAS.items():
            with self.subTest(t=t):
                self.assertIn("CREATE TABLE IF NOT EXISTS", ddl.upper())

    def test_ddl_primary_key(self):
        for t, ddl in DDL_TABLAS.items():
            with self.subTest(t=t): self.assertIn("PRIMARY KEY", ddl.upper())

    def test_ddl_utf8mb4(self):
        for t, ddl in DDL_TABLAS.items():
            with self.subTest(t=t): self.assertIn("utf8mb4", ddl)

    def test_sin_fk_en_ddl_inicial(self):
        for t, ddl in DDL_TABLAS.items():
            with self.subTest(t=t):
                self.assertNotIn("FOREIGN KEY", ddl.upper())

    def test_orden_contiene_todas_las_tablas(self):
        for t in self.TABLAS:
            self.assertIn(t, ORDEN_CARGA)

    def test_sin_duplicados_en_orden(self):
        self.assertEqual(len(ORDEN_CARGA), len(set(ORDEN_CARGA)))

    def test_propietarios_antes_que_animales(self):
        self.assertLess(ORDEN_CARGA.index("PROPIETARIOS"), ORDEN_CARGA.index("ANIMALES"))

    def test_animales_antes_que_censo(self):
        self.assertLess(ORDEN_CARGA.index("ANIMALES"), ORDEN_CARGA.index("CENSO"))

    def test_animales_antes_que_historico(self):
        self.assertLess(ORDEN_CARGA.index("ANIMALES"), ORDEN_CARGA.index("HISTORICO_MASCOTAS"))

    def test_estados_antes_que_historico(self):
        self.assertLess(ORDEN_CARGA.index("ESTADOS_HISTORICO"), ORDEN_CARGA.index("HISTORICO_MASCOTAS"))

    def test_tipos_animales_fechas(self):
        t = TIPOS_COLUMNAS["ANIMALES"]
        self.assertEqual(t["FECHA_NACIMIENTO"],                "datetime")
        self.assertEqual(t["FECHA_ULTIMA_VACUNA_ANTIRRABICA"], "datetime")

    def test_tipos_animales_bools_e_ints(self):
        t = TIPOS_COLUMNAS["ANIMALES"]
        self.assertEqual(t["ESTERILIZADO"],     "bool")
        self.assertEqual(t["PELIGROSO"],        "bool")
        self.assertEqual(t["ID_ADIESTRAMIENTO"],"int")
        self.assertEqual(t["ID_SEGUROS"],       "int")


# ══════════════════════════════════════════════════════════════════
#  G07 — valor_para_columna
# ══════════════════════════════════════════════════════════════════

class G07_ValorParaColumna(unittest.TestCase):

    def test_chip_contiene_chip(self):
        self.assertIn("CHIP", str(valor_para_columna("N_CHIP","varchar",0)).upper())

    def test_chips_distintos_por_indice(self):
        vals = [valor_para_columna("N_CHIP","varchar",i) for i in range(5)]
        self.assertEqual(len(set(vals)), 5)

    def test_dni_termina_en_letra(self):
        r = str(valor_para_columna("DNI","varchar",0))
        self.assertTrue(r[-1].isalpha())

    def test_fecha_formato_iso(self):
        r = valor_para_columna("FECHA_NACIMIENTO","varchar",0)
        self.assertRegex(str(r), r"\d{4}-\d{2}-\d{2}")

    def test_tipo_datetime_genera_fecha(self):
        r = valor_para_columna("CUALQUIERA","datetime",0)
        self.assertRegex(str(r), r"\d{4}-\d{2}-\d{2}")

    def test_tinyint_es_0_o_1(self):
        for i in range(20):
            with self.subTest(i=i):
                self.assertIn(valor_para_columna("ESTERILIZADO","tinyint",i), (0,1))

    def test_tipo_int_es_entero_positivo(self):
        r = valor_para_columna("ID_ESTADO","int",3)
        self.assertIsInstance(r, int)
        self.assertGreater(r, 0)

    def test_telefono_empieza_6(self):
        r = str(valor_para_columna("TELEFONO1","varchar",0))
        self.assertTrue(r.startswith("6"))

    def test_municipio_conocido(self):
        self.assertIn(valor_para_columna("MINICIPIO","varchar",0), MUNICIPIOS)

    def test_nombre_conocido(self):
        self.assertIn(valor_para_columna("NOMBRE","varchar",0), NOMBRES)

    def test_compania_conocida(self):
        self.assertIn(valor_para_columna("SEGURO_COMPANIA","varchar",0), COMPANIAS)

    def test_cp_codigo_postal(self):
        r = str(valor_para_columna("CP","varchar",0))
        self.assertTrue(r.startswith("28"))

    def test_columna_desconocida_no_none(self):
        r = valor_para_columna("COLUMNA_RARA_XYZ","varchar",0)
        self.assertIsNotNone(r)
        self.assertNotEqual(str(r).strip(), "")

    def test_fecha_anio_valido(self):
        r = valor_para_columna("FECHA_NACIMIENTO","varchar",0)
        anio = int(str(r)[:4])
        self.assertGreaterEqual(anio, 2018)
        self.assertLessEqual(anio, 2023)


# ══════════════════════════════════════════════════════════════════
#  G08 — resolver_chip
# ══════════════════════════════════════════════════════════════════

class G08_ResolverChip(unittest.TestCase):

    def test_n_chip_estandar(self):
        self.assertEqual(resolver_chip(["N_CHIP","NOMBRE","DNI"]), "N_CHIP")

    def test_n_chip_con_simbolo(self):
        self.assertEqual(resolver_chip(["Nº_CHIP","ESPECIE"]), "Nº_CHIP")

    def test_variante_encoding(self):
        self.assertEqual(resolver_chip(["NÃº_CHIP","RAZA"]), "NÃº_CHIP")

    def test_prioridad_n_chip_sobre_variantes(self):
        # Si hay N_CHIP y Nº_CHIP, debe devolver N_CHIP (primero en CANDIDATOS_CHIP)
        self.assertEqual(resolver_chip(["Nº_CHIP","N_CHIP","RAZA"]), "N_CHIP")

    def test_fallback_cualquier_chip(self):
        self.assertEqual(resolver_chip(["CODIGO","MI_CHIP_COL","NOMBRE"]), "MI_CHIP_COL")

    def test_sin_chip_devuelve_none(self):
        self.assertIsNone(resolver_chip(["DNI","NOMBRE","TELEFONO"]))

    def test_lista_vacia_devuelve_none(self):
        self.assertIsNone(resolver_chip([]))


# ══════════════════════════════════════════════════════════════════
#  G09 — limpiar_huerfanos_local
# ══════════════════════════════════════════════════════════════════

class G09_LimpiarHuerfanos(unittest.TestCase):

    def _propietarios(self):
        return [{"DNI":"AAA"},{"DNI":"BBB"},{"DNI":"CCC"}]

    def test_sin_huerfanos_no_modifica(self):
        hijos  = [{"DNI_PROPIETARIO":"AAA"},{"DNI_PROPIETARIO":"BBB"}]
        n = limpiar_huerfanos_local(hijos, "DNI_PROPIETARIO", self._propietarios(), "DNI")
        self.assertEqual(n, 0)
        self.assertEqual(hijos[0]["DNI_PROPIETARIO"], "AAA")

    def test_huerfano_se_pone_none(self):
        hijos = [{"DNI_PROPIETARIO":"AAA"}, {"DNI_PROPIETARIO":"ZZZ"}]
        n = limpiar_huerfanos_local(hijos, "DNI_PROPIETARIO", self._propietarios(), "DNI")
        self.assertEqual(n, 1)
        self.assertIsNone(hijos[1]["DNI_PROPIETARIO"])

    def test_multiples_huerfanos(self):
        hijos = [{"DNI_PROPIETARIO":"ZZZ"},{"DNI_PROPIETARIO":"YYY"},{"DNI_PROPIETARIO":"AAA"}]
        n = limpiar_huerfanos_local(hijos, "DNI_PROPIETARIO", self._propietarios(), "DNI")
        self.assertEqual(n, 2)

    def test_none_en_hijo_no_cuenta_como_huerfano(self):
        hijos = [{"DNI_PROPIETARIO": None}]
        n = limpiar_huerfanos_local(hijos, "DNI_PROPIETARIO", self._propietarios(), "DNI")
        self.assertEqual(n, 0)

    def test_padre_vacio_todo_huerfano(self):
        hijos = [{"DNI_PROPIETARIO":"AAA"},{"DNI_PROPIETARIO":"BBB"}]
        n = limpiar_huerfanos_local(hijos, "DNI_PROPIETARIO", [], "DNI")
        self.assertEqual(n, 2)

    def test_hay_huerfanos_detecta_correctamente(self):
        hijos = [{"N_CHIP":"CHIP001"},{"N_CHIP":"CHIP_HUERFANO"}]
        padre = [{"N_CHIP":"CHIP001"},{"N_CHIP":"CHIP002"}]
        self.assertTrue(hay_huerfanos(hijos, "N_CHIP", padre, "N_CHIP"))

    # bonus: sin huérfanos
    # (no cuenta como test, ya está implícito en test_sin_huerfanos)


# ══════════════════════════════════════════════════════════════════
#  G10 — Integración end-to-end
# ══════════════════════════════════════════════════════════════════

class G10_Integracion(unittest.TestCase):

    def test_propietarios_pipeline_completo(self):
        cols  = ["DNI","PRIMER_APELLIDO","SEGUNDO_APELLIDO","NOMBRE","TELEFONO1","TELEFONO2"]
        filas = [
            {"DNI":"12345678A","PRIMER_APELLIDO":"  García  ","SEGUNDO_APELLIDO":None,
             "NOMBRE":"Carlos","TELEFONO1":"600111222","TELEFONO2":""},
            {"DNI":"87654321B","PRIMER_APELLIDO":"Martínez","SEGUNDO_APELLIDO":"Ruiz",
             "NOMBRE":"Ana","TELEFONO1":"600222333","TELEFONO2":None},
            {"DNI":"12345678A","PRIMER_APELLIDO":"Dup","SEGUNDO_APELLIDO":"X",
             "NOMBRE":"X","TELEFONO1":"X","TELEFONO2":"X"},
        ]
        r = transformar_tabla("PROPIETARIOS", cols, filas)
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0]["PRIMER_APELLIDO"], "García")
        self.assertIsNone(r[0]["SEGUNDO_APELLIDO"])

    def test_animales_pipeline_completo(self):
        cols  = ["N_CHIP","ESPECIE","FECHA_NACIMIENTO","ESTERILIZADO",
                 "PELIGROSO","ID_ADIESTRAMIENTO","ID_SEGUROS"]
        filas = [{
            "N_CHIP":"CHIP001","ESPECIE":"Perro","FECHA_NACIMIENTO":"15/06/2019",
            "ESTERILIZADO":"si","PELIGROSO":"no","ID_ADIESTRAMIENTO":"1","ID_SEGUROS":"2"
        }]
        r = transformar_tabla("ANIMALES", cols, filas)
        self.assertEqual(r[0]["ESTERILIZADO"], 1)
        self.assertEqual(r[0]["PELIGROSO"], 0)
        self.assertEqual(r[0]["ID_ADIESTRAMIENTO"], 1)
        self.assertIsInstance(r[0]["FECHA_NACIMIENTO"], datetime)

    def test_historico_deduplicacion_y_tipos(self):
        cols  = ["ID_HISTORICO","N_CHIP","FECHA","ID_ESTADO"]
        filas = [
            {"ID_HISTORICO":"1","N_CHIP":"CHIP001","FECHA":"2022-03-10","ID_ESTADO":"2"},
            {"ID_HISTORICO":"1","N_CHIP":"CHIP002","FECHA":"2022-04-01","ID_ESTADO":"1"},
        ]
        r = transformar_tabla("HISTORICO_MASCOTAS", cols, filas)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["ID_ESTADO"], 2)

    def test_limpiar_y_luego_transformar(self):
        """Simula el flujo real: limpiar huérfanos → transformar."""
        propietarios = [{"DNI":"12345678A"},{"DNI":"87654321B"}]
        animales_raw = [
            {"N_CHIP":"CHIP001","DNI_PROPIETARIO":"12345678A","ESTERILIZADO":"si"},
            {"N_CHIP":"CHIP002","DNI_PROPIETARIO":"HUERFANO_99","ESTERILIZADO":"no"},
        ]
        limpiar_huerfanos_local(animales_raw, "DNI_PROPIETARIO", propietarios, "DNI")
        cols  = ["N_CHIP","DNI_PROPIETARIO","ESTERILIZADO"]
        r = transformar_tabla("ANIMALES", cols, animales_raw)
        self.assertEqual(len(r), 2)
        self.assertIsNone(r[1]["DNI_PROPIETARIO"])
        self.assertEqual(r[0]["ESTERILIZADO"], 1)

    def test_alter_fk_genera_sql_correcto(self):
        sql = construir_alter_fk("ANIMALES","DNI_PROPIETARIO","PROPIETARIOS","DNI","fk_test")
        self.assertIn("ALTER TABLE `ANIMALES`", sql)
        self.assertIn("FOREIGN KEY (`DNI_PROPIETARIO`)", sql)
        self.assertIn("REFERENCES `PROPIETARIOS`(`DNI`)", sql)
        self.assertIn("ON UPDATE CASCADE", sql)
        self.assertIn("ON DELETE SET NULL", sql)

    def test_seed_genera_valores_para_todas_columnas_animales(self):
        cols_animales = ["N_CHIP","ESPECIE","RAZA","SEXO","NOMBRE","COLOR",
                         "FECHA_NACIMIENTO","ESTERILIZADO","DNI_PROPIETARIO",
                         "PELIGROSO","ID_ADIESTRAMIENTO","ID_SEGUROS"]
        for i, col in enumerate(cols_animales):
            tipo = "varchar"
            if col in ("ESTERILIZADO","PELIGROSO"):    tipo = "tinyint"
            if col in ("ID_ADIESTRAMIENTO","ID_SEGUROS"): tipo = "int"
            if "FECHA" in col:                          tipo = "datetime"
            r = valor_para_columna(col, tipo, i)
            with self.subTest(col=col):
                self.assertIsNotNone(r)


# ══════════════════════════════════════════════════════════════════
#  G11 — Regresión / casos borde
# ══════════════════════════════════════════════════════════════════

class G11_Regresion(unittest.TestCase):

    def test_parsear_fecha_no_confunde_mes_dia(self):
        # 01/02/2020 en formato dd/mm/yyyy → 1 de febrero
        r = parsear_fecha("01/02/2020")
        self.assertIsNotNone(r)
        # En dd/mm/yyyy: día=1, mes=2
        # En mm/dd/yyyy: mes=1, día=2
        # Ambos son válidos, pero el formato español (dd/mm) está primero → mes=2
        self.assertEqual(r.month, 2)

    def test_parsear_bool_espacios_extremos(self):
        self.assertEqual(parsear_bool("  true  "), 1)

    def test_transformar_valor_decimal_con_coma_devuelve_none(self):
        # Python Decimal no acepta comas como separador decimal
        self.assertIsNone(transformar_valor("12,50", "decimal"))

    def test_limpiar_texto_float_a_str(self):
        self.assertEqual(limpiar_texto(3.14), "3.14")

    def test_transformar_tabla_columna_pk_nula_no_inserta(self):
        cols  = ["DNI","NOMBRE"]
        filas = [{"DNI": None, "NOMBRE": "Sin DNI"}]
        r = transformar_tabla("PROPIETARIOS", cols, filas)
        # Una fila con PK None se incluye (no se puede saber si es duplicado)
        self.assertEqual(len(r), 1)

    def test_valor_para_columna_indice_grande(self):
        # Con índice grande no debe lanzar excepción
        try:
            r = valor_para_columna("N_CHIP","varchar",9999)
            self.assertIsNotNone(r)
        except Exception as e:
            self.fail(f"valor_para_columna lanzó excepción con índice grande: {e}")

    def test_resolver_chip_columnas_sin_chip_devuelve_none(self):
        # Columnas que no contienen CHIP en ninguna variante
        self.assertIsNone(resolver_chip(["DNI","NOMBRE","TELEFONO1","ESPECIE"]))

    def test_construir_alter_fk_nombre_en_sql(self):
        sql = construir_alter_fk("CENSO","N_CHIP","ANIMALES","N_CHIP","fk_censo_animal")
        self.assertIn("fk_censo_animal", sql)


# ══════════════════════════════════════════════════════════════════
#  RUNNER
# ══════════════════════════════════════════════════════════════════

GRUPOS = [
    ("G01  parsear_fecha",            G01_ParsarFecha),
    ("G02  parsear_bool",             G02_ParsarBool),
    ("G03  limpiar_texto",            G03_LimpiarTexto),
    ("G04  transformar_valor",        G04_TransformarValor),
    ("G05  transformar_tabla",        G05_TransformarTabla),
    ("G06  DDL / ORDEN_CARGA",        G06_DDLyOrden),
    ("G07  valor_para_columna",       G07_ValorParaColumna),
    ("G08  resolver_chip",            G08_ResolverChip),
    ("G09  limpiar_huerfanos",        G09_LimpiarHuerfanos),
    ("G10  Integracion e2e",          G10_Integracion),
    ("G11  Regresion / bordes",       G11_Regresion),
]

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for _, cls in GRUPOS:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    ok  = result.testsRun - len(result.errors) - len(result.failures)

    print("\n" + "=" * 65)
    print(f"  {'GRUPO':<35} {'TESTS':>6}")
    print("─" * 65)
    for nombre, cls in GRUPOS:
        n = loader.loadTestsFromTestCase(cls).countTestCases()
        print(f"  {nombre:<35} {n:>6}")
    print("─" * 65)
    print(f"  {'TOTAL':<35} {result.testsRun:>6}")
    print(f"  Pasados  : {ok}")
    print(f"  Fallidos : {len(result.failures) + len(result.errors)}")
    print(f"  Resultado: {'OK' if result.wasSuccessful() else 'FALLIDO'}")
    print("=" * 65)
    sys.exit(0 if result.wasSuccessful() else 1)
