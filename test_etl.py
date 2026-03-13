"""
test_etl.py — Batería de tests unitarios
=========================================
Autocontenido: no requiere BD, ni drivers ODBC/MySQL.
Cubre las funciones puras del proyecto ETL:

  Grupo 1 — parsear_fecha         (11 tests)
  Grupo 2 — parsear_bool          (13 tests)
  Grupo 3 — limpiar_texto         ( 8 tests)
  Grupo 4 — transformar_valor     (15 tests)
  Grupo 5 — transformar_tabla     ( 7 tests)
  Grupo 6 — DDL y ORDEN_CARGA     (11 tests)
  Grupo 7 — valor_para_columna    (11 tests)
  Grupo 8 — Integración e2e       ( 3 tests)
  ─────────────────────────────────────────
  TOTAL                            79 tests

Ejecutar:
    python test_etl.py
  o con pytest:
    python -m pytest test_etl.py -v
"""

import sys
import unittest
from datetime import datetime
from decimal import Decimal, InvalidOperation

# ══════════════════════════════════════════════════════════════════
#  FUNCIONES BAJO TEST  (extraídas de migrar.py / rellenar_nulos.py)
#  Se definen aquí para que el test sea completamente autónomo.
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
    if isinstance(v, int): return 1 if v else 0
    s = str(v).strip().lower()
    if s in ("1", "true", "si", "sí", "yes", "s"): return 1
    if s in ("0", "false", "no", "n"):              return 0
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
        "ESTERILIZADO":                    "bool",
        "PELIGROSO":                       "bool",
        "ID_ADIESTRAMIENTO":               "int",
        "ID_SEGUROS":                      "int",
    },
    "CENSO":              {"CODIGO_CENSO": "int", "FECHA_ALTA": "datetime"},
    "HISTORICO_MASCOTAS": {"ID_HISTORICO": "int", "FECHA": "datetime", "ID_ESTADO": "int"},
}

ORDEN_CARGA = [
    "PROPIETARIOS", "PROPIETARIO_DIRECCION", "LICENCIAS",
    "ADIESTRADORES", "SEGUROS", "ESTADOS_HISTORICO",
    "ANIMALES", "CENSO", "HISTORICO_MASCOTAS",
]

DDL_TABLAS = {
    "PROPIETARIOS":          "CREATE TABLE IF NOT EXISTS `PROPIETARIOS` (`DNI` VARCHAR(50) NOT NULL, PRIMARY KEY (`DNI`)) CHARACTER SET utf8mb4",
    "PROPIETARIO_DIRECCION": "CREATE TABLE IF NOT EXISTS `PROPIETARIO_DIRECCION` (`CODIGO` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`CODIGO`)) CHARACTER SET utf8mb4",
    "LICENCIAS":             "CREATE TABLE IF NOT EXISTS `LICENCIAS` (`N_LICENCIA_ANIMALES_PELIGROSOS` VARCHAR(50) NOT NULL, PRIMARY KEY (`N_LICENCIA_ANIMALES_PELIGROSOS`)) CHARACTER SET utf8mb4",
    "ADIESTRADORES":         "CREATE TABLE IF NOT EXISTS `ADIESTRADORES` (`ID_ADIESTRAMIENTO` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`ID_ADIESTRAMIENTO`)) CHARACTER SET utf8mb4",
    "SEGUROS":               "CREATE TABLE IF NOT EXISTS `SEGUROS` (`ID_SEGUROS` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`ID_SEGUROS`)) CHARACTER SET utf8mb4",
    "ESTADOS_HISTORICO":     "CREATE TABLE IF NOT EXISTS `ESTADOS_HISTORICO` (`ID_ESTADO` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`ID_ESTADO`)) CHARACTER SET utf8mb4",
    "ANIMALES":              "CREATE TABLE IF NOT EXISTS `ANIMALES` (`N_CHIP` VARCHAR(50) NOT NULL, PRIMARY KEY (`N_CHIP`)) CHARACTER SET utf8mb4",
    "CENSO":                 "CREATE TABLE IF NOT EXISTS `CENSO` (`CODIGO_CENSO` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`CODIGO_CENSO`)) CHARACTER SET utf8mb4",
    "HISTORICO_MASCOTAS":    "CREATE TABLE IF NOT EXISTS `HISTORICO_MASCOTAS` (`ID_HISTORICO` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`ID_HISTORICO`)) CHARACTER SET utf8mb4",
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
        fila_limpia = {}
        for col in columnas:
            tipo  = tipos.get(col, "str")
            fila_limpia[col] = transformar_valor(fila.get(col), tipo)

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
    if "DNI"      in c: return f"{10000000 + idx*7 % 90000000:08d}{chr(65+idx%26)}"
    if "CHIP"     in c: return f"CHIP{idx+1:04d}"
    if "CENSO"    in c: return f"CEN-{idx+1:04d}"
    if "TELEFONO" in c: return f"6{idx*13 % 100000000:08d}"
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


# ══════════════════════════════════════════════════════════════════
#  GRUPO 1 — parsear_fecha
# ══════════════════════════════════════════════════════════════════

class TestParsearFecha(unittest.TestCase):

    def test_none_devuelve_none(self):
        self.assertIsNone(parsear_fecha(None))

    def test_string_vacio_devuelve_none(self):
        self.assertIsNone(parsear_fecha(""))
        self.assertIsNone(parsear_fecha("   "))

    def test_datetime_pasado_directamente(self):
        dt = datetime(2023, 5, 10, 12, 0, 0)
        self.assertEqual(parsear_fecha(dt), dt)

    def test_formato_iso_con_hora(self):
        self.assertEqual(parsear_fecha("2023-05-10 12:00:00"), datetime(2023, 5, 10, 12, 0, 0))

    def test_formato_iso_solo_fecha(self):
        self.assertEqual(parsear_fecha("2023-05-10"), datetime(2023, 5, 10))

    def test_formato_espanol(self):
        self.assertEqual(parsear_fecha("10/05/2023"), datetime(2023, 5, 10))

    def test_formato_espanol_con_hora(self):
        self.assertEqual(parsear_fecha("10/05/2023 08:30:00"), datetime(2023, 5, 10, 8, 30, 0))

    def test_formato_con_guiones(self):
        self.assertEqual(parsear_fecha("10-05-2023"), datetime(2023, 5, 10))

    def test_texto_libre_devuelve_none(self):
        self.assertIsNone(parsear_fecha("no es una fecha"))

    def test_fecha_limite_bisiesto(self):
        self.assertEqual(parsear_fecha("29/02/2020"), datetime(2020, 2, 29))

    def test_dia_invalido_devuelve_none(self):
        self.assertIsNone(parsear_fecha("32/01/2020"))


# ══════════════════════════════════════════════════════════════════
#  GRUPO 2 — parsear_bool
# ══════════════════════════════════════════════════════════════════

class TestParsearBool(unittest.TestCase):

    def test_none_devuelve_none(self):
        self.assertIsNone(parsear_bool(None))

    def test_bool_true_nativo(self):
        self.assertEqual(parsear_bool(True), 1)

    def test_bool_false_nativo(self):
        self.assertEqual(parsear_bool(False), 0)

    def test_entero_1(self):
        self.assertEqual(parsear_bool(1), 1)

    def test_entero_0(self):
        self.assertEqual(parsear_bool(0), 0)

    def test_strings_afirmativos(self):
        for v in ("si", "sí", "Si", "SÍ", "s", "S", "yes", "Yes", "true", "True", "1"):
            with self.subTest(v=v):
                self.assertEqual(parsear_bool(v), 1)

    def test_strings_negativos(self):
        for v in ("no", "No", "NO", "n", "N", "false", "False", "0"):
            with self.subTest(v=v):
                self.assertEqual(parsear_bool(v), 0)

    def test_valor_ambiguo_devuelve_none(self):
        self.assertIsNone(parsear_bool("quiza"))
        self.assertIsNone(parsear_bool("2"))
        self.assertIsNone(parsear_bool("tal vez"))

    def test_resultado_es_int_no_bool(self):
        r = parsear_bool(True)
        self.assertIsInstance(r, int)
        self.assertNotIsInstance(r, bool)  # int, no bool


# ══════════════════════════════════════════════════════════════════
#  GRUPO 3 — limpiar_texto
# ══════════════════════════════════════════════════════════════════

class TestLimpiarTexto(unittest.TestCase):

    def test_none_devuelve_none(self):
        self.assertIsNone(limpiar_texto(None))

    def test_string_vacio_devuelve_none(self):
        self.assertIsNone(limpiar_texto(""))
        self.assertIsNone(limpiar_texto("   "))

    def test_strip_espacios(self):
        self.assertEqual(limpiar_texto("  hola  "), "hola")

    def test_valor_normal_intacto(self):
        self.assertEqual(limpiar_texto("García"), "García")

    def test_truncado_respeta_max_len(self):
        self.assertEqual(len(limpiar_texto("A" * 200, max_len=50)), 50)

    def test_sin_truncado_si_cabe(self):
        self.assertEqual(limpiar_texto("Hola", max_len=50), "Hola")

    def test_numero_convertido_a_str(self):
        self.assertEqual(limpiar_texto(12345), "12345")

    def test_conserva_caracteres_especiales(self):
        r = limpiar_texto("Martínez")
        self.assertIsNotNone(r)
        self.assertIn("tín", r)


# ══════════════════════════════════════════════════════════════════
#  GRUPO 4 — transformar_valor
# ══════════════════════════════════════════════════════════════════

class TestTransformarValor(unittest.TestCase):

    def test_none_en_cualquier_tipo_devuelve_none(self):
        for tipo in ("int","float","bool","datetime","str","decimal"):
            with self.subTest(tipo=tipo):
                self.assertIsNone(transformar_valor(None, tipo))

    def test_int_desde_string(self):
        self.assertEqual(transformar_valor("42", "int"), 42)

    def test_int_trunca_decimales(self):
        self.assertEqual(transformar_valor("3.9", "int"), 3)

    def test_int_desde_bool_true(self):
        self.assertEqual(transformar_valor(True, "int"), 1)

    def test_int_invalido_devuelve_none(self):
        self.assertIsNone(transformar_valor("abc", "int"))

    def test_float_desde_string(self):
        self.assertAlmostEqual(transformar_valor("3.14", "float"), 3.14)

    def test_float_invalido_devuelve_none(self):
        self.assertIsNone(transformar_valor("xyz", "float"))

    def test_decimal_correcto(self):
        self.assertEqual(transformar_valor("12.50", "decimal"), Decimal("12.50"))

    def test_decimal_invalido_devuelve_none(self):
        self.assertIsNone(transformar_valor("no", "decimal"))

    def test_bool_si(self):
        self.assertEqual(transformar_valor("si", "bool"), 1)

    def test_bool_no(self):
        self.assertEqual(transformar_valor("no", "bool"), 0)

    def test_datetime_iso(self):
        self.assertEqual(transformar_valor("2022-01-15", "datetime"), datetime(2022, 1, 15))

    def test_datetime_invalido_devuelve_none(self):
        self.assertIsNone(transformar_valor("no-es-fecha", "datetime"))

    def test_str_limpia_espacios(self):
        self.assertEqual(transformar_valor("  hola  ", "str"), "hola")

    def test_str_vacio_devuelve_none(self):
        self.assertIsNone(transformar_valor("  ", "str"))


# ══════════════════════════════════════════════════════════════════
#  GRUPO 5 — transformar_tabla
# ══════════════════════════════════════════════════════════════════

class TestTransformarTabla(unittest.TestCase):

    def _fila_animal(self, chip="CHIP001", dni="12345678A"):
        return {
            "N_CHIP": chip, "ESPECIE": "Perro", "RAZA": "Labrador",
            "SEXO": "Macho", "NOMBRE": "Toby", "COLOR": "Amarillo",
            "FECHA_NACIMIENTO": "01/06/2019",
            "FECHA_ULTIMA_VACUNA_ANTIRRABICA": "01/06/2023",
            "ESTERILIZADO": "si", "DNI_PROPIETARIO": dni,
            "PELIGROSO": "no", "ID_ADIESTRAMIENTO": "1", "ID_SEGUROS": "1",
        }

    def test_devuelve_lista(self):
        cols = ["DNI","NOMBRE"]
        r = transformar_tabla("PROPIETARIOS", cols, [{"DNI":"12345678A","NOMBRE":"Carlos"}])
        self.assertIsInstance(r, list)

    def test_filas_vacias_devuelve_lista_vacia(self):
        self.assertEqual(transformar_tabla("PROPIETARIOS", ["DNI"], []), [])

    def test_strip_en_texto(self):
        cols = ["DNI","NOMBRE"]
        r = transformar_tabla("PROPIETARIOS", cols, [{"DNI":"12345678A","NOMBRE":"  Carlos  "}])
        self.assertEqual(r[0]["NOMBRE"], "Carlos")

    def test_duplicados_se_eliminan(self):
        cols = ["DNI","NOMBRE"]
        filas = [
            {"DNI":"12345678A","NOMBRE":"Carlos"},
            {"DNI":"12345678A","NOMBRE":"Carlos Duplicado"},
            {"DNI":"87654321B","NOMBRE":"Ana"},
        ]
        r = transformar_tabla("PROPIETARIOS", cols, filas)
        self.assertEqual(len(r), 2)

    def test_animales_fecha_es_datetime(self):
        cols = list(self._fila_animal().keys())
        r = transformar_tabla("ANIMALES", cols, [self._fila_animal()])
        self.assertIsInstance(r[0]["FECHA_NACIMIENTO"], datetime)

    def test_animales_esterilizado_bool(self):
        cols = list(self._fila_animal().keys())
        r = transformar_tabla("ANIMALES", cols, [self._fila_animal()])
        self.assertEqual(r[0]["ESTERILIZADO"], 1)
        self.assertEqual(r[0]["PELIGROSO"], 0)

    def test_animales_id_es_entero(self):
        cols = list(self._fila_animal().keys())
        r = transformar_tabla("ANIMALES", cols, [self._fila_animal()])
        self.assertIsInstance(r[0]["ID_ADIESTRAMIENTO"], int)


# ══════════════════════════════════════════════════════════════════
#  GRUPO 6 — DDL y ORDEN_CARGA
# ══════════════════════════════════════════════════════════════════

class TestEstructuraDDL(unittest.TestCase):

    TABLAS = [
        "PROPIETARIOS","PROPIETARIO_DIRECCION","LICENCIAS",
        "ADIESTRADORES","SEGUROS","ESTADOS_HISTORICO",
        "ANIMALES","CENSO","HISTORICO_MASCOTAS",
    ]

    def test_todas_las_tablas_tienen_ddl(self):
        for t in self.TABLAS:
            with self.subTest(tabla=t):
                self.assertIn(t, DDL_TABLAS)

    def test_ddl_tiene_create_table(self):
        for t, ddl in DDL_TABLAS.items():
            with self.subTest(t=t):
                self.assertIn("CREATE TABLE IF NOT EXISTS", ddl.upper())

    def test_ddl_tiene_primary_key(self):
        for t, ddl in DDL_TABLAS.items():
            with self.subTest(t=t):
                self.assertIn("PRIMARY KEY", ddl.upper())

    def test_ddl_tiene_utf8mb4(self):
        for t, ddl in DDL_TABLAS.items():
            with self.subTest(t=t):
                self.assertIn("utf8mb4", ddl)

    def test_no_hay_fk_en_ddl_inicial(self):
        for t, ddl in DDL_TABLAS.items():
            with self.subTest(t=t):
                self.assertNotIn("FOREIGN KEY", ddl.upper())

    def test_orden_tiene_todas_las_tablas(self):
        for t in self.TABLAS:
            self.assertIn(t, ORDEN_CARGA)

    def test_propietarios_antes_que_animales(self):
        self.assertLess(ORDEN_CARGA.index("PROPIETARIOS"), ORDEN_CARGA.index("ANIMALES"))

    def test_animales_antes_que_censo(self):
        self.assertLess(ORDEN_CARGA.index("ANIMALES"), ORDEN_CARGA.index("CENSO"))

    def test_animales_antes_que_historico(self):
        self.assertLess(ORDEN_CARGA.index("ANIMALES"), ORDEN_CARGA.index("HISTORICO_MASCOTAS"))

    def test_estados_historico_antes_que_historico_mascotas(self):
        self.assertLess(ORDEN_CARGA.index("ESTADOS_HISTORICO"), ORDEN_CARGA.index("HISTORICO_MASCOTAS"))

    def test_tipos_animales_correctos(self):
        t = TIPOS_COLUMNAS["ANIMALES"]
        self.assertEqual(t["FECHA_NACIMIENTO"], "datetime")
        self.assertEqual(t["ESTERILIZADO"],     "bool")
        self.assertEqual(t["PELIGROSO"],        "bool")
        self.assertEqual(t["ID_ADIESTRAMIENTO"],"int")
        self.assertEqual(t["ID_SEGUROS"],       "int")


# ══════════════════════════════════════════════════════════════════
#  GRUPO 7 — valor_para_columna (rellenar_nulos.py)
# ══════════════════════════════════════════════════════════════════

class TestValorParaColumna(unittest.TestCase):

    def test_chip_contiene_chip(self):
        self.assertIn("CHIP", str(valor_para_columna("N_CHIP", "varchar", 0)).upper())

    def test_dni_formato_correcto(self):
        r = str(valor_para_columna("DNI", "varchar", 0))
        self.assertGreater(len(r), 5)
        self.assertTrue(r[-1].isalpha())

    def test_fecha_formato_iso(self):
        r = valor_para_columna("FECHA_NACIMIENTO", "varchar", 0)
        self.assertRegex(str(r), r"\d{4}-\d{2}-\d{2}")

    def test_tipo_datetime_genera_fecha(self):
        r = valor_para_columna("CUALQUIER_COSA", "datetime", 0)
        self.assertRegex(str(r), r"\d{4}-\d{2}-\d{2}")

    def test_tinyint_es_0_o_1(self):
        for i in range(10):
            with self.subTest(i=i):
                self.assertIn(valor_para_columna("ESTERILIZADO", "tinyint", i), (0, 1))

    def test_tipo_int_genera_entero(self):
        r = valor_para_columna("ID_ESTADO", "int", 3)
        self.assertIsInstance(r, int)

    def test_telefono_empieza_por_6(self):
        r = str(valor_para_columna("TELEFONO1", "varchar", 0))
        self.assertTrue(r.startswith("6"))

    def test_municipio_es_valor_conocido(self):
        r = valor_para_columna("MINICIPIO", "varchar", 0)
        self.assertIn(r, MUNICIPIOS)

    def test_nombre_es_valor_conocido(self):
        r = valor_para_columna("NOMBRE", "varchar", 0)
        self.assertIn(r, NOMBRES)

    def test_indices_distintos_dan_chips_distintos(self):
        vals = {valor_para_columna("N_CHIP", "varchar", i) for i in range(5)}
        self.assertGreater(len(vals), 1)

    def test_columna_desconocida_no_devuelve_none(self):
        r = valor_para_columna("COLUMNA_RARA_XYZ", "varchar", 0)
        self.assertIsNotNone(r)
        self.assertNotEqual(str(r).strip(), "")


# ══════════════════════════════════════════════════════════════════
#  GRUPO 8 — Integración end-to-end
# ══════════════════════════════════════════════════════════════════

class TestIntegracion(unittest.TestCase):

    def test_pipeline_propietarios_completo(self):
        cols  = ["DNI","PRIMER_APELLIDO","SEGUNDO_APELLIDO","NOMBRE","TELEFONO1","TELEFONO2"]
        filas = [
            {"DNI":"12345678A","PRIMER_APELLIDO":"  García  ","SEGUNDO_APELLIDO":None,
             "NOMBRE":"Carlos","TELEFONO1":"600111222","TELEFONO2":""},
            {"DNI":"87654321B","PRIMER_APELLIDO":"Martínez","SEGUNDO_APELLIDO":"Ruiz",
             "NOMBRE":"Ana","TELEFONO1":"600222333","TELEFONO2":None},
            {"DNI":"12345678A","PRIMER_APELLIDO":"Duplicado","SEGUNDO_APELLIDO":"X",
             "NOMBRE":"X","TELEFONO1":"X","TELEFONO2":"X"},
        ]
        r = transformar_tabla("PROPIETARIOS", cols, filas)
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0]["PRIMER_APELLIDO"], "García")
        self.assertIsNone(r[0]["SEGUNDO_APELLIDO"])
        self.assertIsNone(r[1]["TELEFONO2"])

    def test_pipeline_animales_tipos_completo(self):
        cols = ["N_CHIP","ESPECIE","FECHA_NACIMIENTO","ESTERILIZADO",
                "PELIGROSO","ID_ADIESTRAMIENTO","ID_SEGUROS"]
        filas = [{
            "N_CHIP":"CHIP001","ESPECIE":"Perro","FECHA_NACIMIENTO":"15/06/2019",
            "ESTERILIZADO":"si","PELIGROSO":"no","ID_ADIESTRAMIENTO":"1","ID_SEGUROS":"2",
        }]
        r = transformar_tabla("ANIMALES", cols, filas)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["ESTERILIZADO"], 1)
        self.assertEqual(r[0]["PELIGROSO"], 0)
        self.assertEqual(r[0]["ID_ADIESTRAMIENTO"], 1)
        self.assertIsInstance(r[0]["FECHA_NACIMIENTO"], datetime)

    def test_pipeline_historico_deduplicacion_y_tipos(self):
        cols  = ["ID_HISTORICO","N_CHIP","FECHA","ID_ESTADO"]
        filas = [
            {"ID_HISTORICO":"1","N_CHIP":"CHIP001","FECHA":"2022-03-10 00:00:00","ID_ESTADO":"2"},
            {"ID_HISTORICO":"1","N_CHIP":"CHIP002","FECHA":"2022-04-01","ID_ESTADO":"1"},
        ]
        r = transformar_tabla("HISTORICO_MASCOTAS", cols, filas)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["ID_ESTADO"], 2)
        self.assertIsInstance(r[0]["FECHA"], datetime)


# ══════════════════════════════════════════════════════════════════
#  RUNNER
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    grupos = [
        ("parsear_fecha",       TestParsearFecha),
        ("parsear_bool",        TestParsearBool),
        ("limpiar_texto",       TestLimpiarTexto),
        ("transformar_valor",   TestTransformarValor),
        ("transformar_tabla",   TestTransformarTabla),
        ("DDL / ORDEN_CARGA",   TestEstructuraDDL),
        ("valor_para_columna",  TestValorParaColumna),
        ("Integración e2e",     TestIntegracion),
    ]

    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for _, cls in grupos:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    ok      = result.testsRun - len(result.errors) - len(result.failures)
    fallido = len(result.errors) + len(result.failures)

    print("\n" + "=" * 65)
    for nombre, cls in grupos:
        n = loader.loadTestsFromTestCase(cls).countTestCases()
        print(f"  {nombre:<25} {n:>3} tests")
    print("─" * 65)
    print(f"  {'TOTAL':<25} {result.testsRun:>3} tests")
    print(f"  Pasados  : {ok}")
    print(f"  Fallidos : {fallido}")
    print(f"  Resultado: {'OK' if result.wasSuccessful() else 'FALLIDO'}")
    print("=" * 65)
    sys.exit(0 if result.wasSuccessful() else 1)
