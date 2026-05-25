import sys
sys.path.append(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src")

from datetime import date
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
from sisap_light.schemas import SisapQuery, ModuloSisap
from sisap_light.procesamiento.parsers.html_tables import detect_primary_table

query = SisapQuery(
    modulo=ModuloSisap.MAYORISTA_PRECIOS,
    producto_codigo="0614",
    producto_nombre="Mandarina",
    fecha_inicio=date(2016, 1, 5),
    fecha_fin=date(2016, 1, 7),
    procedencia_codigo="010000",
    procedencia_nombre="Amazonas",
    mercado_codigo="15011503",
    mercado_nombre="Mcdo mod. de frutas",
)

extractor = SisapMayoristaExtractor()
html = extractor.fetch_report(query, variable="precio")
table = detect_primary_table(html)

print("--- RESULTADOS DEL PORTAL PARA MANDARINA DE AMAZONAS ---")
if table:
    for row in table:
        print(row)
else:
    print("El portal devolvio vacio (No existen resultados para los criterios elegidos)")
