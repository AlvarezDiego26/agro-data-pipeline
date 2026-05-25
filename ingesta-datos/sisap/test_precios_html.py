import sys
sys.path.append(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src")

from datetime import date
from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
from sisap_light.schemas import SisapQuery, ModuloSisap

query1 = SisapQuery(
    modulo=ModuloSisap.MAYORISTA_PRECIOS,
    producto_codigo="0204",
    producto_nombre="Ajo",
    fecha_inicio=date(2016, 1, 2),
    fecha_fin=date(2016, 1, 3),
    procedencia_codigo="040000",
    procedencia_nombre="Arequipa",
    mercado_codigo="15011501",
    mercado_nombre="Gran mercado mayorista de lima",
)

query2 = SisapQuery(
    modulo=ModuloSisap.MAYORISTA_PRECIOS,
    producto_codigo="0204",
    producto_nombre="Ajo",
    fecha_inicio=date(2016, 1, 2),
    fecha_fin=date(2016, 1, 3),
    procedencia_codigo="",
    procedencia_nombre="",
    mercado_codigo="15011501",
    mercado_nombre="Gran mercado mayorista de lima",
)

extractor = SisapMayoristaExtractor()

print("--- CON PROCEDENCIA (AREQUIPA) ---")
html1 = extractor.fetch_report(query1, variable="precio")
print(html1[:1000])

print("\n--- SIN PROCEDENCIA ---")
html2 = extractor.fetch_report(query2, variable="precio")
print(html2[:1000])
