import sys
sys.path.append(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src")

from datetime import date
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
from sisap_light.schemas import SisapQuery, ModuloSisap
from sisap_light.procesamiento.parsers.html_tables import detect_primary_table
from sisap_light.procesamiento.transformers.volumen import build_volumen_frame

# Testing Esparrago (0216) for Arequipa around Jan 2nd 2016
query = SisapQuery(
    modulo=ModuloSisap.MAYORISTA_VOLUMEN,
    producto_codigo="0216",
    producto_nombre="Esparrago",
    fecha_inicio=date(2016, 1, 1),
    fecha_fin=date(2016, 1, 3),
    procedencia_codigo="040000",
    procedencia_nombre="Arequipa",
    mercado_codigo="15011501",
    mercado_nombre="Gran mercado mayorista de lima",
)

extractor = SisapMayoristaExtractor()
html = extractor.fetch_report(query)
table = detect_primary_table(html)
if table:
    print("Table found for Esparrago:")
    for row in table:
        print(row)
    df = build_volumen_frame(table, query)
    print("\nDataFrame:\n", df)
else:
    print("No data at all for Esparrago in this range")

