import sys
sys.path.append(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src")

from datetime import date
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
from sisap_light.schemas import SisapQuery, ModuloSisap
from sisap_light.procesamiento.parsers.html_tables import detect_primary_table
from sisap_light.procesamiento.transformers.volumen import build_volumen_frame

query = SisapQuery(
    modulo=ModuloSisap.MAYORISTA_VOLUMEN,
    producto_codigo="0204",
    producto_nombre="Ajo",
    fecha_inicio=date(2016, 1, 1),
    fecha_fin=date(2016, 1, 3),
    procedencia_codigo="",
    mercado_codigo="15011501",
)

extractor = SisapMayoristaExtractor()
html = extractor.fetch_report(query)
table = detect_primary_table(html)
df = build_volumen_frame(table, query)
print("--- DATAFRAME PROCESADO DE AJO ---")
for row in df.to_dicts():
    print(row)
