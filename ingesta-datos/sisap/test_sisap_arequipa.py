import sys
sys.path.append(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src")

from datetime import date
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
from sisap_light.schemas import SisapQuery, ModuloSisap
from sisap_light.procesamiento.parsers.html_tables import detect_primary_table
from sisap_light.procesamiento.transformers.volumen import build_volumen_frame

# Let's try 2016 for Arequipa just like the user
query = SisapQuery(
    modulo=ModuloSisap.MAYORISTA_VOLUMEN,
    producto_codigo="0401",
    producto_nombre="Arroz",
    fecha_inicio=date(2016, 1, 1),
    fecha_fin=date(2016, 1, 31),
    procedencia_codigo="040000",
    procedencia_nombre="Arequipa",
    mercado_codigo="15011501",
    mercado_nombre="Gran mercado mayorista de lima",
)

extractor = SisapMayoristaExtractor()
html = extractor.fetch_report(query)
print("HTML length:", len(html))
print("First 150 chars:", html[:150])

table = detect_primary_table(html)
print("Extracted rows count:", len(table))
df = build_volumen_frame(table, query)
print(df)
