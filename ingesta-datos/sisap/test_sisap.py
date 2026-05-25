import sys
sys.path.append(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src")

from datetime import date
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
from sisap_light.schemas import SisapQuery, ModuloSisap

query = SisapQuery(
    modulo=ModuloSisap.MAYORISTA_VOLUMEN,
    producto_codigo="0401",
    producto_nombre="Arroz",
    fecha_inicio=date(2016, 1, 1),
    fecha_fin=date(2016, 1, 5),
    procedencia_codigo="",
    mercado_codigo="15011501",
)

extractor = SisapMayoristaExtractor()
html = extractor.fetch_report(query)
print("HTML LENGTH:", len(html))
print("HTML:", html[:500])
