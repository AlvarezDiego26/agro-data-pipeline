import sys
sys.path.append(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src")

from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor

extractor = SisapMayoristaExtractor()
html = extractor.fetch_productos_por_mercado_html("15011501")
print(html[:2000])
