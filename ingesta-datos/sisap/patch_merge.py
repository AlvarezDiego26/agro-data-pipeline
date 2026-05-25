import sys
from pathlib import Path

path_str = r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src\sisap_light\procesamiento\storage\merge.py"
p = Path(path_str)
content = p.read_text(encoding='utf-8')

old = """DATASET_BUSINESS_KEYS = {
    "precios_diarios_mercado_lima": [
        "fecha",
        "mercado_codigo",
        "producto_codigo",
        "variedad",
        "procedencia",
    ],"""

new = """DATASET_BUSINESS_KEYS = {
    "precios_diarios_mercado_lima": [
        "fecha",
        "mercado_codigo",
        "producto_codigo",
        "variedad",
    ],"""

content = content.replace(old, new)
p.write_text(content, encoding='utf-8')
print("Patched merge.py")
