import sys
from pathlib import Path

def replace_in_file(path_str, replacements):
    p = Path(path_str)
    content = p.read_text(encoding='utf-8')
    for old, new in replacements:
        if old not in content:
            print(f"WARNING: Could not find '{old}' in {path_str}")
        content = content.replace(old, new)
    p.write_text(content, encoding='utf-8')

base = Path(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src\sisap_light")

# 1. precios_job.py
precios_job_path = base / "jobs" / "precios_job.py"
replace_in_file(precios_job_path, [
    ("    'variedad',\n    'procedencia',\n    'precio_min',", "    'variedad',\n    'precio_min',"),
    ("sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha']", "sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'fecha']"),
    ("procedencia=procedencia", "procedencia=None"),
])

# 2. precios.py transformer
precios_path = base / "procesamiento" / "transformers" / "precios.py"
replace_in_file(precios_path, [
    ('    "variedad",\n    "procedencia",\n    "procedencia_filtro_codigo",\n    "procedencia_filtro_nombre",', '    "variedad",'),
    ('            pl.lit("").alias("procedencia"),\n            pl.lit(query.procedencia_codigo or "000000").alias("procedencia_filtro_codigo"),\n            pl.lit(query.procedencia_nombre or "TODOS").alias("procedencia_filtro_nombre"),\n', ''),
    ('            pl.lit(query.procedencia_codigo or "").alias("procedencia_filtro_codigo"),\n            pl.lit(query.procedencia_nombre or "").alias("procedencia_filtro_nombre"),\n', ''),
    ('            pl.col(col_map["procedencia"]).str.strip_chars().alias("procedencia") if "procedencia" in col_map else pl.lit("").alias("procedencia"),\n', ''),
])

# 3. master_job.py
master_job_path = base / "jobs" / "master_job.py"
replace_in_file(master_job_path, [
    ("iter_values_getter=lambda: (\n                ['consolidado'] if settings.sisap_mercado_codigo == '*' \n                else settings.procedencias_resueltas\n            ),", "iter_values_getter=lambda: ['consolidado'],"),
])

print("DONE")
