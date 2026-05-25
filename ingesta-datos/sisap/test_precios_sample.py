import sys
sys.path.append(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\src")

from sisap_light.jobs.precios_job import run_sample
from sisap_light.config import get_settings

get_settings().sisap_procedencia_codigo = ""
get_settings().sisap_procedencia_nombre = ""
print(run_sample("Gran mercado mayorista de lima"))
