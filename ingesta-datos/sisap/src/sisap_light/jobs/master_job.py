from __future__ import annotations

import time

from loguru import logger

from sisap_light.config import get_settings
from sisap_light.jobs.ciudades_job import run_full as run_ciudades_full
from sisap_light.jobs.precios_job import run_full as run_precios_full
from sisap_light.jobs.volumen_job import run_full as run_volumen_full
from sisap_light.schemas import ModuloSisap


def run_pipeline_main() -> dict[str, object]:
    settings = get_settings()
    resultados: list[str] = []
    pause_seconds = max(settings.sisap_pause_seconds, 0)

    for modulo in settings.modulos_resueltos:
        if modulo == 'volumen':
            for procedencia in settings.procedencias_resueltas:
                logger.info('Ejecutando volumen para procedencia={}', procedencia)
                settings.sisap_procedencia_nombre = procedencia
                output = run_volumen_full()
                resultados.append(f'volumen -> {output}')
                if pause_seconds:
                    time.sleep(pause_seconds)
        elif modulo == 'precios':
            for procedencia in settings.procedencias_resueltas:
                logger.info('Ejecutando precios para procedencia={}', procedencia)
                settings.sisap_procedencia_nombre = procedencia
                output = run_precios_full()
                resultados.append(f'precios -> {output}')
                if pause_seconds:
                    time.sleep(pause_seconds)
        elif modulo == 'ciudades-mayoristas':
            for region in settings.regiones_resueltas:
                logger.info('Ejecutando ciudades mayoristas para region={}', region)
                settings.sisap_region_nombre = region
                output = run_ciudades_full(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS)
                resultados.append(f'ciudades-mayoristas -> {output}')
                if pause_seconds:
                    time.sleep(pause_seconds)
        elif modulo == 'ciudades-minoristas':
            for region in settings.regiones_resueltas:
                logger.info('Ejecutando ciudades minoristas para region={}', region)
                settings.sisap_region_nombre = region
                output = run_ciudades_full(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS)
                resultados.append(f'ciudades-minoristas -> {output}')
                if pause_seconds:
                    time.sleep(pause_seconds)
        else:
            raise ValueError(f'Modulo no soportado: {modulo}')

    return {
        'modulos': settings.modulos_resueltos,
        'procedencias': settings.procedencias_resueltas,
        'regiones': settings.regiones_resueltas,
        'resultados': resultados,
    }
