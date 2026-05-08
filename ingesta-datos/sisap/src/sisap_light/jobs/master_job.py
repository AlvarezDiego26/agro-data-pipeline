from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from dataclasses import dataclass
from typing import Callable

from loguru import logger

from sisap_light.config import get_settings
from sisap_light.jobs.ciudades_job import run_full as run_ciudades_full
from sisap_light.jobs.precios_job import run_full as run_precios_full
from sisap_light.jobs.volumen_job import run_full as run_volumen_full
from sisap_light.procesamiento.storage.control import (
    get_control_sync_status,
    sync_pending_control_events,
    sync_pending_control_state,
)
from sisap_light.procesamiento.storage.delta import warm_delta_runtime
from sisap_light.schemas import ModuloSisap


@dataclass(frozen=True)
class ModuleRunSpec:
    scope_name: str
    iter_values_getter: Callable[[], list[str]]
    scope_attr: str
    runner: Callable[[str], str]


def _run_volumen(_: str) -> str:
    return str(run_volumen_full(procedencia_nombre=_))


def _run_precios(_: str) -> str:
    return str(run_precios_full(procedencia_nombre=_))


def _run_ciudades_mayoristas(_: str) -> str:
    return str(run_ciudades_full(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS, region_nombre=_))


def _run_ciudades_minoristas(_: str) -> str:
    return str(run_ciudades_full(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS, region_nombre=_))


def _module_specs() -> dict[str, ModuleRunSpec]:
    settings = get_settings()
    return {
        'volumen': ModuleRunSpec(
            scope_name='procedencia',
            iter_values_getter=lambda: settings.procedencias_resueltas,
            scope_attr='sisap_procedencia_nombre',
            runner=_run_volumen,
        ),
        'precios': ModuleRunSpec(
            scope_name='procedencia',
            iter_values_getter=lambda: settings.procedencias_resueltas,
            scope_attr='sisap_procedencia_nombre',
            runner=_run_precios,
        ),
        'ciudades-mayoristas': ModuleRunSpec(
            scope_name='region',
            iter_values_getter=lambda: settings.regiones_resueltas,
            scope_attr='sisap_region_nombre',
            runner=_run_ciudades_mayoristas,
        ),
        'ciudades-minoristas': ModuleRunSpec(
            scope_name='region',
            iter_values_getter=lambda: settings.regiones_resueltas,
            scope_attr='sisap_region_nombre',
            runner=_run_ciudades_minoristas,
        ),
    }


def _sync_control_queues() -> tuple[dict[str, object], dict[str, object]]:
    control_sync = sync_pending_control_state()
    events_sync = sync_pending_control_events()
    if not control_sync['synced']:
        logger.warning(
            'Se mantiene una cola local de control pendiente: {} registros',
            control_sync['pending_records'],
        )
    if not events_sync['synced']:
        logger.warning(
            'Se mantiene una cola local de eventos de control pendiente: {} registros',
            events_sync['pending_records'],
        )
    return control_sync, events_sync


def _run_module_scope(modulo: str, spec: ModuleRunSpec, pause_seconds: int) -> list[str]:
    settings = get_settings()
    scope_values = spec.iter_values_getter()
    if not scope_values:
        return []

    def run_scope(scope_value: str) -> str:
        logger.info('Ejecutando {} para {}={}', modulo, spec.scope_name, scope_value)
        output = spec.runner(scope_value)
        if pause_seconds:
            time.sleep(pause_seconds)
        return f'{modulo} [{spec.scope_name}={scope_value}] -> {output}'

    if not settings.parallel_enabled or settings.scope_max_workers == 1 or len(scope_values) == 1:
        return [run_scope(scope_value) for scope_value in scope_values]

    resultados: list[str] = []
    workers = min(settings.scope_max_workers, len(scope_values))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='sisap-scope') as executor:
        future_map = {
            executor.submit(run_scope, scope_value): scope_value
            for scope_value in scope_values
        }
        for future in as_completed(future_map):
            scope_value = future_map[future]
            try:
                resultados.append(future.result())
            except Exception as exc:
                logger.exception(
                    'Fallo el bloque {} para {}={}',
                    modulo,
                    spec.scope_name,
                    scope_value,
                )
                resultados.append(
                    f'{modulo} [{spec.scope_name}={scope_value}] -> ERROR: {exc}'
                )
    return resultados


def run_pipeline_main() -> dict[str, object]:
    settings = get_settings()
    resultados: list[str] = []
    pause_seconds = max(settings.sisap_pause_seconds, 0)
    module_specs = _module_specs()

    if settings.delta_enabled:
        warm_delta_runtime()

    _sync_control_queues()

    for modulo in settings.modulos_resueltos:
        spec = module_specs.get(modulo)
        if spec is None:
            raise ValueError(f'Modulo no soportado: {modulo}')
        resultados.extend(_run_module_scope(modulo, spec, pause_seconds))

    final_control_sync, final_events_sync = _sync_control_queues()
    control_status = get_control_sync_status()
    return {
        'modulos': settings.modulos_resueltos,
        'procedencias': settings.procedencias_resueltas,
        'regiones': settings.regiones_resueltas,
        'resultados': resultados,
        'scope_workers': settings.scope_max_workers,
        'shard_workers': settings.shard_max_workers,
        'product_batch_size': settings.product_batch_size,
        'control_sync': final_control_sync,
        'control_events_sync': final_events_sync,
        'control_status': control_status,
    }
