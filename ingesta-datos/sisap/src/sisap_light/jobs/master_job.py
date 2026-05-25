from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from dataclasses import dataclass
from typing import Callable

from loguru import logger

from sisap_light.config import get_settings
from sisap_light.jobs.ciudades_job import (
    _expected_columns as ciudades_expected_columns,
    _output_name as ciudades_output_name,
    run_full as run_ciudades_full,
)
from sisap_light.jobs.common import finalize_staged_delta_output, has_staged_delta_output
from sisap_light.jobs.precios_job import (
    EXPECTED_COLUMNS as PRECIOS_EXPECTED_COLUMNS,
    run_full as run_precios_full,
)
from sisap_light.jobs.volumen_job import (
    EXPECTED_COLUMNS as VOLUMEN_EXPECTED_COLUMNS,
    run_full as run_volumen_full,
)
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
    runner: Callable[[str, bool], str]


def _run_volumen(_: str, finalize_delta: bool = True) -> str:
    return str(run_volumen_full(procedencia_nombre=_, finalize_delta=finalize_delta))


def _run_precios(_: str, finalize_delta: bool = True) -> str:
    return str(run_precios_full(procedencia_nombre=_, finalize_delta=finalize_delta))


def _run_ciudades_mayoristas(_: str, finalize_delta: bool = True) -> str:
    return str(
        run_ciudades_full(
            ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS,
            region_nombre=_,
            finalize_delta=finalize_delta,
        )
    )


def _run_ciudades_minoristas(_: str, finalize_delta: bool = True) -> str:
    return str(
        run_ciudades_full(
            ModuloSisap.CIUDADES_PRECIOS_MINORISTAS,
            region_nombre=_,
            finalize_delta=finalize_delta,
        )
    )


def _run_regiones(_: str, finalize_delta: bool = True) -> str:
    mayoristas = str(
        run_ciudades_full(
            ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS,
            region_nombre=_,
            finalize_delta=finalize_delta,
        )
    )
    minoristas = str(
        run_ciudades_full(
            ModuloSisap.CIUDADES_PRECIOS_MINORISTAS,
            region_nombre=_,
            finalize_delta=finalize_delta,
        )
    )
    return f'mayoristas -> {mayoristas} | minoristas -> {minoristas}'


def _finalize_module_delta_outputs(modulo: str) -> None:
    settings = get_settings()
    append_only_backfill = settings.is_backfill and settings.sisap_delta_append_only_backfill

    if modulo == 'volumen' and has_staged_delta_output('volumen_diario_mercado_lima'):
        finalize_staged_delta_output(
            output_name='volumen_diario_mercado_lima',
            expected_columns=VOLUMEN_EXPECTED_COLUMNS,
            sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
            append_only=append_only_backfill,
        )
    elif modulo == 'precios' and has_staged_delta_output('precios_diarios_mercado_lima'):
        finalize_staged_delta_output(
            output_name='precios_diarios_mercado_lima',
            expected_columns=PRECIOS_EXPECTED_COLUMNS,
            sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
            append_only=append_only_backfill,
        )
    elif modulo == 'ciudades-mayoristas':
        output_name = ciudades_output_name(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS)
        if has_staged_delta_output(output_name):
            finalize_staged_delta_output(
                output_name=output_name,
                expected_columns=ciudades_expected_columns(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS),
                sort_columns=['producto_codigo', 'ciudad', 'variedad', 'fecha'],
                append_only=append_only_backfill,
            )
    elif modulo == 'ciudades-minoristas':
        output_name = ciudades_output_name(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS)
        if has_staged_delta_output(output_name):
            finalize_staged_delta_output(
                output_name=output_name,
                expected_columns=ciudades_expected_columns(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS),
                sort_columns=['producto_codigo', 'ciudad', 'variedad', 'fecha'],
                append_only=append_only_backfill,
            )
    elif modulo == 'regiones':
        for ciudades_modulo in (
            ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS,
            ModuloSisap.CIUDADES_PRECIOS_MINORISTAS,
        ):
            output_name = ciudades_output_name(ciudades_modulo)
            if has_staged_delta_output(output_name):
                finalize_staged_delta_output(
                    output_name=output_name,
                    expected_columns=ciudades_expected_columns(ciudades_modulo),
                    sort_columns=['producto_codigo', 'ciudad', 'variedad', 'fecha'],
                    append_only=append_only_backfill,
                )


def _module_specs() -> dict[str, ModuleRunSpec]:
    settings = get_settings()
    return {
        'volumen': ModuleRunSpec(
            scope_name='procedencia',
            iter_values_getter=lambda: (
                ['consolidado'] if settings.sisap_mercado_codigo == '*'
                else settings.procedencias_resueltas
            ),
            scope_attr='sisap_procedencia_nombre',
            runner=_run_volumen,
        ),
        'precios': ModuleRunSpec(
            scope_name='procedencia',
            iter_values_getter=lambda: (
                ['consolidado'] if settings.sisap_mercado_codigo == '*' 
                else settings.procedencias_resueltas
            ),
            scope_attr='sisap_procedencia_nombre',
            runner=_run_precios,
        ),
        'regiones': ModuleRunSpec(
            scope_name='region',
            iter_values_getter=lambda: settings.regiones_resueltas,
            scope_attr='sisap_region_nombre',
            runner=_run_regiones,
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
    settings = get_settings()
    if not settings.sisap_use_control_table:
        return (
            {'synced': True, 'pending_records': 0, 'target': 'control-disabled'},
            {'synced': True, 'pending_records': 0, 'target': 'control-disabled'},
        )

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


def _run_module_scope(modulo: str, spec: ModuleRunSpec, pause_seconds: int) -> tuple[list[str], list[str]]:
    settings = get_settings()
    scope_values = spec.iter_values_getter()
    if not scope_values:
        return [], []

    use_parallel_scopes = (
        settings.delta_enabled
        and settings.parallel_enabled
        and settings.scope_max_workers > 1
        and len(scope_values) > 1
    )
    finalize_delta_per_scope = not use_parallel_scopes

    def run_scope(scope_value: str) -> str:
        logger.info('Ejecutando {} para {}={}', modulo, spec.scope_name, scope_value)
        output = spec.runner(scope_value, finalize_delta=finalize_delta_per_scope)
        if pause_seconds:
            time.sleep(pause_seconds)
        return f'{modulo} [{spec.scope_name}={scope_value}] -> {output}'

    if not settings.parallel_enabled or settings.scope_max_workers == 1 or len(scope_values) == 1:
        resultados: list[str] = []
        errores: list[str] = []
        for scope_value in scope_values:
            try:
                resultados.append(run_scope(scope_value))
            except Exception as exc:
                logger.exception(
                    'Fallo el bloque {} para {}={}',
                    modulo,
                    spec.scope_name,
                    scope_value,
                )
                error_message = f'{modulo} [{spec.scope_name}={scope_value}] -> ERROR: {exc}'
                resultados.append(error_message)
                errores.append(error_message)
        return resultados, errores

    resultados: list[str] = []
    errores: list[str] = []
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
                error_message = f'{modulo} [{spec.scope_name}={scope_value}] -> ERROR: {exc}'
                resultados.append(error_message)
                errores.append(error_message)
    if use_parallel_scopes:
        _finalize_module_delta_outputs(modulo)
    return resultados, errores


def run_pipeline_main() -> dict[str, object]:
    settings = get_settings()
    resultados: list[str] = []
    errores: list[str] = []
    pause_seconds = max(settings.sisap_pause_seconds, 0)
    module_specs = _module_specs()

    if settings.delta_enabled:
        warm_delta_runtime()

    if settings.sisap_use_control_table:
        _sync_control_queues()

    for modulo in settings.modulos_resueltos:
        spec = module_specs.get(modulo)
        if spec is None:
            raise ValueError(f'Modulo no soportado: {modulo}')
        module_results, module_errors = _run_module_scope(modulo, spec, pause_seconds)
        resultados.extend(module_results)
        errores.extend(module_errors)

    if settings.sisap_use_control_table:
        final_control_sync, final_events_sync = _sync_control_queues()
        control_status = get_control_sync_status()
    else:
        final_control_sync = {'synced': True, 'pending_records': 0, 'target': 'control-disabled'}
        final_events_sync = {'synced': True, 'pending_records': 0, 'target': 'control-disabled'}
        control_status = {
            'pending_records': 0,
            'local_records': 0,
            'pending_path': 'control-disabled',
            'local_path': 'control-disabled',
            'pending_event_records': 0,
            'local_event_records': 0,
            'pending_events_path': 'control-disabled',
            'local_events_path': 'control-disabled',
        }
    result = {
        'modulos': settings.modulos_resueltos,
        'procedencias': settings.procedencias_resueltas,
        'mercados': settings.mercados_resueltos,
        'regiones': settings.regiones_resueltas,
        'resultados': resultados,
        'errores': errores,
        'scope_workers': settings.scope_max_workers,
        'shard_workers': settings.shard_max_workers,
        'product_batch_size': settings.product_batch_size,
        'control_sync': final_control_sync,
        'control_events_sync': final_events_sync,
        'control_status': control_status,
    }
    if errores:
        raise RuntimeError(
            'La corrida SISAP finalizo con errores en uno o mas bloques: '
            + ' | '.join(errores)
        )
    return result
