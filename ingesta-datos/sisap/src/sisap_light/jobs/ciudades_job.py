from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_ciudades import SisapCiudadesExtractor
from sisap_light.jobs.common import (
    append_partitioned_output,
    build_control_event_row,
    build_scope_output_dir,
    filter_plan,
    init_control_states,
    persist_control_events_batch,
    persist_control_states,
    register_control_failure,
    register_control_query,
    register_control_success,
    resolve_item,
    resolve_productos,
    resolve_query_dates,
)
from sisap_light.jobs.parallel import build_grouped_shards, run_shards
from sisap_light.ingesta_datos.planners import build_ciudades_queries
from sisap_light.schemas import ModuloSisap, SisapQuery
from sisap_light.procesamiento.storage.parquet import save_parquet
from sisap_light.procesamiento.storage.raw import save_html_snapshot
from sisap_light.procesamiento.transformers.ciudades import build_ciudades_metric_frame, merge_ciudades_metrics
from sisap_light.procesamiento.limpieza import validate_expected_columns, validate_non_empty

CITY_VARIABLES = {
    ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS: ['may_precio_min', 'may_precio_prom', 'may_precio_max'],
    ModuloSisap.CIUDADES_PRECIOS_MINORISTAS: ['min_precio_min', 'min_precio_prom', 'min_precio_max'],
}

EXPECTED_COLUMNS_MAY = [
    'fecha',
    'producto_codigo',
    'producto_nombre',
    'ciudad',
    'variedad',
    'unidad_medida',
    'equiv_kg_lt',
    'precio_may_min',
    'precio_may_prom',
    'precio_may_max',
]
EXPECTED_COLUMNS_MIN = [
    'fecha',
    'producto_codigo',
    'producto_nombre',
    'ciudad',
    'variedad',
    'unidad_medida',
    'equiv_kg_lt',
    'precio_min_min',
    'precio_min_prom',
    'precio_min_max',
]
MAX_SAMPLE_QUERIES = 12
NUMERIC_CITY_COLUMNS = {
    'equiv_kg_lt': pl.Float64,
    'precio_may_min': pl.Float64,
    'precio_may_prom': pl.Float64,
    'precio_may_max': pl.Float64,
    'precio_min_min': pl.Float64,
    'precio_min_prom': pl.Float64,
    'precio_min_max': pl.Float64,
}


def _with_expected_columns(df: pl.DataFrame, expected_columns: list[str], dataset_label: str) -> pl.DataFrame:
    missing_columns = [column for column in expected_columns if column not in df.columns]
    if missing_columns:
        logger.warning(
            '{} llego sin columnas {}. Se completaran con nulos para mantener el esquema.',
            dataset_label,
            missing_columns,
        )
        df = df.with_columns(
            [
                pl.lit(None, dtype=NUMERIC_CITY_COLUMNS.get(column)).alias(column)
                for column in missing_columns
            ]
        )
    return df


def _resolve_region(region_nombre: str | None = None) -> dict:
    settings = get_settings()
    return resolve_item(
        PROCEDENCIAS_SISAP,
        settings.sisap_region_codigo,
        region_nombre or settings.sisap_region_nombre,
        'la region',
    )


def _build_raw_plan(
    modulo: ModuloSisap,
    region_nombre: str | None = None,
    productos_override: list[dict] | None = None,
) -> list[SisapQuery]:
    settings = get_settings()
    region = _resolve_region(region_nombre)
    productos = productos_override or resolve_productos(settings.sisap_producto_codigo, settings.sisap_producto_nombre)
    plan: list[SisapQuery] = []

    for producto in productos:
        resolved_dates = resolve_query_dates(
            control_modulo=_output_name(modulo),
            output_name=_output_name(modulo),
            scope_label='region',
            scope_value=region['nombre'],
            producto_codigo=producto['codigo'],
            producto_nombre=producto['nombre'],
            fecha_inicio=settings.fecha_inicio_resuelta,
            fecha_fin=settings.fecha_fin_resuelta,
        )
        if resolved_dates is None:
            continue

        fecha_inicio, fecha_fin = resolved_dates
        plan.extend(
            build_ciudades_queries(
                modulo=modulo,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                region_codigo=region['codigo'],
                productos=[producto],
            )
        )

    return plan


def _build_plan(modulo: ModuloSisap) -> list[SisapQuery]:
    settings = get_settings()
    return filter_plan(_build_raw_plan(modulo), settings.sisap_max_queries)


def build_plan_mayoristas() -> list[SisapQuery]:
    return _build_plan(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS)


def build_plan_minoristas() -> list[SisapQuery]:
    return _build_plan(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS)


def _expected_columns(modulo: ModuloSisap) -> list[str]:
    return EXPECTED_COLUMNS_MAY if modulo == ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS else EXPECTED_COLUMNS_MIN


def _variables(modulo: ModuloSisap) -> list[str]:
    return CITY_VARIABLES[modulo]


def _output_name(modulo: ModuloSisap) -> str:
    return 'ciudades_precios_mayoristas' if modulo == ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS else 'ciudades_precios_minoristas'


def _fetch_city_frames(extractor: SisapCiudadesExtractor, query: SisapQuery, modulo: ModuloSisap, save_raw: bool = False) -> pl.DataFrame:
    metric_frames: list[pl.DataFrame] = []
    for variable in _variables(modulo):
        report_html = extractor.fetch_report(query, variable=variable)
        if save_raw:
            save_html_snapshot(modulo, query, report_html, suffix=variable)
        frame = build_ciudades_metric_frame(report_html, query=query, metric_name=variable)
        metric_frames.append(frame)
    return merge_ciudades_metrics(metric_frames)


def _find_first_non_empty_report(extractor: SisapCiudadesExtractor, plan: list[SisapQuery], modulo: ModuloSisap) -> tuple[SisapQuery, pl.DataFrame]:
    for query in plan[:MAX_SAMPLE_QUERIES]:
        df = _fetch_city_frames(extractor, query, modulo=modulo, save_raw=True)
        if not df.is_empty():
            return query, df
    raise ValueError('No se encontraron resultados con datos en las primeras consultas de muestra.')


def run_sample(modulo: ModuloSisap) -> Path:
    plan = _build_plan(modulo)
    if not plan:
        raise ValueError('No hay queries armadas para ciudades.')

    extractor = SisapCiudadesExtractor()
    _, df = _find_first_non_empty_report(extractor, plan, modulo)
    output_name = _output_name(modulo)
    df = _with_expected_columns(df, _expected_columns(modulo), output_name)
    validate_non_empty(df, output_name)
    validate_expected_columns(df, _expected_columns(modulo), output_name)
    output = get_settings().clean_dir / f'{output_name}_sample.parquet'
    save_parquet(df, output)
    return output


def run_full(modulo: ModuloSisap, region_nombre: str | None = None) -> Path:
    settings = get_settings()
    region = _resolve_region(region_nombre)
    plan = filter_plan(_build_raw_plan(modulo, region['nombre']), settings.sisap_max_queries)
    if not plan:
        logger.info('No hay queries pendientes para {}.', _output_name(modulo))
        return build_scope_output_dir(_output_name(modulo), 'region', region['nombre'])

    errores: list[dict[str, str]] = []
    output_name = _output_name(modulo)
    output = build_scope_output_dir(output_name, 'region', region['nombre'])
    output.mkdir(parents=True, exist_ok=True)

    shards = build_grouped_shards(
        plan,
        group_key=lambda query: query.producto_codigo,
        chunk_size=settings.product_batch_size,
        shard_prefix=f'{output_name}-{region["codigo"]}',
    )

    def process_shard(shard) -> list[dict[str, str]]:
        extractor = SisapCiudadesExtractor()
        shard_errors: list[dict[str, str]] = []
        control_states = init_control_states()
        control_event_rows: list[dict[str, object]] = []
        for idx, query in enumerate(shard.items, start=1):
            logger.info(
                'Procesando {} shard={} {}/{} producto={} codigo={}',
                output_name,
                shard.shard_id,
                idx,
                len(shard.items),
                query.producto_nombre,
                query.producto_codigo,
            )
            register_control_query(control_states, output_name, output_name, 'region', region['nombre'], query)
            try:
                df = _fetch_city_frames(extractor, query, modulo=modulo, save_raw=True)
                if df.is_empty():
                    register_control_success(
                        control_states,
                        output_name,
                        output_name,
                        'region',
                        region['nombre'],
                        query,
                        estado='empty',
                    )
                    control_event_rows.append(
                        build_control_event_row(
                            output_name,
                            output_name,
                            'region',
                            region['nombre'],
                            query,
                            'empty',
                            'sin_resultados',
                        )
                    )
                    shard_errors.append({'producto_codigo': query.producto_codigo, 'producto_nombre': query.producto_nombre, 'motivo': 'sin_resultados'})
                    continue
                df = _with_expected_columns(
                    df,
                    _expected_columns(modulo),
                    f'{output_name}_{query.producto_codigo}',
                )
                validate_expected_columns(df, _expected_columns(modulo), f'{output_name}_{query.producto_codigo}')
                append_partitioned_output(
                    frames=[df],
                    output_name=output_name,
                    expected_columns=_expected_columns(modulo),
                    sort_columns=['producto_codigo', 'ciudad', 'variedad', 'fecha'],
                    scope_label='region',
                    scope_value=region['nombre'],
                )
                register_control_success(control_states, output_name, output_name, 'region', region['nombre'], query)
                control_event_rows.append(
                    build_control_event_row(
                        output_name,
                        output_name,
                        'region',
                        region['nombre'],
                        query,
                        'success',
                    )
                )
            except Exception as exc:
                logger.exception('Fallo extrayendo {} para {} ({})', output_name, query.producto_nombre, query.producto_codigo)
                register_control_failure(control_states, output_name, output_name, 'region', region['nombre'], query, str(exc))
                control_event_rows.append(
                    build_control_event_row(
                        output_name,
                        output_name,
                        'region',
                        region['nombre'],
                        query,
                        'error',
                        str(exc),
                    )
                )
                shard_errors.append({'producto_codigo': query.producto_codigo, 'producto_nombre': query.producto_nombre, 'motivo': str(exc)})

        persist_control_events_batch(control_event_rows)
        persist_control_states(control_states)
        return shard_errors

    shard_error_groups = run_shards(
        shards,
        process_shard,
        max_workers=settings.shard_max_workers if settings.parallel_enabled else 1,
        label=f'{output_name}/region={region["nombre"]}',
    )
    for shard_errors in shard_error_groups:
        errores.extend(shard_errors)

    if errores:
        error_path = output / 'errores.csv'
        pl.DataFrame(errores).write_csv(error_path)
    return output

