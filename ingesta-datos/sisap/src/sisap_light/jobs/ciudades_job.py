from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_ciudades import SisapCiudadesExtractor
from sisap_light.jobs.common import (
    build_delta_staging_run_id,
    build_historical_zero_frame,
    build_control_event_row,
    build_scope_output_dir,
    finalize_staged_delta_output,
    filter_plan,
    flush_accumulated_partitioned_output,
    has_staged_delta_output,
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
from sisap_light.procesamiento.parsers.html_tables import quick_html_data_signals

CITY_VARIABLES = {
    ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS: ['may_precio_min', 'may_precio_prom', 'may_precio_max'],
    ModuloSisap.CIUDADES_PRECIOS_MINORISTAS: ['min_precio_min', 'min_precio_prom', 'min_precio_max'],
}

EXPECTED_COLUMNS_MAY = [
    'fecha',
    'tipo_mercado',
    'region',
    'ciudad',
    'producto_codigo',
    'producto_nombre',
    'variedad',
    'unidad_medida',
    'equiv_kg_lt',
    'precio_min',
    'precio_prom',
    'precio_max',
]
EXPECTED_COLUMNS_MIN = [
    'fecha',
    'tipo_mercado',
    'region',
    'ciudad',
    'producto_codigo',
    'producto_nombre',
    'variedad',
    'unidad_medida',
    'equiv_kg_lt',
    'precio_min',
    'precio_prom',
    'precio_max',
]
MAX_SAMPLE_QUERIES = 12
CONTROL_FLUSH_EVERY = max(get_settings().sisap_control_flush_every, 1)
OUTPUT_FLUSH_EVERY = max(get_settings().sisap_output_flush_every, 1)
USE_LOCAL_DELTA_STAGING = get_settings().sisap_use_local_delta_staging


def _flush_control_batch(control_states: dict, event_rows: list[dict[str, object]]) -> None:
    if event_rows:
        persist_control_events_batch(event_rows)
        event_rows.clear()
    persist_control_states(control_states)


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
    return 'precio_diario_regiones'


def _tipo_mercado(modulo: ModuloSisap) -> str:
    return 'mayorista' if modulo == ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS else 'minorista'


def _fetch_city_frames(
    extractor: SisapCiudadesExtractor, query: SisapQuery, modulo: ModuloSisap, save_raw: bool = False
) -> tuple[pl.DataFrame, str | None]:
    metric_frames: list[pl.DataFrame] = []
    last_html: str | None = None
    for variable in _variables(modulo):
        report_html = extractor.fetch_report(query, variable=variable)
        last_html = report_html
        if save_raw:
            save_html_snapshot(modulo, query, report_html, suffix=variable)
        frame = build_ciudades_metric_frame(report_html, query=query, metric_name=variable)
        metric_frames.append(frame)
    df = merge_ciudades_metrics(metric_frames)
    if df.is_empty():
        return df, last_html

    tipo_mercado = _tipo_mercado(modulo)
    missing_metric_exprs: list[pl.Expr] = []
    if modulo == ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS:
        for column in ['precio_may_min', 'precio_may_prom', 'precio_may_max']:
            if column not in df.columns:
                missing_metric_exprs.append(pl.lit(None, dtype=pl.Float64).alias(column))
    else:
        for column in ['precio_min_min', 'precio_min_prom', 'precio_min_max']:
            if column not in df.columns:
                missing_metric_exprs.append(pl.lit(None, dtype=pl.Float64).alias(column))

    if missing_metric_exprs:
        df = df.with_columns(missing_metric_exprs)

    selected_columns: list[pl.Expr | str] = [
        'fecha',
        'ciudad',
        'producto_codigo',
        'producto_nombre',
        'variedad',
        'unidad_medida',
        'equiv_kg_lt',
        pl.col('region_nombre').alias('region'),
        pl.lit(tipo_mercado).alias('tipo_mercado'),
    ]
    if modulo == ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS:
        selected_columns.extend(['precio_may_min', 'precio_may_prom', 'precio_may_max'])
    else:
        selected_columns.extend(['precio_min_min', 'precio_min_prom', 'precio_min_max'])

    df = df.select(selected_columns)

    if modulo == ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS:
        df = df.with_columns(
            pl.col('precio_may_min').alias('precio_min'),
            pl.col('precio_may_prom').alias('precio_prom'),
            pl.col('precio_may_max').alias('precio_max'),
        ).drop(['precio_may_min', 'precio_may_prom', 'precio_may_max'])
    else:
        df = df.with_columns(
            pl.col('precio_min_min').alias('precio_min'),
            pl.col('precio_min_prom').alias('precio_prom'),
            pl.col('precio_min_max').alias('precio_max'),
        ).drop(['precio_min_min', 'precio_min_prom', 'precio_min_max'])

    return df, last_html


def _find_first_non_empty_report(extractor: SisapCiudadesExtractor, plan: list[SisapQuery], modulo: ModuloSisap) -> tuple[SisapQuery, pl.DataFrame]:
    for query in plan[:MAX_SAMPLE_QUERIES]:
        df, _ = _fetch_city_frames(
            extractor,
            query,
            modulo=modulo,
            save_raw=get_settings().sisap_save_debug_html,
        )
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
    validate_non_empty(df, output_name)
    validate_expected_columns(df, _expected_columns(modulo), output_name)
    output = get_settings().clean_dir / f'{output_name}_sample.parquet'
    save_parquet(df, output)
    return output


def run_full(
    modulo: ModuloSisap,
    region_nombre: str | None = None,
    *,
    finalize_delta: bool = True,
) -> Path:
    settings = get_settings()
    append_only_delta = settings.is_backfill and settings.sisap_delta_append_only_backfill
    region = _resolve_region(region_nombre)
    plan = filter_plan(_build_raw_plan(modulo, region['nombre']), settings.sisap_max_queries)
    if not plan:
        if (
            finalize_delta
            and settings.delta_enabled
            and USE_LOCAL_DELTA_STAGING
            and has_staged_delta_output(_output_name(modulo))
        ):
            finalize_staged_delta_output(
                output_name=_output_name(modulo),
                expected_columns=_expected_columns(modulo),
                sort_columns=['producto_codigo', 'ciudad', 'variedad', 'fecha'],
                append_only=append_only_delta,
            )
        logger.info('No hay queries pendientes para {}.', _output_name(modulo))
        return build_scope_output_dir(_output_name(modulo), 'region', region['nombre'])

    errores: list[dict[str, str]] = []
    output_name = _output_name(modulo)
    output = build_scope_output_dir(output_name, 'region', region['nombre'])
    output.mkdir(parents=True, exist_ok=True)
    staging_run_id = (
        build_delta_staging_run_id(output_name)
        if settings.delta_enabled and USE_LOCAL_DELTA_STAGING
        else None
    )

    shards = build_grouped_shards(
        plan,
        group_key=lambda query: query.producto_codigo,
        chunk_size=settings.product_batch_size,
        shard_prefix=f'{output_name}-{region["codigo"]}',
        max_shards=settings.target_shards_per_scope,
    )

    def process_shard(shard) -> list[dict[str, str]]:
        import gc
        extractor = SisapCiudadesExtractor()
        shard_errors: list[dict[str, str]] = []
        control_states = init_control_states()
        pending_event_rows: list[dict[str, object]] = []
        accumulated_frames: dict[tuple[str, str], list[pl.DataFrame]] = {}
        pending_output_frames = 0
        try:
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
                    df, sample_html = _fetch_city_frames(
                        extractor,
                        query,
                        modulo=modulo,
                        save_raw=settings.sisap_save_debug_html,
                    )
                    if df.is_empty():
                        sig = quick_html_data_signals(sample_html)
                        logger.warning(
                            'Sin resultados de {} para region={} producto={} codigo={} | '
                            'debug_html={} | senales_ultima_respuesta={}',
                            output_name,
                            region['nombre'],
                            query.producto_nombre,
                            query.producto_codigo,
                            settings.sisap_save_debug_html,
                            sig,
                        )
                        if sig.get('approx_date_tokens', 0) and sig.get('table_tags', 0):
                            logger.warning(
                                'El HTML de ciudades parece incluir tablas y fechas; si el portal muestra datos, '
                                'revisar build_ciudades_metric_frame vs HTML actual.'
                            )
                        zero_df = build_historical_zero_frame(
                            output_name,
                            query,
                            tipo_mercado=_tipo_mercado(modulo),
                        )
                        if not zero_df.is_empty():
                            accumulated_frames.setdefault(('region', region['nombre']), []).append(zero_df)
                            pending_output_frames += 1
                            if pending_output_frames >= OUTPUT_FLUSH_EVERY:
                                flush_accumulated_partitioned_output(
                                    accumulated_frames,
                                    output_name=output_name,
                                    expected_columns=_expected_columns(modulo),
                                    sort_columns=['producto_codigo', 'ciudad', 'variedad', 'fecha'],
                                    staging_run_id=staging_run_id,
                                    shard_id=shard.shard_id,
                                )
                                pending_output_frames = 0
                        register_control_success(
                            control_states,
                            output_name,
                            output_name,
                            'region',
                            region['nombre'],
                            query,
                            estado='empty',
                        )
                        event_row = build_control_event_row(
                            output_name,
                            output_name,
                            'region',
                            region['nombre'],
                            query,
                            'empty',
                            'sin_resultados',
                        )
                        pending_event_rows.append(event_row)
                        if len(pending_event_rows) >= CONTROL_FLUSH_EVERY:
                            _flush_control_batch(control_states, pending_event_rows)
                        shard_errors.append({'producto_codigo': query.producto_codigo, 'producto_nombre': query.producto_nombre, 'motivo': 'sin_resultados'})
                        continue
                    validate_expected_columns(df, _expected_columns(modulo), f'{output_name}_{query.producto_codigo}')
                    accumulated_frames.setdefault(('region', region['nombre']), []).append(df)
                    register_control_success(control_states, output_name, output_name, 'region', region['nombre'], query)
                    event_row = build_control_event_row(
                        output_name,
                        output_name,
                        'region',
                        region['nombre'],
                        query,
                        'success',
                    )
                    pending_event_rows.append(event_row)
                    if len(pending_event_rows) >= CONTROL_FLUSH_EVERY:
                        _flush_control_batch(control_states, pending_event_rows)
                except Exception as exc:
                    logger.exception('Fallo extrayendo {} para {} ({})', output_name, query.producto_nombre, query.producto_codigo)
                    register_control_failure(control_states, output_name, output_name, 'region', region['nombre'], query, str(exc))
                    event_row = build_control_event_row(
                        output_name,
                        output_name,
                        'region',
                        region['nombre'],
                        query,
                        'error',
                        str(exc),
                    )
                    pending_event_rows.append(event_row)
                    if len(pending_event_rows) >= CONTROL_FLUSH_EVERY:
                        _flush_control_batch(control_states, pending_event_rows)
                    shard_errors.append({'producto_codigo': query.producto_codigo, 'producto_nombre': query.producto_nombre, 'motivo': str(exc)})
            flush_accumulated_partitioned_output(
                accumulated_frames,
                output_name=output_name,
                expected_columns=_expected_columns(modulo),
                sort_columns=['producto_codigo', 'ciudad', 'variedad', 'fecha'],
                staging_run_id=staging_run_id,
                shard_id=shard.shard_id,
            )
            _flush_control_batch(control_states, pending_event_rows)
        finally:
            extractor.close()
            accumulated_frames.clear()
            gc.collect()
        return shard_errors

    shard_error_groups = run_shards(
        shards,
        process_shard,
        max_workers=settings.shard_max_workers if settings.parallel_enabled else 1,
        label=f'{output_name}/region={region["nombre"]}',
    )
    for shard_errors in shard_error_groups:
        errores.extend(shard_errors)

    if finalize_delta and settings.delta_enabled and USE_LOCAL_DELTA_STAGING and staging_run_id:
        finalize_staged_delta_output(
            output_name=output_name,
            expected_columns=_expected_columns(modulo),
            sort_columns=['producto_codigo', 'ciudad', 'variedad', 'fecha'],
            append_only=append_only_delta,
        )

    if errores:
        error_path = output / 'errores.csv'
        pl.DataFrame(errores).write_csv(error_path)
        raise RuntimeError(
            f'La corrida de {_output_name(modulo)} finalizo con errores. '
            f'Revisar {error_path} para el detalle.'
        )
    return output
