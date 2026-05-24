from pathlib import Path

import polars as pl
from loguru import logger

from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
from sisap_light.jobs.common import (
    build_delta_staging_run_id,
    build_historical_zero_frame,
    build_control_event_row,
    build_scope_output_dir,
    expand_mayorista_plan_for_procedencia,
    finalize_staged_delta_output,
    filter_plan,
    flush_accumulated_partitioned_output,
    has_staged_delta_output,
    init_control_states,
    iter_mercados_ejecucion,
    persist_control_events_batch,
    persist_control_states,
    register_control_failure,
    register_control_query,
    register_control_success,
    resolve_item,
)
from sisap_light.jobs.parallel import build_grouped_shards, run_shards
from sisap_light.procesamiento.parsers.html_tables import extract_report_titles, detect_primary_table, quick_html_data_signals
from sisap_light.schemas import ModuloSisap, SisapQuery
from sisap_light.procesamiento.storage.parquet import save_parquet
from sisap_light.procesamiento.storage.raw import save_html_snapshot
from sisap_light.procesamiento.transformers.precios import build_precio_metric_frame, merge_precio_metrics
from sisap_light.procesamiento.limpieza import validate_expected_columns, validate_non_empty

PRICE_VARIABLES = ['precio_min', 'precio_prom', 'precio_max']
EXPECTED_COLUMNS = [
    'fecha',
    'mercado_codigo',
    'mercado_nombre',
    'producto_codigo',
    'producto_nombre',
    'variedad',
    'procedencia',
    'precio_min',
    'precio_prom',
    'precio_max',
]
MAX_SAMPLE_QUERIES = 12
CONTROL_FLUSH_EVERY = 20
OUTPUT_FLUSH_EVERY = 20


def _resolve_procedencia(procedencia_nombre: str | None = None) -> dict:
    settings = get_settings()
    return resolve_item(
        PROCEDENCIAS_SISAP,
        settings.sisap_procedencia_codigo,
        procedencia_nombre or settings.sisap_procedencia_nombre,
        'la procedencia',
    )


def _build_raw_plan(
    mercado_nombre: str | None = None,
    procedencia_nombre: str | None = None,
    productos_override: list[dict] | None = None,
) -> list[SisapQuery]:
    procedencia = _resolve_procedencia(procedencia_nombre) if (procedencia_nombre and procedencia_nombre != 'consolidado') else None
    mercados = iter_mercados_ejecucion(mercado_nombre)
    plan: list[SisapQuery] = []
    for mercado in mercados:
        plan.extend(
            expand_mayorista_plan_for_procedencia(
                control_modulo='precios',
                output_name='precios_diarios_mercado_lima',
                modulo=ModuloSisap.MAYORISTA_PRECIOS,
                mercado=mercado,
                procedencia=procedencia,
                productos_override=productos_override,
            )
        )
    return plan


def build_plan(mercado_nombre: str | None = None) -> list[SisapQuery]:
    settings = get_settings()
    return filter_plan(_build_raw_plan(mercado_nombre), settings.sisap_max_queries)


def _fetch_price_frames(extractor: SisapMayoristaExtractor, query: SisapQuery, save_raw: bool = False) -> tuple[pl.DataFrame, dict[str, list[str]], str | None]:
    metric_frames: list[pl.DataFrame] = []
    titles_by_metric: dict[str, list[str]] = {}
    last_html: str | None = None

    for variable in PRICE_VARIABLES:
        report_html = extractor.fetch_report(query, variable=variable)
        last_html = report_html
        titles_by_metric[variable] = extract_report_titles(report_html)
        if save_raw:
            save_html_snapshot(ModuloSisap.MAYORISTA_PRECIOS, query, report_html, suffix=variable)
        rows = detect_primary_table(report_html)
        frame = build_precio_metric_frame(rows, query=query, metric_name=variable)
        metric_frames.append(frame)

    return merge_precio_metrics(metric_frames), titles_by_metric, last_html


def _find_first_non_empty_report(extractor: SisapMayoristaExtractor, plan: list[SisapQuery]) -> tuple[SisapQuery, pl.DataFrame, dict[str, list[str]]]:
    for query in plan[:MAX_SAMPLE_QUERIES]:
        df, titles, _ = _fetch_price_frames(
            extractor,
            query,
            save_raw=get_settings().sisap_save_debug_html,
        )
        if not df.is_empty():
            return query, df, titles
    raise ValueError('No se encontraron resultados con datos en las primeras consultas de muestra.')


def run_sample(mercado_nombre: str | None = None) -> Path:
    plan = build_plan(mercado_nombre)
    if not plan:
        raise ValueError('No hay queries armadas para precios.')

    extractor = SisapMayoristaExtractor()
    _, df, _ = _find_first_non_empty_report(extractor, plan)
    validate_non_empty(df, 'precios')
    validate_expected_columns(df, EXPECTED_COLUMNS, 'precios')
    output = get_settings().clean_dir / 'precios_diarios_sample.parquet'
    save_parquet(df, output)
    return output


def _precios_control_scope(query: SisapQuery) -> tuple[str, str]:
    # Si mercado es '*', usamos consolidado por mercado
    # Si procedencia es None, es consolidado de procedencias
    m_code = str(query.mercado_codigo or '')
    if m_code == '*':
        return 'volumen_mercado', 'consolidado'
    
    # Por defecto usamos procedencia (comportamiento original)
    return 'procedencia', query.procedencia_nombre or 'desconocida'


def _flush_control_batch(control_states: dict, event_rows: list[dict[str, object]]) -> None:
    if event_rows:
        persist_control_events_batch(event_rows)
        event_rows.clear()
    persist_control_states(control_states)


def run_full(mercado_nombre: str | None = None, procedencia_nombre: str | None = None) -> Path:
    settings = get_settings()
    
    # Si el mercado es '*', forzamos procedencia=None para reporte consolidado
    if settings.sisap_mercado_codigo == '*':
        procedencia = None
        scope_label, scope_value = 'volumen_mercado', 'consolidado'
    else:
        procedencia = _resolve_procedencia(procedencia_nombre)
        scope_label, scope_value = 'procedencia', procedencia['nombre']

    plan = filter_plan(_build_raw_plan(mercado_nombre, procedencia['nombre'] if procedencia else None), settings.sisap_max_queries)
    if not plan:
        if settings.delta_enabled and has_staged_delta_output('precios_diarios_mercado_lima'):
            finalize_staged_delta_output(
                output_name='precios_diarios_mercado_lima',
                expected_columns=EXPECTED_COLUMNS,
                sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
            )
        logger.info('No hay queries pendientes para precios.')
        return build_scope_output_dir('precios_diarios_mercado_lima', scope_label, scope_value)

    errores: list[dict[str, str]] = []
    output = build_scope_output_dir('precios_diarios_mercado_lima', scope_label, scope_value)
    output.mkdir(parents=True, exist_ok=True)
    staging_run_id = build_delta_staging_run_id('precios_diarios_mercado_lima') if settings.delta_enabled else None

    shards = build_grouped_shards(
        plan,
        group_key=lambda query: f'{query.mercado_codigo or ""}-{query.producto_codigo}',
        chunk_size=settings.product_batch_size,
        shard_prefix=f'precios-{scope_value}',
        max_shards=settings.target_shards_per_scope,
    )

    def process_shard(shard) -> list[dict[str, str]]:
        import gc
        extractor = SisapMayoristaExtractor()
        shard_errors: list[dict[str, str]] = []
        control_states = init_control_states()
        pending_event_rows: list[dict[str, object]] = []
        accumulated_frames: dict[tuple[str, str], list[pl.DataFrame]] = {}
        pending_output_frames = 0
        try:
            for idx, query in enumerate(shard.items, start=1):
                logger.info(
                    'Procesando precios shard={} {}/{} mercado={} producto={} codigo={}',
                    shard.shard_id,
                    idx,
                    len(shard.items),
                    query.mercado_codigo,
                    query.producto_nombre,
                    query.producto_codigo,
                )
                register_control_query(control_states, 'precios', 'precios_diarios_mercado_lima', scope_label, scope_value, query)
                try:
                    df, _, sample_html = _fetch_price_frames(
                        extractor,
                        query,
                        save_raw=settings.sisap_save_debug_html,
                    )
                    if df.is_empty():
                        sig = quick_html_data_signals(sample_html)
                        logger.warning(
                            'Sin resultados de precios para mercado={} producto={} codigo={} | '
                            'debug_html={} | senales_ultima_respuesta={}',
                            query.mercado_codigo,
                            query.producto_nombre,
                            query.producto_codigo,
                            settings.sisap_save_debug_html,
                            sig,
                        )
                        if sig.get('approx_date_tokens', 0) and sig.get('table_tags', 0):
                            logger.warning(
                                'El HTML de precios parece incluir tablas y fechas; si el portal muestra datos, '
                                'revisar build_precio_metric_frame / detect_primary_table vs estructura actual del portal.'
                            )
                        zero_df = build_historical_zero_frame(
                            'precios_diarios_mercado_lima',
                            query,
                        )
                        if not zero_df.is_empty():
                            accumulated_frames.setdefault((scope_label, scope_value), []).append(zero_df)
                            pending_output_frames += 1
                            if pending_output_frames >= OUTPUT_FLUSH_EVERY:
                                flush_accumulated_partitioned_output(
                                    accumulated_frames,
                                    output_name='precios_diarios_mercado_lima',
                                    expected_columns=EXPECTED_COLUMNS,
                                    sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
                                    staging_run_id=staging_run_id,
                                    shard_id=shard.shard_id,
                                )
                                pending_output_frames = 0
                        register_control_success(
                            control_states,
                            'precios',
                            'precios_diarios_mercado_lima',
                            scope_label,
                            scope_value,
                            query,
                            estado='empty',
                        )
                        event_row = build_control_event_row(
                            'precios',
                            'precios_diarios_mercado_lima',
                            scope_label,
                            scope_value,
                            query,
                            'empty',
                            'sin_resultados',
                        )
                        pending_event_rows.append(event_row)
                        if len(pending_event_rows) >= CONTROL_FLUSH_EVERY:
                            _flush_control_batch(control_states, pending_event_rows)
                        continue

                    validate_expected_columns(df, EXPECTED_COLUMNS, f'precios_{query.producto_codigo}')
                    accumulated_frames.setdefault((scope_label, scope_value), []).append(df)
                    pending_output_frames += 1
                    if pending_output_frames >= OUTPUT_FLUSH_EVERY:
                        flush_accumulated_partitioned_output(
                            accumulated_frames,
                            output_name='precios_diarios_mercado_lima',
                            expected_columns=EXPECTED_COLUMNS,
                            sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
                            staging_run_id=staging_run_id,
                            shard_id=shard.shard_id,
                        )
                        pending_output_frames = 0
                    register_control_success(control_states, 'precios', 'precios_diarios_mercado_lima', scope_label, scope_value, query)
                    event_row = build_control_event_row(
                        'precios',
                        'precios_diarios_mercado_lima',
                        scope_label,
                        scope_value,
                        query,
                        'success',
                    )
                    pending_event_rows.append(event_row)
                    if len(pending_event_rows) >= CONTROL_FLUSH_EVERY:
                        _flush_control_batch(control_states, pending_event_rows)
                except Exception as exc:
                    logger.exception('Fallo extrayendo precios para {} ({})', query.producto_nombre, query.producto_codigo)
                    register_control_failure(control_states, 'precios', 'precios_diarios_mercado_lima', scope_label, scope_value, query, str(exc))
                    event_row = build_control_event_row(
                        'precios',
                        'precios_diarios_mercado_lima',
                        scope_label,
                        scope_value,
                        query,
                        'error',
                        str(exc),
                    )
                    pending_event_rows.append(event_row)
                    if len(pending_event_rows) >= CONTROL_FLUSH_EVERY:
                        _flush_control_batch(control_states, pending_event_rows)
                    shard_errors.append(
                        {
                            'mercado_codigo': query.mercado_codigo or '',
                            'producto_codigo': query.producto_codigo,
                            'producto_nombre': query.producto_nombre,
                            'motivo': str(exc),
                        }
                    )
            flush_accumulated_partitioned_output(
                accumulated_frames,
                output_name='precios_diarios_mercado_lima',
                expected_columns=EXPECTED_COLUMNS,
                sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
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
        label=f'precios/procedencia={procedencia["nombre"] if procedencia else "consolidado"}',
    )
    for shard_errors in shard_error_groups:
        errores.extend(shard_errors)

    if settings.delta_enabled and staging_run_id:
        finalize_staged_delta_output(
            output_name='precios_diarios_mercado_lima',
            expected_columns=EXPECTED_COLUMNS,
            sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
        )

    if errores:
        error_path = output / 'errores.csv'
        pl.DataFrame(errores).write_csv(error_path)
        raise RuntimeError(
            'La corrida de precios finalizo con errores. '
            f'Revisar {error_path} para el detalle.'
        )
    return output
