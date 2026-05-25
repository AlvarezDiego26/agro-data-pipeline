from pathlib import Path
import re

import polars as pl
from loguru import logger

from sisap_light.config import get_settings
from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
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
from sisap_light.procesamiento.parsers.html_forms import (
    extract_checkbox_products,
    extract_hidden_inputs,
    extract_market_options,
    extract_post_id,
    extract_procedencia_options,
    extract_variable_options,
)
from sisap_light.procesamiento.parsers.html_tables import (
    detect_primary_table,
    extract_report_titles,
    quick_html_data_signals,
)
from sisap_light.schemas import ModuloSisap, SisapQuery
from sisap_light.procesamiento.storage.parquet import save_parquet
from sisap_light.procesamiento.storage.raw import save_html_snapshot
from sisap_light.procesamiento.transformers.volumen import build_volumen_frame
from sisap_light.procesamiento.limpieza import validate_expected_columns, validate_non_empty

EXPECTED_COLUMNS = [
    'fecha',
    'mercado_codigo',
    'mercado_nombre',
    'producto_codigo',
    'producto_nombre',
    'variedad',
    'procedencia',
    'volumen_ton',
]

MAX_SAMPLE_QUERIES = 12
CONTROL_FLUSH_EVERY = max(get_settings().sisap_control_flush_every, 1)
OUTPUT_FLUSH_EVERY = max(get_settings().sisap_output_flush_every, 1)
USE_LOCAL_DELTA_STAGING = get_settings().sisap_use_local_delta_staging
_NUMERIC_VALUE_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")
_DATE_VALUE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")


def _table_has_materialized_values(rows: list[list[str]]) -> bool:
    for row in rows[1:]:
        for cell in row[1:]:
            value = (cell or '').strip()
            if not value or value in {'...', '....', '-'}:
                continue
            normalized = value.replace(',', '')
            if _DATE_VALUE_RE.fullmatch(value):
                continue
            if _NUMERIC_VALUE_RE.fullmatch(normalized):
                return True
    return False


def _flush_control_batch(control_states: dict, event_rows: list[dict[str, object]]) -> None:
    if event_rows:
        persist_control_events_batch(event_rows)
        event_rows.clear()
    persist_control_states(control_states)


def _volumen_control_scope(query: SisapQuery) -> tuple[str, str]:
    mercado = str(query.mercado_codigo or '')
    if mercado == '*':
        mercado = 'consolidado'
        return 'volumen_mercado', mercado
    return 'procedencia', query.procedencia_nombre or 'desconocida'


def _resolve_procedencia(procedencia_nombre: str | None = None) -> dict:
    settings = get_settings()
    return resolve_item(
        PROCEDENCIAS_SISAP,
        settings.sisap_procedencia_codigo,
        procedencia_nombre or settings.sisap_procedencia_nombre,
        'la procedencia',
    )


def inspect_home() -> dict:
    extractor = SisapMayoristaExtractor()
    html = extractor.fetch_home()
    return {
        'hidden_inputs': extract_hidden_inputs(html),
        'post_id': extract_post_id(html),
        'mercado_options': extract_market_options(html),
        'producto_options': extract_checkbox_products(html),
        'procedencia_options': extract_procedencia_options(html),
        'variable_options': extract_variable_options(html),
        'html': html,
    }


def inspect_home_mercado(mercado_codigo: str) -> dict:
    """Productos para un mercado concreto (misma logica que la corrida)."""
    extractor = SisapMayoristaExtractor()
    html = extractor.fetch_productos_por_mercado_html(mercado_codigo)
    return {
        'mercado_codigo': mercado_codigo,
        'producto_options': extract_checkbox_products(html),
        'html': html,
    }


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
                control_modulo='volumen',
                output_name='volumen_diario_mercado_lima',
                modulo=ModuloSisap.MAYORISTA_VOLUMEN,
                mercado=mercado,
                procedencia=procedencia,
                productos_override=productos_override,
            )
        )
    return plan


def build_plan(mercado_nombre: str | None = None, procedencia_nombre: str | None = None) -> list[SisapQuery]:
    settings = get_settings()
    return filter_plan(_build_raw_plan(mercado_nombre, procedencia_nombre), settings.sisap_max_queries)


def _find_first_non_empty_report(extractor: SisapMayoristaExtractor, plan: list[SisapQuery]) -> tuple[SisapQuery, str, list[list[str]]]:
    for query in plan[:MAX_SAMPLE_QUERIES]:
        report_html = extractor.fetch_report(query, variable='volumen')
        rows = detect_primary_table(report_html)
        if rows:
            return query, report_html, rows
    raise ValueError('No se encontraron resultados con datos en las primeras consultas de muestra.')


def _normalize_report(query: SisapQuery, report_html: str, rows: list[list[str]] | None = None) -> pl.DataFrame:
    parsed_rows = rows or detect_primary_table(report_html)
    if not parsed_rows:
        return pl.DataFrame()
    return build_volumen_frame(parsed_rows, query=query)


def run_sample() -> Path:
    plan = build_plan()
    if not plan:
        raise ValueError('No hay queries armadas para volumen.')

    extractor = SisapMayoristaExtractor()
    query, report_html, _ = _find_first_non_empty_report(extractor, plan)
    save_html_snapshot(
        ModuloSisap.MAYORISTA_VOLUMEN,
        query,
        report_html,
    )
    df = _normalize_report(query, report_html)
    validate_non_empty(df, 'volumen')
    validate_expected_columns(df, EXPECTED_COLUMNS, 'volumen')
    output = get_settings().clean_dir / 'volumen_diario_sample.parquet'
    save_parquet(df, output)
    return output


def run_full(
    mercado_nombre: str | None = None,
    procedencia_nombre: str | None = None,
    *,
    finalize_delta: bool = True,
) -> Path:
    settings = get_settings()
    append_only_delta = settings.is_backfill and settings.sisap_delta_append_only_backfill
    if settings.sisap_mercado_codigo == '*':
        procedencia_nombre = None

    plan = filter_plan(_build_raw_plan(mercado_nombre, procedencia_nombre), settings.sisap_max_queries)
    scope_value = procedencia_nombre or 'consolidado'
    scope_label = 'procedencia' if procedencia_nombre else 'volumen_mercado'
    if not plan:
        if (
            finalize_delta
            and settings.delta_enabled
            and USE_LOCAL_DELTA_STAGING
            and has_staged_delta_output('volumen_diario_mercado_lima')
        ):
            finalize_staged_delta_output(
                output_name='volumen_diario_mercado_lima',
                expected_columns=EXPECTED_COLUMNS,
                sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
                append_only=append_only_delta,
            )
        logger.info('No hay queries pendientes para volumen.')
        return build_scope_output_dir('volumen_diario_mercado_lima', scope_label, scope_value)

    errores: list[dict[str, str]] = []
    output = build_scope_output_dir('volumen_diario_mercado_lima', scope_label, scope_value)
    output.mkdir(parents=True, exist_ok=True)
    shards = build_grouped_shards(
        plan,
        group_key=lambda query: f'{query.mercado_codigo or ""}-{query.producto_codigo}',
        chunk_size=settings.product_batch_size,
        shard_prefix=(f'volumen-{scope_label}-{scope_value}'[:80]),
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
        staging_run_id = (
            build_delta_staging_run_id(f'volumen_diario_mercado_lima-{shard.shard_id}')
            if settings.delta_enabled and USE_LOCAL_DELTA_STAGING
            else None
        )
        try:
            for idx, query in enumerate(shard.items, start=1):
                logger.info(
                    'Procesando volumen shard={} {}/{} mercado={} producto={} codigo={}',
                    shard.shard_id,
                    idx,
                    len(shard.items),
                    query.mercado_codigo,
                    query.producto_nombre,
                    query.producto_codigo,
                )
                sl, sv = _volumen_control_scope(query)
                register_control_query(control_states, 'volumen', 'volumen_diario_mercado_lima', sl, sv, query)
                try:
                    report_html = extractor.fetch_report(query, variable='volumen')
                    snap_path = save_html_snapshot(
                        ModuloSisap.MAYORISTA_VOLUMEN, query, report_html, suffix='fetched'
                    )
                    rows = detect_primary_table(report_html)
                    df = _normalize_report(query, report_html, rows=rows)

                    if df.is_empty():
                        sig = quick_html_data_signals(report_html)
                        logger.warning(
                            'Sin resultados de volumen para mercado={} producto={} codigo={} rango={}..{} '
                            '| debug_html={} | senales={}',
                            query.mercado_codigo,
                            query.producto_nombre,
                            query.producto_codigo,
                            query.fecha_inicio,
                            query.fecha_fin,
                            settings.sisap_save_debug_html,
                            sig,
                        )
                        if rows and _table_has_materialized_values(rows):
                            logger.warning(
                                'El HTML trae celdas con valores, pero el dataframe quedo vacio; '
                                'esto si apunta a parser/transformer (detect_primary_table / build_volumen_frame).'
                            )
                        zero_df = build_historical_zero_frame(
                            'volumen_diario_mercado_lima',
                            query,
                        )
                        if not zero_df.is_empty():
                            accumulated_frames.setdefault((sl, sv), []).append(zero_df)
                            pending_output_frames += 1
                            if pending_output_frames >= OUTPUT_FLUSH_EVERY:
                                flush_accumulated_partitioned_output(
                                    accumulated_frames,
                                    output_name='volumen_diario_mercado_lima',
                                    expected_columns=EXPECTED_COLUMNS,
                                    sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
                                    staging_run_id=staging_run_id,
                                    shard_id=shard.shard_id,
                                )
                                pending_output_frames = 0
                        register_control_success(
                            control_states,
                            'volumen',
                            'volumen_diario_mercado_lima',
                            sl,
                            sv,
                            query,
                            estado='empty',
                        )
                        event_row = build_control_event_row(
                            'volumen',
                            'volumen_diario_mercado_lima',
                            sl,
                            sv,
                            query,
                            'empty',
                            'sin_resultados',
                        )
                        pending_event_rows.append(event_row)
                        if len(pending_event_rows) >= CONTROL_FLUSH_EVERY:
                            _flush_control_batch(control_states, pending_event_rows)
                        continue

                    validate_expected_columns(df, EXPECTED_COLUMNS, f'volumen_{query.producto_codigo}')
                    accumulated_frames.setdefault((sl, sv), []).append(df)
                    pending_output_frames += 1
                    if pending_output_frames >= OUTPUT_FLUSH_EVERY:
                        flush_accumulated_partitioned_output(
                            accumulated_frames,
                            output_name='volumen_diario_mercado_lima',
                            expected_columns=EXPECTED_COLUMNS,
                            sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
                            staging_run_id=staging_run_id,
                            shard_id=shard.shard_id,
                        )
                        pending_output_frames = 0
                    register_control_success(control_states, 'volumen', 'volumen_diario_mercado_lima', sl, sv, query)
                    event_row = build_control_event_row(
                        'volumen',
                        'volumen_diario_mercado_lima',
                        sl,
                        sv,
                        query,
                        'success',
                    )
                    pending_event_rows.append(event_row)
                    if len(pending_event_rows) >= CONTROL_FLUSH_EVERY:
                        _flush_control_batch(control_states, pending_event_rows)
                except Exception as exc:
                    logger.exception('Fallo extrayendo volumen para {} ({})', query.producto_nombre, query.producto_codigo)
                    register_control_failure(control_states, 'volumen', 'volumen_diario_mercado_lima', sl, sv, query, str(exc))
                    event_row = build_control_event_row(
                        'volumen',
                        'volumen_diario_mercado_lima',
                        sl,
                        sv,
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
                output_name='volumen_diario_mercado_lima',
                expected_columns=EXPECTED_COLUMNS,
                sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
                staging_run_id=staging_run_id,
                shard_id=shard.shard_id,
            )
            if finalize_delta and settings.delta_enabled and USE_LOCAL_DELTA_STAGING and staging_run_id:
                finalize_staged_delta_output(
                    output_name='volumen_diario_mercado_lima',
                    expected_columns=EXPECTED_COLUMNS,
                    sort_columns=['mercado_codigo', 'producto_codigo', 'variedad', 'procedencia', 'fecha'],
                    append_only=append_only_delta,
                    run_id=staging_run_id,
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
        label=f'volumen/{scope_label}={scope_value}',
    )
    for shard_errors in shard_error_groups:
        errores.extend(shard_errors)

    if errores:
        error_path = output / 'errores.csv'
        pl.DataFrame(errores).write_csv(error_path)
        raise RuntimeError(
            'La corrida de volumen finalizo con errores. '
            f'Revisar {error_path} para el detalle.'
        )
    return output


def inspect_sample_report() -> dict:
    plan = build_plan()
    if not plan:
        raise ValueError('No hay queries armadas para volumen.')

    extractor = SisapMayoristaExtractor()
    query, report_html, rows = _find_first_non_empty_report(extractor, plan)
    return {
        'query': query.model_dump(),
        'titles': extract_report_titles(report_html),
        'rows': rows[:5],
        'html': report_html,
    }
