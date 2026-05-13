from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
from loguru import logger

from sunat_file.config import get_settings
from sunat_file.storage.control import (
    append_control_events,
    build_control_event_id,
    build_control_record_timestamp,
    get_last_successful_date,
    upsert_control_records,
)
from sunat_file.storage.delta import save_delta_table
from sunat_file.transformers.agro import (
    build_catalog_file,
    build_data_dictionary,
    build_region_summary,
    build_review_files,
    build_sunat_exportaciones_frescas,
    build_territory_catalog,
    build_ubigeo_quality_report,
)

OUTPUT_DATASET = 'sunat_exportaciones_agrarias_frescas'


def _source_path() -> Path:
    settings = get_settings()
    return settings.extraccion_dir


def _clean_path() -> str:
    settings = get_settings()
    return settings.build_delta_uri('exportaciones_filtradas/tablon_maestro_agrario')


def _get_last_clean_date(uri: str) -> date | None:
    settings = get_settings()
    try:
        df = pl.read_delta(
            uri, 
            storage_options=settings.delta_storage_options
        ).select(['fecha']).drop_nulls()
        if df.is_empty():
            return None
        return df.get_column('fecha').max()
    except Exception:
        return None


def _resolve_processing_dates() -> tuple[date, date] | None:
    settings = get_settings()
    fecha_inicio = settings.fecha_inicio_resuelta
    fecha_fin = settings.fecha_fin_resuelta

    if settings.is_manual or not settings.is_incremental:
        return fecha_inicio, fecha_fin

    last_loaded: date | None = None
    if settings.sunat_use_control_table:
        control_last = get_last_successful_date('sunat', OUTPUT_DATASET)
        if control_last is not None:
            last_loaded = control_last

    if last_loaded is None:
        last_loaded = _get_last_clean_date(_clean_path())

    if last_loaded is None:
        return fecha_inicio, fecha_fin

    if settings.sunat_incremental_overlap_dias > 0:
        next_start = last_loaded - timedelta(days=settings.sunat_incremental_overlap_dias)
        if next_start < fecha_inicio:
            next_start = fecha_inicio
    else:
        next_start = last_loaded + timedelta(days=1)

    if next_start > fecha_fin:
        return None

    return next_start, fecha_fin


def _persist_control(fecha_inicio: date, fecha_fin: date, ultima_fecha_exitosa: date | None, estado: str, mensaje_error: str | None = None) -> str:
    now = build_control_record_timestamp()
    record = pl.DataFrame([
        {
            'fuente': 'sunat',
            'modulo': 'agro_filter',
            'dataset': OUTPUT_DATASET,
            'scope_tipo': '',
            'scope_valor': '',
            'modo_carga': get_settings().sunat_modo_carga,
            'fecha_inicio_solicitada': fecha_inicio,
            'fecha_fin_solicitada': fecha_fin,
            'fecha_inicio_ejecutada': fecha_inicio,
            'fecha_fin_ejecutada': fecha_fin,
            'ultima_fecha_exitosa': ultima_fecha_exitosa,
            'estado': estado,
            'mensaje_error': mensaje_error,
            'ejecutado_por': 'sunat_file',
            'fecha_ejecucion': now,
            'fecha_actualizacion': now,
        }
    ])
    return upsert_control_records(record)


def _persist_control_event(
    fecha_inicio: date,
    fecha_fin: date,
    estado: str,
    ultima_fecha_exitosa: date | None = None,
    mensaje_error: str | None = None,
) -> str:
    now = build_control_record_timestamp()
    event_df = pl.DataFrame([
        {
            'evento_id': build_control_event_id(),
            'fuente': 'sunat',
            'modulo': 'agro_filter',
            'dataset': OUTPUT_DATASET,
            'scope_tipo': '',
            'scope_valor': '',
            'modo_carga': get_settings().sunat_modo_carga,
            'fecha_inicio_solicitada': fecha_inicio,
            'fecha_fin_solicitada': fecha_fin,
            'fecha_inicio_ejecutada': fecha_inicio,
            'fecha_fin_ejecutada': fecha_fin,
            'ultima_fecha_exitosa': ultima_fecha_exitosa,
            'estado': estado,
            'mensaje_error': mensaje_error or '',
            'ejecutado_por': 'sunat_file',
            'fecha_ejecucion': now,
            'fecha_actualizacion': now,
        }
    ])
    return append_control_events(event_df)


def run_filter_agro() -> dict[str, str | int]:
    settings = get_settings()
    source_path = _source_path()
    resolved_dates = _resolve_processing_dates()
    clean_path = _clean_path()
    fecha_consolidacion = date.today().isoformat()
    if not source_path.exists():
        if resolved_dates is not None:
            fecha_inicio, fecha_fin = resolved_dates
            _persist_control(fecha_inicio, fecha_fin, None, 'no_data', f'no_existe_base_fuente:{source_path}')
            _persist_control_event(fecha_inicio, fecha_fin, 'empty', None, f'no_existe_base_fuente:{source_path}')
        return {
            'rows': 0,
            'raw_path': '',
            'preview_path': '',
            'resumen_path': '',
            'resumen_subpartidas_path': '',
            'catalog_path': '',
            'territory_path': '',
            'region_summary_path': '',
            'ubigeo_quality_path': '',
            'diccionario_path': '',
            'clean_path': str(clean_path),
        }

    if resolved_dates is None:
        return {
            'rows': 0,
            'raw_path': '',
            'preview_path': '',
            'resumen_path': '',
            'resumen_subpartidas_path': '',
            'catalog_path': '',
            'territory_path': '',
            'region_summary_path': '',
            'ubigeo_quality_path': '',
            'diccionario_path': '',
            'clean_path': str(clean_path),
        }

    fecha_inicio, fecha_fin = resolved_dates
    try:
        if settings.is_minio:
            from sunat_file.jobs.import_job import ZIP_CONSOLIDATED_DATASET
            source_uri = settings.build_delta_uri(ZIP_CONSOLIDATED_DATASET)
            logger.info(f"Leyendo base Delta desde MinIO: {source_uri}")
            source_df = pl.read_delta(
                source_uri,
                storage_options=settings.delta_storage_options
            )
        else:
            source_df = pl.scan_parquet(source_path / '**/*.parquet').collect()
        
        logger.info(f"Procesando {source_df.height} registros base para filtrado agrícola")
        fresh_df = build_sunat_exportaciones_frescas(source_df)
        if fresh_df.is_empty():
            logger.warning("No se encontraron registros que cumplan los criterios de 'agrícola fresco'")
            _persist_control(fecha_inicio, fecha_fin, None, 'no_data', 'sin_registros_frescos_en_base')
            _persist_control_event(fecha_inicio, fecha_fin, 'empty', None, 'sin_registros_frescos_en_base')
            return {
                'rows': 0,
                'base_rows': source_df.height,
                'raw_path': '',
                'preview_path': '',
                'resumen_path': '',
                'resumen_subpartidas_path': '',
                'catalog_path': '',
                'territory_path': '',
                'region_summary_path': '',
                'ubigeo_quality_path': '',
                'diccionario_path': '',
                'clean_path': str(_clean_path()),
            }

        # Asegurar que comparamos fechas contra fechas
        window_df = fresh_df.filter(
            (pl.col('fecha') >= fecha_inicio) & 
            (pl.col('fecha') <= fecha_fin)
        )

        if window_df.is_empty():
            _persist_control(fecha_inicio, fecha_fin, None, 'no_data', 'sin_registros_en_rango')
            _persist_control_event(fecha_inicio, fecha_fin, 'empty', None, 'sin_registros_en_rango')
            return {
                'rows': 0,
                'base_rows': source_df.height,
                'raw_path': '',
                'preview_path': '',
                'resumen_path': '',
                'resumen_subpartidas_path': '',
                'catalog_path': '',
                'territory_path': '',
                'region_summary_path': '',
                'ubigeo_quality_path': '',
                'diccionario_path': '',
                'clean_path': str(clean_path),
            }

        # Añadir metadatos de auditoría al tablón maestro
        fecha_proceso_str = date.today().isoformat()
        merged_df = window_df.with_columns(
            pl.lit(fecha_proceso_str).alias('fecha_proceso')
        )

        # Ingestión atómica incremental en el Tablón Maestro Delta
        # Se particiona por fecha_particion (la fecha real del archivo SUNAT)
        # El modo incremental permite ir acumulando historial de diferentes ZIPs
        table_uri = save_delta_table(
            merged_df, 
            'exportaciones_filtradas/tablon_maestro_agrario', 
            ['fecha_particion'],
            overwrite=False
        )

        review_dir = settings.consolidacion_agricola_dir / 'reportes'
        preview_path, resumen_path, resumen_subpartidas_path = build_review_files(merged_df, review_dir)
        catalog_path = build_catalog_file(merged_df, review_dir)
        territory_path = build_territory_catalog(merged_df, review_dir)
        region_summary_path = build_region_summary(merged_df, review_dir)
        ubigeo_quality_path = build_ubigeo_quality_report(merged_df, review_dir)
        dict_path = build_data_dictionary(review_dir)

        ultima_fecha_exitosa = window_df.get_column('fecha').max()
        _persist_control(fecha_inicio, fecha_fin, ultima_fecha_exitosa, 'success')
        _persist_control_event(fecha_inicio, fecha_fin, 'success', ultima_fecha_exitosa)

        return {
            'rows': window_df.height,
            'base_rows': source_df.height,
            'raw_path': '',
            'preview_path': str(preview_path),
            'resumen_path': str(resumen_path),
            'resumen_subpartidas_path': str(resumen_subpartidas_path),
            'catalog_path': str(catalog_path),
            'territory_path': str(territory_path),
            'region_summary_path': str(region_summary_path),
            'ubigeo_quality_path': str(ubigeo_quality_path),
            'diccionario_path': str(dict_path),
            'clean_path': table_uri,
        }
    except Exception as exc:
        logger.exception('Fallo filtrando el dataset agrario fresco SUNAT')
        _persist_control(fecha_inicio, fecha_fin, None, 'error', str(exc))
        _persist_control_event(fecha_inicio, fecha_fin, 'error', None, str(exc))
        raise
