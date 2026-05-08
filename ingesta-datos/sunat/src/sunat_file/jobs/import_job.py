from __future__ import annotations

import shutil
import re

import polars as pl
from loguru import logger

from sunat_file.config import get_settings
from sunat_file.jobs.scanner import scan_inbox
from sunat_file.readers.files import extract_supported_zip_members, read_supported_file
from sunat_file.readers.remote import download_remote_file, fetch_remote_listing
from sunat_file.storage.control import (
    append_control_events,
    build_control_event_id,
    build_control_record_timestamp,
    list_scope_values_by_status,
    upsert_control_records,
)
from sunat_file.storage.delta import save_delta_table
from sunat_file.storage.parquet import save_raw_parquet
from sunat_file.transformers.base import normalize_columns

ZIP_CONSOLIDATED_DATASET = 'sunat_exportaciones_base'
REMOTE_LISTING_DATASET = 'sunat_remote_files'
SUPPORTED_DIRECT_EXTENSIONS = {'.dbf'}
SKIPPED_DIRECT_EXTENSIONS = {'.xlsx', '.xlsm', '.xls'}
REMOTE_NAME_PATTERN = re.compile(r'^(?:x)(\d{2})(\d{2})(\d{2})(\d{2})\.zip$', re.IGNORECASE)


def _is_target_export_zip(file_name: str) -> bool:
    return REMOTE_NAME_PATTERN.match(file_name) is not None


def _persist_import_event(source_name: str, estado: str, mensaje_error: str | None = None) -> None:
    now = build_control_record_timestamp()
    event_df = pl.DataFrame([
        {
            'evento_id': build_control_event_id(),
            'fuente': 'sunat',
            'modulo': 'import',
            'dataset': ZIP_CONSOLIDATED_DATASET,
            'scope_tipo': 'archivo',
            'scope_valor': source_name,
            'modo_carga': get_settings().sunat_modo_carga,
            'fecha_inicio_solicitada': None,
            'fecha_fin_solicitada': None,
            'fecha_inicio_ejecutada': None,
            'fecha_fin_ejecutada': None,
            'ultima_fecha_exitosa': None,
            'estado': estado,
            'mensaje_error': mensaje_error or '',
            'ejecutado_por': 'sunat_file',
            'fecha_ejecucion': now,
            'fecha_actualizacion': now,
        }
    ])
    append_control_events(event_df)


def _persist_import_control(source_name: str, estado: str, mensaje_error: str | None = None) -> None:
    now = build_control_record_timestamp()
    control_df = pl.DataFrame([
        {
            'fuente': 'sunat',
            'modulo': 'import',
            'dataset': ZIP_CONSOLIDATED_DATASET,
            'scope_tipo': 'archivo',
            'scope_valor': source_name,
            'modo_carga': get_settings().sunat_modo_carga,
            'fecha_inicio_solicitada': None,
            'fecha_fin_solicitada': None,
            'fecha_inicio_ejecutada': None,
            'fecha_fin_ejecutada': None,
            'ultima_fecha_exitosa': None,
            'estado': estado,
            'mensaje_error': mensaje_error or '',
            'ejecutado_por': 'sunat_file',
            'fecha_ejecucion': now,
            'fecha_actualizacion': now,
        }
    ])
    upsert_control_records(control_df)


def _persist_download_event(source_name: str, estado: str, mensaje_error: str | None = None) -> None:
    now = build_control_record_timestamp()
    event_df = pl.DataFrame([
        {
            'evento_id': build_control_event_id(),
            'fuente': 'sunat',
            'modulo': 'download',
            'dataset': REMOTE_LISTING_DATASET,
            'scope_tipo': 'archivo_remoto',
            'scope_valor': source_name,
            'modo_carga': get_settings().sunat_modo_carga,
            'fecha_inicio_solicitada': None,
            'fecha_fin_solicitada': None,
            'fecha_inicio_ejecutada': None,
            'fecha_fin_ejecutada': None,
            'ultima_fecha_exitosa': None,
            'estado': estado,
            'mensaje_error': mensaje_error or '',
            'ejecutado_por': 'sunat_file',
            'fecha_ejecucion': now,
            'fecha_actualizacion': now,
        }
    ])
    append_control_events(event_df)


def _persist_download_control(source_name: str, estado: str, mensaje_error: str | None = None) -> None:
    now = build_control_record_timestamp()
    control_df = pl.DataFrame([
        {
            'fuente': 'sunat',
            'modulo': 'download',
            'dataset': REMOTE_LISTING_DATASET,
            'scope_tipo': 'archivo_remoto',
            'scope_valor': source_name,
            'modo_carga': get_settings().sunat_modo_carga,
            'fecha_inicio_solicitada': None,
            'fecha_fin_solicitada': None,
            'fecha_inicio_ejecutada': None,
            'fecha_fin_ejecutada': None,
            'ultima_fecha_exitosa': None,
            'estado': estado,
            'mensaje_error': mensaje_error or '',
            'ejecutado_por': 'sunat_file',
            'fecha_ejecucion': now,
            'fecha_actualizacion': now,
        }
    ])
    upsert_control_records(control_df)


def _persist_download_source_status(estado: str, mensaje_error: str | None = None) -> None:
    source_page = get_settings().sunat_source_page_url
    _persist_download_control(source_page, estado, mensaje_error)
    _persist_download_event(source_page, estado, mensaje_error)


def _extract_remote_period_metadata(source_name: str) -> tuple[int | None, str | None]:
    match = REMOTE_NAME_PATTERN.match(source_name)
    if match is None:
        return None, None
    _, _, month_raw, year_raw = match.groups()
    year = 2000 + int(year_raw)
    month = f'{int(month_raw):02d}'
    return year, month


def _with_lineage(raw_df: pl.DataFrame, source_file, member_file=None) -> pl.DataFrame:
    member_name = member_file.name if member_file is not None else None
    archivo_anio_publicacion, archivo_mes_publicacion = _extract_remote_period_metadata(source_file.name)
    lineage_df = raw_df.with_columns(
        pl.lit(source_file.name).alias('archivo_origen'),
        pl.lit(member_name).alias('archivo_miembro'),
        pl.lit(source_file.suffix.lower()).alias('tipo_archivo_origen'),
        pl.lit(archivo_anio_publicacion).cast(pl.Int32, strict=False).alias('archivo_anio_publicacion'),
        pl.lit(archivo_mes_publicacion).alias('archivo_mes_publicacion'),
    )
    hash_columns = sorted(lineage_df.columns)
    return lineage_df.with_columns(
        pl.concat_str(
            [pl.col(column).cast(pl.Utf8, strict=False).fill_null('') for column in hash_columns],
            separator='|',
        ).hash().cast(pl.Utf8).alias('registro_hash_fuente')
    )


def _persist_base_dataset(df: pl.DataFrame) -> None:
    raw_df = normalize_columns(df)
    save_raw_parquet(raw_df, ZIP_CONSOLIDATED_DATASET)
    settings = get_settings()
    clean_path = settings.clean_dir / f'{ZIP_CONSOLIDATED_DATASET}.parquet'
    merged_df = raw_df
    if clean_path.exists():
        existing_df = pl.read_parquet(clean_path)
        merged_df = pl.concat([existing_df, raw_df], how='diagonal_relaxed').unique(
            subset=['registro_hash_fuente'] if 'registro_hash_fuente' in raw_df.columns else None
        )
    merged_df.write_parquet(clean_path)
    if settings.sunat_delta_enabled:
        save_delta_table(
            raw_df,
            ZIP_CONSOLIDATED_DATASET,
            ['archivo_anio_publicacion', 'archivo_mes_publicacion'],
        )


def _persist_source_dataset(frames: list[pl.DataFrame]) -> list[str]:
    if not frames:
        return []
    merged_source_df = pl.concat(frames, how='diagonal_relaxed').unique(
        subset=['registro_hash_fuente'] if 'registro_hash_fuente' in frames[0].columns else None
    )
    _persist_base_dataset(merged_source_df)
    return [ZIP_CONSOLIDATED_DATASET]


def _existing_remote_file_names() -> set[str]:
    settings = get_settings()
    known_names: set[str] = set()
    for folder in (settings.sunat_inbox_dir, settings.sunat_processed_dir):
        if not folder.exists():
            continue
        known_names.update(path.name.upper() for path in folder.glob('*') if path.is_file())
    return known_names


def sync_remote_files_to_inbox() -> list[str]:
    settings = get_settings()
    downloaded: list[str] = []
    try:
        discovered_files = fetch_remote_listing()
        if discovered_files:
            _persist_download_source_status('success', f'archivos_detectados={len(discovered_files)}')
        else:
            _persist_download_source_status('no_data', 'sin_archivos_remotos_detectados')
    except Exception as exc:
        _persist_download_source_status('error', str(exc))
        raise
    successful_imports = list_scope_values_by_status(
        fuente='sunat',
        modulo='import',
        dataset=ZIP_CONSOLIDATED_DATASET,
        scope_tipo='archivo',
        estados={'success'},
    )
    existing_names = _existing_remote_file_names()

    for remote_file in discovered_files:
        file_name = remote_file.file_name.upper()
        if file_name in successful_imports or file_name in existing_names:
            _persist_download_event(file_name, 'skipped', 'archivo_ya_descargado')
            continue
        try:
            temp_path = download_remote_file(remote_file, settings.sunat_downloads_dir)
            inbox_path = settings.sunat_inbox_dir / remote_file.file_name
            if inbox_path.exists():
                inbox_path.unlink()
            temp_path.replace(inbox_path)
            _persist_download_control(file_name, 'success')
            _persist_download_event(file_name, 'success')
            downloaded.append(remote_file.file_name)
        except Exception as exc:
            logger.exception('Fallo descargando archivo remoto SUNAT {name}', name=remote_file.file_name)
            _persist_download_control(file_name, 'error', str(exc))
            _persist_download_event(file_name, 'error', str(exc))

    return downloaded


def _process_direct_dbf(source_path) -> list[str]:
    df = read_supported_file(source_path)
    df = _with_lineage(df, source_path)
    return _persist_source_dataset([df])


def _process_zip_file(source_path) -> list[str]:
    temp_dir, members = extract_supported_zip_members(source_path)
    frames: list[pl.DataFrame] = []
    try:
        if not members:
            raise ValueError('El zip no contiene archivos .dbf soportados para el flujo final SUNAT.')
        for member in members:
            df = read_supported_file(member)
            df = _with_lineage(df, source_path, member)
            frames.append(df)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return _persist_source_dataset(frames)


def run_import() -> list[str]:
    settings = get_settings()
    processed: list[str] = []
    for file in scan_inbox():
        try:
            logger.info('Procesando archivo {name}', name=file.source_name)
            if file.extension == '.zip':
                if not _is_target_export_zip(file.source_name.upper()):
                    shutil.move(str(file.path), str(settings.sunat_processed_dir / file.path.name))
                    _persist_import_control(file.source_name, 'skipped', 'zip_fuera_de_alcance_exportacion')
                    _persist_import_event(file.source_name, 'skipped', 'zip_fuera_de_alcance_exportacion')
                    processed.append(f'{file.source_name}: SKIPPED zip fuera del alcance de exportacion')
                    continue
                dataset_names = _process_zip_file(file.path)
            elif file.extension in SUPPORTED_DIRECT_EXTENSIONS:
                dataset_names = _process_direct_dbf(file.path)
            elif file.extension in SKIPPED_DIRECT_EXTENSIONS:
                shutil.move(str(file.path), str(settings.sunat_processed_dir / file.path.name))
                _persist_import_control(file.source_name, 'skipped', 'formato_complementario_fuera_de_flujo')
                _persist_import_event(file.source_name, 'skipped', 'formato_complementario_fuera_de_flujo')
                processed.append(f'{file.source_name}: SKIPPED formato complementario fuera del flujo final')
                continue
            else:
                logger.warning('Archivo no soportado: {name}', name=file.source_name)
                _persist_import_control(file.source_name, 'skipped', 'archivo_no_soportado')
                _persist_import_event(file.source_name, 'skipped', 'archivo_no_soportado')
                continue

            shutil.move(str(file.path), str(settings.sunat_processed_dir / file.path.name))
            _persist_import_control(file.source_name, 'success')
            _persist_import_event(file.source_name, 'success')
            for dataset_name in dataset_names:
                processed.append(f'{file.source_name} -> {dataset_name}')
        except Exception as exc:
            logger.exception('Fallo procesando {name}', name=file.source_name)
            shutil.move(str(file.path), str(settings.sunat_error_dir / file.path.name))
            _persist_import_control(file.source_name, 'error', str(exc))
            _persist_import_event(file.source_name, 'error', str(exc))
            processed.append(f'{file.source_name}: ERROR {exc}')
    return processed
