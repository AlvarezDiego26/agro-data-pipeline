from __future__ import annotations

import shutil

import polars as pl
from loguru import logger

from sunat_file.config import get_settings
from sunat_file.jobs.scanner import scan_inbox
from sunat_file.readers.files import extract_supported_zip_members, read_supported_file
from sunat_file.storage.parquet import save_raw_parquet
from sunat_file.transformers.base import normalize_columns

ZIP_CONSOLIDATED_DATASET = 'sunat_exportaciones_base'
SUPPORTED_DIRECT_EXTENSIONS = {'.dbf'}
SKIPPED_DIRECT_EXTENSIONS = {'.xlsx', '.xlsm', '.xls'}


def _with_lineage(raw_df: pl.DataFrame, source_file, member_file=None) -> pl.DataFrame:
    member_name = member_file.name if member_file is not None else None
    return raw_df.with_columns(
        pl.lit(source_file.name).alias('archivo_origen'),
        pl.lit(member_name).alias('archivo_miembro'),
        pl.lit(source_file.suffix.lower()).alias('tipo_archivo_origen'),
    )


def _persist_base_dataset(df: pl.DataFrame) -> None:
    raw_df = normalize_columns(df)
    save_raw_parquet(raw_df, ZIP_CONSOLIDATED_DATASET)
    settings = get_settings()
    raw_df.write_parquet(settings.clean_dir / f'{ZIP_CONSOLIDATED_DATASET}.parquet')


def _process_direct_dbf(source_path) -> list[str]:
    df = read_supported_file(source_path)
    df = _with_lineage(df, source_path)
    _persist_base_dataset(df)
    return [ZIP_CONSOLIDATED_DATASET]


def _process_zip_file(source_path) -> list[str]:
    temp_dir, members = extract_supported_zip_members(source_path)
    processed = []
    try:
        if not members:
            raise ValueError('El zip no contiene archivos .dbf soportados para el flujo final SUNAT.')
        for member in members:
            df = read_supported_file(member)
            df = _with_lineage(df, source_path, member)
            _persist_base_dataset(df)
            if ZIP_CONSOLIDATED_DATASET not in processed:
                processed.append(ZIP_CONSOLIDATED_DATASET)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return processed


def run_import() -> list[str]:
    settings = get_settings()
    processed: list[str] = []
    for file in scan_inbox():
        try:
            logger.info('Procesando archivo {name}', name=file.source_name)
            if file.extension == '.zip':
                dataset_names = _process_zip_file(file.path)
            elif file.extension in SUPPORTED_DIRECT_EXTENSIONS:
                dataset_names = _process_direct_dbf(file.path)
            elif file.extension in SKIPPED_DIRECT_EXTENSIONS:
                shutil.move(str(file.path), str(settings.sunat_processed_dir / file.path.name))
                processed.append(f'{file.source_name}: SKIPPED formato complementario fuera del flujo final')
                continue
            else:
                logger.warning('Archivo no soportado: {name}', name=file.source_name)
                continue

            shutil.move(str(file.path), str(settings.sunat_processed_dir / file.path.name))
            for dataset_name in dataset_names:
                processed.append(f'{file.source_name} -> {dataset_name}')
        except Exception as exc:
            logger.exception('Fallo procesando {name}', name=file.source_name)
            shutil.move(str(file.path), str(settings.sunat_error_dir / file.path.name))
            processed.append(f'{file.source_name}: ERROR {exc}')
    return processed
