from __future__ import annotations

from pathlib import Path

import polars as pl

from sunat_file.config import get_settings
from sunat_file.storage.delta import save_delta_table
from sunat_file.storage.parquet import save_parquet, save_raw_parquet
from sunat_file.transformers.agro import (
    build_catalog_file,
    build_data_dictionary,
    build_region_summary,
    build_review_files,
    build_sunat_exportaciones_frescas,
    build_territory_catalog,
    build_ubigeo_quality_report,
)

SOURCE_DATASET = 'sunat_exportaciones_base.parquet'
OUTPUT_DATASET = 'sunat_exportaciones_agrarias_frescas'
OUTPUT_FILE = f'{OUTPUT_DATASET}.parquet'


def _source_path() -> Path:
    settings = get_settings()
    return settings.clean_dir / SOURCE_DATASET


def run_filter_agro() -> dict[str, str | int]:
    settings = get_settings()
    source_path = _source_path()
    if not source_path.exists():
        raise FileNotFoundError(f'No existe la base fuente esperada: {source_path}')

    source_df = pl.read_parquet(source_path)
    fresh_df = build_sunat_exportaciones_frescas(source_df)
    if fresh_df.is_empty():
        raise ValueError('No se encontraron registros agrarios frescos en la base SUNAT actual.')

    raw_path = save_raw_parquet(fresh_df, f'{OUTPUT_DATASET}_raw')
    clean_path = settings.clean_dir / OUTPUT_FILE
    save_parquet(fresh_df, clean_path)
    if settings.sunat_delta_enabled:
        save_delta_table(fresh_df, OUTPUT_DATASET, ['anio', 'mes'])

    review_dir = settings.data_dir / 'review'
    preview_path, resumen_path, resumen_subpartidas_path = build_review_files(fresh_df, review_dir)
    catalog_path = build_catalog_file(fresh_df, review_dir)
    territory_path = build_territory_catalog(fresh_df, review_dir)
    region_summary_path = build_region_summary(fresh_df, review_dir)
    ubigeo_quality_path = build_ubigeo_quality_report(fresh_df, review_dir)
    dict_path = build_data_dictionary(review_dir)

    return {
        'rows': fresh_df.height,
        'raw_path': str(raw_path),
        'preview_path': str(preview_path),
        'resumen_path': str(resumen_path),
        'resumen_subpartidas_path': str(resumen_subpartidas_path),
        'catalog_path': str(catalog_path),
        'territory_path': str(territory_path),
        'region_summary_path': str(region_summary_path),
        'ubigeo_quality_path': str(ubigeo_quality_path),
        'diccionario_path': str(dict_path),
        'clean_path': str(clean_path),
    }
