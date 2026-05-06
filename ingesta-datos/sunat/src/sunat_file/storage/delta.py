from pathlib import Path

import polars as pl

from sunat_file.config import get_settings


def save_delta_table(df: pl.DataFrame, dataset_name: str, partition_cols: list[str]) -> str:
    if df.is_empty():
        return ''
    settings = get_settings()
    table_uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options
    from deltalake import DeltaTable
    from deltalake.writer import write_deltalake

    merged_df = df
    try:
        existing_table = DeltaTable(table_uri, storage_options=storage_options)
        existing_df = pl.from_arrow(existing_table.to_pyarrow_table())
        if not existing_df.is_empty():
            merged_df = pl.concat([existing_df, df], how='diagonal_relaxed').unique()
    except Exception:
        pass

    if not settings.is_minio:
        Path(table_uri).mkdir(parents=True, exist_ok=True)

    write_deltalake(
        table_uri,
        merged_df.to_arrow(),
        mode='overwrite',
        schema_mode='overwrite',
        partition_by=partition_cols,
        storage_options=storage_options,
    )
    return table_uri
