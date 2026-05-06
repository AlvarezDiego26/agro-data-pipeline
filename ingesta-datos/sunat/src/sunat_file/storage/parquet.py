from pathlib import Path

import polars as pl

from sunat_file.config import get_settings

PARTITION_FILE_NAME = 'data.parquet'


def save_parquet(df: pl.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    return output_path


def save_raw_parquet(df: pl.DataFrame, dataset_name: str) -> Path:
    settings = get_settings()
    path = settings.raw_dir / f'{dataset_name}.parquet'
    merged_df = df
    if path.exists():
        existing_df = pl.read_parquet(path)
        merged_df = pl.concat([existing_df, df], how='diagonal_relaxed').unique()
    save_parquet(merged_df, path)
    return path


def save_partitioned_parquet(df: pl.DataFrame, base_dir: Path, partition_cols: list[str]) -> None:
    if df.is_empty() or not partition_cols:
        if df.is_empty():
            return
        save_parquet(df, base_dir.with_suffix('.parquet'))
        return
    base_dir.mkdir(parents=True, exist_ok=True)
    partitions = df.select(partition_cols).unique().to_dicts()
    for part in partitions:
        partition_df = df
        folder = base_dir
        for col in partition_cols:
            value = part[col]
            folder = folder / f'{col}={value}'
            partition_df = partition_df.filter(pl.col(col) == value)
        output_path = folder / PARTITION_FILE_NAME
        folder.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            existing_df = pl.read_parquet(output_path)
            partition_df = pl.concat([existing_df, partition_df], how='diagonal_relaxed').unique()
        partition_df.write_parquet(output_path)
