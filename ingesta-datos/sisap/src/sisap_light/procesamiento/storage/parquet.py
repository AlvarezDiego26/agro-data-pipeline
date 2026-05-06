from pathlib import Path

import polars as pl

from sisap_light.config import get_settings


PARTITION_FILE_NAME = "data.parquet"


def save_parquet(df: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)


def save_partitioned_parquet(df: pl.DataFrame, base_dir: Path, partition_cols: list[str]) -> None:
    if df.is_empty():
        return

    base_dir.mkdir(parents=True, exist_ok=True)
    partitions = df.select(partition_cols).unique().to_dicts()

    for part in partitions:
        partition_df = df
        folder = base_dir
        for col in partition_cols:
            value = part[col]
            folder = folder / f"{col}={value}"
            partition_df = partition_df.filter(pl.col(col) == value)

        output_path = folder / PARTITION_FILE_NAME
        folder.mkdir(parents=True, exist_ok=True)

        if output_path.exists():
            existing_df = pl.read_parquet(output_path)
            partition_df = pl.concat([existing_df, partition_df], how="vertical_relaxed").unique()

        partition_df.write_parquet(output_path)


def save_raw_parquet(df: pl.DataFrame, dataset_name: str) -> Path:
    settings = get_settings()
    output_path = settings.raw_dir / f"{dataset_name}.parquet"
    save_parquet(df, output_path)
    return output_path

