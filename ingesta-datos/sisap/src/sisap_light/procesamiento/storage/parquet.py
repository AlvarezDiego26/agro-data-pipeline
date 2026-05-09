from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import pyarrow.parquet as pq
from pyarrow import fs

from sisap_light.config import get_settings
from sisap_light.procesamiento.storage.merge import deduplicate_dataset


PARTITION_FILE_NAME = "data.parquet"


def save_parquet(df: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)


def _build_s3_filesystem() -> fs.S3FileSystem:
    settings = get_settings()
    endpoint = urlparse(settings.minio_endpoint)
    return fs.S3FileSystem(
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        region=settings.minio_region,
        scheme=endpoint.scheme or "http",
        endpoint_override=endpoint.netloc,
    )


def _build_minio_object_path(base_dir: Path, partition_folder: Path) -> str:
    settings = get_settings()
    relative_base = base_dir.relative_to(settings.clean_dir).as_posix().strip("/")
    relative_partition = partition_folder.relative_to(base_dir).as_posix().strip("/")
    prefix = settings.minio_prefix.strip("/")
    parts = [settings.minio_bucket]
    if prefix:
        parts.append(prefix)
    if relative_base:
        parts.append(relative_base)
    if relative_partition:
        parts.append(relative_partition)
    parts.append(PARTITION_FILE_NAME)
    return "/".join(parts)


def _read_minio_parquet(s3_fs: fs.S3FileSystem, object_path: str) -> pl.DataFrame | None:
    info = s3_fs.get_file_info(object_path)
    if info.type != fs.FileType.File:
        return None
    with s3_fs.open_input_file(object_path) as source:
        table = pq.read_table(source)
    return pl.from_arrow(table)


def _write_minio_parquet(s3_fs: fs.S3FileSystem, object_path: str, df: pl.DataFrame) -> None:
    with s3_fs.open_output_stream(object_path) as sink:
        pq.write_table(df.to_arrow(), sink)


def save_partitioned_parquet(
    df: pl.DataFrame,
    dataset_name: str,
    base_dir: Path,
    partition_cols: list[str],
) -> None:
    if df.is_empty():
        return

    settings = get_settings()
    if not settings.is_minio:
        base_dir.mkdir(parents=True, exist_ok=True)

    partitions = df.select(partition_cols).unique().to_dicts()
    s3_fs = _build_s3_filesystem() if settings.is_minio else None

    for part in partitions:
        partition_df = df
        folder = base_dir
        for col in partition_cols:
            value = part[col]
            folder = folder / f"{col}={value}"
            partition_df = partition_df.filter(pl.col(col) == value)

        output_path = folder / PARTITION_FILE_NAME
        if settings.is_minio:
            object_path = _build_minio_object_path(base_dir, folder)
            existing_df = _read_minio_parquet(s3_fs, object_path)
            if existing_df is not None:
                partition_df = deduplicate_dataset(
                    pl.concat([existing_df, partition_df], how="vertical_relaxed"),
                    dataset_name,
                )
            else:
                partition_df = deduplicate_dataset(partition_df, dataset_name)
            _write_minio_parquet(s3_fs, object_path, partition_df)
            continue

        folder.mkdir(parents=True, exist_ok=True)

        if output_path.exists():
            existing_df = pl.read_parquet(output_path)
            partition_df = deduplicate_dataset(
                pl.concat([existing_df, partition_df], how="vertical_relaxed"),
                dataset_name,
            )
        else:
            partition_df = deduplicate_dataset(partition_df, dataset_name)

        partition_df.write_parquet(output_path)


def save_raw_parquet(df: pl.DataFrame, dataset_name: str) -> Path:
    settings = get_settings()
    output_path = settings.raw_dir / f"{dataset_name}.parquet"
    save_parquet(df, output_path)
    return output_path

