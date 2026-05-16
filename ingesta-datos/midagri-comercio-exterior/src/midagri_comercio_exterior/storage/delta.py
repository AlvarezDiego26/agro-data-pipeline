from __future__ import annotations

from pathlib import Path
from threading import RLock

import polars as pl

from midagri_comercio_exterior.config import get_settings
from midagri_comercio_exterior.storage.merge import (
    business_key_columns,
    deduplicate_dataset,
    normalize_dataset,
)

DELTA_RUNTIME_LOCK = RLock()
_DELTA_RUNTIME: tuple[object, object] | None = None


def get_delta_runtime() -> tuple[object, object]:
    global _DELTA_RUNTIME
    with DELTA_RUNTIME_LOCK:
        if _DELTA_RUNTIME is None:
            from deltalake import DeltaTable
            from deltalake.writer import write_deltalake

            _DELTA_RUNTIME = (DeltaTable, write_deltalake)
        return _DELTA_RUNTIME


def _merge_predicate(dataset_name: str, columns: list[str]) -> str | None:
    keys = business_key_columns(dataset_name, columns)
    if not keys:
        return None
    return " AND ".join(f"target.{column} = source.{column}" for column in keys)


def _quote_identifier(column: str) -> str:
    escaped = column.replace("`", "``")
    return f"`{escaped}`"


def _change_predicate(dataset_name: str, columns: list[str]) -> str | None:
    business_keys = set(business_key_columns(dataset_name, columns))
    comparable_columns = [column for column in columns if column not in business_keys]
    if not comparable_columns:
        return None

    comparisons = []
    for column in comparable_columns:
        quoted = _quote_identifier(column)
        comparisons.append(
            "("
            f"(target.{quoted} IS NULL AND source.{quoted} IS NOT NULL) OR "
            f"(target.{quoted} IS NOT NULL AND source.{quoted} IS NULL) OR "
            f"(target.{quoted} != source.{quoted})"
            ")"
        )
    return " OR ".join(comparisons)


def save_delta_table(df: pl.DataFrame, dataset_name: str, partition_cols: list[str], overwrite: bool = False) -> str:
    if df.is_empty():
        return ""

    settings = get_settings()
    table_uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options
    DeltaTable, write_deltalake = get_delta_runtime()

    with DELTA_RUNTIME_LOCK:
        source_df = normalize_dataset(df, dataset_name)
        source_df = deduplicate_dataset(source_df, dataset_name)
        merge_predicate = _merge_predicate(dataset_name, source_df.columns)

        try:
            existing_table = DeltaTable(table_uri, storage_options=storage_options) if not overwrite else None
        except Exception:
            existing_table = None

        if existing_table is not None:
            if not merge_predicate:
                raise ValueError(
                    f"No se puede hacer merge incremental en '{dataset_name}': no se encontraron llaves de negocio validas."
                )
            change_predicate = _change_predicate(dataset_name, source_df.columns)
            merge_builder = existing_table.merge(
                source=source_df.to_arrow(),
                predicate=merge_predicate,
                source_alias="source",
                target_alias="target",
            )
            if change_predicate:
                merge_builder = merge_builder.when_matched_update_all(predicate=change_predicate)
            merge_builder.when_not_matched_insert_all().execute()
            return table_uri

        if not settings.is_minio:
            Path(table_uri).mkdir(parents=True, exist_ok=True)

        write_deltalake(
            table_uri,
            source_df.to_arrow(),
            mode="overwrite",
            partition_by=partition_cols,
            storage_options=storage_options,
        )
        return table_uri
