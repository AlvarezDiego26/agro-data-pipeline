from __future__ import annotations

from pathlib import Path
from threading import RLock

import polars as pl

from sisap_light.config import get_settings
from sisap_light.procesamiento.storage.merge import business_key_columns, deduplicate_dataset

DELTA_RUNTIME_LOCK = RLock()
_DELTA_RUNTIME: tuple[object, object] | None = None


def get_delta_lock() -> RLock:
    return DELTA_RUNTIME_LOCK


def get_delta_runtime() -> tuple[object, object]:
    global _DELTA_RUNTIME

    with DELTA_RUNTIME_LOCK:
        if _DELTA_RUNTIME is None:
            from deltalake import DeltaTable
            from deltalake.writer import write_deltalake

            _DELTA_RUNTIME = (DeltaTable, write_deltalake)
        return _DELTA_RUNTIME


def warm_delta_runtime() -> None:
    settings = get_settings()
    if not settings.delta_enabled:
        return
    get_delta_runtime()


def _merge_predicate(dataset_name: str, columns: list[str]) -> str | None:
    keys = business_key_columns(dataset_name, columns)
    if not keys:
        return None
    return " AND ".join(
        f"target.{column} = source.{column}"
        for column in keys
    )


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


def save_delta_table(df: pl.DataFrame, dataset_name: str, partition_cols: list[str]) -> str:
    if df.is_empty():
        return ""

    settings = get_settings()
    table_uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options

    DeltaTable, write_deltalake = get_delta_runtime()

    with DELTA_RUNTIME_LOCK:
        source_df = deduplicate_dataset(df, dataset_name)
        merge_predicate = _merge_predicate(dataset_name, source_df.columns)
        existing_table = None

        try:
            existing_table = DeltaTable(table_uri, storage_options=storage_options)
        except Exception:
            existing_table = None

        if existing_table is not None:
            if merge_predicate:
                try:
                    change_predicate = _change_predicate(dataset_name, source_df.columns)
                    (
                        existing_table.merge(
                            source=source_df.to_arrow(),
                            predicate=merge_predicate,
                            source_alias="source",
                            target_alias="target",
                        )
                        .when_matched_update_all(predicate=change_predicate)
                        .when_not_matched_insert_all()
                        .execute()
                    )
                    return table_uri
                except Exception:
                    pass

            existing_df = pl.from_arrow(existing_table.to_pyarrow_table())
            source_df = deduplicate_dataset(
                pl.concat([existing_df, source_df], how="vertical_relaxed"),
                dataset_name,
            )

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

