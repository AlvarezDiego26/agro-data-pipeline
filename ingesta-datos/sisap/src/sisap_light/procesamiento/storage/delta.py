from __future__ import annotations

from pathlib import Path
from threading import RLock
from time import sleep

import polars as pl
from loguru import logger

from sisap_light.config import get_settings
from sisap_light.procesamiento.storage.merge import (
    business_key_columns,
    deduplicate_dataset,
    normalize_dataset,
)

_DELTA_RUNTIME_INIT_LOCK = RLock()
_DELTA_TABLE_LOCKS_GUARD = RLock()
_DELTA_TABLE_LOCKS: dict[str, RLock] = {}
_DELTA_RUNTIME: tuple[object, object] | None = None


def get_delta_lock(resource_key: str | None = None) -> RLock:
    key = resource_key or "__delta_runtime__"
    with _DELTA_TABLE_LOCKS_GUARD:
        lock = _DELTA_TABLE_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _DELTA_TABLE_LOCKS[key] = lock
        return lock


def get_delta_runtime() -> tuple[object, object]:
    global _DELTA_RUNTIME

    with _DELTA_RUNTIME_INIT_LOCK:
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


def _is_too_many_requests_error(exc: Exception) -> bool:
    message = str(exc)
    normalized = message.lower()
    return "429" in normalized or "too many requests" in normalized


def _run_delta_write_with_retry(
    write_operation,
    *,
    dataset_name: str,
    table_uri: str,
    settings,
):
    max_attempts = settings.delta_retry_attempts_429
    base_wait = settings.delta_retry_wait_seconds_429

    for attempt in range(1, max_attempts + 1):
        try:
            return write_operation()
        except Exception as exc:
            is_429 = _is_too_many_requests_error(exc)
            should_retry = is_429 and attempt < max_attempts
            if not should_retry:
                raise

            wait_seconds = base_wait * attempt
            logger.warning(
                "Delta write retry dataset={} uri={} intento={}/{} espera={}s motivo=429",
                dataset_name,
                table_uri,
                attempt,
                max_attempts,
                wait_seconds,
            )
            sleep(wait_seconds)


def save_delta_table(df: pl.DataFrame, dataset_name: str, partition_cols: list[str]) -> str:
    if df.is_empty():
        logger.warning("Delta skip: dataframe vacio para {}", dataset_name)
        return ""

    settings = get_settings()
    table_uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options

    logger.info("Delta write start dataset={} uri={} rows={}", dataset_name, table_uri, df.height)

    DeltaTable, write_deltalake = get_delta_runtime()
    table_lock = get_delta_lock(table_uri)

    with table_lock:
        source_df = normalize_dataset(df, dataset_name)
        source_df = deduplicate_dataset(source_df, dataset_name)
        merge_predicate = _merge_predicate(dataset_name, source_df.columns)

        try:
            existing_table = DeltaTable(table_uri, storage_options=storage_options)
        except Exception as exc:
            logger.info("Delta table nueva o no encontrada dataset={} uri={} detail={}", dataset_name, table_uri, exc)
            existing_table = None

        try:
            if existing_table is not None:
                if not merge_predicate:
                    raise ValueError(
                        f"No se puede hacer merge incremental en '{dataset_name}': "
                        "no se encontraron llaves de negocio validas."
                    )

                change_predicate = _change_predicate(dataset_name, source_df.columns)

                def execute_merge():
                    merge_builder = existing_table.merge(
                        source=source_df.to_arrow(),
                        predicate=merge_predicate,
                        source_alias="source",
                        target_alias="target",
                    )
                    if change_predicate:
                        merge_builder = merge_builder.when_matched_update_all(
                            predicate=change_predicate
                        )
                    return merge_builder.when_not_matched_insert_all().execute()

                _run_delta_write_with_retry(
                    execute_merge,
                    dataset_name=dataset_name,
                    table_uri=table_uri,
                    settings=settings,
                )
                logger.info("Delta merge OK dataset={} uri={}", dataset_name, table_uri)
                return table_uri

            if not settings.is_minio:
                Path(table_uri).mkdir(parents=True, exist_ok=True)

            _run_delta_write_with_retry(
                lambda: write_deltalake(
                    table_uri,
                    source_df.to_arrow(),
                    mode="overwrite",
                    partition_by=partition_cols,
                    storage_options=storage_options,
                    engine="rust",
                ),
                dataset_name=dataset_name,
                table_uri=table_uri,
                settings=settings,
            )
            logger.info("Delta overwrite OK dataset={} uri={}", dataset_name, table_uri)
            return table_uri

        except Exception:
            logger.exception("Delta write FAILED dataset={} uri={}", dataset_name, table_uri)
            raise
