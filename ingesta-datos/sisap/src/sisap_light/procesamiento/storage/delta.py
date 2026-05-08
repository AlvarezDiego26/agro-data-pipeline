from __future__ import annotations

from pathlib import Path
from threading import RLock

import polars as pl

from sisap_light.config import get_settings

DELTA_RUNTIME_LOCK = RLock()
_DELTA_RUNTIME: tuple[object, object] | None = None
DATASET_BUSINESS_KEYS = {
    "volumen_diario": [
        "fecha",
        "producto_codigo",
        "variedad",
        "procedencia",
        "procedencia_filtro_codigo",
        "mercado_codigo",
    ],
    "precios_diarios": [
        "fecha",
        "producto_codigo",
        "variedad",
        "procedencia",
        "procedencia_filtro_codigo",
        "mercado_codigo",
    ],
    "ciudades_precios_mayoristas": [
        "fecha",
        "producto_codigo",
        "ciudad",
        "variedad",
        "unidad_medida",
        "region_codigo",
    ],
    "ciudades_precios_minoristas": [
        "fecha",
        "producto_codigo",
        "ciudad",
        "variedad",
        "unidad_medida",
        "region_codigo",
    ],
}
RECENCY_COLUMNS = [
    "fecha_inicio_consulta",
    "fecha_fin_consulta",
]


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


def _dataset_root_name(dataset_name: str) -> str:
    return dataset_name.split("/", 1)[0]


def _business_key_columns(dataset_name: str, columns: list[str]) -> list[str]:
    configured = DATASET_BUSINESS_KEYS.get(_dataset_root_name(dataset_name), [])
    return [column for column in configured if column in columns]


def _deduplicate_dataset(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    if df.is_empty():
        return df

    business_keys = _business_key_columns(dataset_name, df.columns)
    if not business_keys:
        return df.unique()

    original_columns = list(df.columns)
    sort_columns = [column for column in RECENCY_COLUMNS if column in df.columns]
    sorted_df = df.sort(sort_columns) if sort_columns else df

    aggregate_columns = [column for column in original_columns if column not in business_keys]
    if not aggregate_columns:
        return sorted_df.unique(subset=business_keys, keep="last")

    aggregated = sorted_df.group_by(business_keys, maintain_order=True).agg(
        [
            pl.col(column).drop_nulls().last().alias(column)
            for column in aggregate_columns
        ]
    )
    return aggregated.select([column for column in original_columns if column in aggregated.columns])


def save_delta_table(df: pl.DataFrame, dataset_name: str, partition_cols: list[str]) -> str:
    if df.is_empty():
        return ""

    settings = get_settings()
    table_uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options

    DeltaTable, write_deltalake = get_delta_runtime()

    with DELTA_RUNTIME_LOCK:
        merged_df = df
        try:
            existing_table = DeltaTable(table_uri, storage_options=storage_options)
            existing_df = pl.from_arrow(existing_table.to_pyarrow_table())
            if not existing_df.is_empty():
                merged_df = _deduplicate_dataset(
                    pl.concat([existing_df, df], how="vertical_relaxed"),
                    dataset_name,
                )
        except Exception:
            pass

        merged_df = _deduplicate_dataset(merged_df, dataset_name)

        if not settings.is_minio:
            Path(table_uri).mkdir(parents=True, exist_ok=True)

        write_deltalake(
            table_uri,
            merged_df.to_arrow(),
            mode="overwrite",
            partition_by=partition_cols,
            storage_options=storage_options,
        )
    return table_uri

