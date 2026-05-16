from __future__ import annotations

import polars as pl

DATASET_BUSINESS_KEYS = {
    "fuentes_remotas_midagri": ["archivo_firma_remota"],
    "base_comercio_exterior": ["registro_hash_fuente"],
    "catalogo_cuadros_comercio_exterior": ["registro_hash_fuente"],
    "comercio_exterior_agrario": ["registro_hash_fuente"],
}


def dataset_root_name(dataset_name: str) -> str:
    normalized = dataset_name.split("/", 1)[0]
    if normalized.endswith(".parquet"):
        normalized = normalized[: -len(".parquet")]
    if normalized.endswith("_raw"):
        normalized = normalized[: -len("_raw")]
    return normalized


def configured_business_keys(dataset_name: str) -> list[str]:
    return DATASET_BUSINESS_KEYS.get(dataset_root_name(dataset_name), [])


def business_key_columns(dataset_name: str, columns: list[str]) -> list[str]:
    configured = configured_business_keys(dataset_name)
    return [column for column in configured if column in columns]


def validate_business_keys(df: pl.DataFrame, dataset_name: str) -> None:
    configured = configured_business_keys(dataset_name)
    if not configured:
        return

    missing_columns = [column for column in configured if column not in df.columns]
    if missing_columns:
        raise ValueError(
            f"El dataset '{dataset_name}' no contiene las llaves de negocio esperadas: {missing_columns}"
        )


def normalize_dataset(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    if df.is_empty():
        return df

    business_keys = set(configured_business_keys(dataset_name))
    expressions: list[pl.Expr] = []

    for column, dtype in df.schema.items():
        if column in business_keys:
            continue
        if dtype in (pl.Utf8, pl.String, pl.Categorical):
            expressions.append(pl.col(column).cast(pl.Utf8, strict=False).fill_null(""))
        elif dtype in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Float32,
            pl.Float64,
        ):
            expressions.append(pl.col(column).fill_null(0))

    if not expressions:
        return df
    return df.with_columns(expressions)


def deduplicate_dataset(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    if df.is_empty():
        return df

    validate_business_keys(df, dataset_name)
    business_keys = business_key_columns(dataset_name, df.columns)
    if not business_keys:
        return df.unique(maintain_order=True)
    return df.unique(subset=business_keys, keep="last", maintain_order=True)
