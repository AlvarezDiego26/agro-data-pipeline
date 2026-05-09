from __future__ import annotations

import polars as pl

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


def dataset_root_name(dataset_name: str) -> str:
    return dataset_name.split("/", 1)[0]


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
            f"El dataset '{dataset_name}' no contiene las llaves de negocio esperadas: "
            f"{missing_columns}"
        )

    null_key_columns: list[str] = []
    empty_key_columns: list[str] = []

    for column in configured:
        if df.get_column(column).null_count() > 0:
            null_key_columns.append(column)
            continue

        dtype = df.schema.get(column)
        if dtype in (pl.Utf8, pl.String, pl.Categorical):
            has_empty = (
                df.select(
                    pl.col(column)
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .eq("")
                    .any()
                ).item()
            )
            if has_empty:
                empty_key_columns.append(column)

    if null_key_columns or empty_key_columns:
        details: list[str] = []
        if null_key_columns:
            details.append(f"nulas={null_key_columns}")
        if empty_key_columns:
            details.append(f"vacias={empty_key_columns}")
        raise ValueError(
            f"El dataset '{dataset_name}' contiene llaves de negocio invalidas: "
            f"{', '.join(details)}"
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

    unique_df = df.unique(maintain_order=True)
    duplicate_groups = (
        unique_df.group_by(business_keys)
        .len(name="conflict_count")
        .filter(pl.col("conflict_count") > 1)
    )
    if duplicate_groups.height > 0:
        sample_conflicts = (
            unique_df.join(
                duplicate_groups.select(business_keys),
                on=business_keys,
                how="inner",
            )
            .head(5)
            .to_dicts()
        )
        raise ValueError(
            f"Fallo rapido de ingesta: se detectaron registros conflictivos para las "
            f"llaves de negocio {business_keys} en el dataset '{dataset_name}'. "
            f"Muestra de conflictos: {sample_conflicts}"
        )

    return unique_df
