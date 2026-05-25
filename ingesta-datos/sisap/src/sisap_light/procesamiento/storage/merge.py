from __future__ import annotations

import polars as pl
from loguru import logger

DATASET_BUSINESS_KEYS = {
    "precios_diarios_mercado_lima": [
        "fecha",
        "mercado_codigo",
        "producto_codigo",
        "variedad",
    ],
    "volumen_diario_mercado_lima": [
        "fecha",
        "mercado_codigo",
        "producto_codigo",
        "variedad",
        "procedencia",
    ],
    "precio_diario_regiones": [
        "fecha",
        "tipo_mercado",
        "region",
        "ciudad",
        "producto_codigo",
        "variedad",
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

    # Eliminar metadata de consultas para no ensuciar el Data Lake
    cols_to_drop = [
        "fecha_inicio_consulta",
        "fecha_fin_consulta",
        "procedencia_filtro_codigo",
        "procedencia_filtro_nombre",
    ]
    df = df.drop([c for c in cols_to_drop if c in df.columns], strict=False)

    business_keys = set(configured_business_keys(dataset_name))
    expressions: list[pl.Expr] = []

    for column, dtype in df.schema.items():
        if column in business_keys:
            if column == "variedad" and dtype in (pl.Utf8, pl.String, pl.Categorical):
                expressions.append(
                    pl.when(
                        pl.col(column).is_null()
                        | pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars().eq("")
                    )
                    .then(pl.lit("sin_variedad"))
                    .otherwise(pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars())
                    .alias(column)
                )
            continue

        if dtype in (pl.Utf8, pl.String, pl.Categorical):
            expressions.append(pl.col(column).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
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
        if dataset_root_name(dataset_name) == "volumen_diario_mercado_lima":
            conflict_rows = int(duplicate_groups.get_column("conflict_count").sum())
            logger.warning(
                "Se consolidaran {} filas conflictivas en {} usando suma de volumen_ton por llave {}.",
                conflict_rows,
                dataset_name,
                business_keys,
            )
            aggregated_expressions: list[pl.Expr] = []
            for column, dtype in unique_df.schema.items():
                if column in business_keys:
                    continue
                if column == "volumen_ton":
                    aggregated_expressions.append(
                        pl.col(column).cast(pl.Float64, strict=False).sum().alias(column)
                    )
                elif dtype in (pl.Utf8, pl.String, pl.Categorical):
                    aggregated_expressions.append(
                        pl.col(column).drop_nulls().first().fill_null("").alias(column)
                    )
                else:
                    aggregated_expressions.append(pl.col(column).drop_nulls().first().alias(column))

            resolved_df = unique_df.group_by(business_keys, maintain_order=True).agg(aggregated_expressions)
            return resolved_df.select(unique_df.columns)

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
