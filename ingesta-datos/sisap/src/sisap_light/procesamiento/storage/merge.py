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

RECENCY_COLUMNS = [
    "fecha_inicio_consulta",
    "fecha_fin_consulta",
]


def dataset_root_name(dataset_name: str) -> str:
    return dataset_name.split("/", 1)[0]


def business_key_columns(dataset_name: str, columns: list[str]) -> list[str]:
    configured = DATASET_BUSINESS_KEYS.get(dataset_root_name(dataset_name), [])
    return [column for column in configured if column in columns]


def deduplicate_dataset(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    if df.is_empty():
        return df

    business_keys = business_key_columns(dataset_name, df.columns)
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
