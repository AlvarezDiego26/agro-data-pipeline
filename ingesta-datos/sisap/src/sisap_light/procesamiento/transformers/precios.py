from __future__ import annotations

import polars as pl

from sisap_light.schemas import SisapQuery

KEY_COLUMNS = [
    "fecha",
    "producto_codigo",
    "producto_nombre",
    "variedad",
    "procedencia",
    "procedencia_filtro_codigo",
    "procedencia_filtro_nombre",
    "mercado_codigo",
    "mercado_nombre",
    "fecha_inicio_consulta",
    "fecha_fin_consulta",
]


METRIC_LABELS = {
    "precio_min": "precio_min",
    "precio_prom": "precio_prom",
    "precio_max": "precio_max",
}


def _parse_metric_rows(rows: list[list[str]]) -> tuple[list[str], list[list[str]]]:
    if len(rows) < 3:
        return [], []

    header = rows[0]
    data = rows[2:]
    return header, data


def build_precio_metric_frame(rows: list[list[str]], query: SisapQuery, metric_name: str) -> pl.DataFrame:
    if metric_name not in METRIC_LABELS:
        raise ValueError(f"Metrica no soportada: {metric_name}")

    header, data = _parse_metric_rows(rows)
    if not header or not data:
        return pl.DataFrame()

    df = pl.DataFrame(data, schema=header, orient="row")
    if df.is_empty() or "Fecha" not in df.columns:
        return pl.DataFrame()

    value_columns = [col for col in df.columns if col != "Fecha"]
    if not value_columns:
        return pl.DataFrame()

    long_df = (
        df.unpivot(index="Fecha", on=value_columns, variable_name="variedad", value_name=METRIC_LABELS[metric_name])
        .rename({"Fecha": "fecha"})
        .with_columns(
            pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            pl.col("variedad").str.strip_chars().alias("variedad"),
            pl.when(pl.col(METRIC_LABELS[metric_name]).str.strip_chars() == "...")
            .then(None)
            .otherwise(pl.col(METRIC_LABELS[metric_name]).str.replace(",", ".", literal=True))
            .cast(pl.Float64, strict=False)
            .alias(METRIC_LABELS[metric_name]),
        )
        .with_columns(
            pl.lit(query.producto_codigo).alias("producto_codigo"),
            pl.lit(query.producto_nombre).alias("producto_nombre"),
            pl.lit(query.procedencia_nombre or "").alias("procedencia"),
            pl.lit(query.procedencia_codigo or "").alias("procedencia_filtro_codigo"),
            pl.lit(query.procedencia_nombre or "").alias("procedencia_filtro_nombre"),
            pl.lit(query.mercado_codigo or "").alias("mercado_codigo"),
            pl.lit(query.mercado_nombre or "").alias("mercado_nombre"),
            pl.lit(query.fecha_inicio.isoformat()).alias("fecha_inicio_consulta"),
            pl.lit(query.fecha_fin.isoformat()).alias("fecha_fin_consulta"),
        )
        .select(KEY_COLUMNS + [METRIC_LABELS[metric_name]])
        .filter(pl.col("fecha").is_not_null())
    )

    return long_df


def merge_precio_metrics(metric_frames: list[pl.DataFrame]) -> pl.DataFrame:
    usable = [frame for frame in metric_frames if not frame.is_empty()]
    if not usable:
        return pl.DataFrame()

    merged = usable[0]
    for frame in usable[1:]:
        merged = merged.join(frame, on=KEY_COLUMNS, how="full", coalesce=True)

    metric_cols = [col for col in ["precio_min", "precio_prom", "precio_max"] if col in merged.columns]
    if metric_cols:
        merged = merged.filter(pl.any_horizontal([pl.col(col).is_not_null() for col in metric_cols]))

    return merged

