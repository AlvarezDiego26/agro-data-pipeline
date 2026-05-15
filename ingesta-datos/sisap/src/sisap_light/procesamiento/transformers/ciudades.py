import polars as pl

from sisap_light.procesamiento.parsers.html_tables import detect_primary_table
from sisap_light.schemas import SisapQuery


CITY_KEY_COLUMNS = [
    "fecha",
    "producto_codigo",
    "producto_nombre",
    "ciudad",
    "variedad",
    "unidad_medida",
    "equiv_kg_lt",
    "region_codigo",
    "region_nombre",
    "fecha_inicio_consulta",
    "fecha_fin_consulta",
]

CITY_METRIC_MAP = {
    "may_precio_min": "precio_may_min",
    "may_precio_prom": "precio_may_prom",
    "may_precio_max": "precio_may_max",
    "min_precio_min": "precio_min_min",
    "min_precio_prom": "precio_min_prom",
    "min_precio_max": "precio_min_max",
}


def _metric_search_keywords(metric_name: str) -> list[str]:
    return {
        "may_precio_min": ["mín", "min", "mÃ­n"],
        "may_precio_prom": ["prom"],
        "may_precio_max": ["máx", "max", "mÃ¡x"],
        "min_precio_min": ["mín", "min", "mÃ­n"],
        "min_precio_prom": ["prom"],
        "min_precio_max": ["máx", "max", "mÃ¡x"],
    }.get(metric_name, [])


def _find_group_value(
    header_row: list[str],
    data_row: list[str],
    group_row: list[str],
    idx: int,
    current_group: str,
    keyword: str,
) -> str:
    search_idx = idx
    while search_idx > 0:
        header_label = (header_row[search_idx] if search_idx < len(header_row) else "").lower()
        group_value = group_row[search_idx] if search_idx < len(group_row) else ""
        if keyword in header_label:
            return data_row[search_idx] if search_idx < len(data_row) else ""
        if group_value != current_group:
            break
        search_idx -= 1
    return ""


def _resolve_ciudad(city_row: list[str], idx: int, region_fallback: str | None) -> str:
    if idx < len(city_row):
        city_value = city_row[idx].strip()
        if city_value:
            return city_value
    return region_fallback or "Varios"


def _find_snapshot_metric_column(df: pl.DataFrame, metric_name: str) -> str | None:
    search_keywords = _metric_search_keywords(metric_name)
    return next(
        (
            column
            for column in df.columns
            if "precio" in column.lower()
            and any(keyword in column.lower() for keyword in search_keywords)
        ),
        None,
    )


def build_ciudades_metric_frame(html: str, query: SisapQuery, metric_name: str) -> pl.DataFrame:
    output_metric = CITY_METRIC_MAP.get(metric_name)
    if output_metric is None:
        raise ValueError(f"Metrica de ciudades no soportada: {metric_name}")

    rows = detect_primary_table(html)
    if not rows or len(rows) < 2:
        return pl.DataFrame()

    header_row = rows[0]
    is_snapshot = (
        any("producto" in cell.lower() for cell in header_row)
        and any("ciudad" in cell.lower() for cell in header_row)
        and any("fecha" in cell.lower() for cell in header_row)
    )

    if is_snapshot:
        return _build_snapshot_ciudades_frame(rows, query, metric_name, output_metric)

    price_row_idx = -1
    for idx, row in enumerate(rows[:5]):
        if any("precio" in cell.lower() for cell in row):
            price_row_idx = idx
            break

    if price_row_idx != -1:
        return _build_multilevel_ciudades_frame(
            rows,
            price_row_idx,
            query,
            metric_name,
            output_metric,
        )

    return pl.DataFrame()


def _build_multilevel_ciudades_frame(
    rows: list[list[str]],
    price_row_idx: int,
    query: SisapQuery,
    metric_name: str,
    output_metric: str,
) -> pl.DataFrame:
    city_row = rows[0] if rows else []
    variety_row = rows[1] if len(rows) > 1 else []
    unit_row = rows[price_row_idx - 1] if price_row_idx > 0 else []
    metric_row = rows[price_row_idx]
    data = rows[price_row_idx + 1 :]
    search_keywords = _metric_search_keywords(metric_name)

    records = []
    for row in data:
        if not row or not row[0]:
            continue

        fecha = row[0]
        for idx, metric_label in enumerate(metric_row):
            if idx == 0:
                continue

            if any(keyword in metric_label.lower() for keyword in search_keywords):
                variedad = variety_row[idx] if idx < len(variety_row) else ""
                ciudad = _resolve_ciudad(city_row, idx, query.region_nombre)
                unidad = _find_group_value(unit_row, row, variety_row, idx, variedad, "unidad")
                equiv = _find_group_value(unit_row, row, variety_row, idx, variedad, "equiv")
                valor = row[idx] if idx < len(row) else None

                records.append(
                    {
                        "fecha": fecha,
                        "ciudad": ciudad,
                        "variedad": variedad,
                        "unidad_medida": unidad,
                        "equiv_kg_lt": equiv,
                        output_metric: valor,
                    }
                )

    if not records:
        return pl.DataFrame()

    df = pl.DataFrame(records)
    return (
        df.with_columns(
            pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            pl.col("equiv_kg_lt").str.replace(",", ".", literal=True).cast(
                pl.Float64, strict=False
            ),
            pl.when(pl.col(output_metric).str.strip_chars().is_in(["...", ""]))
            .then(None)
            .otherwise(pl.col(output_metric).str.replace(",", ".", literal=True))
            .cast(pl.Float64, strict=False)
            .alias(output_metric),
        )
        .with_columns(
            pl.lit(query.producto_codigo).alias("producto_codigo"),
            pl.lit(query.producto_nombre).alias("producto_nombre"),
            pl.lit(query.region_codigo or "").alias("region_codigo"),
            pl.lit(query.region_nombre or "").alias("region_nombre"),
            pl.lit(query.fecha_inicio.isoformat()).alias("fecha_inicio_consulta"),
            pl.lit(query.fecha_fin.isoformat()).alias("fecha_fin_consulta"),
        )
        .filter(pl.col("fecha").is_not_null())
    )


def _build_snapshot_ciudades_frame(
    rows: list[list[str]],
    query: SisapQuery,
    metric_name: str,
    output_metric: str,
) -> pl.DataFrame:
    header = rows[0]
    data = rows[1:]
    df = pl.DataFrame(data, schema=header, orient="row")

    rename_map = {
        "Fecha": "fecha",
        "Ciudad": "ciudad",
        "Variedad": "variedad",
        "Unidad": "unidad_medida",
        "Equiv": "equiv_kg_lt",
    }
    metric_col = _find_snapshot_metric_column(df, metric_name)
    if metric_col:
        rename_map[metric_col] = output_metric

    df = df.rename({old: new for old, new in rename_map.items() if old in df.columns})

    return (
        df.with_columns(
            pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            pl.col("equiv_kg_lt").str.replace(",", ".", literal=True).cast(
                pl.Float64, strict=False
            ),
            pl.when(pl.col(output_metric).str.strip_chars().is_in(["...", ""]))
            .then(None)
            .otherwise(pl.col(output_metric).str.replace(",", ".", literal=True))
            .cast(pl.Float64, strict=False)
            .alias(output_metric),
        )
        .with_columns(
            pl.lit(query.producto_codigo).alias("producto_codigo"),
            pl.lit(query.producto_nombre).alias("producto_nombre"),
            pl.lit(query.region_codigo or "").alias("region_codigo"),
            pl.lit(query.region_nombre or "").alias("region_nombre"),
            pl.lit(query.fecha_inicio.isoformat()).alias("fecha_inicio_consulta"),
            pl.lit(query.fecha_fin.isoformat()).alias("fecha_fin_consulta"),
        )
        .filter(pl.col("fecha").is_not_null())
    )


def merge_ciudades_metrics(metric_frames: list[pl.DataFrame]) -> pl.DataFrame:
    usable = [frame for frame in metric_frames if not frame.is_empty()]
    if not usable:
        return pl.DataFrame()

    merged = usable[0]
    for frame in usable[1:]:
        merged = merged.join(frame, on=CITY_KEY_COLUMNS, how="full", coalesce=True)

    metric_cols = [
        column
        for column in [
            "precio_may_min",
            "precio_may_prom",
            "precio_may_max",
            "precio_min_min",
            "precio_min_prom",
            "precio_min_max",
        ]
        if column in merged.columns
    ]
    if metric_cols:
        merged = merged.filter(
            pl.any_horizontal([pl.col(column).is_not_null() for column in metric_cols])
        )
    return merged
