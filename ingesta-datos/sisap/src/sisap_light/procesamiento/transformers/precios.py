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


def _clean_variedad_expr(column_name: str = "variedad") -> pl.Expr:
    raw = pl.col(column_name).cast(pl.Utf8, strict=False).str.strip_chars()
    parts = raw.str.split_exact("__", 1)
    suffix = parts.struct.field("field_1").fill_null("")

    suffix_is_noise = (
        suffix.str.contains(r"^\d{1,2}/\d{1,2}/\d{4}$")
        | suffix.str.contains(r"^\d+(?:[.,]\d+)?$")
        | suffix.is_in(["", "...", "....", "-", "Precio Mínimo", "Precio Maximo", "Precio Máximo", "Precio Promedio"])
    )

    return (
        pl.when(raw.str.contains("__") & suffix_is_noise)
        .then(parts.struct.field("field_0"))
        .otherwise(raw)
        .str.strip_chars()
        .alias("variedad")
    )


def _parse_metric_rows(rows: list[list[str]]) -> tuple[list[str], list[list[str]]]:
    if len(rows) < 3:
        return [], []

    header = rows[0]
    data = rows[2:]
    return header, data


def build_precio_metric_frame(rows: list[list[str]], query: SisapQuery, metric_name: str) -> pl.DataFrame:
    if not rows or len(rows) < 2:
        return pl.DataFrame()

    header_row = rows[0]
    
    # Detectamos si es Snapshot (Producto, Variedad, Precio..., Procedencia)
    is_snapshot = any("producto" in h.lower() for h in header_row) and \
                  any("variedad" in h.lower() for h in header_row) and \
                  any("fecha" in h.lower() for h in header_row)

    if is_snapshot:
        return _build_snapshot_precio_frame(header_row, rows[1:], query, metric_name)
    
    # Detectamos si es Multilevel Header (Pivoteado: Fila 0 Variedades, Fila 1 Metricas)
    # Ejemplo: [Fecha, Aji, Aji, Aji]
    #          [Fecha, Max, Prom, Min]
    is_multilevel = len(rows) > 2 and \
                   any("precio" in r.lower() for r in rows[1]) and \
                   not any("precio" in r.lower() for r in rows[0])

    if is_multilevel:
        return _build_multilevel_precio_frame(rows, query, metric_name)

    # Si no, asumimos Intervalo Simple (Pivoteado: Fila 0 Variedad, Fila 1 Unidades)
    if len(rows) < 3:
        return pl.DataFrame()
    
    header = rows[0]
    data = rows[2:] # Saltamos encabezado y unidades
    
    df = pl.DataFrame(data, schema=header, orient="row")
    if df.is_empty() or "Fecha" not in df.columns:
        return pl.DataFrame()

    value_columns = [col for col in df.columns if col != "Fecha"]
    if not value_columns:
        return pl.DataFrame()

    return (
        df.unpivot(index="Fecha", on=value_columns, variable_name="variedad", value_name=METRIC_LABELS[metric_name])
        .rename({"Fecha": "fecha"})
        .with_columns(
            pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            _clean_variedad_expr("variedad"),
            pl.when(pl.col(METRIC_LABELS[metric_name]).str.strip_chars().is_in(["...", ""]))
            .then(None)
            .otherwise(pl.col(METRIC_LABELS[metric_name]).str.replace(",", ".", literal=True))
            .cast(pl.Float64, strict=False)
            .alias(METRIC_LABELS[metric_name]),
        )
        .with_columns(
            pl.lit(query.producto_codigo).alias("producto_codigo"),
            pl.lit(query.producto_nombre).alias("producto_nombre"),
            pl.lit(query.procedencia_nombre or "TODOS").alias("procedencia"),
            pl.lit(query.procedencia_codigo or "000000").alias("procedencia_filtro_codigo"),
            pl.lit(query.procedencia_nombre or "TODOS").alias("procedencia_filtro_nombre"),
            pl.lit(query.mercado_codigo or "").alias("mercado_codigo"),
            pl.lit(query.mercado_nombre or "").alias("mercado_nombre"),
            pl.lit(query.fecha_inicio.isoformat()).alias("fecha_inicio_consulta"),
            pl.lit(query.fecha_fin.isoformat()).alias("fecha_fin_consulta"),
        )
        .select(KEY_COLUMNS + [METRIC_LABELS[metric_name]])
        .filter(pl.col("fecha").is_not_null())
    )


def _build_multilevel_precio_frame(rows: list[list[str]], query: SisapQuery, metric_name: str) -> pl.DataFrame:
    variety_row = rows[0]
    metric_row = rows[1]
    data = rows[2:]

    # Identificamos el label de la metrica buscada
    # metric_name: precio_min, precio_prom, precio_max
    search_keywords = {
        "precio_min": ["mín", "min"],
        "precio_prom": ["prom"],
        "precio_max": ["máx", "max"],
    }.get(metric_name, [])

    records = []
    for row in data:
        if not row or not row[0]: continue
        fecha = row[0]
        for idx, m_label in enumerate(metric_row):
            if idx == 0: continue # Fecha
            
            # Verificamos si esta columna corresponde a la metrica buscada
            if any(kw in m_label.lower() for kw in search_keywords):
                variedad = variety_row[idx] if idx < len(variety_row) else ""
                valor = row[idx] if idx < len(row) else None
                records.append({
                    "fecha": fecha,
                    "variedad": variedad,
                    METRIC_LABELS[metric_name]: valor
                })

    if not records:
        return pl.DataFrame()

    df = pl.DataFrame(records)
    return (
        df.with_columns(
            pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            _clean_variedad_expr("variedad"),
            pl.when(pl.col(METRIC_LABELS[metric_name]).str.strip_chars().is_in(["...", ""]))
            .then(None)
            .otherwise(pl.col(METRIC_LABELS[metric_name]).str.replace(",", ".", literal=True))
            .cast(pl.Float64, strict=False)
            .alias(METRIC_LABELS[metric_name]),
        )
        .with_columns(
            pl.lit(query.producto_codigo).alias("producto_codigo"),
            pl.lit(query.producto_nombre).alias("producto_nombre"),
            pl.lit(query.procedencia_nombre or "TODOS").alias("procedencia"),
            pl.lit(query.procedencia_codigo or "000000").alias("procedencia_filtro_codigo"),
            pl.lit(query.procedencia_nombre or "TODOS").alias("procedencia_filtro_nombre"),
            pl.lit(query.mercado_codigo or "").alias("mercado_codigo"),
            pl.lit(query.mercado_nombre or "").alias("mercado_nombre"),
            pl.lit(query.fecha_inicio.isoformat()).alias("fecha_inicio_consulta"),
            pl.lit(query.fecha_fin.isoformat()).alias("fecha_fin_consulta"),
        )
        .select(KEY_COLUMNS + [METRIC_LABELS[metric_name]])
        .filter(pl.col("fecha").is_not_null())
    )


def _build_snapshot_precio_frame(header: list[str], data: list[list[str]], query: SisapQuery, metric_name: str) -> pl.DataFrame:
    df = pl.DataFrame(data, schema=header, orient="row")
    
    # Buscamos columnas de interes
    col_map = {}
    for h in header:
        low = h.lower()
        if "fecha" in low: col_map["fecha"] = h
        if "variedad" in low: col_map["variedad"] = h
        if "procedencia" in low: col_map["procedencia"] = h
        if "precio" in low and "min" in low: col_map["precio_min"] = h
        if "precio" in low and "prom" in low: col_map["precio_prom"] = h
        if "precio" in low and "max" in low: col_map["precio_max"] = h

    if "fecha" not in col_map or "variedad" not in col_map:
        return pl.DataFrame()

    target_metric_col = col_map.get(metric_name)
    if not target_metric_col:
        return pl.DataFrame()

    return (
        df.rename({
            col_map["fecha"]: "fecha",
            col_map["variedad"]: "variedad",
            target_metric_col: METRIC_LABELS[metric_name]
        })
        .with_columns(
            pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            _clean_variedad_expr("variedad"),
            pl.when(pl.col(METRIC_LABELS[metric_name]).str.strip_chars().is_in(["...", ""]))
            .then(None)
            .otherwise(pl.col(METRIC_LABELS[metric_name]).str.replace(",", ".", literal=True))
            .cast(pl.Float64, strict=False)
            .alias(METRIC_LABELS[metric_name]),
            pl.col(col_map["procedencia"]).str.strip_chars().alias("procedencia") if "procedencia" in col_map else pl.lit(query.procedencia_nombre or "").alias("procedencia"),
        )
        .with_columns(
            pl.lit(query.producto_codigo).alias("producto_codigo"),
            pl.lit(query.producto_nombre).alias("producto_nombre"),
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

