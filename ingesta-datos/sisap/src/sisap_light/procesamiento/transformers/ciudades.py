import polars as pl
from selectolax.parser import HTMLParser

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


from sisap_light.procesamiento.parsers.html_tables import detect_primary_table

def build_ciudades_metric_frame(html: str, query: SisapQuery, metric_name: str) -> pl.DataFrame:
    output_metric = CITY_METRIC_MAP.get(metric_name)
    if output_metric is None:
        raise ValueError(f"Metrica de ciudades no soportada: {metric_name}")

    rows = detect_primary_table(html)
    if not rows or len(rows) < 2:
        return pl.DataFrame()

    header_row = rows[0]
    
    # Detectamos si es Snapshot (Producto, Variedad, Precio..., Ciudad)
    is_snapshot = any("producto" in h.lower() for h in header_row) and \
                  any("ciudad" in h.lower() for h in header_row) and \
                  any("fecha" in h.lower() for h in header_row)

    if is_snapshot:
        return _build_snapshot_ciudades_frame(rows, query, output_metric)
    
    # Detectamos si es Multilevel Header (Pivoteado)
    # Buscamos la fila donde estan los precios (min, prom, max)
    price_row_idx = -1
    for i, row in enumerate(rows[:5]): # Buscamos en las primeras 5 filas
        if any("precio" in cell.lower() for cell in row):
            price_row_idx = i
            break
    
    if price_row_idx != -1:
        return _build_multilevel_ciudades_frame(rows, price_row_idx, query, metric_name, output_metric)

    # Si no detectamos estructura conocida, devolvemos vacio
    return pl.DataFrame()


def _build_multilevel_ciudades_frame(
    rows: list[list[str]], 
    price_row_idx: int, 
    query: SisapQuery, 
    metric_name: str,
    output_metric: str
) -> pl.DataFrame:
    # Estructura tipica de Ciudades:
    # Fila 0: Region
    # Fila 1: Variedad
    # Fila 2: Unidades / Mayorista-Minorista
    # Fila 3: Precio min / Precio prom / Precio max (price_row_idx)
    variety_row = rows[1] if len(rows) > 1 else []
    unit_row = rows[price_row_idx - 1] if price_row_idx > 0 else []
    equiv_row = rows[price_row_idx - 1] if price_row_idx > 0 else [] # A veces estan en la misma fila
    metric_row = rows[price_row_idx]
    data = rows[price_row_idx + 1:]

    # Identificamos el label de la metrica buscada
    search_keywords = {
        "may_precio_min": ["mín", "min"],
        "may_precio_prom": ["prom"],
        "may_precio_max": ["máx", "max"],
        "min_precio_min": ["mín", "min"],
        "min_precio_prom": ["prom"],
        "min_precio_max": ["máx", "max"],
    }.get(metric_name, [])

    records = []
    for row in data:
        if not row or not row[0]: continue
        fecha = row[0]
        
        # Iteramos por columnas buscando la metrica
        for idx, m_label in enumerate(metric_row):
            if idx == 0: continue # Fecha
            
            if any(kw in m_label.lower() for kw in search_keywords):
                # Encontramos una columna de la metrica buscada
                variedad = variety_row[idx] if idx < len(variety_row) else ""
                
                # Buscamos Unidad y Equiv (están a la izquierda de la primera metrica del grupo)
                # O en la fila superior (unit_row)
                # En la estructura de Ciudades, Unidad y Equiv suelen estar 1 o 2 columnas antes
                # de los precios para esa variedad.
                
                # Buscamos hacia atras hasta encontrar "Unidad" o cambiar de variedad
                u_idx = idx
                unidad = ""
                equiv = ""
                while u_idx > 0:
                    u_label = unit_row[u_idx].lower()
                    if "unidad" in u_label:
                        unidad = row[u_idx]
                        break
                    if variety_row[u_idx] != variedad:
                        break
                    u_idx -= 1
                
                e_idx = idx
                while e_idx > 0:
                    e_label = unit_row[e_idx].lower()
                    if "equiv" in e_label:
                        equiv = row[e_idx]
                        break
                    if variety_row[e_idx] != variedad:
                        break
                    e_idx -= 1

                valor = row[idx] if idx < len(row) else None
                
                records.append({
                    "fecha": fecha,
                    "ciudad": query.region_nombre or "Varios",
                    "variedad": variedad,
                    "unidad_medida": unidad,
                    "equiv_kg_lt": equiv,
                    output_metric: valor
                })

    if not records:
        return pl.DataFrame()

    df = pl.DataFrame(records)
    return (
        df.with_columns(
            pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            pl.col("equiv_kg_lt").str.replace(",", ".", literal=True).cast(pl.Float64, strict=False),
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


def _build_snapshot_ciudades_frame(rows: list[list[str]], query: SisapQuery, output_metric: str) -> pl.DataFrame:
    header = rows[0]
    data = rows[1:]
    df = pl.DataFrame(data, schema=header, orient="row")
    
    # Mapeo de columnas snapshot a nombres estandar
    rename_map = {
        "Fecha": "fecha",
        "Ciudad": "ciudad",
        "Variedad": "variedad",
        "Unidad": "unidad_medida",
        "Equiv": "equiv_kg_lt",
    }
    # Buscamos la metrica en el header
    metric_col = next((c for c in df.columns if output_metric.replace("precio_", "") in c.lower()), None)
    if metric_col:
        rename_map[metric_col] = output_metric

    df = df.rename({old: new for old, new in rename_map.items() if old in df.columns})
    
    return (
        df.with_columns(
            pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            pl.col("equiv_kg_lt").str.replace(",", ".", literal=True).cast(pl.Float64, strict=False),
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
        col
        for col in [
            "precio_may_min",
            "precio_may_prom",
            "precio_may_max",
            "precio_min_min",
            "precio_min_prom",
            "precio_min_max",
        ]
        if col in merged.columns
    ]
    if metric_cols:
        merged = merged.filter(pl.any_horizontal([pl.col(col).is_not_null() for col in metric_cols]))
    return merged

