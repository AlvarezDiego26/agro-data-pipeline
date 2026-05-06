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


def build_ciudades_metric_frame(html: str, query: SisapQuery, metric_name: str) -> pl.DataFrame:
    output_metric = CITY_METRIC_MAP.get(metric_name)
    if output_metric is None:
        raise ValueError(f"Metrica de ciudades no soportada: {metric_name}")

    tree = HTMLParser(html)
    table = tree.css_first("table")
    if table is None:
        return pl.DataFrame()

    rows = table.css("tr")
    if len(rows) < 5:
        return pl.DataFrame()

    group_cells = rows[0].css("th, td")[1:]
    variety_cells = rows[1].css("th, td")

    if not group_cells or not variety_cells:
        return pl.DataFrame()

    groups: list[dict[str, int | str]] = []
    for cell in group_cells:
        groups.append(
            {
                "name": cell.text(strip=True) or (query.region_nombre or ""),
                "colspan": int(cell.attributes.get("colspan", "1") or "1"),
            }
        )

    varieties: list[dict[str, str]] = []
    group_index = 0
    group_used = 0
    for cell in variety_cells:
        colspan = int(cell.attributes.get("colspan", "1") or "1")
        while group_index < len(groups) and group_used >= int(groups[group_index]["colspan"]):
            group_index += 1
            group_used = 0
        if group_index >= len(groups):
            break
        group_name = str(groups[group_index]["name"])
        varieties.append(
            {
                "ciudad": group_name,
                "variedad": cell.text(strip=True),
            }
        )
        group_used += colspan

    if not varieties:
        return pl.DataFrame()

    records: list[dict[str, object]] = []
    for tr in rows[4:]:
        cells = [cell.text(strip=True) for cell in tr.css("th, td")]
        if not cells or not cells[0]:
            continue
        fecha = cells[0]
        values = cells[1:]
        needed = len(varieties) * 3
        if len(values) < needed:
            values.extend([""] * (needed - len(values)))
        elif len(values) > needed:
            values = values[:needed]

        for idx, meta in enumerate(varieties):
            start = idx * 3
            unidad = values[start] if start < len(values) else ""
            equiv = values[start + 1] if start + 1 < len(values) else ""
            valor = values[start + 2] if start + 2 < len(values) else ""
            records.append(
                {
                    "fecha": fecha,
                    "producto_codigo": query.producto_codigo,
                    "producto_nombre": query.producto_nombre,
                    "ciudad": meta["ciudad"],
                    "variedad": meta["variedad"],
                    "unidad_medida": unidad,
                    "equiv_kg_lt": equiv,
                    output_metric: valor,
                    "region_codigo": query.region_codigo or "",
                    "region_nombre": query.region_nombre or "",
                    "fecha_inicio_consulta": query.fecha_inicio.isoformat(),
                    "fecha_fin_consulta": query.fecha_fin.isoformat(),
                }
            )

    if not records:
        return pl.DataFrame()

    df = pl.DataFrame(records)
    df = df.with_columns(
        pl.col("fecha").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
        pl.col("equiv_kg_lt").str.replace(",", ".", literal=True).cast(pl.Float64, strict=False),
        pl.when(pl.col(output_metric).str.strip_chars() == "...")
        .then(None)
        .otherwise(pl.col(output_metric).str.replace(",", ".", literal=True))
        .cast(pl.Float64, strict=False)
        .alias(output_metric),
    )
    return df.filter(pl.col("fecha").is_not_null())


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

