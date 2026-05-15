from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from loguru import logger

from sunat_file.catalogs.agro_productos import (
    AGRO_HS_CHAPTERS,
    EXCLUSION_KEYWORDS,
    PRODUCTOS_AGRO_CATALOGO,
    PRODUCTOS_AGRO_EXTRA,
    PRODUCT_ALLOWED_PREFIXES,
)
from sunat_file.catalogs.territorio import DEPARTAMENTOS_PERU

FUENTE = "SUNAT"
DATASET = "exportaciones_agrarias_frescas"
VERSION = "v1"
FRESH_ALLOWED_PREFIXES = tuple([f"070{i}" for i in range(1, 10)] + [f"08{str(i).zfill(2)}" for i in range(1, 11)])
FRESH_TEXT_HINTS = [
    "fresco",
    "fresca",
    "frescos",
    "frescas",
    "fresh",
    "refrigerado",
    "refrigerada",
    "cold treatment",
    "consumo humano",
]


def normalize_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _build_product_candidates() -> list[dict]:
    candidates: list[dict] = []
    for item in PRODUCTOS_AGRO_CATALOGO + PRODUCTOS_AGRO_EXTRA:
        aliases = sorted({normalize_text(alias) for alias in item["aliases"] if alias}, key=len, reverse=True)
        candidates.append(
            {
                "producto_id": item["producto_id"],
                "producto_nombre": item["producto_nombre"],
                "categoria": item["categoria"],
                "aliases": aliases,
            }
        )
    return candidates


PRODUCT_MATCHERS = _build_product_candidates()
EXCLUSION_PATTERNS = [normalize_text(item) for item in EXCLUSION_KEYWORDS]
FRESH_HINT_PATTERNS = [normalize_text(item) for item in FRESH_TEXT_HINTS]


def _match_product(text: str) -> dict | None:
    if not text:
        return None
    haystack = f" {text} "
    for item in PRODUCT_MATCHERS:
        for alias in item["aliases"]:
            needle = f" {alias} "
            if alias and needle in haystack:
                return item
    return None


def _should_exclude(text: str) -> bool:
    if not text:
        return False
    return any(pattern in text for pattern in EXCLUSION_PATTERNS if pattern)


def _is_fresh_subpartida(part_nandi: str | None) -> bool:
    if not part_nandi:
        return False
    return any(str(part_nandi).startswith(prefix) for prefix in FRESH_ALLOWED_PREFIXES)


def _looks_fresh(text: str) -> bool:
    if not text:
        return False
    return any(pattern in text for pattern in FRESH_HINT_PATTERNS if pattern)


def _subpartida_matches_product(producto_id: str | None, part_nandi: str | None) -> bool:
    if not producto_id or not part_nandi:
        return False
    allowed_prefixes = PRODUCT_ALLOWED_PREFIXES.get(producto_id)
    if not allowed_prefixes:
        return True
    return any(str(part_nandi).startswith(prefix) for prefix in allowed_prefixes)


def _derive_fecha(value: str | int | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if len(raw) != 8 or not raw.isdigit():
        return None
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_ubigeo(value: object) -> str | None:
    if value is None:
        return None
    raw = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not raw:
        return None
    if len(raw) > 6:
        raw = raw[:6]
    return raw.zfill(6)


def _territory_from_ubigeo(ubigeo: str | None) -> dict[str, str | None]:
    if not ubigeo or len(ubigeo) != 6:
        return {
            "region_codigo": None,
            "region_nombre": None,
            "provincia_codigo": None,
            "distrito_codigo": None,
        }
    region_codigo = ubigeo[:2]
    provincia_codigo = ubigeo[:4]
    distrito_codigo = ubigeo[:6]
    return {
        "region_codigo": region_codigo,
        "region_nombre": DEPARTAMENTOS_PERU.get(region_codigo),
        "provincia_codigo": provincia_codigo,
        "distrito_codigo": distrito_codigo,
    }


def build_sunat_exportaciones_frescas(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    work_df = df.with_columns(
        pl.col("part_nandi").cast(pl.Utf8).str.zfill(10).alias("part_nandi"),
        pl.coalesce([pl.col("dcom"), pl.col("dmer2"), pl.col("dmer3")]).cast(pl.Utf8).alias("descripcion_base"),
    )

    filtered_rows: list[dict] = []
    extraction_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    for row in work_df.to_dicts():
        part_nandi = row.get("part_nandi")
        chapter = str(part_nandi or "")[:2]
        text_parts = [row.get("dcom"), row.get("dmer2"), row.get("dmer3"), row.get("dmer4"), row.get("dmer5")]
        search_text = normalize_text(" ".join(str(part or "") for part in text_parts))
        
        if _should_exclude(search_text):
            continue

        product_match = _match_product(search_text)
        producto_id = str(product_match["producto_id"]) if product_match and product_match["producto_id"] is not None else None
        
        if chapter not in AGRO_HS_CHAPTERS:
            continue
        if product_match is None:
            continue
        if not _is_fresh_subpartida(part_nandi) and not _looks_fresh(search_text):
            continue
        if not _subpartida_matches_product(producto_id, part_nandi):
            continue

        fecha = _derive_fecha(row.get("femb")) or _derive_fecha(row.get("freg")) or _derive_fecha(row.get("fech_recep"))
        valor_fob = _safe_float(row.get("vfobserdol"))
        peso_neto = _safe_float(row.get("vpesnet"))
        precio_fob_por_kg = None
        if valor_fob is not None and peso_neto not in (None, 0):
            precio_fob_por_kg = valor_fob / peso_neto

        producto_nombre = product_match["producto_nombre"]
        ubigeo = _normalize_ubigeo(row.get("ubigeo"))
        territory = _territory_from_ubigeo(ubigeo)

        filtered_rows.append(
            {
                "fecha": fecha,
                "producto_id": producto_id,
                "producto_key": normalize_text(producto_nombre),
                "producto_nombre_catalogo": producto_nombre,
                "categoria_producto": product_match["categoria"],
                "estado_producto": "fresco",
                "subpartida_nacional": part_nandi,
                "descripcion_comercial": row.get("dcom"),
                "codigo_pais_destino": row.get("cpaides"),
                "codigo_puerto_destino": row.get("cpuedes"),
                "valor_fob_usd": valor_fob,
                "peso_neto_kg": peso_neto,
                "cantidad_fisica": _safe_float(row.get("qunifis")),
                "unidad_fisica": row.get("tunifis"),
                "nombre_exportador": row.get("dnombre"),
                "precio_fob_usd_por_kg": precio_fob_por_kg,
                "peso_bruto_kg": _safe_float(row.get("vpesbru")),
                "cantidad_comercial": _safe_float(row.get("qunicom")),
                "unidad_comercial": row.get("tunicom"),
                "ubigeo": ubigeo,
                "region_codigo": territory["region_codigo"],
                "region_nombre": territory["region_nombre"],
                "provincia_codigo": territory["provincia_codigo"],
                "distrito_codigo": territory["distrito_codigo"],
                "nombre_productor": row.get("dnompro"),
                "archivo_origen": row.get("archivo_origen"),
                "archivo_miembro": row.get("archivo_miembro"),
                # El tablon filtrado debe seguir la fecha real del registro SUNAT,
                # no la semana de publicacion del ZIP.
                "fecha_particion": fecha,
                "registro_hash_fuente": row.get("registro_hash_fuente"),
                "fuente": FUENTE,
                "dataset": DATASET,
                "fecha_extraccion": extraction_ts,
                "version": VERSION,
            }
        )

    if not filtered_rows:
        return pl.DataFrame()

    result = (
        pl.DataFrame(filtered_rows)
        .with_columns(pl.col("fecha").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        .with_columns(
            pl.col("fecha").dt.year().cast(pl.Int32).alias("anio"),
            pl.col("fecha").dt.month().cast(pl.Int32).alias("mes"),
            pl.col("fecha").dt.day().cast(pl.Int32).alias("dia"),
        )
        .unique()
    )

    ordered_cols = [
        "fecha", "anio", "mes", "dia", "producto_id", "producto_key", "producto_nombre_catalogo",
        "categoria_producto", "estado_producto", "subpartida_nacional", "descripcion_comercial",
        "codigo_pais_destino", "codigo_puerto_destino", "valor_fob_usd", "peso_neto_kg",
        "cantidad_fisica", "unidad_fisica", "nombre_exportador", "precio_fob_usd_por_kg", "ubigeo",
        "region_codigo", "region_nombre", "provincia_codigo", "distrito_codigo", "peso_bruto_kg",
        "cantidad_comercial", "unidad_comercial", "nombre_productor", "archivo_origen", "archivo_miembro",
        "fecha_particion", "registro_hash_fuente", "fuente", "dataset", "fecha_extraccion", "version",
    ]
    return result.select([c for c in ordered_cols if c in result.columns])


def build_review_files(df: pl.DataFrame, review_dir: Path) -> tuple[Path, Path, Path]:
    review_dir.mkdir(parents=True, exist_ok=True)
    preview_path = review_dir / "sunat_exportaciones_frescas_preview.csv"
    resumen_productos_path = review_dir / "sunat_exportaciones_frescas_resumen_productos.csv"
    resumen_subpartidas_path = review_dir / "sunat_exportaciones_frescas_resumen_subpartidas.csv"

    preview_cols = [
        "fecha", "producto_id", "producto_nombre_catalogo", "producto_key", "subpartida_nacional",
        "descripcion_comercial", "codigo_pais_destino", "codigo_puerto_destino", "valor_fob_usd",
        "peso_neto_kg", "precio_fob_usd_por_kg", "ubigeo", "region_nombre", "nombre_exportador",
    ]
    df.select([c for c in preview_cols if c in df.columns]).head(200).write_csv(preview_path)

    resumen_productos = (
        df.group_by(["producto_id", "producto_nombre_catalogo", "producto_key", "categoria_producto"])
        .agg(
            pl.len().alias("registros"),
            pl.col("valor_fob_usd").sum().alias("valor_fob_total_usd"),
            pl.col("peso_neto_kg").sum().alias("peso_neto_total_kg"),
            pl.col("precio_fob_usd_por_kg").mean().alias("precio_fob_promedio_kg_usd"),
        )
        .sort("valor_fob_total_usd", descending=True)
    )
    resumen_productos.write_csv(resumen_productos_path)

    resumen_subpartidas = (
        df.group_by(["subpartida_nacional", "producto_key", "producto_nombre_catalogo"])
        .agg(
            pl.len().alias("registros"),
            pl.col("valor_fob_usd").sum().alias("valor_fob_total_usd"),
            pl.col("peso_neto_kg").sum().alias("peso_neto_total_kg"),
        )
        .sort("valor_fob_total_usd", descending=True)
    )
    resumen_subpartidas.write_csv(resumen_subpartidas_path)
    return preview_path, resumen_productos_path, resumen_subpartidas_path


def build_catalog_file(df: pl.DataFrame, review_dir: Path) -> Path:
    review_dir.mkdir(parents=True, exist_ok=True)
    catalog_path = review_dir / "sunat_catalogo_productos_homologado.csv"
    (
        df.select(["producto_id", "producto_nombre_catalogo", "producto_key", "categoria_producto"])
        .drop_nulls(["producto_id", "producto_nombre_catalogo"])
        .unique()
        .sort(["producto_id"])
        .write_csv(catalog_path)
    )
    return catalog_path


def build_territory_catalog(df: pl.DataFrame, review_dir: Path) -> Path:
    review_dir.mkdir(parents=True, exist_ok=True)
    territory_path = review_dir / "sunat_catalogo_territorial_base.csv"
    (
        df.select(["ubigeo", "region_codigo", "region_nombre", "provincia_codigo", "distrito_codigo"])
        .drop_nulls(["ubigeo"])
        .unique()
        .sort(["region_codigo", "provincia_codigo", "distrito_codigo"])
        .write_csv(territory_path)
    )
    return territory_path


def build_region_summary(df: pl.DataFrame, review_dir: Path) -> Path:
    review_dir.mkdir(parents=True, exist_ok=True)
    region_path = review_dir / "sunat_exportaciones_frescas_resumen_regiones.csv"
    (
        df.group_by(["region_codigo", "region_nombre"])
        .agg(
            pl.len().alias("registros"),
            pl.col("valor_fob_usd").sum().alias("valor_fob_total_usd"),
            pl.col("peso_neto_kg").sum().alias("peso_neto_total_kg"),
            pl.col("producto_id").n_unique().alias("productos_distintos"),
            pl.col("nombre_exportador").n_unique().alias("exportadores_distintos"),
        )
        .sort("valor_fob_total_usd", descending=True)
        .write_csv(region_path)
    )
    return region_path


def build_ubigeo_quality_report(df: pl.DataFrame, review_dir: Path) -> Path:
    review_dir.mkdir(parents=True, exist_ok=True)
    quality_path = review_dir / "sunat_exportaciones_frescas_calidad_ubigeo.csv"
    total = df.height
    con_ubigeo = df.filter(pl.col("ubigeo").is_not_null()).height
    sin_ubigeo = total - con_ubigeo
    con_region = df.filter(pl.col("region_nombre").is_not_null()).height
    pl.DataFrame(
        {
            "metrica": [
                "registros_totales", "registros_con_ubigeo", "registros_sin_ubigeo",
                "registros_con_region", "porcentaje_con_ubigeo", "porcentaje_con_region",
            ],
            "valor": [
                float(total), float(con_ubigeo), float(sin_ubigeo), float(con_region),
                float((con_ubigeo / total) * 100) if total else 0.0,
                float((con_region / total) * 100) if total else 0.0,
            ],
        }
    ).write_csv(quality_path)
    return quality_path


def build_data_dictionary(review_dir: Path) -> Path:
    review_dir.mkdir(parents=True, exist_ok=True)
    dict_path = review_dir / "sunat_exportaciones_frescas_diccionario.csv"
    pl.DataFrame(
        {
            "columna": [
                "fecha", "anio", "mes", "dia", "producto_id", "producto_key", "producto_nombre_catalogo",
                "categoria_producto", "estado_producto", "subpartida_nacional", "descripcion_comercial",
                "codigo_pais_destino", "codigo_puerto_destino", "valor_fob_usd", "peso_neto_kg",
                "cantidad_fisica", "unidad_fisica", "nombre_exportador", "precio_fob_usd_por_kg",
                "ubigeo", "region_codigo", "region_nombre", "provincia_codigo", "distrito_codigo",
                "fuente", "dataset", "fecha_extraccion", "version",
            ],
            "descripcion": [
                "Fecha de embarque", "Ano de la operacion", "Mes de la operacion", "Dia de la operacion",
                "Identificador homologado del producto para relacionarlo con otras fuentes",
                "Clave normalizada del producto detectado", "Nombre homologado del producto segun catalogo agrario",
                "Categoria agraria homologada del producto", "Estado del producto conservado en la tabla final",
                "Codigo arancelario de 10 digitos", "Descripcion comercial del producto exportado",
                "Codigo del pais destino", "Codigo del puerto destino", "Valor FOB en dolares",
                "Peso neto en kilogramos", "Cantidad fisica exportada", "Unidad de medida fisica",
                "Nombre del exportador", "Precio FOB estimado en USD por kilogramo", "Codigo ubigeo reportado por SUNAT",
                "Codigo de region derivado del ubigeo", "Nombre de region derivado del ubigeo",
                "Codigo de provincia derivado del ubigeo", "Codigo de distrito derivado del ubigeo",
                "Fuente de datos", "Nombre del dataset", "Fecha de generacion del dataset", "Version del dataset",
            ],
        }
    ).write_csv(dict_path)
    return dict_path


build_sunat_exportaciones_agrarias = build_sunat_exportaciones_frescas
