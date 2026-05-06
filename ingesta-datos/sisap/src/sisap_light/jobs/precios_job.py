from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
from sisap_light.jobs.common import (
    build_scope_output_dir,
    filter_plan,
    finalize_partitioned_output,
    resolve_item,
    resolve_productos,
    resolve_query_dates,
)
from sisap_light.procesamiento.parsers.html_tables import extract_report_titles, extract_tables
from sisap_light.ingesta_datos.planners import build_mayorista_queries
from sisap_light.schemas import ModuloSisap, SisapQuery
from sisap_light.procesamiento.storage.parquet import save_parquet
from sisap_light.procesamiento.storage.raw import save_html_snapshot
from sisap_light.procesamiento.transformers.precios import build_precio_metric_frame, merge_precio_metrics
from sisap_light.procesamiento.validators.quality import validate_expected_columns, validate_non_empty

PRICE_VARIABLES = ["precio_min", "precio_prom", "precio_max"]
EXPECTED_COLUMNS = [
    "fecha",
    "producto_codigo",
    "producto_nombre",
    "variedad",
    "procedencia",
    "precio_min",
    "precio_prom",
    "precio_max",
]
MAX_SAMPLE_QUERIES = 12


def _resolve_procedencia() -> dict:
    settings = get_settings()
    return resolve_item(
        PROCEDENCIAS_SISAP,
        settings.sisap_procedencia_codigo,
        settings.sisap_procedencia_nombre,
        "la procedencia",
    )


def _build_raw_plan() -> list[SisapQuery]:
    settings = get_settings()
    procedencia = _resolve_procedencia()
    productos = resolve_productos(settings.sisap_producto_codigo, settings.sisap_producto_nombre)
    plan: list[SisapQuery] = []

    for producto in productos:
        resolved_dates = resolve_query_dates(
            output_name="precios_diarios",
            scope_label="procedencia",
            scope_value=procedencia["nombre"],
            producto_nombre=producto["nombre"],
            fecha_inicio=settings.fecha_inicio_resuelta,
            fecha_fin=settings.fecha_fin_resuelta,
        )
        if resolved_dates is None:
            continue

        fecha_inicio, fecha_fin = resolved_dates
        plan.extend(
            build_mayorista_queries(
                modulo=ModuloSisap.MAYORISTA_PRECIOS,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                procedencia_codigo=procedencia["codigo"],
                mercado_codigo=settings.sisap_mercado_codigo,
                mercado_nombre=settings.sisap_mercado_nombre,
                productos=[producto],
            )
        )

    return plan


def build_plan() -> list[SisapQuery]:
    settings = get_settings()
    return filter_plan(_build_raw_plan(), settings.sisap_max_queries)


def _fetch_price_frames(extractor: SisapMayoristaExtractor, query: SisapQuery, save_raw: bool = False) -> tuple[pl.DataFrame, dict[str, list[str]]]:
    metric_frames: list[pl.DataFrame] = []
    titles_by_metric: dict[str, list[str]] = {}

    for variable in PRICE_VARIABLES:
        report_html = extractor.fetch_report(query, variable=variable)
        titles_by_metric[variable] = extract_report_titles(report_html)
        if save_raw:
            save_html_snapshot(ModuloSisap.MAYORISTA_PRECIOS, query, report_html, suffix=variable)
        rows = extract_tables(report_html)
        frame = build_precio_metric_frame(rows, query=query, metric_name=variable)
        metric_frames.append(frame)

    return merge_precio_metrics(metric_frames), titles_by_metric


def _find_first_non_empty_report(extractor: SisapMayoristaExtractor, plan: list[SisapQuery]) -> tuple[SisapQuery, pl.DataFrame, dict[str, list[str]]]:
    for query in plan[:MAX_SAMPLE_QUERIES]:
        df, titles = _fetch_price_frames(extractor, query, save_raw=True)
        if not df.is_empty():
            return query, df, titles
    raise ValueError("No se encontraron resultados con datos en las primeras consultas de muestra.")


def run_sample() -> Path:
    plan = build_plan()
    if not plan:
        raise ValueError("No hay queries armadas para precios.")

    extractor = SisapMayoristaExtractor()
    _, df, _ = _find_first_non_empty_report(extractor, plan)
    validate_non_empty(df, "precios")
    validate_expected_columns(df, EXPECTED_COLUMNS, "precios")
    output = get_settings().clean_dir / "precios_diarios_sample.parquet"
    save_parquet(df, output)
    return output


def run_full() -> Path:
    procedencia = _resolve_procedencia()
    plan = build_plan()
    if not plan:
        logger.info("No hay queries pendientes para precios.")
        return build_scope_output_dir("precios_diarios", "procedencia", procedencia["nombre"])

    extractor = SisapMayoristaExtractor()
    frames: list[pl.DataFrame] = []
    errores: list[dict[str, str]] = []

    for idx, query in enumerate(plan, start=1):
        logger.info("Procesando precios {}/{} producto={} codigo={}", idx, len(plan), query.producto_nombre, query.producto_codigo)
        try:
            df, _ = _fetch_price_frames(extractor, query, save_raw=True)
            if df.is_empty():
                errores.append({"producto_codigo": query.producto_codigo, "producto_nombre": query.producto_nombre, "motivo": "sin_resultados"})
                continue

            validate_expected_columns(df, EXPECTED_COLUMNS, f"precios_{query.producto_codigo}")
            frames.append(df)
        except Exception as exc:
            logger.exception("Fallo extrayendo precios para {} ({})", query.producto_nombre, query.producto_codigo)
            errores.append({"producto_codigo": query.producto_codigo, "producto_nombre": query.producto_nombre, "motivo": str(exc)})

    return finalize_partitioned_output(
        frames=frames,
        output_name="precios_diarios",
        expected_columns=EXPECTED_COLUMNS,
        sort_columns=["producto_codigo", "variedad", "procedencia", "fecha"],
        error_rows=errores,
        scope_label="procedencia",
        scope_value=procedencia["nombre"],
    )

