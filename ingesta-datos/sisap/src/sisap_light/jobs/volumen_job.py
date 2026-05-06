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
from sisap_light.procesamiento.parsers.html_forms import (
    extract_checkbox_products,
    extract_hidden_inputs,
    extract_market_options,
    extract_post_id,
    extract_procedencia_options,
    extract_variable_options,
)
from sisap_light.procesamiento.parsers.html_tables import detect_primary_table, extract_report_titles
from sisap_light.ingesta_datos.planners import build_mayorista_queries
from sisap_light.schemas import ModuloSisap, SisapQuery
from sisap_light.procesamiento.storage.parquet import save_parquet
from sisap_light.procesamiento.storage.raw import save_html_snapshot
from sisap_light.procesamiento.transformers.volumen import build_volumen_frame
from sisap_light.procesamiento.validators.quality import validate_expected_columns, validate_non_empty

EXPECTED_COLUMNS = ["fecha", "producto_codigo", "producto_nombre", "variedad", "procedencia", "volumen_ton"]
MAX_SAMPLE_QUERIES = 12


def _resolve_procedencia() -> dict:
    settings = get_settings()
    return resolve_item(
        PROCEDENCIAS_SISAP,
        settings.sisap_procedencia_codigo,
        settings.sisap_procedencia_nombre,
        "la procedencia",
    )


def inspect_home() -> dict:
    extractor = SisapMayoristaExtractor()
    html = extractor.fetch_home()
    return {
        "hidden_inputs": extract_hidden_inputs(html),
        "post_id": extract_post_id(html),
        "mercado_options": extract_market_options(html),
        "producto_options": extract_checkbox_products(html),
        "procedencia_options": extract_procedencia_options(html),
        "variable_options": extract_variable_options(html),
        "html": html,
    }


def _build_raw_plan() -> list[SisapQuery]:
    settings = get_settings()
    procedencia = _resolve_procedencia()
    productos = resolve_productos(settings.sisap_producto_codigo, settings.sisap_producto_nombre)
    plan: list[SisapQuery] = []

    for producto in productos:
        resolved_dates = resolve_query_dates(
            output_name="volumen_diario",
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
                modulo=ModuloSisap.MAYORISTA_VOLUMEN,
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


def _find_first_non_empty_report(extractor: SisapMayoristaExtractor, plan: list[SisapQuery]) -> tuple[SisapQuery, str, list[list[str]]]:
    for query in plan[:MAX_SAMPLE_QUERIES]:
        report_html = extractor.fetch_report(query, variable="volumen")
        rows = detect_primary_table(report_html)
        if rows:
            return query, report_html, rows
    raise ValueError("No se encontraron resultados con datos en las primeras consultas de muestra.")


def _normalize_report(query: SisapQuery, report_html: str) -> pl.DataFrame:
    rows = detect_primary_table(report_html)
    if not rows:
        return pl.DataFrame()
    return build_volumen_frame(rows, query=query)


def run_sample() -> Path:
    plan = build_plan()
    if not plan:
        raise ValueError("No hay queries armadas para volumen.")

    extractor = SisapMayoristaExtractor()
    query, report_html, _ = _find_first_non_empty_report(extractor, plan)
    save_html_snapshot(ModuloSisap.MAYORISTA_VOLUMEN, query, report_html)
    df = _normalize_report(query, report_html)
    validate_non_empty(df, "volumen")
    validate_expected_columns(df, EXPECTED_COLUMNS, "volumen")
    output = get_settings().clean_dir / "volumen_diario_sample.parquet"
    save_parquet(df, output)
    return output


def run_full() -> Path:
    procedencia = _resolve_procedencia()
    plan = build_plan()
    if not plan:
        logger.info("No hay queries pendientes para volumen.")
        return build_scope_output_dir("volumen_diario", "procedencia", procedencia["nombre"])

    extractor = SisapMayoristaExtractor()
    frames: list[pl.DataFrame] = []
    errores: list[dict[str, str]] = []

    for idx, query in enumerate(plan, start=1):
        logger.info("Procesando volumen {}/{} producto={} codigo={}", idx, len(plan), query.producto_nombre, query.producto_codigo)
        try:
            report_html = extractor.fetch_report(query, variable="volumen")
            df = _normalize_report(query, report_html)
            save_html_snapshot(ModuloSisap.MAYORISTA_VOLUMEN, query, report_html)

            if df.is_empty():
                errores.append({"producto_codigo": query.producto_codigo, "producto_nombre": query.producto_nombre, "motivo": "sin_resultados"})
                continue

            validate_expected_columns(df, EXPECTED_COLUMNS, f"volumen_{query.producto_codigo}")
            frames.append(df)
        except Exception as exc:
            logger.exception("Fallo extrayendo volumen para {} ({})", query.producto_nombre, query.producto_codigo)
            errores.append({"producto_codigo": query.producto_codigo, "producto_nombre": query.producto_nombre, "motivo": str(exc)})

    return finalize_partitioned_output(
        frames=frames,
        output_name="volumen_diario",
        expected_columns=EXPECTED_COLUMNS,
        sort_columns=["producto_codigo", "variedad", "procedencia", "fecha"],
        error_rows=errores,
        scope_label="procedencia",
        scope_value=procedencia["nombre"],
    )


def inspect_sample_report() -> dict:
    plan = build_plan()
    if not plan:
        raise ValueError("No hay queries armadas para volumen.")

    extractor = SisapMayoristaExtractor()
    query, report_html, rows = _find_first_non_empty_report(extractor, plan)
    return {
        "query": query.model_dump(),
        "titles": extract_report_titles(report_html),
        "rows": rows[:5],
        "html": report_html,
    }

