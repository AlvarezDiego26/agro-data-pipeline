from __future__ import annotations

from datetime import date, timedelta
import unicodedata
from pathlib import Path

import polars as pl

from sisap_light.ingesta_datos.catalogos.productos import PRODUCTOS_AGRICOLAS_PRIORITARIOS
from sisap_light.config import get_settings
from sisap_light.procesamiento.storage.delta import save_delta_table
from sisap_light.procesamiento.storage.parquet import save_partitioned_parquet
from sisap_light.procesamiento.validators.quality import validate_expected_columns, validate_non_empty


def normalize_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.strip().lower()


def slugify(value: str | None) -> str:
    text = normalize_text(value)
    text = text.replace(" ", "_").replace("/", "_").replace("-", "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text or "no_definido"


def find_by_codigo(items: list[dict], codigo: str | None) -> dict | None:
    if not codigo:
        return None
    for item in items:
        if item["codigo"] == codigo:
            return item
    return None


def find_by_nombre(items: list[dict], nombre: str | None) -> dict | None:
    if not nombre:
        return None
    target = normalize_text(nombre)
    for item in items:
        if normalize_text(item["nombre"]) == target:
            return item
    return None


def resolve_item(items: list[dict], codigo: str | None, nombre: str | None, entity_label: str) -> dict:
    item = find_by_codigo(items, codigo)
    if item is None:
        item = find_by_nombre(items, nombre)
    if item is None:
        raise ValueError(f"No se pudo resolver {entity_label}. Usa codigo o nombre valido.")
    return item


def resolve_productos(producto_codigo: str | None, producto_nombre: str | None) -> list[dict]:
    if producto_codigo:
        producto = find_by_codigo(PRODUCTOS_AGRICOLAS_PRIORITARIOS, producto_codigo)
        if producto is None:
            raise ValueError("No se pudo resolver el producto por codigo.")
        return [producto]

    if producto_nombre:
        producto = find_by_nombre(PRODUCTOS_AGRICOLAS_PRIORITARIOS, producto_nombre)
        if producto is None:
            raise ValueError("No se pudo resolver el producto por nombre.")
        return [producto]

    return PRODUCTOS_AGRICOLAS_PRIORITARIOS


def filter_plan(plan: list, max_queries: int | None) -> list:
    if max_queries and max_queries > 0:
        return plan[:max_queries]
    return plan


def build_product_folder(producto_nombre: str) -> str:
    return f"producto={slugify(producto_nombre)}"


def build_scope_folder(scope_label: str, scope_value: str) -> str:
    return f"{scope_label}={slugify(scope_value)}"


def build_dataset_name(output_name: str, scope_label: str, scope_value: str, producto_nombre: str) -> str:
    return f"{output_name}/{build_scope_folder(scope_label, scope_value)}/{build_product_folder(producto_nombre)}"


def build_local_output_dir(output_name: str, scope_label: str, scope_value: str, producto_nombre: str) -> Path:
    settings = get_settings()
    return settings.clean_dir / output_name / build_scope_folder(scope_label, scope_value) / build_product_folder(producto_nombre)


def build_scope_output_dir(output_name: str, scope_label: str, scope_value: str) -> Path:
    settings = get_settings()
    return settings.clean_dir / output_name / build_scope_folder(scope_label, scope_value)


def _get_last_delta_date(dataset_name: str) -> date | None:
    settings = get_settings()
    try:
        from deltalake import DeltaTable

        table = DeltaTable(settings.build_delta_uri(dataset_name), storage_options=settings.delta_storage_options)
        fecha_df = pl.from_arrow(table.to_pyarrow_table(columns=["fecha"])).drop_nulls()
        if fecha_df.is_empty():
            return None
        return fecha_df.get_column("fecha").max()
    except Exception:
        return None


def _get_last_local_parquet_date(base_dir: Path) -> date | None:
    parquet_files = list(base_dir.rglob("data.parquet"))
    if not parquet_files:
        return None

    maxima: date | None = None
    for file_path in parquet_files:
        try:
            fecha_df = pl.read_parquet(file_path, columns=["fecha"]).drop_nulls()
            if fecha_df.is_empty():
                continue
            current_max = fecha_df.get_column("fecha").max()
            if maxima is None or current_max > maxima:
                maxima = current_max
        except Exception:
            continue
    return maxima


def get_last_loaded_date(output_name: str, scope_label: str, scope_value: str, producto_nombre: str) -> date | None:
    dataset_name = build_dataset_name(output_name, scope_label, scope_value, producto_nombre)
    last_delta_date = _get_last_delta_date(dataset_name)
    if last_delta_date is not None:
        return last_delta_date

    local_dir = build_local_output_dir(output_name, scope_label, scope_value, producto_nombre)
    return _get_last_local_parquet_date(local_dir)


def resolve_query_dates(
    output_name: str,
    scope_label: str,
    scope_value: str,
    producto_nombre: str,
    fecha_inicio: date,
    fecha_fin: date,
) -> tuple[date, date] | None:
    settings = get_settings()
    if not settings.is_incremental:
        return fecha_inicio, fecha_fin

    last_loaded = get_last_loaded_date(output_name, scope_label, scope_value, producto_nombre)
    if last_loaded is None:
        return fecha_inicio, fecha_fin

    if settings.sisap_incremental_overlap_dias > 0:
        next_start = last_loaded - timedelta(days=settings.sisap_incremental_overlap_dias)
        if next_start < fecha_inicio:
            next_start = fecha_inicio
    else:
        next_start = last_loaded + timedelta(days=1)

    if next_start > fecha_fin:
        return None

    return next_start, fecha_fin


def finalize_partitioned_output(
    frames: list[pl.DataFrame],
    output_name: str,
    expected_columns: list[str],
    sort_columns: list[str],
    error_rows: list[dict[str, str]],
    scope_label: str,
    scope_value: str,
) -> Path:
    if not frames:
        raise ValueError(f"La corrida completa de {output_name} no produjo data util.")

    final_df = pl.concat(frames, how="vertical_relaxed")
    final_df = final_df.unique().sort(sort_columns)
    final_df = final_df.with_columns(
        pl.col("fecha").dt.year().cast(pl.Int32).alias("anio"),
        pl.col("fecha").dt.strftime("%m").alias("mes"),
    )

    validate_non_empty(final_df, output_name)
    validate_expected_columns(final_df, expected_columns, output_name)

    settings = get_settings()
    scope_folder = build_scope_folder(scope_label, scope_value)
    scope_output = build_scope_output_dir(output_name, scope_label, scope_value)

    productos = [
        str(item)
        for item in final_df.get_column("producto_nombre").drop_nulls().unique().sort().to_list()
    ]

    for producto_nombre in productos:
        product_folder = build_product_folder(producto_nombre)
        product_df = final_df.filter(pl.col("producto_nombre") == producto_nombre)
        output = scope_output / product_folder
        save_partitioned_parquet(product_df, output, ["anio", "mes"])

        if settings.delta_enabled:
            dataset_name = f"{output_name}/{scope_folder}/{product_folder}"
            save_delta_table(product_df, dataset_name, ["anio", "mes"])

    if error_rows:
        error_path = scope_output / "errores.csv"
        pl.DataFrame(error_rows).write_csv(error_path)

    return scope_output

