from __future__ import annotations

from calendar import monthrange
from datetime import date
import hashlib
import re
import unicodedata
from collections import defaultdict

import polars as pl


INVENTORY_DATASET = "catalogo_cuadros_comercio_exterior"
EXPORTACION_DATASET = "comercio_exportacion_agrario"
IMPORTACION_DATASET = "comercio_importacion_agrario"

EXPORTACION_SCHEMA: dict[str, pl.DataType] = {
    "archivo_origen": pl.Utf8,
    "archivo_miembro": pl.Utf8,
    "tipo_archivo_origen": pl.Utf8,
    "archivo_anio_publicacion": pl.Int32,
    "archivo_fecha_descarga": pl.Utf8,
    "anio_publicacion": pl.Int32,
    "hoja_nombre": pl.Utf8,
    "hoja_titulo": pl.Utf8,
    "tipo_hoja": pl.Utf8,
    "frecuencia": pl.Utf8,
    "periodo_texto_fuente": pl.Utf8,
    "fecha_referencia_inicio": pl.Date,
    "fecha_referencia_fin": pl.Date,
    "fecha_particion": pl.Date,
    "anio": pl.Int32,
    "mes": pl.Int32,
    "mes_nombre": pl.Utf8,
    "nivel_agregacion": pl.Utf8,
    "ranking": pl.Int32,
    "subpartida_nacional": pl.Utf8,
    "descripcion": pl.Utf8,
    "pais": pl.Utf8,
    "aduana": pl.Utf8,
    "capitulo": pl.Utf8,
    "peso_neto_t": pl.Float64,
    "valor_fob_miles_usd": pl.Float64,
    "precio_fob_usd_t": pl.Float64,
    "participacion_pct": pl.Float64,
    "participacion_acumulada_pct": pl.Float64,
    "unidad_medida": pl.Utf8,
    "cantidad": pl.Float64,
    "es_total": pl.Boolean,
    "registro_hash_fuente": pl.Utf8,
}

IMPORTACION_SCHEMA: dict[str, pl.DataType] = {
    "archivo_origen": pl.Utf8,
    "archivo_miembro": pl.Utf8,
    "tipo_archivo_origen": pl.Utf8,
    "archivo_anio_publicacion": pl.Int32,
    "archivo_fecha_descarga": pl.Utf8,
    "anio_publicacion": pl.Int32,
    "hoja_nombre": pl.Utf8,
    "hoja_titulo": pl.Utf8,
    "tipo_hoja": pl.Utf8,
    "frecuencia": pl.Utf8,
    "periodo_texto_fuente": pl.Utf8,
    "fecha_referencia_inicio": pl.Date,
    "fecha_referencia_fin": pl.Date,
    "fecha_particion": pl.Date,
    "anio": pl.Int32,
    "mes": pl.Int32,
    "mes_nombre": pl.Utf8,
    "nivel_agregacion": pl.Utf8,
    "ranking": pl.Int32,
    "subpartida_nacional": pl.Utf8,
    "descripcion": pl.Utf8,
    "pais": pl.Utf8,
    "aduana": pl.Utf8,
    "capitulo": pl.Utf8,
    "peso_neto_t": pl.Float64,
    "valor_cif_miles_usd": pl.Float64,
    "precio_cif_usd_t": pl.Float64,
    "participacion_pct": pl.Float64,
    "participacion_acumulada_pct": pl.Float64,
    "unidad_medida": pl.Utf8,
    "cantidad": pl.Float64,
    "es_total": pl.Boolean,
    "registro_hash_fuente": pl.Utf8,
}

INVENTORY_SCHEMA: dict[str, pl.DataType] = {
    "archivo_origen": pl.Utf8,
    "archivo_miembro": pl.Utf8,
    "tipo_archivo_origen": pl.Utf8,
    "archivo_anio_publicacion": pl.Int32,
    "archivo_fecha_descarga": pl.Utf8,
    "anio_publicacion": pl.Utf8,
    "hoja_nombre": pl.Utf8,
    "hoja_titulo": pl.Utf8,
    "header_signature": pl.Utf8,
    "tipo_hoja": pl.Utf8,
    "flujo": pl.Utf8,
    "frecuencia": pl.Utf8,
    "filas_detectadas": pl.Int32,
    "registro_hash_fuente": pl.Utf8,
}

MONTH_INDEX = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "setiembre": 9,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

SUBPARTIDA_PATTERN = re.compile(r"^\d{10}$")


def _normalize_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.strip().lower()


def _clean_text(value: str | None) -> str:
    return (value or "").replace("\r", " ").replace("\n", " ").strip()


def _parse_float(value: object) -> float | None:
    if value is None:
        return None
    text = _clean_text(str(value))
    if not text or text in {"-", ".", "..", "...", "...."}:
        return None
    text = text.replace(" ", "")
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: object) -> int | None:
    number = _parse_float(value)
    if number is None:
        return None
    return int(number)


def _hash_record(record: dict[str, object]) -> str:
    payload = "|".join("" if value is None else str(value) for key, value in sorted(record.items()))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _matrix_from_sheet_df(sheet_df: pl.DataFrame) -> list[list[str]]:
    rows_map: dict[int, dict[int, str]] = defaultdict(dict)
    for row in (
        sheet_df.select(["fila_idx", "columna_idx", "celda_valor"])
        .sort(["fila_idx", "columna_idx"])
        .iter_rows(named=True)
    ):
        rows_map[int(row["fila_idx"])][int(row["columna_idx"])] = _clean_text(row["celda_valor"])

    matrix: list[list[str]] = []
    for row_idx in sorted(rows_map):
        row_cells = rows_map[row_idx]
        max_col = max(row_cells)
        values = [row_cells.get(col_idx, "") for col_idx in range(1, max_col + 1)]
        while values and values[-1] == "":
            values.pop()
        if any(values):
            matrix.append(values)
    return matrix


def _safe_row(matrix: list[list[str]], idx: int) -> list[str]:
    return matrix[idx] if idx < len(matrix) else []


def _joined_row(matrix: list[list[str]], idx: int) -> str:
    return " | ".join(cell for cell in _safe_row(matrix, idx) if cell)


def _sheet_context(matrix: list[list[str]], sheet_name: str) -> str:
    context_parts = [_clean_text(sheet_name)]
    for idx in range(4):
        row_text = _joined_row(matrix, idx)
        if row_text:
            context_parts.append(row_text)
    return " ".join(part for part in context_parts if part)


def _sheet_title(matrix: list[list[str]], fallback: str) -> str:
    for idx in range(4):
        row_text = _joined_row(matrix, idx)
        normalized = _normalize_text(row_text)
        if row_text and normalized not in {"", "system.xml.xmlelement"} and "continua c " not in normalized:
            return row_text
    return fallback


def _is_valid_subpartida(value: str) -> bool:
    return bool(SUBPARTIDA_PATTERN.fullmatch(_clean_text(value)))


def _detect_sheet_type(title: str) -> tuple[str, str, str]:
    normalized = _normalize_text(title)
    if "indice de cuadros" in normalized:
        return "ignored_indice", "metadata", "indice"
    if "balanza comercial" in normalized:
        return "resumen_balanza_comercial", "resumen", "anual"
    if "exportaciones agrarias por subpartidas arancelarias" in normalized and "mensuales" not in normalized:
        return "exportaciones_subpartida_anual", "exportacion", "anual"
    if "exportaciones agrarias mensuales, por capitulos" in normalized:
        return "exportaciones_capitulo_mensual", "exportacion", "mensual"
    if "exportaciones agrarias mensuales, por subpartidas arancelarias" in normalized:
        if normalized.startswith("c10"):
            return "exportaciones_subpartida_mensual_peso", "exportacion", "mensual"
        if normalized.startswith("c11"):
            return "exportaciones_subpartida_mensual_valor", "exportacion", "mensual"
        return "exportaciones_subpartida_mensual", "exportacion", "mensual"
    if "exportaciones agrarias, por paises de destino" in normalized and "segun principales" not in normalized:
        return "exportaciones_pais_anual", "exportacion", "anual"
    if "exportaciones agrarias, por principales aduanas de salida" in normalized:
        return "exportaciones_aduana_anual", "exportacion", "anual"
    if "importaciones agrarias por subpartidas arancelarias" in normalized and "mensuales" not in normalized:
        return "importaciones_subpartida_anual", "importacion", "anual"
    if "importaciones agrarias mensuales, por capitulos" in normalized:
        return "importaciones_capitulo_mensual", "importacion", "mensual"
    if "importaciones agrarias mensuales, por subpartidas arancelarias" in normalized:
        if normalized.startswith("c17"):
            return "importaciones_subpartida_mensual_valor", "importacion", "mensual"
        if normalized.startswith("c18"):
            return "importaciones_subpartida_mensual_peso", "importacion", "mensual"
        return "importaciones_subpartida_mensual", "importacion", "mensual"
    if "importaciones agrarias, por paises de origen" in normalized and "segun principales" not in normalized:
        return "importaciones_pais_anual", "importacion", "anual"
    if "importaciones agrarias, por aduanas de ingreso" in normalized:
        return "importaciones_aduana_anual", "importacion", "anual"
    return "unclassified", "desconocido", "desconocida"


def build_sheet_inventory(base_df: pl.DataFrame) -> pl.DataFrame:
    if base_df.is_empty():
        return pl.DataFrame(schema=INVENTORY_SCHEMA)

    records: list[dict[str, object]] = []
    group_cols = [
        "archivo_origen",
        "archivo_miembro",
        "tipo_archivo_origen",
        "archivo_anio_publicacion",
        "archivo_fecha_descarga",
        "anio_publicacion",
        "hoja_nombre",
    ]
    for sheet_df in base_df.partition_by(group_cols, maintain_order=True):
        matrix = _matrix_from_sheet_df(sheet_df)
        title = _sheet_title(matrix, str(sheet_df["hoja_nombre"][0]))
        context = _sheet_context(matrix, str(sheet_df["hoja_nombre"][0]))
        header_signature = _joined_row(matrix, 2) or _joined_row(matrix, 3)
        tipo_hoja, flujo, frecuencia = _detect_sheet_type(context)
        record = {
            "archivo_origen": sheet_df["archivo_origen"][0],
            "archivo_miembro": sheet_df["archivo_miembro"][0],
            "tipo_archivo_origen": sheet_df["tipo_archivo_origen"][0],
            "archivo_anio_publicacion": sheet_df["archivo_anio_publicacion"][0],
            "archivo_fecha_descarga": sheet_df["archivo_fecha_descarga"][0],
            "anio_publicacion": sheet_df["anio_publicacion"][0],
            "hoja_nombre": sheet_df["hoja_nombre"][0],
            "hoja_titulo": title,
            "header_signature": header_signature,
            "tipo_hoja": tipo_hoja,
            "flujo": flujo,
            "frecuencia": frecuencia,
            "filas_detectadas": len(matrix),
        }
        record["registro_hash_fuente"] = _hash_record(record)
        records.append(record)
    if not records:
        return pl.DataFrame(schema=INVENTORY_SCHEMA)
    return pl.DataFrame(records, schema=INVENTORY_SCHEMA).with_columns(
        pl.col("archivo_miembro").fill_null(""),
        pl.col("header_signature").fill_null(""),
        pl.col("hoja_titulo").fill_null(""),
        pl.col("tipo_hoja").fill_null(""),
        pl.col("flujo").fill_null(""),
        pl.col("frecuencia").fill_null(""),
    )


def _metadata_from_sheet_df(sheet_df: pl.DataFrame, title: str, sheet_type: str, flujo: str, frecuencia: str) -> dict[str, object]:
    return {
        "archivo_origen": sheet_df["archivo_origen"][0],
        "archivo_miembro": sheet_df["archivo_miembro"][0],
        "tipo_archivo_origen": sheet_df["tipo_archivo_origen"][0],
        "archivo_anio_publicacion": sheet_df["archivo_anio_publicacion"][0],
        "archivo_fecha_descarga": sheet_df["archivo_fecha_descarga"][0],
        "anio_publicacion": _parse_int(sheet_df["anio_publicacion"][0]) or _parse_int(sheet_df["archivo_anio_publicacion"][0]),
        "hoja_nombre": sheet_df["hoja_nombre"][0],
        "hoja_titulo": title,
        "tipo_hoja": sheet_type,
        "flujo": flujo,
        "frecuencia": frecuencia,
    }


def _annual_period_fields(metadata: dict[str, object]) -> dict[str, object]:
    year = _parse_int(metadata.get("anio_publicacion"))
    if year is None:
        return {
            "periodo_texto_fuente": _clean_text(str(metadata.get("hoja_titulo", ""))),
            "fecha_referencia_inicio": None,
            "fecha_referencia_fin": None,
            "fecha_particion": None,
        }
    return {
        "periodo_texto_fuente": _clean_text(str(metadata.get("hoja_titulo", ""))) or f"Ano {year}",
        "fecha_referencia_inicio": date(year, 1, 1),
        "fecha_referencia_fin": date(year, 12, 31),
        "fecha_particion": date(year, 12, 31),
    }


def _monthly_period_fields(metadata: dict[str, object], month_name: str) -> dict[str, object]:
    year = _parse_int(metadata.get("anio_publicacion"))
    month = MONTH_INDEX.get(month_name)
    if year is None or month is None:
        return {
            "periodo_texto_fuente": _clean_text(f"{month_name} {metadata.get('anio_publicacion', '')}"),
            "fecha_referencia_inicio": None,
            "fecha_referencia_fin": None,
            "fecha_particion": None,
        }
    last_day = monthrange(year, month)[1]
    return {
        "periodo_texto_fuente": f"{month_name.capitalize()} {year}",
        "fecha_referencia_inicio": date(year, month, 1),
        "fecha_referencia_fin": date(year, month, last_day),
        "fecha_particion": date(year, month, last_day),
    }


def _build_annual_rows(matrix: list[list[str]], metadata: dict[str, object], dimension_kind: str) -> list[dict[str, object]]:
    rows = matrix[3:]
    results: list[dict[str, object]] = []
    period_fields = _annual_period_fields(metadata)
    for raw_row in rows:
        if not any(raw_row):
            continue
        first = _clean_text(raw_row[0] if len(raw_row) > 0 else "")
        second = _clean_text(raw_row[1] if len(raw_row) > 1 else "")
        is_total = first.upper() == "TOTAL" or second.upper() == "TOTAL"
        if not is_total and not first:
            continue

        base = {
            **metadata,
            **period_fields,
            "anio": metadata["anio_publicacion"],
            "mes": None,
            "mes_nombre": "",
            "nivel_agregacion": dimension_kind,
            "ranking": _parse_int(first) if dimension_kind == "aduana" and not is_total else None,
            "subpartida_nacional": None,
            "descripcion": "",
            "pais": "",
            "aduana": "",
            "capitulo": "",
            "peso_neto_t": None,
            "valor_fob_miles_usd": None,
            "valor_cif_miles_usd": None,
            "precio_fob_usd_t": None,
            "precio_cif_usd_t": None,
            "participacion_pct": None,
            "participacion_acumulada_pct": None,
            "unidad_medida": "",
            "cantidad": None,
            "es_total": is_total,
        }

        if dimension_kind == "subpartida":
            if not is_total and not _is_valid_subpartida(first):
                continue
            base["subpartida_nacional"] = "TOTAL" if is_total else first
            base["descripcion"] = "TOTAL" if is_total else second
            base["peso_neto_t"] = _parse_float(raw_row[2] if len(raw_row) > 2 else None)
            if metadata["flujo"] == "exportacion":
                base["valor_fob_miles_usd"] = _parse_float(raw_row[3] if len(raw_row) > 3 else None)
                base["precio_fob_usd_t"] = _parse_float(raw_row[4] if len(raw_row) > 4 else None)
            else:
                base["valor_cif_miles_usd"] = _parse_float(raw_row[3] if len(raw_row) > 3 else None)
                base["precio_cif_usd_t"] = _parse_float(raw_row[4] if len(raw_row) > 4 else None)
            base["participacion_pct"] = _parse_float(raw_row[5] if len(raw_row) > 5 else None)
            base["participacion_acumulada_pct"] = _parse_float(raw_row[6] if len(raw_row) > 6 else None)
        elif dimension_kind == "pais":
            base["pais"] = "TOTAL" if is_total else first
            base["peso_neto_t"] = _parse_float(raw_row[1] if len(raw_row) > 1 else None)
            if metadata["flujo"] == "exportacion":
                base["valor_fob_miles_usd"] = _parse_float(raw_row[2] if len(raw_row) > 2 else None)
            else:
                base["valor_cif_miles_usd"] = _parse_float(raw_row[2] if len(raw_row) > 2 else None)
            base["participacion_pct"] = _parse_float(raw_row[3] if len(raw_row) > 3 else None)
            base["participacion_acumulada_pct"] = _parse_float(raw_row[4] if len(raw_row) > 4 else None)
        elif dimension_kind == "aduana":
            base["aduana"] = "TOTAL" if is_total else second
            base["peso_neto_t"] = _parse_float(raw_row[2] if len(raw_row) > 2 else None)
            if metadata["flujo"] == "exportacion":
                base["valor_fob_miles_usd"] = _parse_float(raw_row[3] if len(raw_row) > 3 else None)
            else:
                base["valor_cif_miles_usd"] = _parse_float(raw_row[3] if len(raw_row) > 3 else None)
            base["participacion_pct"] = _parse_float(raw_row[4] if len(raw_row) > 4 else None)
            base["participacion_acumulada_pct"] = _parse_float(raw_row[5] if len(raw_row) > 5 else None)

        base["registro_hash_fuente"] = _hash_record(base)
        results.append(base)
    return results


def _build_monthly_subpartida_rows(matrix: list[list[str]], metadata: dict[str, object], metric_kind: str) -> list[dict[str, object]]:
    rows = matrix[4:]
    month_positions = [
        ("enero", 4), ("febrero", 5), ("marzo", 6), ("abril", 7), ("mayo", 8),
        ("junio", 12), ("julio", 13), ("agosto", 14), ("setiembre", 15),
        ("octubre", 16), ("noviembre", 17), ("diciembre", 18),
    ]
    results: list[dict[str, object]] = []
    for raw_row in rows:
        if not any(raw_row):
            continue
        first = _clean_text(raw_row[0] if len(raw_row) > 0 else "")
        second = _clean_text(raw_row[1] if len(raw_row) > 1 else "")
        third = _clean_text(raw_row[2] if len(raw_row) > 2 else "")
        is_total = first.upper() == "TOTAL"
        if not is_total and not _is_valid_subpartida(second):
            continue

        for month_name, idx in month_positions:
            value = _parse_float(raw_row[idx] if len(raw_row) > idx else None)
            period_fields = _monthly_period_fields(metadata, month_name)
            record = {
                **metadata,
                **period_fields,
                "anio": metadata["anio_publicacion"],
                "mes": MONTH_INDEX[month_name],
                "mes_nombre": month_name,
                "nivel_agregacion": "subpartida",
                "ranking": _parse_int(first) if not is_total else None,
                "subpartida_nacional": "TOTAL" if is_total else second,
                "descripcion": "TOTAL" if is_total else third,
                "pais": "",
                "aduana": "",
                "capitulo": "",
                "peso_neto_t": value if metric_kind == "peso" else None,
                "valor_fob_miles_usd": value if metric_kind == "valor_fob" else None,
                "valor_cif_miles_usd": value if metric_kind == "valor_cif" else None,
                "precio_fob_usd_t": None,
                "precio_cif_usd_t": None,
                "participacion_pct": None,
                "participacion_acumulada_pct": None,
                "unidad_medida": "",
                "cantidad": None,
                "es_total": is_total,
            }
            record["registro_hash_fuente"] = _hash_record(record)
            results.append(record)
    return results


def _build_monthly_capitulo_rows(matrix: list[list[str]], metadata: dict[str, object], metric_kind: str) -> list[dict[str, object]]:
    rows = matrix[4:]
    month_positions = [
        ("enero", 3), ("febrero", 4), ("marzo", 5), ("abril", 6), ("mayo", 7), ("junio", 8),
        ("julio", 11), ("agosto", 12), ("setiembre", 13), ("octubre", 14), ("noviembre", 15), ("diciembre", 16),
    ]
    results: list[dict[str, object]] = []
    for raw_row in rows:
        if not any(raw_row):
            continue
        first = _clean_text(raw_row[0] if len(raw_row) > 0 else "")
        second = _clean_text(raw_row[1] if len(raw_row) > 1 else "")
        is_total = first.upper() == "TOTAL"
        if not is_total and not first:
            continue
        for month_name, idx in month_positions:
            value = _parse_float(raw_row[idx] if len(raw_row) > idx else None)
            period_fields = _monthly_period_fields(metadata, month_name)
            record = {
                **metadata,
                **period_fields,
                "anio": metadata["anio_publicacion"],
                "mes": MONTH_INDEX[month_name],
                "mes_nombre": month_name,
                "nivel_agregacion": "capitulo",
                "ranking": None,
                "subpartida_nacional": "",
                "descripcion": second,
                "pais": "",
                "aduana": "",
                "capitulo": "TOTAL" if is_total else first,
                "peso_neto_t": value if metric_kind == "peso" else None,
                "valor_fob_miles_usd": value if metric_kind == "valor_fob" else None,
                "valor_cif_miles_usd": value if metric_kind == "valor_cif" else None,
                "precio_fob_usd_t": None,
                "precio_cif_usd_t": None,
                "participacion_pct": None,
                "participacion_acumulada_pct": None,
                "unidad_medida": "",
                "cantidad": None,
                "es_total": is_total,
            }
            record["registro_hash_fuente"] = _hash_record(record)
            results.append(record)
    return results


def build_analytics_datasets(base_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    if base_df.is_empty():
        return pl.DataFrame(schema=EXPORTACION_SCHEMA), pl.DataFrame(schema=IMPORTACION_SCHEMA)

    records: list[dict[str, object]] = []
    group_cols = [
        "archivo_origen",
        "archivo_miembro",
        "tipo_archivo_origen",
        "archivo_anio_publicacion",
        "archivo_fecha_descarga",
        "anio_publicacion",
        "hoja_nombre",
    ]
    for sheet_df in base_df.partition_by(group_cols, maintain_order=True):
        matrix = _matrix_from_sheet_df(sheet_df)
        title = _sheet_title(matrix, str(sheet_df["hoja_nombre"][0]))
        context = _sheet_context(matrix, str(sheet_df["hoja_nombre"][0]))
        sheet_type, flujo, frecuencia = _detect_sheet_type(context)
        metadata = _metadata_from_sheet_df(sheet_df, title, sheet_type, flujo, frecuencia)

        if sheet_type == "exportaciones_subpartida_anual":
            records.extend(_build_annual_rows(matrix, metadata, "subpartida"))
        elif sheet_type == "exportaciones_pais_anual":
            records.extend(_build_annual_rows(matrix, metadata, "pais"))
        elif sheet_type == "exportaciones_aduana_anual":
            records.extend(_build_annual_rows(matrix, metadata, "aduana"))
        elif sheet_type == "importaciones_subpartida_anual":
            records.extend(_build_annual_rows(matrix, metadata, "subpartida"))
        elif sheet_type == "importaciones_pais_anual":
            records.extend(_build_annual_rows(matrix, metadata, "pais"))
        elif sheet_type == "importaciones_aduana_anual":
            records.extend(_build_annual_rows(matrix, metadata, "aduana"))
        elif sheet_type == "exportaciones_subpartida_mensual_peso":
            records.extend(_build_monthly_subpartida_rows(matrix, metadata, "peso"))
        elif sheet_type == "exportaciones_subpartida_mensual_valor":
            records.extend(_build_monthly_subpartida_rows(matrix, metadata, "valor_fob"))
        elif sheet_type == "importaciones_subpartida_mensual_peso":
            records.extend(_build_monthly_subpartida_rows(matrix, metadata, "peso"))
        elif sheet_type == "importaciones_subpartida_mensual_valor":
            records.extend(_build_monthly_subpartida_rows(matrix, metadata, "valor_cif"))
        elif sheet_type == "exportaciones_capitulo_mensual":
            records.extend(_build_monthly_capitulo_rows(matrix, metadata, "valor_fob"))
        elif sheet_type == "importaciones_capitulo_mensual":
            records.extend(_build_monthly_capitulo_rows(matrix, metadata, "valor_cif"))

    if not records:
        return pl.DataFrame(schema=EXPORTACION_SCHEMA), pl.DataFrame(schema=IMPORTACION_SCHEMA)

    # Creamos un DataFrame general temporal con la unión de campos y el flujo para filtrar
    temp_schema = {**EXPORTACION_SCHEMA, **IMPORTACION_SCHEMA, "flujo": pl.Utf8}
    general_df = pl.from_dicts(records, schema=temp_schema)

    export_df = general_df.filter(pl.col("flujo") == "exportacion").select(list(EXPORTACION_SCHEMA.keys()))
    import_df = general_df.filter(pl.col("flujo") == "importacion").select(list(IMPORTACION_SCHEMA.keys()))

    return export_df, import_df
