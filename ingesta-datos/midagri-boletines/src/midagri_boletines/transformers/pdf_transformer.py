import io
import hashlib
import re
from datetime import date
import polars as pl
import pdfplumber
from loguru import logger


def _clean_numeric(value: str | None) -> float:
    """Convierte un string numérico con posibles comas y espacios en un float limpio."""
    if not value:
        return 0.0
    cleaned = str(value).strip().replace(" ", "").replace(",", "")
    # Si hay un punto al final, lo quitamos
    if cleaned.endswith("."):
        cleaned = cleaned[:-1]
    try:
        return float(cleaned)
    except ValueError:
        # Intentar extraer el primer número flotante que aparezca
        match = re.search(r"[-+]?\d*\.?\d+", cleaned)
        if match:
            return float(match.group())
        return 0.0


def _is_gmml_row(row: list[str]) -> bool:
    """
    Determina si una fila de celdas extraída del PDF es un registro válido de precios.
    Una fila de precios del GMML tiene al menos 5 columnas y contiene números para los precios.
    """
    if len(row) < 5:
        return False
    # El producto y la procedencia suelen ser textos largos
    # Las columnas finales deben parecer precios (ej. 1.20, 2.5, etc.)
    prices = [row[-4], row[-3], row[-2]]
    # Validamos que al menos los campos numéricos tengan dígitos
    digits_found = 0
    for p in prices:
        if p and any(c.isdigit() for c in str(p)):
            digits_found += 1
    return digits_found >= 2


def parse_daily_gmml_pdf(pdf_bytes: bytes, target_date: date) -> pl.DataFrame:
    """
    Parsea el contenido PDF de un boletín diario usando pdfplumber en memoria.
    Retorna un DataFrame estructurado y tipado de Polars.
    """
    records = []
    
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            logger.debug(f"Parseando PDF de {len(pdf.pages)} páginas...")
            
            for page_idx, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                logger.debug(f"Página {page_idx + 1}: {len(tables)} tablas detectadas.")
                
                # Método 1: Intentar extraer usando tablas tabulares nativas
                for table in tables:
                    for row in table:
                        # Filtrar nulos y limpiar strings
                        cleaned_row = [str(cell).strip() if cell is not None else "" for cell in row]
                        # Remover columnas completamente vacías al inicio/final
                        cleaned_row = [c for c in cleaned_row if c]
                        
                        if _is_gmml_row(cleaned_row):
                            # Estructura usual: [Producto, Procedencia, P. Min, P. Max, P. Prom, Ingreso (opcional)]
                            # Tomamos las últimas columnas como números
                            try:
                                prod = " ".join(cleaned_row[:-4]).strip()
                                proc = cleaned_row[-4].strip()
                                p_min = _clean_numeric(cleaned_row[-3])
                                p_max = _clean_numeric(cleaned_row[-2])
                                p_prom = _clean_numeric(cleaned_row[-1])
                                
                                # Si tiene volumen
                                vol = 0.0
                                if len(cleaned_row) >= 6:
                                    # A veces el volumen está después o antes, tomamos el último y re-ordenamos
                                    vol = _clean_numeric(cleaned_row[-1])
                                    p_prom = _clean_numeric(cleaned_row[-2])
                                    p_max = _clean_numeric(cleaned_row[-3])
                                    p_min = _clean_numeric(cleaned_row[-4])
                                    proc = cleaned_row[-5].strip()
                                    prod = " ".join(cleaned_row[:-5]).strip()

                                if prod and proc and p_prom > 0:
                                    records.append({
                                        "producto_raw": prod.upper(),
                                        "unidad_medida_raw": proc.upper(),
                                        "precio_minimo": p_min,
                                        "precio_maximo": p_max,
                                        "precio_promedio": p_prom,
                                        "ingreso_t": vol
                                    })
                            except Exception as e:
                                logger.trace(f"Error procesando fila tabular {cleaned_row}: {e}")

                # Método 2 (Fallback): Lector de líneas basado en Regex sobre el texto plano de la página
                if not records:
                    logger.debug("No se extrajeron registros mediante tablas. Intentando con Regex sobre texto plano...")
                    text = page.extract_text() or ""
                    lines = text.split("\n")
                    
                    # Patrón: Producto... Procedencia... Número Número Número [Número]
                    # Ej: "Papa Única Huánuco 1.20 1.40 1.30 180"
                    regex_pattern = r"^([a-zA-ZáéíóúÁÉÍÓÚñÑüÜ\s\-\/\.]+)\s+([a-zA-ZáéíóúÁÉÍÓÚñÑüÜ\s]+)\s+([0-9\.,]+)\s+([0-9\.,]+)\s+([0-9\.,]+)(?:\s+([0-9\.,]+))?$"
                    
                    for line in lines:
                        match = re.match(regex_pattern, line.strip())
                        if match:
                            prod = match.group(1).strip()
                            proc = match.group(2).strip()
                            p_min = _clean_numeric(match.group(3))
                            p_max = _clean_numeric(match.group(4))
                            p_prom = _clean_numeric(match.group(5))
                            vol = _clean_numeric(match.group(6)) if match.group(6) else 0.0
                            
                            # Filtro básico
                            if prod and proc and p_prom > 0:
                                records.append({
                                    "producto_raw": prod.upper(),
                                    "unidad_medida_raw": proc.upper(),
                                    "precio_minimo": p_min,
                                    "precio_maximo": p_max,
                                    "precio_promedio": p_prom,
                                    "ingreso_t": vol
                                })

    except Exception as e:
        logger.error(f"Falla crítica procesando el PDF para la fecha {target_date}: {e}")
        raise e

    if not records:
        logger.warning(f"No se pudieron parsear filas de precios válidas en el PDF de la fecha: {target_date}")
        return pl.DataFrame(schema={
            "fecha": pl.Date,
            "mercado": pl.Utf8,
            "producto_raw": pl.Utf8,
            "unidad_medida_raw": pl.Utf8,
            "precio_minimo": pl.Float64,
            "precio_maximo": pl.Float64,
            "precio_promedio": pl.Float64,
            "ingreso_t": pl.Float64,
            "registro_hash_fuente": pl.Utf8
        })

    # Convertir a DataFrame de Polars y estructurar
    df = pl.DataFrame(records)
    
    # Agregar metadatos del reporte
    df = df.with_columns([
        pl.lit(target_date).alias("fecha"),
        pl.lit("GMML").alias("mercado")
    ])

    # Generar Hash único para deduplicación robusta (registro_hash_fuente)
    df = df.with_columns(
        pl.struct(["fecha", "mercado", "producto_raw", "unidad_medida_raw"])
        .map_batches(lambda s: s.map_elements(
            lambda x: hashlib.md5(
                f"{x['fecha']}_{x['mercado']}_{x['producto_raw']}_{x['unidad_medida_raw']}".encode('utf-8')
            ).hexdigest(),
            return_dtype=pl.Utf8
        ))
        .alias("registro_hash_fuente")
    )

    # Reordenar y tipar columnas
    df = df.select([
        "fecha",
        "mercado",
        "producto_raw",
        "unidad_medida_raw",
        pl.col("precio_minimo").cast(pl.Float64),
        pl.col("precio_maximo").cast(pl.Float64),
        pl.col("precio_promedio").cast(pl.Float64),
        pl.col("ingreso_t").cast(pl.Float64),
        "registro_hash_fuente"
    ])

    logger.info(f"Parsea exitoso: {len(df)} registros de precios obtenidos para la fecha {target_date}.")
    return df


_TRAILING_NUMERIC_PATTERN = re.compile(r"^[-+]?\d[\d,]*\.?\d*$")


def _extract_trailing_numeric_quartet(producto_raw: str) -> tuple[str, float | None, float | None, float | None, float | None]:
    tokens = producto_raw.split()
    if len(tokens) < 5:
        return producto_raw.strip(), None, None, None, None

    tail = tokens[-4:]
    if not all(_TRAILING_NUMERIC_PATTERN.match(token) for token in tail):
        return producto_raw.strip(), None, None, None, None

    def _to_float(token: str) -> float:
        return float(token.replace(",", ""))

    producto = " ".join(tokens[:-4]).strip(" :-")
    return producto, _to_float(tail[0]), _to_float(tail[1]), _to_float(tail[2]), _to_float(tail[3])


def normalize_daily_gmml_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "fecha": pl.Date,
                "mercado": pl.Utf8,
                "producto": pl.Utf8,
                "producto_raw": pl.Utf8,
                "unidad_medida": pl.Utf8,
                "abastecimiento_origen_1": pl.Float64,
                "abastecimiento_origen_2": pl.Float64,
                "abastecimiento_origen_3": pl.Float64,
                "abastecimiento_total_reportado": pl.Float64,
                "precio_minimo": pl.Float64,
                "precio_maximo": pl.Float64,
                "precio_promedio": pl.Float64,
                "ingreso_t": pl.Float64,
                "ruta_raw_origen": pl.Utf8,
                "registro_hash_fuente": pl.Utf8,
                "fecha_particion": pl.Date,
            }
        )

    parsed = (
        df.select(["producto_raw"])
        .to_series()
        .map_elements(
            lambda value: _extract_trailing_numeric_quartet(value or ""),
            return_dtype=pl.Struct(
                [
                    pl.Field("producto", pl.Utf8),
                    pl.Field("abastecimiento_origen_1", pl.Float64),
                    pl.Field("abastecimiento_origen_2", pl.Float64),
                    pl.Field("abastecimiento_origen_3", pl.Float64),
                    pl.Field("abastecimiento_total_reportado", pl.Float64),
                ]
            ),
        )
        .alias("producto_parseado")
    )

    normalized = df.with_columns(parsed).unnest("producto_parseado")
    return normalized.select(
        [
            "fecha",
            "mercado",
            pl.col("producto").fill_null(pl.col("producto_raw")).alias("producto"),
            "producto_raw",
            pl.col("unidad_medida_raw").alias("unidad_medida"),
            "abastecimiento_origen_1",
            "abastecimiento_origen_2",
            "abastecimiento_origen_3",
            "abastecimiento_total_reportado",
            "precio_minimo",
            "precio_maximo",
            "precio_promedio",
            "ingreso_t",
            "ruta_raw_origen",
            "registro_hash_fuente",
            pl.col("fecha").alias("fecha_particion"),
        ]
    )
